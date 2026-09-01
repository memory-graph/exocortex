//! D27 (bug-prd-standalone-submit-hang): rmcp's serve loop treats stdin
//! EOF as an immediate shutdown — every tool-call task it has spawned
//! (and any it has read but not yet polled) is dropped and its response
//! is silently lost. For the write path that turns a submit which is
//! still perfectly in flight into a "hang" the harness can only observe
//! as a missing response. The reader below withholds EOF until the
//! process is QUIET: no tool call in flight, and no dispatch in the
//! last grace window (covering requests rmcp already read but whose
//! handler tasks have not been polled yet), bounded by a budget that
//! exceeds the end_session gRPC deadlines (20s register + 30s submit)
//! so a normal or deadline-failing submit is always answered before
//! the process exits.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::task::AtomicWaker;
use futures::Future;
use tokio::io::AsyncRead;

fn epoch() -> &'static std::time::Instant {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH.get_or_init(std::time::Instant::now)
}

fn now_ms() -> u64 {
    epoch().elapsed().as_millis() as u64
}

/// Shared in-flight state: the count of dispatched-but-unfinished tool
/// calls plus the instant of the most recent dispatch transition. Every
/// transition wakes the draining reader through an `AtomicWaker`
/// (register-then-recheck, so no completion can be lost between the
/// count check and the waker registration).
pub struct InFlightCalls {
    count: AtomicUsize,
    last_change_ms: AtomicU64,
    drain_waker: AtomicWaker,
}

impl InFlightCalls {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            last_change_ms: AtomicU64::new(0),
            drain_waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn guard(self: &Arc<Self>) -> InFlightGuard {
        self.count.fetch_add(1, Ordering::Release);
        self.last_change_ms.store(now_ms(), Ordering::Release);
        self.drain_waker.wake();
        InFlightGuard {
            calls: Arc::clone(self),
        }
    }

    fn load(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn ms_since_last_change(&self) -> u64 {
        now_ms().saturating_sub(self.last_change_ms.load(Ordering::Acquire))
    }

    fn drain_waker(&self) -> &AtomicWaker {
        &self.drain_waker
    }
}

/// RAII in-flight marker: entered when a tool call starts dispatching,
/// dropped on every exit path (including errors), decrementing the
/// count, stamping the transition, and waking a draining EOF reader.
pub(crate) struct InFlightGuard {
    calls: Arc<InFlightCalls>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.calls.count.fetch_sub(1, Ordering::Release);
        self.calls.last_change_ms.store(now_ms(), Ordering::Release);
        self.calls.drain_waker.wake();
    }
}

/// How long EOF is withheld while calls remain in flight. The end_session
/// deadlines bound a wedged call at 20s (register) + 30s (submit); the
/// budget exceeds that sum so deadline failures still reach the harness.
pub const EOF_DRAIN_BUDGET: Duration = Duration::from_secs(55);

/// How long EOF is additionally withheld after the most recent dispatch
/// transition even when nothing is in flight: rmcp spawns each request
/// handler as a task, and a request it has READ but not yet polled when
/// EOF arrives has taken no guard — the grace window keeps the sink
/// alive long enough for that handler to run and answer.
pub const EOF_DRAIN_GRACE: Duration = Duration::from_millis(300);

/// Wraps the stdio input; on inner EOF, withholds the EOF notification
/// until the process is quiet (no in-flight calls and no dispatch in
/// the grace window) or the drain budget expires, whichever comes
/// first. Everything else passes through unchanged.
pub struct EofDrainReader<R> {
    inner: R,
    in_flight: Arc<InFlightCalls>,
    budget: Duration,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    grace_timer: Option<Pin<Box<tokio::time::Sleep>>>,
    grace_target_ms: u64,
}

impl<R> EofDrainReader<R> {
    pub fn new(inner: R, in_flight: Arc<InFlightCalls>, budget: Duration) -> Self {
        Self {
            inner,
            in_flight,
            budget,
            deadline: None,
            grace_timer: None,
            grace_target_ms: 0,
        }
    }

    /// EOF may pass now: budget exhausted, or quiet (nothing in flight
    /// and the last dispatch transition is older than the grace).
    fn quiet_or_expired(&self) -> bool {
        self.in_flight.load() == 0
            && Duration::from_millis(self.in_flight.ms_since_last_change()) >= EOF_DRAIN_GRACE
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for EofDrainReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) if buf.filled().is_empty() => {
                    // Inner EOF. Arm the drain deadline once (polling it
                    // immediately so the timer driver registers the
                    // waker), then hold the EOF while the process is not
                    // quiet and budget remains: register the waker
                    // BEFORE re-checking so a dispatch racing this poll
                    // still wakes us.
                    if this.deadline.is_none() {
                        this.deadline = Some(Box::pin(tokio::time::sleep(this.budget)));
                    }
                    let budget_expired = this
                        .deadline
                        .as_mut()
                        .expect("deadline armed")
                        .as_mut()
                        .poll(cx)
                        .is_ready();
                    if budget_expired || this.quiet_or_expired() {
                        return Poll::Ready(Ok(()));
                    }
                    // Quiet except for the grace window: arm a timer for
                    // the grace remainder so EOF is released on time (the
                    // in-flight waker only fires on dispatch transitions).
                    if this.in_flight.load() == 0 {
                        let elapsed = Duration::from_millis(this.in_flight.ms_since_last_change());
                        if elapsed < EOF_DRAIN_GRACE {
                            let target_ms = this.in_flight.ms_since_last_change();
                            if this.grace_timer.is_none() || target_ms != this.grace_target_ms {
                                this.grace_target_ms = target_ms;
                                this.grace_timer =
                                    Some(Box::pin(tokio::time::sleep(EOF_DRAIN_GRACE - elapsed)));
                            }
                            if let Some(timer) = this.grace_timer.as_mut() {
                                if timer.as_mut().poll(cx).is_ready() {
                                    continue;
                                }
                            }
                        }
                    }
                    this.in_flight.drain_waker().register(cx.waker());
                    if this.quiet_or_expired() {
                        continue;
                    }
                    return Poll::Pending;
                }
                other => return other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn setup() -> Arc<InFlightCalls> {
        InFlightCalls::new()
    }

    #[tokio::test]
    async fn eof_is_withheld_while_a_call_is_in_flight() {
        let calls = setup();
        let mut reader =
            EofDrainReader::new(std::io::Cursor::new(b""), calls.clone(), EOF_DRAIN_BUDGET);
        let guard = calls.guard();
        let mut held = [0u8; 8];
        let read = tokio::time::timeout(Duration::from_millis(150), reader.read(&mut held));
        assert!(
            read.await.is_err(),
            "EOF must be withheld while the guard is held"
        );
        drop(guard);
        let mut out = [0u8; 8];
        let n = reader.read(&mut out).await.unwrap();
        assert_eq!(n, 0, "EOF passes once the call drains and grace lapses");
    }

    #[tokio::test]
    async fn eof_passes_when_the_drain_budget_expires() {
        let calls = setup();
        let mut reader = EofDrainReader::new(
            std::io::Cursor::new(b""),
            calls.clone(),
            Duration::from_millis(100),
        );
        let _guard = calls.guard();
        let started = std::time::Instant::now();
        let mut out = [0u8; 8];
        let n = reader.read(&mut out).await.unwrap();
        assert_eq!(n, 0, "budget expiry releases the EOF");
        assert!(
            started.elapsed() >= Duration::from_millis(90),
            "the hold lasted for the budget, not an immediate pass"
        );
    }

    #[tokio::test]
    async fn eof_holds_the_grace_window_after_a_recent_dispatch() {
        let calls = setup();
        // A dispatch that already completed still counts: rmcp may have
        // read another request whose handler has not been polled yet.
        drop(calls.guard());
        let mut reader =
            EofDrainReader::new(std::io::Cursor::new(b""), calls.clone(), EOF_DRAIN_BUDGET);
        let started = std::time::Instant::now();
        let mut out = [0u8; 8];
        let n = reader.read(&mut out).await.unwrap();
        assert_eq!(n, 0, "EOF passes after the grace window");
        assert!(
            started.elapsed() + Duration::from_millis(10) >= EOF_DRAIN_GRACE,
            "a dispatch right before EOF holds EOF for the grace window"
        );
    }

    #[tokio::test]
    async fn eof_passes_immediately_when_long_idle() {
        let calls = setup();
        calls
            .last_change_ms
            .store(now_ms().saturating_sub(10_000), Ordering::Release);
        let mut reader =
            EofDrainReader::new(std::io::Cursor::new(b""), calls.clone(), EOF_DRAIN_BUDGET);
        let started = std::time::Instant::now();
        let mut out = [0u8; 8];
        let n = reader.read(&mut out).await.unwrap();
        assert_eq!(n, 0, "idle EOF passes");
        assert!(
            started.elapsed() < EOF_DRAIN_GRACE * 2,
            "no grace hold on a long-idle connection (scheduler noise aside)"
        );
    }

    #[tokio::test]
    async fn data_reads_pass_through_untouched() {
        let calls = setup();
        let mut reader =
            EofDrainReader::new(std::io::Cursor::new(b"payload"), calls, EOF_DRAIN_BUDGET);
        let mut out = [0u8; 16];
        let n = reader.read(&mut out).await.unwrap();
        assert_eq!(&out[..n], b"payload");
    }
}
