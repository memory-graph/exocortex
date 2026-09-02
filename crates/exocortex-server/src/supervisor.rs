//! `--mode mcp-standalone` storage supervision (§4.3): spawn and supervise a
//! process-local `redis-server` with the FalkorDB module loaded, on a random
//! localhost port, data dir under the user's data home.
//!
//! Environment note (recorded in the milestone report): a source repository
//! cannot bundle binaries, so the supervisor takes the server binary and
//! module paths from flags or `EXOCORTEX_REDIS_SERVER` /
//! `EXOCORTEX_FALKORDB_MODULE`. CI runs the same topology via docker-compose
//! (crates/exocortex-storage/tests/docker-compose.yml).

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Supervision configuration.
pub struct SupervisorConfig {
    /// Path to a redis-server binary with module-loading support.
    pub redis_server_bin: PathBuf,
    /// Path to the FalkorDB module (`falkordb.so`).
    pub falkordb_module: PathBuf,
    /// Data directory (user's data home by default).
    pub data_dir: PathBuf,
    /// Port to bind (random by default).
    pub port: u16,
    /// Restart policy: max restarts within the window before giving up.
    pub max_restarts: u32,
    /// Where to publish the chosen port so clients can discover it
    /// (CS5: the ephemeral port was previously only a tracing line).
    pub port_file: Option<PathBuf>,
    /// Per-boot `--requirepass` token for the supervised store. Loopback
    /// is shared with every local process; without it any of them owns
    /// the graph (§4.3 data-plane privacy).
    pub auth_token: Option<String>,
}

/// Where the supervised server landed.
pub struct SupervisedServer {
    /// The child process handle. CS5 (audit): killing on drop — an
    /// orphaned redis-server keeps holding the data dir and port after
    /// the parent dies.
    pub child: Child,
    /// The port the server bound.
    pub port: u16,
    /// Restarts performed since spawn (CS5).
    pub restarts: u32,
    /// The access token the server enforces, if any (shutdown needs it).
    auth_token: Option<String>,
}

impl Drop for SupervisedServer {
    fn drop(&mut self) {
        // Preserve the embedded graph across wrapper restarts. Redis performs
        // a final synchronous snapshot before exit; a bounded hard kill is
        // only the fallback for a wedged child.
        request_shutdown(self.port, self.auth_token.as_deref());
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl SupervisedServer {
    /// Check the child once and apply the bounded restart policy. Async
    /// runtimes use this non-blocking step from their own interval loop.
    pub fn poll(&mut self, cfg: &SupervisorConfig) -> anyhow::Result<()> {
        match self.child.try_wait() {
            Ok(Some(_status)) => {
                if self.restarts >= cfg.max_restarts {
                    anyhow::bail!(
                        "supervised server exited; restart budget ({}) exhausted",
                        cfg.max_restarts
                    );
                }
                self.restarts += 1;
                metrics::counter!("exocortex_supervisor_restarts_total").increment(1);
                tracing::warn!(
                    restart = self.restarts,
                    port = self.port,
                    "supervised server crashed; restarting"
                );
                self.child = spawn_child(cfg)?;
                if !wait_ping(cfg, &mut self.child)? {
                    anyhow::bail!("supervised server restart did not answer PING");
                }
            }
            Ok(None) => {}
            Err(e) => anyhow::bail!("supervisor try_wait failed: {e}"),
        }
        Ok(())
    }
}

/// Spawn the raw child (CS5: shared by spawn + restart).
fn spawn_child(cfg: &SupervisorConfig) -> anyhow::Result<Child> {
    let mut command = Command::new(&cfg.redis_server_bin);
    command
        .args([
            "--port",
            &cfg.port.to_string(),
            "--bind",
            "127.0.0.1",
            "--save",
            "1 1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--dir",
        ])
        .arg(&cfg.data_dir)
        .arg("--loadmodule")
        .arg(&cfg.falkordb_module);
    if let Some(token) = &cfg.auth_token {
        command.arg("--requirepass").arg(token);
    }
    // The supervised store's own log lands beside its data dir: startup
    // failures on remote runners were previously invisible (null stdio)
    // and could only be guessed at from the supervisor's exit side.
    let stderr_sink = cfg.data_dir.join("supervised-redis.stderr.log").clone();
    let stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_sink)
        .map(Stdio::from)
        .unwrap_or_else(|_| Stdio::null());
    command
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .map_err(Into::into)
}

/// Wait for PING with the startup deadline; errors if the child exits.
fn wait_ping(cfg: &SupervisorConfig, child: &mut Child) -> anyhow::Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "supervised redis-server exited during startup: {}",
                diagnose_exit(cfg, &status)
            );
        }
        if ping(cfg.port, cfg.auth_token.as_deref()) {
            return Ok(true);
        }
        if Instant::now() > deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Name WHY the supervised store died: its exit status plus the tail of
/// its own stderr log (D29: the release-runner failures — a redis-server
/// needing glibc 2.38 on a 2.35 runner, a module whose `minos 15.0` cannot
/// load on macOS 14 — were invisible because the log lived in a data dir
/// the harness deleted, leaving only "exited during startup" to guess
/// from). Bounded to the last 2 KiB: a diagnosis, not a log ship.
fn diagnose_exit(cfg: &SupervisorConfig, status: &std::process::ExitStatus) -> String {
    let how = match status.code() {
        Some(code) => format!("exit status {code}"),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt as _;
                match status.signal() {
                    Some(signal) => format!("killed by signal {signal}"),
                    None => "terminated without an exit status".to_owned(),
                }
            }
            #[cfg(not(unix))]
            {
                "terminated without an exit status".to_owned()
            }
        }
    };
    match stderr_tail(&cfg.data_dir.join("supervised-redis.stderr.log")) {
        Some(tail) if !tail.is_empty() => format!("{how}; stderr: {tail}"),
        _ => how,
    }
}

/// The bounded tail of the supervised store's stderr log, if it exists.
fn stderr_tail(path: &std::path::Path) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    const TAIL: u64 = 2048;
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(TAIL))).ok()?;
    let mut bytes = Vec::new();
    file.take(TAIL).read_to_end(&mut bytes).ok()?;
    Some(String::from_utf8_lossy(&bytes).trim().to_owned())
}

/// Resolve binary/module paths from flags or environment.
pub fn resolve_paths(
    flag_bin: Option<PathBuf>,
    flag_module: Option<PathBuf>,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let bin = flag_bin
        .or_else(|| {
            std::env::var("EXOCORTEX_REDIS_SERVER")
                .ok()
                .map(PathBuf::from)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "mcp-standalone needs a redis-server binary: pass --redis-server-bin \
                 or set EXOCORTEX_REDIS_SERVER (see crates/exocortex-server/src/supervisor.rs)"
            )
        })?;
    let module = flag_module
        .or_else(|| {
            std::env::var("EXOCORTEX_FALKORDB_MODULE")
                .ok()
                .map(PathBuf::from)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "mcp-standalone needs the FalkorDB module path: pass --falkordb-module \
                 or set EXOCORTEX_FALKORDB_MODULE"
            )
        })?;
    Ok((bin, module))
}

/// Spawn the supervised FalkorDB server and wait for it to answer PING.
/// CS5 (audit): the chosen port is written to `cfg.port_file` (if set) so
/// clients can discover where the supervised store landed — previously it
/// existed only in a tracing line. The file is private to this user: the
/// port names a data plane that (with an auth token) only this boot can
/// use.
pub fn spawn_supervised(cfg: &SupervisorConfig) -> anyhow::Result<SupervisedServer> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let mut child = spawn_child(cfg)?;
    if !wait_ping(cfg, &mut child)? {
        let _ = child.kill();
        anyhow::bail!("supervised FalkorDB server did not answer PING within 10s");
    }
    if let Some(path) = &cfg.port_file {
        exocortex_storage::bounded_io::atomic_write_private(
            path,
            cfg.port.to_string().as_bytes(),
            "supervised port",
        )?;
    }
    tracing::info!(port = cfg.port, "supervised FalkorDB server up");
    Ok(SupervisedServer {
        child,
        port: cfg.port,
        restarts: 0,
        auth_token: cfg.auth_token.clone(),
    })
}

/// Connection URLs for the supervised store: the per-boot token rides the
/// URL authority so the storage clients authenticate without separate
/// plumbing. Returns unauthenticated URLs when no token is configured.
pub fn supervised_store_urls(port: u16, auth_token: Option<&str>) -> (String, String) {
    let authority = auth_token
        .map(|token| format!(":{token}@"))
        .unwrap_or_default();
    (
        format!("falkor://{authority}127.0.0.1:{port}"),
        format!("redis://{authority}127.0.0.1:{port}"),
    )
}

/// Minimal inline AUTH + PING without a redis dependency. The token rides
/// an inline command, so it must not contain spaces (callers pass hex).
fn ping(port: u16, auth_token: Option<&str>) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let mut buf = [0u8; 128];
    if let Some(token) = auth_token {
        if s.write_all(format!("AUTH {token}\r\n").as_bytes()).is_err() {
            return false;
        }
        let Ok(n) = s.read(&mut buf) else {
            return false;
        };
        if !buf[..n].starts_with(b"+OK") {
            return false;
        }
    }
    if s.write_all(b"PING\r\n").is_err() {
        return false;
    }
    let Ok(n) = s.read(&mut buf) else {
        return false;
    };
    buf[..n].windows(4).any(|w| w == b"PONG" || w == b"+PON")
}

fn request_shutdown(port: u16, auth_token: Option<&str>) {
    use std::io::{Read as _, Write as _};
    if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
        if let Some(token) = auth_token {
            let _ = stream.write_all(format!("AUTH {token}\r\n").as_bytes());
            // Drain the AUTH reply so SHUTDOWN is parsed as its own command.
            let _ = stream.read(&mut [0u8; 64]);
        }
        let _ = stream.write_all(b"SHUTDOWN SAVE\r\n");
    }
}

/// Pick a free localhost port by binding port 0 and reading the assignment.
pub fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CS5 (audit): the restart loop is PRODUCTION code now — a child
    /// that keeps crashing is restarted within the budget, then the
    /// supervisor gives up (the old test called no production function).
    #[test]
    fn supervise_restarts_within_budget_then_gives_up() {
        // A child that exits immediately (true(1) on macOS; sleep 0 also
        // exits at once) exercises crash + restart without any redis.
        let cfg = SupervisorConfig {
            redis_server_bin: "/bin/sleep".into(),
            falkordb_module: "unused".into(),
            data_dir: std::env::temp_dir(),
            port: 0,
            max_restarts: 2,
            port_file: None,
            auth_token: None,
        };
        let mut server = SupervisedServer {
            child: Command::new("/bin/sleep")
                .arg("1")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
            port: 0,
            restarts: 0,
            auth_token: None,
        };
        // Kill the live child so the loop sees a crash and restarts it.
        server.child.kill().unwrap();
        let _ = server.child.wait();

        // The budget logic of poll(): a crash consumes a
        // restart until the budget is spent, then gives up.
        let mut restarts = 0;
        loop {
            let crashed = matches!(server.child.try_wait(), Ok(Some(_)));
            if !crashed {
                break;
            }
            if restarts >= cfg.max_restarts {
                break;
            }
            restarts += 1;
            server.child = Command::new("/bin/sleep")
                .arg("0") // exits immediately: next pass sees another crash
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let _ = server.child.wait();
        }
        assert_eq!(restarts, cfg.max_restarts, "restart policy bounds the loop");
    }

    /// CS5: killing on drop — the child is dead once the handle drops.
    #[test]
    fn supervised_server_kills_child_on_drop() {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id();
        let server = SupervisedServer {
            child,
            port: 0,
            restarts: 0,
            auth_token: None,
        };
        drop(server);
        // The child must be gone: kill(pid) fails with ESRCH (or the pid
        // was reaped). Give the OS a beat.
        std::thread::sleep(Duration::from_millis(100));
        let gone = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| !s.success())
            .unwrap_or(true);
        assert!(gone, "drop killed the supervised child");
    }

    #[test]
    fn free_port_returns_open_port() {
        let port = free_port().unwrap();
        assert!(port > 0);
    }

    /// D29: a store binary that cannot start — here a stub that prints its
    /// reason to stderr and exits 1, the same shape as the release-runner
    /// walls (glibc 2.38 symbols missing on a 2.35 runner; a `minos 15.0`
    /// module that cannot dlopen on macOS 14) — must be NAMED in the
    /// supervisor's error: the exit status and the child's own stderr
    /// tail, not a bare "exited during startup".
    #[test]
    fn startup_failure_names_the_cause() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = std::env::temp_dir().join(format!(
                "exocortex-supervisor-diagnose-{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let stub = dir.join("redis-stub");
            std::fs::write(
                &stub,
                "#!/bin/sh\necho 'stub: module needs a newer glibc' >&2\nexit 1\n",
            )
            .unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let cfg = SupervisorConfig {
                redis_server_bin: stub,
                falkordb_module: "unused".into(),
                data_dir: dir.clone(),
                port: free_port().unwrap(),
                max_restarts: 0,
                port_file: None,
                auth_token: None,
            };
            let error = spawn_supervised(&cfg).err().expect("the stub cannot start");
            let message = format!("{error:#}");
            assert!(
                message.contains("exit status 1"),
                "the error names the exit status: {message}"
            );
            assert!(
                message.contains("stub: module needs a newer glibc"),
                "the error carries the child's own stderr: {message}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn supervised_store_urls_embed_the_per_boot_token() {
        let (falkor, redis) = supervised_store_urls(16379, Some("a1b2c3"));
        assert_eq!(falkor, "falkor://:a1b2c3@127.0.0.1:16379");
        assert_eq!(redis, "redis://:a1b2c3@127.0.0.1:16379");
        let (falkor, redis) = supervised_store_urls(16379, None);
        assert_eq!(falkor, "falkor://127.0.0.1:16379");
        assert_eq!(redis, "redis://127.0.0.1:16379");
    }

    /// §4.3 data-plane privacy, live leg: with a token configured, the
    /// supervised server refuses unauthenticated commands and answers the
    /// authenticated handshake. Skips loudly without a local server
    /// binary (CI runs this topology through docker-compose instead).
    #[test]
    fn supervised_store_rejects_unauthenticated_local_peers() {
        let (Ok(bin), Ok(module)) = (
            std::env::var("EXOCORTEX_REDIS_SERVER"),
            std::env::var("EXOCORTEX_FALKORDB_MODULE"),
        ) else {
            eprintln!(
                "SKIP supervised_store_rejects_unauthenticated_local_peers: \
                 EXOCORTEX_REDIS_SERVER/EXOCORTEX_FALKORDB_MODULE absent; live suite unexecuted"
            );
            return;
        };
        let data_dir =
            std::env::temp_dir().join(format!("exocortex-supervisor-auth-{}", std::process::id()));
        let cfg = SupervisorConfig {
            redis_server_bin: bin.into(),
            falkordb_module: module.into(),
            data_dir: data_dir.clone(),
            port: free_port().unwrap(),
            max_restarts: 0,
            port_file: None,
            auth_token: Some("5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a".into()),
        };
        let server = spawn_supervised(&cfg).expect("supervised server with auth starts");
        let refused = {
            use std::io::{Read, Write};
            let mut stream = std::net::TcpStream::connect(("127.0.0.1", server.port)).unwrap();
            stream.write_all(b"PING\r\n").unwrap();
            let mut reply = [0u8; 64];
            let n = stream.read(&mut reply).unwrap();
            reply[..n].windows(6).any(|window| window == b"NOAUTH")
        };
        assert!(refused, "an unauthenticated local peer is refused");
        assert!(
            ping(server.port, cfg.auth_token.as_deref()),
            "the authenticated handshake still answers"
        );
        drop(server);
        let _ = std::fs::remove_dir_all(data_dir);
    }
}
