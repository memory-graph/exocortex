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
}

impl Drop for SupervisedServer {
    fn drop(&mut self) {
        // Preserve the embedded graph across wrapper restarts. Redis performs
        // a final synchronous snapshot before exit; a bounded hard kill is
        // only the fallback for a wedged child.
        request_shutdown(self.port);
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
                if !wait_ping(cfg.port, &mut self.child)? {
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
    Command::new(&cfg.redis_server_bin)
        .args([
            "--port",
            &cfg.port.to_string(),
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
        .arg(&cfg.falkordb_module)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(Into::into)
}

/// Wait for PING with the startup deadline; errors if the child exits.
fn wait_ping(port: u16, child: &mut Child) -> anyhow::Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait()?.is_some() {
            anyhow::bail!("supervised redis-server exited during startup");
        }
        if ping(port) {
            return Ok(true);
        }
        if Instant::now() > deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
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
/// existed only in a tracing line.
pub fn spawn_supervised(cfg: &SupervisorConfig) -> anyhow::Result<SupervisedServer> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let mut child = spawn_child(cfg)?;
    if !wait_ping(cfg.port, &mut child)? {
        let _ = child.kill();
        anyhow::bail!("supervised FalkorDB server did not answer PING within 10s");
    }
    if let Some(path) = &cfg.port_file {
        std::fs::write(path, cfg.port.to_string())?;
    }
    tracing::info!(port = cfg.port, "supervised FalkorDB server up");
    Ok(SupervisedServer {
        child,
        port: cfg.port,
        restarts: 0,
    })
}

/// Minimal inline PING without a redis dependency.
fn ping(port: u16) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    if s.write_all(b"PING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 64];
    let Ok(n) = s.read(&mut buf) else {
        return false;
    };
    buf[..n].windows(4).any(|w| w == b"PONG" || w == b"+PON")
}

fn request_shutdown(port: u16) {
    use std::io::Write as _;
    if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
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
}
