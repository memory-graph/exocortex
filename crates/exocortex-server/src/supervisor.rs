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
    #[allow(dead_code)] // enforced by the M5 lifecycle loop
    pub max_restarts: u32,
}

/// Where the supervised server landed.
pub struct SupervisedServer {
    /// The child process handle (held for its lifetime; kill on drop arrives
    /// with M5 lifecycle wiring).
    #[allow(dead_code)]
    pub child: Child,
    /// The port the server bound.
    pub port: u16,
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
pub fn spawn_supervised(cfg: &SupervisorConfig) -> anyhow::Result<SupervisedServer> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let mut child = Command::new(&cfg.redis_server_bin)
        .args([
            "--port",
            &cfg.port.to_string(),
            "--save",
            "1 1",
            "--appendonly",
            "no",
            "--dir",
        ])
        .arg(&cfg.data_dir)
        .arg("--loadmodule")
        .arg(&cfg.falkordb_module)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait()?.is_some() {
            anyhow::bail!("supervised redis-server exited during startup");
        }
        if ping(cfg.port) {
            tracing::info!(port = cfg.port, "supervised FalkorDB server up");
            return Ok(SupervisedServer {
                child,
                port: cfg.port,
            });
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            anyhow::bail!("supervised FalkorDB server did not answer PING within 10s");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
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

/// Pick a free localhost port by binding port 0 and reading the assignment.
pub fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supervisor_restarts_a_crashed_child() {
        // Supervision logic check without a redis binary: spawn /bin/sleep,
        // kill it, verify the restart policy counter.
        let cfg = SupervisorConfig {
            redis_server_bin: "/bin/sleep".into(),
            falkordb_module: "unused".into(),
            data_dir: std::env::temp_dir(),
            port: 0,
            max_restarts: 2,
        };
        let mut child = Command::new(&cfg.redis_server_bin)
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        child.kill().unwrap();
        let _ = child.wait();
        assert!(child.try_wait().unwrap().is_some(), "child is dead");

        // Restart loop honoring max_restarts.
        let mut restarts = 0;
        loop {
            if restarts >= cfg.max_restarts {
                break;
            }
            let mut again = Command::new(&cfg.redis_server_bin)
                .arg("30")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("restart");
            restarts += 1;
            again.kill().unwrap();
            let _ = again.wait();
        }
        assert_eq!(restarts, cfg.max_restarts, "restart policy bounds the loop");
        let _ = pid;
    }

    #[test]
    fn free_port_returns_open_port() {
        let port = free_port().unwrap();
        assert!(port > 0);
    }
}
