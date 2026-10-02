//! D49 (GitHub issue #6): the four checkable breakages `--verify` used to
//! miss. Each check is dep-free and never mutates: orphaned/foreign store
//! processes on the data dir, a stale store `port` artifact, an unwired
//! harness (PRD S6 promises this row RED, never absent), and an
//! org/user partition mismatch against the WAL's own ledger.

use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

/// Parse `ps -axo pid=,command=` output: pids of processes whose argv
/// references `data_dir` AND look like a store (redis-server or the
/// falkordb module). A supervised store legitimately matches ONE pid; a
/// dead supervisor's store or a second foreign writer is the hazard.
pub fn store_pids_on_dir(ps_output: &str, data_dir: &Path) -> Vec<u32> {
    let dir = data_dir.to_string_lossy();
    let mut pids = Vec::new();
    for line in ps_output.lines() {
        let line = line.trim_start();
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid.parse::<u32>() else {
            continue;
        };
        let command = command.trim();
        let is_store = command.contains("redis-server")
            || command.contains("falkordb.so")
            || (command.contains("redis") && command.contains("--dir"));
        if is_store && command.contains(dir.as_ref()) {
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    pids
}

/// Is a pid live? (`ps -p <pid>` — dep-free, Unix.)
pub fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Speak enough RESP to prove a live Redis answers on `port`: any reply
/// line (`+PONG`, an auth error, anything) means a server; a refused or
/// silent connection means stale. The port file carries no token, so full
/// AUTH is impossible client-side — liveness is the question.
pub fn redis_answers(port: u16, timeout: Duration) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    if stream.write_all(b"PING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 16];
    matches!(stream.read(&mut buf), Ok(n) if n > 0)
}

/// The pid recorded in the supervised store's lock file, if present.
pub fn lock_holder_pid(data_dir: &Path) -> Option<u32> {
    let raw = std::fs::read_to_string(data_dir.join(".supervised.lock")).ok()?;
    raw.trim().parse::<u32>().ok()
}

/// Outcome of the harness-config check (PRD S6): green iff some known
/// harness config names a binary from the install directory this client
/// runs from.
#[derive(Debug, PartialEq, Eq)]
pub enum HarnessCheck {
    /// A config wired this install (config name, exe name it names).
    Wired(String, String),
    /// Configs exist but none reference this install (their names).
    NotWired(Vec<String>),
    /// No known harness config exists at all.
    NoConfigFound,
}

/// Check the known harness configs for a wiring to `install_dir` (the
/// directory of the running binary; any exocortex binary from the same
/// install satisfies S6's "points at this binary"). `configs` is
/// (display name, Some(content) if the file was readable).
pub fn harness_check(install_dir: &Path, configs: &[(&str, Option<String>)]) -> HarnessCheck {
    let dir = install_dir.to_string_lossy();
    let mut existing = Vec::new();
    for (name, content) in configs {
        let Some(content) = content else { continue };
        existing.push((*name).to_string());
        if content.contains(dir.as_ref()) && content.contains("exocortex") {
            // Name the binary the config points at, for the operator.
            let exe = [
                "exocortex",
                "exocortex-mcp-client",
                "exocortex-node",
                "exocortex-cli",
            ]
            .into_iter()
            .find(|b| content.contains(&format!("{dir}/{b}")))
            .unwrap_or("exocortex")
            .to_string();
            return HarnessCheck::Wired((*name).to_string(), exe);
        }
    }
    if existing.is_empty() {
        HarnessCheck::NoConfigFound
    } else {
        HarnessCheck::NotWired(existing)
    }
}

/// Where `--verify` looks for harness wiring (names are printed in the
/// row). `EXOCORTEX_VERIFY_HARNESS_CONFIG` (a file path) overrides the
/// list for custom harnesses.
pub fn harness_config_paths() -> Vec<(&'static str, std::path::PathBuf)> {
    if let Ok(custom) = std::env::var("EXOCORTEX_VERIFY_HARNESS_CONFIG") {
        return vec![("custom", std::path::PathBuf::from(custom))];
    }
    let home = std::env::var("HOME").unwrap_or_default();
    vec![
        ("crush (~/.config/crush/crushrc)", {
            let mut p = std::path::PathBuf::from(&home);
            p.push(".config/crush/crushrc");
            p
        }),
        ("claude-code (~/.claude.json)", {
            let mut p = std::path::PathBuf::from(&home);
            p.push(".claude.json");
            p
        }),
        ("claude-code (~/.claude/settings.json)", {
            let mut p = std::path::PathBuf::from(&home);
            p.push(".claude/settings.json");
            p
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_pids_find_redis_on_the_dir_only() {
        let ps = "  10 /usr/bin/sleep 5\n \
                   20 /usr/local/bin/redis-server 127.0.0.1:6379 --dir /data/exo *:0\n \
                   21 /usr/local/bin/redis-server *:6399 --dir /data/exo\n \
                   22 /usr/local/bin/redis-server *:6399 --dir /other/dir\n";
        let pids = store_pids_on_dir(ps, Path::new("/data/exo"));
        assert_eq!(pids, vec![20, 21], "foreign store processes on the dir");
    }

    #[test]
    fn harness_check_requires_this_install_in_a_known_config() {
        let dir = Path::new("/Users/x/.cargo/bin");
        let wired = harness_check(
            dir,
            &[
                (
                    "crush",
                    Some("mcp add exocortex --command /Users/x/.cargo/bin/exocortex".into()),
                ),
                ("claude", None),
            ],
        );
        assert_eq!(
            wired,
            HarnessCheck::Wired("crush".into(), "exocortex".into())
        );
        let not_wired = harness_check(
            dir,
            &[("crush", Some("mcp add other --command /bin/other".into()))],
        );
        assert_eq!(not_wired, HarnessCheck::NotWired(vec!["crush".into()]));
        assert_eq!(
            harness_check(dir, &[("crush", None)]),
            HarnessCheck::NoConfigFound
        );
    }

    #[test]
    fn redis_ping_distinguishes_live_from_silent() {
        use std::io::Write as _;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 8];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let _ = stream.write_all(b"+PONG\r\n");
            }
        });
        assert!(redis_answers(port, Duration::from_secs(2)));
        server.join().unwrap();
        // The listener is closed now: the same port must read stale.
        // (Port reuse is not guaranteed instantly, so accept refusal OR
        // silence — both are "no live server".)
        let answered = redis_answers(port, Duration::from_millis(200));
        assert!(!answered, "a closed port must not answer");
    }
}
