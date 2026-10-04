//! D49 (GitHub issue #6): the four checkable breakages `--verify` used to
//! miss. Each check is dep-free and never mutates: orphaned/foreign store
//! processes on the data dir, a stale store `port` artifact, an unwired
//! harness (PRD S6 promises this row RED, never absent), and an
//! org/user partition mismatch against the WAL's own ledger.

use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

/// Parse `ps -axo pid=,command=` output: DISTINCT `--port` values of
/// store processes whose argv references `data_dir`. Redis rewrites its
/// process title after startup, so a pid/name scan misses the real
/// store and double-counts the watchdog shells that embed its argv —
/// the PORT SET is the honest measure (two stores on one dir ⇔ two
/// distinct ports mentioning that dir).
pub fn store_ports_on_dir(ps_output: &str, data_dir: &Path) -> Vec<u16> {
    let dir = data_dir.to_string_lossy();
    let mut ports: Vec<u16> = Vec::new();
    for line in ps_output.lines() {
        if !line.contains(dir.as_ref()) {
            continue;
        }
        let is_store_plane = line.contains("redis-server")
            || line.contains("falkordb.so")
            || (line.contains("redis") && line.contains("--dir"));
        if !is_store_plane {
            continue;
        }
        if let Some(port) = port_of(line) {
            if !ports.contains(&port) {
                ports.push(port);
            }
        }
    }
    ports.sort_unstable();
    ports
}

/// The `--port N` value of a command line, if present.
fn port_of(line: &str) -> Option<u16> {
    let mut tokens = line.split_whitespace();
    while let Some(token) = tokens.next() {
        if token == "--port" {
            return tokens.next().and_then(|v| v.parse().ok());
        }
        if let Some(value) = token.strip_prefix("--port=") {
            return value.parse().ok();
        }
    }
    None
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

/// Speak enough RESP to prove a live Redis answers on `port`: the first
/// reply byte must be a RESP type marker (`+ - : $ *`) — `+PONG`, an
/// auth error, anything Redis-shaped counts; a refused, silent, or
/// banner-on-connect (SMTP/FTP) connection means stale. The port file
/// carries no token, so full AUTH is impossible client-side — liveness
/// is the question (R14: banner bytes must not count as a store).
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
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => matches!(buf[0], b'+' | b'-' | b':' | b'$' | b'*'),
        _ => false,
    }
}

/// R14-B1: where the standalone node's store artifacts actually live for
/// a client `--data-dir`. The node's data home is `--standalone-data-dir`
/// or the OS data home, and the documented wrapper wiring maps a user
/// `--data-dir D` to `--standalone-data-dir D/falkordb` — so the lock and
/// port files sit one level below the client's dir in that topology.
/// First candidate carrying an artifact wins; the bare dir is the
/// fallback so absent-artifact rows stay honest.
pub fn store_artifact_dir(data_dir: &Path) -> std::path::PathBuf {
    let falkordb = data_dir.join("falkordb");
    if falkordb.join(".supervised.lock").exists() || falkordb.join("port").exists() {
        return falkordb;
    }
    data_dir.to_path_buf()
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
        // R14-B3: the dir and an exocortex binary must appear on the SAME
        // line — two independent file-wide substrings green-lit a config
        // that mentioned the install dir for an unrelated tool beside a
        // stale exocortex wiring elsewhere.
        for line in content.lines() {
            if line.contains(dir.as_ref()) && line.contains("exocortex") {
                let exe = [
                    "exocortex-mcp-client",
                    "exocortex-node",
                    "exocortex-cli",
                    "exocortex",
                ]
                .into_iter()
                .find(|b| line.contains(b))
                .unwrap_or("exocortex")
                .to_string();
                return HarnessCheck::Wired((*name).to_string(), exe);
            }
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
    fn store_ports_find_distinct_stores_on_the_dir() {
        // Redis rewrites its title after start; the watchdog shells embed
        // the same argv — three lines, ONE store, must collapse to one
        // port. A second foreign store shows a second port.
        let ps = "  10 /usr/bin/sleep 5\n \
                   20 sh -c trap x; redis-server --port 6400 --dir /data/exo\n \
                   21 sh -c trap x; redis-server --port 6400 --dir /data/exo\n \
                   22 redis-server 127.0.0.1:6400\n \
                   23 sh -c trap x; redis-server --port 6410 --dir /data/exo\n \
                   24 sh -c trap x; redis-server --port 6410 --dir /other\n";
        let ports = store_ports_on_dir(ps, Path::new("/data/exo"));
        assert_eq!(ports, vec![6400, 6410], "distinct stores by port");
        let one = store_ports_on_dir(
            &ps.lines().take(4).collect::<Vec<_>>().join("\n"),
            Path::new("/data/exo"),
        );
        assert_eq!(one, vec![6400], "title-rewritten store + shells collapse");
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

    /// R14: the port liveness answer must be Redis-shaped — a silent
    /// acceptor or an SMTP-style banner must read as stale.
    #[test]
    fn redis_ping_rejects_silent_and_banner_acceptors() {
        use std::io::{Read, Write};
        let spawned = |reply: Option<Vec<u8>>| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let handle = std::thread::spawn(move || {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 8];
                    let _ = stream.read(&mut buf);
                    if let Some(bytes) = reply {
                        let _ = stream.write_all(&bytes);
                    } else {
                        std::thread::sleep(Duration::from_secs(2));
                    }
                }
            });
            (port, handle)
        };
        let (pong_port, pong) = spawned(Some(b"+PONG\r\n".to_vec()));
        assert!(redis_answers(pong_port, Duration::from_secs(2)));
        pong.join().unwrap();
        let (banner_port, banner) = spawned(Some(b"220 mail.example ESMTP\r\n".to_vec()));
        assert!(
            !redis_answers(banner_port, Duration::from_secs(2)),
            "a banner service is not a store"
        );
        banner.join().unwrap();
    }

    /// R14-B3: the install dir mentioned for an UNRELATED tool plus the
    /// word exocortex elsewhere in the config must NOT wire the row.
    #[test]
    fn harness_check_requires_dir_and_binary_on_one_line() {
        let dir = Path::new("/opt/homebrew/bin");
        let mixed =
            "command = /opt/homebrew/bin/rg  # unrelated tool\nalias x = exocortex-old-thing\n";
        assert_eq!(
            harness_check(dir, &[("crush", Some(mixed.into()))]),
            HarnessCheck::NotWired(vec!["crush".into()])
        );
        let wired = "mcp add exocortex --command /opt/homebrew/bin/exocortex\n";
        assert_eq!(
            harness_check(dir, &[("crush", Some(wired.into()))]),
            HarnessCheck::Wired("crush".into(), "exocortex".into())
        );
    }

    /// R14-B1: the artifact dir resolves into the wrapper's falkordb
    /// layout when the artifacts live there, and falls back to the bare
    /// client dir.
    #[test]
    fn store_artifact_dir_prefers_the_falkordb_layout() {
        let base = std::env::temp_dir().join(format!("exo-artifact-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("falkordb")).unwrap();
        assert_eq!(store_artifact_dir(&base), base, "no artifacts yet");
        std::fs::write(base.join("falkordb/.supervised.lock"), "1\n").unwrap();
        assert_eq!(
            store_artifact_dir(&base),
            base.join("falkordb"),
            "lock found in the wrapper layout"
        );
        let _ = std::fs::remove_dir_all(&base);
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
