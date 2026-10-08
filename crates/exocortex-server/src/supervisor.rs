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
    /// D44: the pid the store's watchdog watches — the store dies when
    /// THIS process is gone, even if nothing runs Drop (kill -9).
    /// `None` means this process (production).
    pub supervisor_pid: Option<u32>,
    /// D44: startup PING deadline override (tests shrink the 10s
    /// default so failing spawns return fast).
    pub startup_timeout: Option<Duration>,
    /// D45: override the snapshot policy (`--save`). `None` keeps the
    /// production `1 1` (BGSAVE one second after the first dirtying
    /// change); `Some("")` disables snapshots entirely — the
    /// discriminator leg of the store-death repro harness.
    pub save_policy: Option<String>,
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
    /// D44: the exclusive data-dir lock, held for the lifetime of the
    /// server (flock releases automatically on process death — a
    /// crashed node never leaves a stale lock behind).
    _data_dir_lock: Option<std::fs::File>,
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
        kill_store_process_group(self.child.id());
        let _ = self.child.wait();
    }
}

/// D44: the store runs in its own process group (spawn_child), so
/// teardown signals the GROUP — killing only the watchdog shell would
/// race its poll loop and could leave the store alive a tick longer,
/// or forever if the shell itself was SIGKILLed.
#[cfg(unix)]
fn kill_store_process_group(child_pid: u32) {
    unsafe {
        libc::kill(-(child_pid as i32), libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_store_process_group(_child_pid: u32) {}

impl SupervisedServer {
    /// Check the child once and apply the bounded restart policy. Async
    /// runtimes use this non-blocking step from their own interval loop.
    pub fn poll(&mut self, cfg: &SupervisorConfig) -> anyhow::Result<()> {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                // D45: post-startup deaths carried no diagnosis (only
                // the startup path ran diagnose_exit) — the store-death
                // investigation had nothing but a bare WARN. The death
                // is ERROR-level: under the default ERROR-only filter
                // a store restart is exactly what must NOT be silent.
                let why = describe_death(cfg, &status);
                if self.restarts >= cfg.max_restarts {
                    anyhow::bail!(
                        "supervised server exited; restart budget ({}) exhausted; last death: {why}",
                        cfg.max_restarts
                    );
                }
                self.restarts += 1;
                metrics::counter!("exocortex_supervisor_restarts_total").increment(1);
                tracing::error!(
                    restart = self.restarts,
                    port = self.port,
                    death = %why,
                    "supervised server crashed; restarting"
                );
                self.child = spawn_child(cfg)?;
                if !wait_ping(cfg, &mut self.child)? {
                    anyhow::bail!("supervised server restart did not answer PING");
                }
                // D46: every fresh store boot must re-disarm the fork
                // and re-arm the D46b quiesce window.
                disarm_async_delete_fork(cfg)?;
                exocortex_storage::fork_window::arm();
            }
            Ok(None) => {}
            Err(e) => anyhow::bail!("supervisor try_wait failed: {e}"),
        }
        Ok(())
    }
}

/// The darwin-arm64 module's homebrew dependencies (libomp, openssl@3)
/// are bundled BESIDE it by the runtime fetcher, and dyld resolves an
/// absolute install name through DYLD_LIBRARY_PATH's leaf name first —
/// so the bundled copies win over any system/homebrew ones (or stand in
/// when none exist, e.g. on stock CI runners; verified live with
/// DYLD_PRINT_LIBRARIES against the real store binary). The store is not
/// a hardened binary, so the loader honors the variable. Tested via the
/// constructed Command because Apple-protected interpreters such as
/// /bin/sh scrub DYLD_* at load and cannot observe it from a stub.
#[cfg(target_os = "macos")]
fn apply_bundled_dyld_path(command: &mut Command, module: &std::path::Path) {
    let Some(dir) = module.parent() else {
        return;
    };
    let dir = dir.to_string_lossy().into_owned();
    let value = match std::env::var("DYLD_LIBRARY_PATH") {
        Ok(existing) if !existing.is_empty() => format!("{dir}:{existing}"),
        _ => dir,
    };
    command.env("DYLD_LIBRARY_PATH", value);
}

/// Spawn the raw child (CS5: shared by spawn + restart).
/// D44: the store runs behind a watchdog shell in its own process
/// group. The shell re-exports the store's exit status (the D29
/// diagnosis keeps working: `wait $child` reaps an early death
/// immediately), kills the store when terminated, and — the reason it
/// exists — a watcher subshell polls the supervisor's pid once a
/// second and kills the store when the supervisor is gone: a node
/// that dies without running Drop (kill -9, crash) must never leave
/// an orphaned redis-server appending to the data dir (GitHub issue
/// #2's live evidence: two orphans on one AOF).
/// D45 (root-caused 2026-10-05 on the owner's real graph): a BGSAVE
/// fork over the loaded FalkorDB graph kills the PARENT with SIGILL
/// ~1s after the first dirtying write (the fork child saves
/// successfully; the parent dies — isolated by the discriminator:
/// identical data with `--save ""` survives the full read/write
/// workload indefinitely, reproduced live on the real 1,644-node
/// graph). Durability rides the AOF (everysec); the RDB snapshot was
/// redundant belt-and-suspenders, so the supervised default DISABLES
/// it. An explicit `save_policy` still overrides (the D45 repro
/// harness keeps its knob).
fn store_save_policy(cfg: &SupervisorConfig) -> String {
    cfg.save_policy.clone().unwrap_or_default()
}

fn spawn_child(cfg: &SupervisorConfig) -> anyhow::Result<Child> {
    let mut command = store_command(cfg);
    #[cfg(target_os = "macos")]
    apply_bundled_dyld_path(&mut command, &cfg.falkordb_module);
    if let Some(token) = &cfg.auth_token {
        command.arg("--requirepass").arg(token);
    }
    // The supervised store's own log lands beside its data dir: startup
    // failures on remote runners were previously invisible (null stdio)
    // and could only be guessed at from the supervisor's exit side. Redis
    // writes its whole startup log to STDOUT when no logfile is set, so
    // BOTH streams join the sink — the stderr-only capture of the first
    // D29 fix proved empty exactly because redis had logged its fatal
    // reason to stdout. Two append handles to one file: `Stdio` cannot be
    // shared across fds on the pinned toolchain.
    let sink_path = cfg.data_dir.join("supervised-redis.stderr.log").clone();
    let open_sink = || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&sink_path)
            .map(Stdio::from)
            .unwrap_or_else(|_| Stdio::null())
    };
    let stdout = open_sink();
    let stderr = open_sink();
    command
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(Into::into)
}

/// Build the store's command line: the watchdog shell in its own
/// process group, with the store binary and its flags as the shell's
/// `$0`/`$@` so quoting survives paths with spaces.
#[cfg(unix)]
fn store_command(cfg: &SupervisorConfig) -> Command {
    use std::os::unix::process::CommandExt as _;
    let supervisor_pid = cfg.supervisor_pid.unwrap_or_else(std::process::id);
    // Apple-protected interpreters scrub DYLD_* from their own
    // environment, so the bundled-lib path rides under a neutral name
    // and the shell exports it for the store (which is not protected).
    let script = format!(
        "trap 'kill $child 2>/dev/null; exit 143' TERM INT HUP; \
[ -n \"$EXOCORTEX_WATCHDOG_DYLD\" ] && export DYLD_LIBRARY_PATH=\"$EXOCORTEX_WATCHDOG_DYLD\"; \
\"$0\" \"$@\" & child=$!; \
( while kill -0 {supervisor_pid} 2>/dev/null; do sleep 1; done; kill $child 2>/dev/null ) & watcher=$!; \
wait $child; status=$?; kill $watcher 2>/dev/null; exit $status"
    );
    let mut command = Command::new("sh");
    command.arg("-c").arg(script).arg(&cfg.redis_server_bin);
    store_args(cfg, &mut command);
    if let Some(value) = bundled_dyld_value(cfg) {
        command.env("EXOCORTEX_WATCHDOG_DYLD", value);
    }
    command.process_group(0);
    command
}

#[cfg(not(unix))]
fn store_command(cfg: &SupervisorConfig) -> Command {
    let mut command = Command::new(&cfg.redis_server_bin);
    store_args(cfg, &mut command);
    command
}

fn store_args(cfg: &SupervisorConfig, command: &mut Command) {
    let save = store_save_policy(cfg);
    let save = save.as_str();
    command
        .args([
            "--port",
            &cfg.port.to_string(),
            "--bind",
            "127.0.0.1",
            "--save",
            save,
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--dir",
        ])
        .arg(&cfg.data_dir)
        .arg("--loadmodule")
        .arg(&cfg.falkordb_module);
}

/// The composed DYLD_LIBRARY_PATH the store needs on macOS (the same
/// value `apply_bundled_dyld_path` sets for the direct-exec form).
#[cfg(target_os = "macos")]
fn bundled_dyld_value(cfg: &SupervisorConfig) -> Option<String> {
    let dir = cfg.falkordb_module.parent()?;
    let dir = dir.to_string_lossy().into_owned();
    Some(match std::env::var("DYLD_LIBRARY_PATH") {
        Ok(existing) if !existing.is_empty() => format!("{dir}:{existing}"),
        _ => dir,
    })
}

#[cfg(not(target_os = "macos"))]
fn bundled_dyld_value(_cfg: &SupervisorConfig) -> Option<String> {
    None
}

/// D44-S2: the typed refusal a second boot gets when another live
/// instance owns the data dir — the signal to ATTACH instead of failing.
#[derive(Debug)]
pub struct DataDirOwned(pub std::path::PathBuf);

impl std::fmt::Display for DataDirOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another supervised store already owns {} — attach to it or pass a different --data-dir",
            self.0.display()
        )
    }
}

impl std::error::Error for DataDirOwned {}

/// D44-S2: the attach record the owning node publishes beside its graph
/// — the endpoint and secrets a second concurrent session reuses in
/// client mode instead of starting a second store. Mode 0600; the data
/// home is single-user by construction in standalone mode.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AttachInfo {
    /// The owner node's loopback HTTP/gRPC base URL.
    pub backend: String,
    /// The org the owner serves (R14-final): attachers refuse a
    /// different `--org` instead of hydrating a foreign partition.
    pub org: String,
    /// The owner's bearer (EXOCORTEX_AUTH_TOKEN).
    pub auth_token: String,
    /// The owner's producer key (EXOCORTEX_HMAC_KEY, 64 hex).
    pub hmac_key: String,
    /// The owner's SSE client key (64 hex).
    pub sse_key: String,
}

impl AttachInfo {
    /// Every field must be present and well-shaped; a malformed record
    /// fails closed (an attacher must never guess at credentials).
    /// R14-B6: the backend is pinned to a literal loopback HOST by
    /// parsing it (a string prefix admits userinfo —
    /// `http://127.0.0.1:1@evil.example/` passes the prefix and dials
    /// evil), with scheme http and no `@` in the authority; the secrets
    /// ride the ONE hex validator (`signing::decode_hex32` — the CL3
    /// one-impl rule; hex also cannot carry shell-significant bytes into
    /// the file the wrapper `source`s).
    fn validate(&self) -> anyhow::Result<()> {
        let uri: http::Uri = self
            .backend
            .parse()
            .map_err(|e| anyhow::anyhow!("attach record backend is not a URI: {e}"))?;
        anyhow::ensure!(
            uri.scheme_str() == Some("http"),
            "attach record backend must be plain http, got {}",
            self.backend
        );
        let authority = uri
            .authority()
            .ok_or_else(|| anyhow::anyhow!("attach record backend has no authority"))?;
        anyhow::ensure!(
            !authority.as_str().contains('@'),
            "attach record backend carries userinfo: {}",
            self.backend
        );
        let host = uri.host().unwrap_or_default();
        anyhow::ensure!(
            host == "127.0.0.1" || host == "::1" || host == "localhost",
            "attach record backend must be loopback, got {host}"
        );
        anyhow::ensure!(
            uri.port_u16().is_some(),
            "attach record backend must carry an explicit port"
        );
        // R14-final (SO-3): nothing may follow the authority — a path or
        // query can carry quote bytes that escape the single-quoted env
        // file the wrapper sources. The owner always renders a bare
        // authority; anything else is not our record.
        anyhow::ensure!(
            uri.path().is_empty() || uri.path() == "/",
            "attach record backend must carry no path, got {}",
            self.backend
        );
        anyhow::ensure!(
            uri.query().is_none(),
            "attach record backend must carry no query"
        );
        for (name, value) in [
            ("auth_token", &self.auth_token),
            ("hmac_key", &self.hmac_key),
            ("sse_key", &self.sse_key),
        ] {
            exocortex_wire::signing::decode_hex32(value)
                .map_err(|e| anyhow::anyhow!("attach record {name}: {e}"))?;
        }
        Ok(())
    }

    /// Render the runtime-env lines a wrapper sources: the backend + SSE
    /// key every session gets, plus the owner's credentials (an attacher
    /// must present the owner's identity — standalone serves ONE
    /// principal) and the attach marker the wrapper keys its per-session
    /// WAL slot on.
    pub fn runtime_env_lines(&self) -> String {
        format!(
            "EXOCORTEX_BACKEND='{}'\nEXOCORTEX_SSE_KEY='{}'\nEXOCORTEX_AUTH_TOKEN='{}'\nEXOCORTEX_HMAC_KEY='{}'\nEXOCORTEX_ATTACHED='1'\n",
            self.backend, self.sse_key, self.auth_token, self.hmac_key
        )
    }

    /// Read + validate the attach record from `<data_dir>/attach.json`.
    pub fn read(data_dir: &std::path::Path) -> anyhow::Result<Self> {
        let path = data_dir.join("attach.json");
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("no attach record at {}: {e}", path.display()))?;
        let info: AttachInfo = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("malformed attach record: {e}"))?;
        info.validate()?;
        Ok(info)
    }

    /// Publish (0600, atomic) as the owning node.
    pub fn write(&self, data_dir: &std::path::Path) -> anyhow::Result<()> {
        let raw = serde_json::to_string(self)?;
        exocortex_storage::bounded_io::atomic_write_private(
            &data_dir.join("attach.json"),
            raw.as_bytes(),
            "attach record",
        )?;
        Ok(())
    }
}

/// D44: exclusively own the data dir for the lifetime of the supervised
/// store. flock releases on process death by itself, so a crashed node
/// never leaves a stale lock; a second live instance is refused while
/// the first owns the directory — two stores appending to one AOF is
/// the corruption class of GitHub issue #2.
pub fn acquire_data_dir_lock(data_dir: &std::path::Path) -> anyhow::Result<std::fs::File> {
    use std::io::Write as _;
    std::fs::create_dir_all(data_dir)?;
    let lock_path = data_dir.join(".supervised.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            // D44-S2: typed so the caller can attach instead of failing.
            return Err(anyhow::Error::new(DataDirOwned(data_dir.to_path_buf())));
        }
    }
    let mut file = file;
    let _ = file.write_all(format!("{}\n", std::process::id()).as_bytes());
    Ok(file)
}

/// D46 (root-caused 2026-10-07 on the owner's real graph): the module's
/// async-delete worker drains its deletion queue in a `RedisModule_Fork`
/// child ~30-60s after the first graph delete. On darwin-arm64 that fork
/// corrupts a parked thread-pool worker's condition variable and macOS
/// pthread traps the PARENT with SIGILL — the crash reports put the
/// faulting thread in `__psynch_cvwait` inside the module's `thread_do`.
/// This is the second D45-class fork trigger (after the BGSAVE fork).
/// The module rejects `ASYNC_DELETE` as a load argument and rejects
/// `0`/`false`/`off` at runtime — only the literal `no` is accepted —
/// so the supervisor flips it over RESP right after the store answers
/// PING. Deletions then run inline on the query thread: correct, and
/// cheap at the supervised store's delete volume. A repro harness that
/// needs the fork back can send `GRAPH.CONFIG SET ASYNC_DELETE yes`.
fn async_delete_no_resp() -> &'static [u8] {
    b"*4\r\n$12\r\nGRAPH.CONFIG\r\n$3\r\nSET\r\n$12\r\nASYNC_DELETE\r\n$2\r\nno\r\n"
}

/// Best-effort `GRAPH.CONFIG SET ASYNC_DELETE no` over RESP, mirroring
/// `ping`/`request_shutdown` (AUTH first when the store enforces a token).
fn set_async_delete_no(port: u16, auth_token: Option<&str>) -> bool {
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
    if s.write_all(async_delete_no_resp()).is_err() {
        return false;
    }
    let Ok(n) = s.read(&mut buf) else {
        return false;
    };
    buf[..n].starts_with(b"+OK")
}

/// Apply the D46 mitigation after the store is up. On darwin the
/// async-delete fork is a known parent-killer, so failure is fatal with
/// a named cause rather than the crash loop it predicts; elsewhere the
/// fork is benign and the setting is still correct, so a warning rides.
fn disarm_async_delete_fork(cfg: &SupervisorConfig) -> anyhow::Result<()> {
    if set_async_delete_no(cfg.port, cfg.auth_token.as_deref()) {
        return Ok(());
    }
    let why = "could not send GRAPH.CONFIG SET ASYNC_DELETE no to the supervised store";
    #[cfg(target_os = "macos")]
    {
        anyhow::bail!("{why}; its async-delete fork can SIGILL the store (D46)");
    }
    #[cfg(not(target_os = "macos"))]
    {
        tracing::warn!("{why}");
        Ok(())
    }
}

/// Wait for PING with the startup deadline; errors if the child exits.
fn wait_ping(cfg: &SupervisorConfig, child: &mut Child) -> anyhow::Result<bool> {
    let deadline = Instant::now() + cfg.startup_timeout.unwrap_or(Duration::from_secs(10));
    loop {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "supervised redis-server exited during startup: {}",
                describe_death(cfg, &status)
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
fn describe_death(cfg: &SupervisorConfig, status: &std::process::ExitStatus) -> String {
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

/// Resolve binary/module paths from flags or environment, then make both
/// ABSOLUTE. Redis chdirs into `--dir` during startup BEFORE loading
/// modules, so a relative `--loadmodule` path is resolved against the data
/// directory and dlopen fails with "no such file" / "server aborting"
/// (D29's primary defect: every release-runner leg failed this way because
/// the wrapper exports `dist/$DIST`-relative runtime paths; local runs with
/// absolute paths never saw it). Canonicalizing here also fails fast when
/// the configured binary or module does not exist.
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
    let bin = std::fs::canonicalize(&bin)
        .map_err(|e| anyhow::anyhow!("redis-server binary not found at {}: {e}", bin.display()))?;
    let module = std::fs::canonicalize(&module)
        .map_err(|e| anyhow::anyhow!("FalkorDB module not found at {}: {e}", module.display()))?;
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
    // D44: exclusive ownership for the whole spawn + serve lifetime.
    let data_dir_lock = acquire_data_dir_lock(&cfg.data_dir)?;
    let mut child = spawn_child(cfg)?;
    if !wait_ping(cfg, &mut child)? {
        let _ = child.kill();
        kill_store_process_group(child.id());
        anyhow::bail!(
            "supervised FalkorDB server did not answer PING within {}",
            cfg.startup_timeout
                .unwrap_or(Duration::from_secs(10))
                .as_secs()
        );
    }
    // D46: disarm the module's async-delete fork before any traffic can
    // queue a deletion, and arm the D46b quiesce window so the one
    // index-GC fork per boot meets an idle store.
    disarm_async_delete_fork(cfg)?;
    exocortex_storage::fork_window::arm();
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
        _data_dir_lock: Some(data_dir_lock),
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

    /// D45: the supervised default must DISABLE the RDB snapshot — the
    /// BGSAVE fork SIGILLs the parent over a real loaded graph. An
    /// explicit harness policy still passes through.
    #[test]
    fn supervised_default_disables_the_bgsave_snapshot() {
        let mut cfg = SupervisorConfig {
            redis_server_bin: "/bin/sleep".into(),
            falkordb_module: "unused".into(),
            data_dir: std::env::temp_dir(),
            port: 0,
            max_restarts: 0,
            port_file: None,
            auth_token: None,
            supervisor_pid: None,
            startup_timeout: None,
            save_policy: None,
        };
        assert_eq!(store_save_policy(&cfg), "", "default: no RDB snapshot");
        cfg.save_policy = Some("1 1".into());
        assert_eq!(store_save_policy(&cfg), "1 1", "harness override rides");
    }

    /// D46: the supervisor's RESP payload must spell the config exactly
    /// as the module's parser accepts — a four-element array with the
    /// literal `no` (0/false/off are rejected, as is a load argument).
    #[test]
    fn async_delete_no_resp_spells_the_only_accepted_form() {
        // ASYNC_DELETE is 12 bytes; a miscounted bulk length is a
        // protocol error that the server answers by closing the link.
        let payload = async_delete_no_resp();
        assert_eq!(
            payload,
            &b"*4\r\n$12\r\nGRAPH.CONFIG\r\n$3\r\nSET\r\n$12\r\nASYNC_DELETE\r\n$2\r\nno\r\n"[..]
        );
        let body = String::from_utf8_lossy(payload);
        for token in ["GRAPH.CONFIG", "ASYNC_DELETE", "no"] {
            let len = token.len();
            assert!(
                body.contains(&format!("${len}\r\n{token}\r\n")),
                "{token} framing"
            );
        }
    }

    #[test]
    fn attach_record_round_trips_and_fails_closed_on_malformation() {
        let dir = std::env::temp_dir().join(format!("exo-attach-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // R14: DISTINCT values per field — a permutation in the format
        // string or the JSON keys must fail the read-back, not pass it.
        let token = "1".repeat(64);
        let hmac = "2".repeat(64);
        let sse = "3".repeat(64);
        let info = AttachInfo {
            backend: "http://127.0.0.1:41234".into(),
            org: "attach-org".into(),
            auth_token: token.clone(),
            hmac_key: hmac.clone(),
            sse_key: sse.clone(),
        };
        info.write(&dir).unwrap();
        let read_back = AttachInfo::read(&dir).unwrap();
        assert_eq!(read_back.backend, info.backend);
        assert_eq!(read_back.org, "attach-org");
        assert_eq!(read_back.auth_token, token);
        assert_eq!(read_back.hmac_key, hmac);
        assert_eq!(read_back.sse_key, sse);
        // Mode 0600: the record carries the owner's credentials.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join("attach.json"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // The runtime env a wrapper sources carries ALL of it + the
        // marker, each field under its own key with its own value.
        let lines = info.runtime_env_lines();
        assert!(lines.contains("EXOCORTEX_ATTACHED='1'"));
        assert!(lines.contains(&format!("EXOCORTEX_AUTH_TOKEN='{token}'")));
        assert!(lines.contains(&format!("EXOCORTEX_HMAC_KEY='{hmac}'")));
        assert!(lines.contains(&format!("EXOCORTEX_SSE_KEY='{sse}'")));
        // Malformed records never authorize an attach.
        std::fs::write(
            dir.join("attach.json"),
            "{\"backend\":\"http://10.0.0.1:1\"}",
        )
        .unwrap();
        assert!(
            AttachInfo::read(&dir).is_err(),
            "non-loopback must fail closed"
        );
        // R14-B6: the userinfo shape passes a naive prefix check but must
        // fail the parsed one.
        let mut evil = serde_json::to_string(&info).unwrap();
        evil = evil.replace(
            "http://127.0.0.1:41234",
            "http://127.0.0.1:1@evil.example:6379/",
        );
        std::fs::write(dir.join("attach.json"), evil).unwrap();
        assert!(
            AttachInfo::read(&dir).is_err(),
            "userinfo-prefixed backend must fail closed"
        );
        let mut evil = serde_json::to_string(&info).unwrap();
        evil = evil.replace(&token, "short"); // not 64 hex
        std::fs::write(dir.join("attach.json"), evil).unwrap();
        assert!(
            AttachInfo::read(&dir).is_err(),
            "short credentials must fail closed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn data_dir_conflict_is_the_typed_attach_signal() {
        let dir = std::env::temp_dir().join(format!("exo-owned-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let first = acquire_data_dir_lock(&dir).unwrap();
        let error = acquire_data_dir_lock(&dir).unwrap_err();
        assert!(
            error.downcast_ref::<DataDirOwned>().is_some(),
            "the second lock attempt must carry the typed signal, got: {error}"
        );
        drop(first);
        // Under full parallel load the kernel's flock release can lag the
        // close by a moment (observed once in the matrix run, green
        // standalone) — retry briefly rather than flaking the gate.
        let reacquired = (0..20).any(|_| {
            if acquire_data_dir_lock(&dir).is_ok() {
                true
            } else {
                std::thread::sleep(std::time::Duration::from_millis(100));
                false
            }
        });
        assert!(reacquired, "released on drop (within 2s)");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D36: `resolve_paths_canonicalizes_relative_runtime_paths`
    /// mutates the PROCESS cwd (`std::env::set_current_dir` is global
    /// to every test thread). A child spawned by a sibling test while
    /// the cwd is mid-swap stalls before exec in uninterruptible wait
    /// — observed live: the D29 diagnose stub's stderr sink stays
    /// empty and its `/bin/sh` sits in state U past `wait_ping`'s 10s
    /// deadline, so the exit-naming assertion fails (≈1 run in 3
    /// multi-threaded; 10/10 green `--test-threads=1`; 12/12 green
    /// with the chdir test ignored). Tests that mutate the cwd and
    /// tests that spawn-and-wait a child serialize on this lock.
    static PROCESS_CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
            supervisor_pid: None,
            startup_timeout: None,
            save_policy: None,
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
            _data_dir_lock: None,
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
            _data_dir_lock: None,
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

    /// D29 (primary defect): relative runtime paths must be canonicalized
    /// to absolute before they reach the child — redis chdirs into `--dir`
    /// before `--loadmodule`, so a relative module path would be resolved
    /// against the data directory and abort every supervised startup.
    #[test]
    fn resolve_paths_canonicalizes_relative_runtime_paths() {
        #[cfg(unix)]
        {
            // D36: the chdir below is a process-global side effect —
            // hold the lock so no sibling spawns a child mid-swap.
            let _cwd_guard = PROCESS_CWD_LOCK.lock().unwrap_or_else(|p| p.into_inner());
            use std::os::unix::fs::PermissionsExt as _;
            let dir = std::env::temp_dir().join(format!(
                "exocortex-supervisor-resolve-{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let stub = dir.join("redis-stub");
            std::fs::write(&stub, "#!/bin/sh\nexit 0\n").unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let module = dir.join("falkordb-stub.so");
            std::fs::write(&module, b"stub").unwrap();
            let previous = std::env::current_dir().unwrap();
            std::env::set_current_dir(&dir).unwrap();
            let (bin, module) = resolve_paths(
                Some(PathBuf::from("redis-stub")),
                Some(PathBuf::from("falkordb-stub.so")),
            )
            .expect("relative paths resolve against the current directory");
            std::env::set_current_dir(previous).unwrap();
            assert!(
                bin.is_absolute(),
                "binary path is canonicalized: {}",
                bin.display()
            );
            assert!(
                module.is_absolute(),
                "module path is canonicalized: {}",
                module.display()
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// D29 companion: a missing binary or module fails fast with the path
    /// named, instead of surfacing later as an opaque child exit.
    #[test]
    fn resolve_paths_names_missing_runtime_files() {
        let error = resolve_paths(
            Some(PathBuf::from("/nonexistent/redis-server")),
            Some(PathBuf::from("/nonexistent/falkordb.so")),
        )
        .expect_err("a missing binary cannot resolve");
        let message = format!("{error:#}");
        assert!(
            message.contains("redis-server binary not found"),
            "the error names the missing binary: {message}"
        );
        assert!(
            message.contains("/nonexistent/redis-server"),
            "the error names the path: {message}"
        );
    }

    /// D29 (macOS leg): the store child must be able to resolve the dylibs
    /// bundled beside the module — the supervisor points the child's
    /// DYLD_LIBRARY_PATH at the module's directory, so a stock machine
    /// without Homebrew libomp/openssl@3 still loads the module.
    #[test]
    #[cfg(target_os = "macos")]
    fn store_child_receives_the_bundled_dyld_library_path() {
        let mut command = Command::new("/bin/true");
        apply_bundled_dyld_path(&mut command, std::path::Path::new("/rt/falkordb.so"));
        let value = command
            .get_envs()
            .find(|(key, _)| *key == "DYLD_LIBRARY_PATH")
            .and_then(|(_, value)| value)
            .expect("DYLD_LIBRARY_PATH is set on the store child");
        assert_eq!(value.to_string_lossy(), "/rt");
    }

    /// D44: the data-dir lock is exclusive while held and releases on
    /// drop — a second live store on one directory is refused, and the
    /// refusal names the directory.
    #[test]
    fn data_dir_lock_is_exclusive_and_released() {
        let dir = std::env::temp_dir().join(format!(
            "exocortex-supervisor-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let first = acquire_data_dir_lock(&dir).expect("first owner locks");
        let message = match acquire_data_dir_lock(&dir) {
            Ok(_) => panic!("a second live store was allowed onto one data dir"),
            Err(error) => format!("{error}"),
        };
        assert!(
            message.contains("another supervised store already owns"),
            "the refusal names the condition: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
        drop(first);
        assert!(
            acquire_data_dir_lock(&dir).is_ok(),
            "the lock releases with its owner"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D44 (GitHub issue #2's orphan evidence): the store must die with
    /// its SUPERVISOR, even when nothing runs Drop. Drives spawn_child
    /// DIRECTLY — a spawn that fails startup tears the group down by
    /// itself, which would satisfy this test without the watchdog
    /// existing at all. The stub here serves no PING and no error path
    /// runs: only the watchdog's watcher subshell can reap it after the
    /// supervisor is kill -9ed. Without the watchdog this test times
    /// out red.
    #[test]
    #[cfg(unix)]
    fn store_child_dies_with_its_supervisor() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!(
            "exocortex-supervisor-watchdog-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let stub = dir.join("store-stub");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\necho $$ > {}/stub.pid\nexec sleep 60\n",
                dir.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut supervisor = Command::new("/bin/sleep")
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let cfg = SupervisorConfig {
            redis_server_bin: stub,
            falkordb_module: "unused".into(),
            data_dir: dir.clone(),
            port: free_port().unwrap(),
            max_restarts: 0,
            port_file: None,
            auth_token: None,
            supervisor_pid: Some(supervisor.id()),
            startup_timeout: None,
            save_policy: None,
        };
        let child = spawn_child(&cfg).expect("watchdog spawns");
        let _leaked: Child = child;
        let pid_file = dir.join("stub.pid");
        let deadline = Instant::now() + Duration::from_secs(5);
        let stub_pid: i32 = loop {
            if let Ok(text) = std::fs::read_to_string(&pid_file) {
                break text.trim().parse().expect("stub pid");
            }
            assert!(Instant::now() < deadline, "the stub never started");
            std::thread::sleep(Duration::from_millis(50));
        };
        supervisor.kill().unwrap();
        let _ = supervisor.wait();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if unsafe { libc::kill(stub_pid, 0) } != 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the store outlived its supervisor (watchdog failed)"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D44: a spawn that fails startup tears down the whole store
    /// PROCESS GROUP — the staying stub behind the watchdog shell must
    /// be gone when spawn_supervised returns its error.
    #[test]
    #[cfg(unix)]
    fn failing_spawn_kills_the_store_group() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!(
            "exocortex-supervisor-groupkill-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let stub = dir.join("store-stub");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\necho $$ > {}/stub.pid\nexec sleep 60\n",
                dir.display()
            ),
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
            supervisor_pid: None,
            startup_timeout: Some(Duration::from_secs(1)),
            save_policy: None,
        };
        assert!(
            spawn_supervised(&cfg).is_err(),
            "the stub never answers PING"
        );
        let pid_file = dir.join("stub.pid");
        // D36 class: under heavy machine contention the stub's spawn can
        // stall pre-exec for seconds — the bounded wait must outlive a
        // loaded machine, not 3 spare seconds (publish-matrix flake
        // 2026-10-06 under 23 parallel jest workers).
        let deadline = Instant::now() + Duration::from_secs(20);
        let stub_pid: i32 = loop {
            if let Ok(text) = std::fs::read_to_string(&pid_file) {
                break text.trim().parse().expect("stub pid");
            }
            assert!(Instant::now() < deadline, "the stub never started");
            std::thread::sleep(Duration::from_millis(50));
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if unsafe { libc::kill(stub_pid, 0) } != 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the store survived the failed spawn's teardown (group kill failed)"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D45: post-startup deaths are diagnosable — the exit status AND
    /// the store's own log tail ride the restart log (and the
    /// budget-exhausted error), where the bare WARN left the
    /// store-death investigation with nothing.
    #[test]
    fn post_startup_death_carries_the_log_tail() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = std::env::temp_dir()
                .join(format!("exocortex-death-diagnosis-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let stub = dir.join("store-stub");
            std::fs::write(&stub, "#!/bin/sh\necho 'stub: dying loud' >&2\nexit 7\n").unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let cfg = SupervisorConfig {
                redis_server_bin: stub,
                falkordb_module: "unused".into(),
                data_dir: dir.clone(),
                port: free_port().unwrap(),
                max_restarts: 0,
                port_file: None,
                auth_token: None,
                supervisor_pid: None,
                startup_timeout: None,
                save_policy: None,
            };
            let child = spawn_child(&cfg).expect("watchdog spawns");
            let output = child.wait_with_output().expect("stub exits");
            let why = describe_death(&cfg, &output.status);
            assert!(
                why.contains("exit status 7"),
                "the diagnosis names the exit status: {why}"
            );
            assert!(
                why.contains("stub: dying loud"),
                "the diagnosis carries the store's own stderr tail: {why}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// D46 (GitHub issue #4): a supervised store restart must not wedge
    /// the node's store consumers. A FalkorStorage is built against a
    /// live supervised store; the store is hard-killed (group SIGKILL,
    /// no Drop) and a NEW store is started on the SAME port and token —
    /// exactly what poll()'s restart does. The storage handle must
    /// answer pings again: with a pinned MultiplexedConnection it never
    /// does (the once-per-second broken pipe, forever); with the
    /// ConnectionManager it reconnects.
    #[tokio::test]
    async fn storage_pings_recover_after_a_store_restart() {
        let (Ok(bin), Ok(module)) = (
            std::env::var("EXOCORTEX_REDIS_SERVER"),
            std::env::var("EXOCORTEX_FALKORDB_MODULE"),
        ) else {
            eprintln!(
                "SKIP storage_pings_recover_after_a_store_restart: EXOCORTEX_REDIS_SERVER/\
                 EXOCORTEX_FALKORDB_MODULE absent; live suite unexecuted"
            );
            return;
        };
        let base = std::env::temp_dir().join(format!(
            "exocortex-reconnect-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let token = "5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a5f4d3c2b1a";
        let cfg = SupervisorConfig {
            redis_server_bin: bin.into(),
            falkordb_module: module.into(),
            data_dir: base.join("a"),
            port: free_port().unwrap(),
            max_restarts: 0,
            port_file: None,
            auth_token: Some(token.into()),
            supervisor_pid: None,
            startup_timeout: None,
            save_policy: None,
        };
        let mut server = spawn_supervised(&cfg).expect("store A starts");
        let (falkor_url, redis_url) = supervised_store_urls(cfg.port, Some(token));
        let ontology = std::sync::Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = exocortex_storage::FalkorStorage::connect(
            exocortex_storage::FalkorConfig {
                falkor_url,
                redis_url,
                graph_name: "exocortex-reconnect".into(),
                org_id: "o".into(),
                node_id: "reconnect-probe".into(),
            },
            ontology,
        )
        .await
        .expect("storage connects to store A");
        use exocortex_storage::Storage as _;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), storage.ping())
                .await
                .expect("ping A")
                .is_ok(),
            "the store answers before the restart"
        );

        // Hard-kill A (group SIGKILL, no Drop), then bring a NEW store
        // up on the same port and token on a fresh dir — poll()'s
        // restart, by hand. The old server handle is leaked so its
        // flock stays on dir A, which the replacement does not need
        // (spawn_child bypasses the lock).
        kill_store_process_group(server.child.id());
        let _ = server.child.wait();
        let replacement = SupervisorConfig {
            data_dir: base.join("b"),
            ..cfg
        };
        std::fs::create_dir_all(&replacement.data_dir).unwrap();
        let mut child_b = spawn_child(&replacement).expect("store B spawns");
        assert!(
            wait_ping(&replacement, &mut child_b).expect("B startup"),
            "store B serves on the same port and token"
        );

        let mut ok = false;
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if tokio::time::timeout(Duration::from_secs(2), storage.ping())
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false)
            {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        kill_store_process_group(child_b.id());
        let _ = child_b.wait();
        let _ = std::fs::remove_dir_all(&base);
        assert!(
            ok,
            "the storage handle recovered after the store restart (a pinned connection would ping dead forever)"
        );
    }

    /// D45: the store-death repro harness (GitHub issue #3), live and
    /// env-gated like the auth suite — without EXOCORTEX_REDIS_SERVER
    /// and EXOCORTEX_FALKORDB_MODULE it skips loudly. Legs:
    /// (control) an empty-dir store survives its first BGSAVE window;
    /// (dual-writer) two 5beaf74-era stores serve CONCURRENTLY on one
    /// data dir — the corruption precondition — then a fresh boot on
    /// the interleaved dir is observed through its first dirty-save
    /// window, with and without snapshots (the discriminator).
    /// Set EXOCORTEX_REPRO_DATA_DIR to observe a COPY of a real data
    /// dir instead of the synthetic corpus (never the live dir).
    #[test]
    fn store_death_repro_harness() {
        let (Ok(bin), Ok(module)) = (
            std::env::var("EXOCORTEX_REDIS_SERVER"),
            std::env::var("EXOCORTEX_FALKORDB_MODULE"),
        ) else {
            eprintln!(
                "SKIP store_death_repro_harness: EXOCORTEX_REDIS_SERVER/\
                 EXOCORTEX_FALKORDB_MODULE absent; live suite unexecuted"
            );
            return;
        };
        let base = std::env::temp_dir().join(format!(
            "exocortex-repro-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));

        let boot = |dir: &std::path::Path, save: Option<&str>| -> SupervisorConfig {
            SupervisorConfig {
                redis_server_bin: bin.clone().into(),
                falkordb_module: module.clone().into(),
                data_dir: dir.to_path_buf(),
                port: free_port().unwrap(),
                max_restarts: 0,
                port_file: None,
                auth_token: None,
                supervisor_pid: None,
                startup_timeout: None,
                save_policy: save.map(str::to_owned),
            }
        };
        // EXOCORTEX_REPRO_SCALE multiplies the write volume (the OOM-
        // during-fork variant of the hypothesis needs real memory
        // pressure; the default corpus is small and fast).
        let scale: usize = std::env::var("EXOCORTEX_REPRO_SCALE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        // One connection for the whole dirty phase: a connection per
        // command exhausts ephemeral ports at EXOCORTEX_REPRO_SCALE.
        let dirty = |cfg: &SupervisorConfig, batches: usize| {
            use std::io::{Read, Write};
            let mut stream =
                std::net::TcpStream::connect(("127.0.0.1", cfg.port)).expect("dirty session");
            for i in 0..batches * scale {
                // Plain SETs dirty the DB (the `--save 1 1` fork fires
                // on any write) without embedding Cypher outside
                // exocortex-storage (CR-10) — the real-data mode is
                // what carries actual graph keys through the module.
                let key = format!("repro:{i}");
                let value = "v".repeat(256);
                let frame = format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                );
                stream.write_all(frame.as_bytes()).expect("write");
                let mut buf = [0u8; 64];
                let n = stream.read(&mut buf).expect("reply");
                let reply = String::from_utf8_lossy(&buf[..n]).into_owned();
                assert!(reply.starts_with('+'), "write {i} succeeded: {reply}");
            }
        };
        let alive = |cfg: &SupervisorConfig| -> bool { ping(cfg.port, None) };

        // ---- control: an empty-dir store survives past its first
        //      dirty BGSAVE (the ~1s `--save 1 1` fork).
        {
            let dir = base.join("control");
            let cfg = boot(&dir, None);
            let server = spawn_supervised(&cfg).expect("control store starts");
            dirty(&cfg, 3);
            std::thread::sleep(Duration::from_secs(4));
            assert!(
                alive(&cfg),
                "control: the store survives its first save fork"
            );
            drop(server);
        }

        // ---- dual-writer precondition: two stores on ONE dir at the
        //      same time (the 5beaf74 hazard, reproduced deliberately
        //      via spawn_child — spawn_supervised's flock refuses it).
        {
            let dir = base.join("dual");
            std::fs::create_dir_all(&dir).unwrap();
            let cfg_a = boot(&dir, None);
            let mut a = spawn_child(&cfg_a).expect("writer A spawns");
            assert!(
                wait_ping(&cfg_a, &mut a).expect("A startup"),
                "writer A serves"
            );
            let cfg_b = SupervisorConfig {
                port: free_port().unwrap(),
                ..boot(&dir, None)
            };
            let mut b = spawn_child(&cfg_b).expect("writer B spawns");
            assert!(
                wait_ping(&cfg_b, &mut b).expect("B startup"),
                "writer B serves on the SAME data dir — the 5beaf74 hazard"
            );
            dirty(&cfg_a, 5);
            dirty(&cfg_b, 5);
            // Hard-kill both, no Drop, no shutdown save.
            kill_store_process_group(a.id());
            kill_store_process_group(b.id());
            let _ = a.wait();
            let _ = b.wait();
            std::thread::sleep(Duration::from_secs(1));

            // ---- observation: a fresh supervised boot on the
            //      interleaved dir, through its first dirty-save
            //      window, WITH and WITHOUT snapshots.
            for (tag, save, expect_alive) in [
                ("corrupted-with-saves", None, true),
                ("corrupted-no-saves", Some(""), true),
            ] {
                let cfg = SupervisorConfig {
                    // A big interleaved corpus replays slowly; the
                    // observation window must not mistake a slow boot
                    // for the death.
                    startup_timeout: Some(Duration::from_secs(60)),
                    ..boot(&dir, save)
                };
                let observed = match spawn_supervised(&cfg) {
                    Ok(mut server) => {
                        dirty(&cfg, 2);
                        std::thread::sleep(Duration::from_secs(4));
                        let survived = alive(&cfg);
                        let why = server
                            .child
                            .try_wait()
                            .ok()
                            .flatten()
                            .map(|status| describe_death(&cfg, &status))
                            .unwrap_or_default();
                        eprintln!(
                            "repro[{tag}]: survived={survived}{}",
                            if why.is_empty() {
                                String::new()
                            } else {
                                format!(" death: {why}")
                            }
                        );
                        drop(server);
                        survived
                    }
                    Err(error) => {
                        eprintln!("repro[{tag}]: boot refused: {error:#}");
                        false
                    }
                };
                // The harness must never fabricate a verdict: both legs
                // assert only that the STORE-LEVEL behavior is coherent
                // (a boot that survives its window answers PING).
                assert_eq!(observed, expect_alive, "repro[{tag}] coherence");
            }
        }

        // ---- real-data mode: EXOCORTEX_REPRO_DATA_DIR=<copy of a real
        //      dir> observes THAT dir's behavior through the same
        //      window (the dir is copied first — never the live one).
        if let Ok(source) = std::env::var("EXOCORTEX_REPRO_DATA_DIR") {
            let dir = base.join("real-copy");
            std::fs::create_dir_all(&dir).unwrap();
            for item in ["appendonlydir", "dump.rdb"] {
                let from = std::path::Path::new(&source).join(item);
                if from.exists() {
                    let status = std::process::Command::new("cp")
                        .args(["-Rp", &from.to_string_lossy(), &dir.to_string_lossy()])
                        .status()
                        .expect("cp");
                    assert!(status.success(), "copied {item}");
                }
            }
            let cfg = SupervisorConfig {
                startup_timeout: Some(Duration::from_secs(60)),
                ..boot(&dir, None)
            };
            match spawn_supervised(&cfg) {
                Ok(mut server) => {
                    let _ = ping(cfg.port, None); // read-only probe: no dirtying
                    std::thread::sleep(Duration::from_secs(6));
                    let survived = alive(&cfg);
                    let why = server
                        .child
                        .try_wait()
                        .ok()
                        .flatten()
                        .map(|status| describe_death(&cfg, &status))
                        .unwrap_or_default();
                    eprintln!(
                        "repro[real-data]: survived={survived}{}",
                        if why.is_empty() {
                            String::new()
                        } else {
                            format!(" death: {why}")
                        }
                    );
                    drop(server);
                }
                Err(error) => eprintln!("repro[real-data]: boot refused: {error:#}"),
            }
        }

        let _ = std::fs::remove_dir_all(&base);
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
            // D36: serialize against the cwd-mutating sibling (the lock's
            // doc comment names the failure mode this prevents).
            let _cwd_guard = PROCESS_CWD_LOCK.lock().unwrap_or_else(|p| p.into_inner());
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
                supervisor_pid: None,
                startup_timeout: None,
                save_policy: None,
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
            supervisor_pid: None,
            startup_timeout: None,
            save_policy: None,
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
