use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, Stdio};

mod support;

#[test]
#[cfg(unix)]
fn installed_wrapper_starts_supervisor_and_serves_real_mcp_runtime() {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-standalone-wrapper-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("supervisor-started");
    let client_args = dir.join("client-args");
    let fake_node = dir.join("exocortex-node");
    let fake_client = dir.join("exocortex-mcp-client");
    let runtime_dir = dir.join("standalone-runtime");
    std::fs::create_dir_all(&runtime_dir).unwrap();
    let redis_server = runtime_dir.join("redis-server");
    let falkor_module = runtime_dir.join("falkordb.so");
    std::fs::write(&redis_server, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::write(&falkor_module, "fixture").unwrap();
    std::fs::write(
        &fake_node,
        format!(
            "#!/bin/sh\nruntime=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = --standalone-runtime-file ]; then runtime=$2; shift 2; else shift; fi\ndone\nprintf '%s\\n%s\\n' \"$EXOCORTEX_REDIS_SERVER\" \"$EXOCORTEX_FALKORDB_MODULE\" > '{}.runtime'\nprintf \"EXOCORTEX_BACKEND='http://127.0.0.1:43119'\\nEXOCORTEX_SSE_KEY='0000000000000000000000000000000000000000000000000000000000000000'\\n\" > \"$runtime\"\ntouch '{}'\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            marker.display(),
            marker.display()
        ),
    )
    .unwrap();
    std::fs::write(
        &fake_client,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\nIFS= read -r request\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{}}}}'\n",
            client_args.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&fake_client, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&redis_server, std::fs::Permissions::from_mode(0o700)).unwrap();

    let wrapper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/exocortex");
    let mut child = Command::new(wrapper)
        .args([
            "--mode",
            "mcp-standalone",
            "--org",
            "standalone-wrapper",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ])
        .env("EXOCORTEX_BIN_DIR", &dir)
        .env_remove("EXOCORTEX_REDIS_SERVER")
        .env_remove("EXOCORTEX_FALKORDB_MODULE")
        .env("EXOCORTEX_STANDALONE_NODE_BIN", &fake_node)
        .env("EXOCORTEX_STANDALONE_CLIENT_BIN", &fake_client)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let responses = support::BoundedLineReader::new(child.stdout.take().unwrap());
    writeln!(
        child.stdin.as_mut().unwrap(),
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": { "name": "wrapper-test", "version": "0" }
            }
        })
    )
    .unwrap();
    child.stdin.as_mut().unwrap().flush().unwrap();
    let response = responses.read_json(&mut child);
    assert!(response.get("result").is_some(), "{response}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(marker.exists(), "standalone supervisor was not started");
    assert!(child.wait().unwrap().success());
    // The cleanup probe also invokes the client (--tail-audit); pick the
    // real session's invocation line.
    let args = std::fs::read_to_string(client_args)
        .unwrap()
        .lines()
        .find(|line| line.contains("--backend"))
        .expect("the session client ran")
        .to_string();
    assert!(args.contains("--backend http://127.0.0.1:43119"), "{args}");
    let resolved_runtime = std::fs::read_to_string(marker.with_extension("runtime")).unwrap();
    assert_eq!(
        resolved_runtime.lines().collect::<Vec<_>>(),
        [
            redis_server.to_str().unwrap(),
            falkor_module.to_str().unwrap()
        ],
        "an extracted archive must resolve its sibling runtime without overrides"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn attached_session_reuses_owner_credentials_and_gets_its_own_wal_slot() {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-standalone-attach-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let client_args = dir.join("client-args");
    let client_env = dir.join("client-env");
    let fake_node = dir.join("exocortex-node");
    let fake_client = dir.join("exocortex-mcp-client");
    // The node simulates D44-S2 attach mode: it owns nothing, publishes
    // the OWNER's endpoint + credentials + the attach marker, and exits.
    let owner_token = "f".repeat(64);
    std::fs::write(
        &fake_node,
        format!(
            "#!/bin/sh\nruntime=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = --standalone-runtime-file ]; then runtime=$2; shift 2; else shift; fi\ndone\nprintf \"EXOCORTEX_BACKEND='http://127.0.0.1:43119'\\nEXOCORTEX_SSE_KEY='0000000000000000000000000000000000000000000000000000000000000000'\\nEXOCORTEX_AUTH_TOKEN='{owner_token}'\\nEXOCORTEX_HMAC_KEY='{owner_token}'\\nEXOCORTEX_ATTACHED='1'\\n\" > \"$runtime\"\n"
        ),
    )
    .unwrap();
    std::fs::write(
        &fake_client,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nprintf '%s\\n' \"$EXOCORTEX_AUTH_TOKEN\" > '{}'\nIFS= read -r request\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{}}}}'\n",
            client_args.display(),
            client_env.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&fake_client, std::fs::Permissions::from_mode(0o700)).unwrap();

    let wrapper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/exocortex");
    let mut child = Command::new(wrapper)
        .args([
            "--mode",
            "mcp-standalone",
            "--org",
            "attach-org",
            "--user",
            "attacher",
            // A user data-dir that must NOT be the attached client's WAL
            // home (it belongs to the owner session).
            "--data-dir",
            dir.to_str().unwrap(),
        ])
        .env("EXOCORTEX_BIN_DIR", &dir)
        .env_remove("EXOCORTEX_REDIS_SERVER")
        .env_remove("EXOCORTEX_FALKORDB_MODULE")
        .env("EXOCORTEX_STANDALONE_NODE_BIN", &fake_node)
        .env("EXOCORTEX_STANDALONE_CLIENT_BIN", &fake_client)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let responses = support::BoundedLineReader::new(child.stdout.take().unwrap());
    writeln!(
        child.stdin.as_mut().unwrap(),
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": { "name": "attach-test", "version": "0" }
            }
        })
    )
    .unwrap();
    child.stdin.as_mut().unwrap().flush().unwrap();
    let response = responses.read_json(&mut child);
    assert!(response.get("result").is_some(), "{response}");
    assert!(child.wait().unwrap().success());

    // The wrapper exported the OWNER's credentials to the client.
    assert_eq!(
        std::fs::read_to_string(&client_env).unwrap().trim(),
        owner_token
    );
    // The client's LAST --data-dir is the per-session slot under the
    // wrapper's temp runtime dir, not the shared user data dir.
    // The cleanup probe also invokes the client (--tail-audit); pick the
    // real session's invocation line.
    let args = std::fs::read_to_string(client_args)
        .unwrap()
        .lines()
        .find(|line| line.contains("--backend"))
        .expect("the session client ran")
        .to_string();
    let last_data_dir = args
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .filter(|w| w[0] == "--data-dir")
        .last()
        .expect("a --data-dir must be forwarded")[1]
        .to_string();
    assert!(
        last_data_dir.contains("exocortex-standalone."),
        "attached client must use a per-session slot, got: {args}"
    );
    assert_ne!(last_data_dir, dir.to_str().unwrap());
    assert!(
        args.contains("--backend http://127.0.0.1:43119"),
        "attached client rides the owner's backend: {args}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// D44-S2 live leg (env-gated on the bundled runtime, the supervisor
/// suite's pattern): two CONCURRENT wrapper sessions on one data dir must
/// share ONE store — the second ATTACHES (client mode, owner's
/// credentials, per-session WAL slot) instead of failing on the data-dir
/// lock, and a write through session 1 is searchable through session 2.
/// Fail-without-it: pre-S2 the second wrapper's node exited with the
/// data-dir-owned refusal and the session never served MCP.
#[test]
#[cfg(unix)]
fn two_concurrent_sessions_share_one_store_via_attach() {
    let (Some(redis), Some(module)) = (
        std::env::var("EXOCORTEX_REDIS_SERVER").ok(),
        std::env::var("EXOCORTEX_FALKORDB_MODULE").ok(),
    ) else {
        eprintln!(
            "live attach suite UNEXECUTED: set EXOCORTEX_REDIS_SERVER and \
             EXOCORTEX_FALKORDB_MODULE to the bundled runtime to run it"
        );
        return;
    };
    let dir = std::env::temp_dir().join(format!(
        "exocortex-standalone-attach-live-{}",
        std::process::id()
    ));
    let dir_marker = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // Panic-proof cleanup: everything this test spawns references the
    // unique dir in its argv, so a sweep by marker reaps the whole tree
    // (an early run leaked a node + store for hours when an assert
    // panicked before the explicit kills).
    let cleanup = |marker: &str| {
        let _ = Command::new("pkill").args(["-f", marker]).status();
    };
    cleanup(&dir_marker);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        attach_live_body(&dir, &redis, &module);
    }));
    cleanup(&dir_marker);
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

fn attach_live_body(dir: &std::path::Path, redis: &str, module: &str) {
    let bin_dir = std::path::Path::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
        .parent()
        .unwrap();
    let _ = (redis, module);
    let wrapper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/exocortex");
    let common_args = [
        "--mode".to_string(),
        "mcp-standalone".to_string(),
        "--org".to_string(),
        "attach-live".to_string(),
        "--user".to_string(),
        "tester".to_string(),
        "--data-dir".to_string(),
        dir.to_str().unwrap().to_string(),
    ];

    let spawn_session = || {
        let mut child = Command::new(&wrapper)
            .args(&common_args)
            .env("EXOCORTEX_BIN_DIR", bin_dir)
            .env(
                "EXOCORTEX_STANDALONE_NODE_BIN",
                bin_dir.join("exocortex-node"),
            )
            .env(
                "EXOCORTEX_STANDALONE_CLIENT_BIN",
                bin_dir.join("exocortex-mcp-client"),
            )
            .env("EXOCORTEX_REDIS_SERVER", redis)
            .env("EXOCORTEX_FALKORDB_MODULE", module)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let responses = support::BoundedLineReader::new(child.stdout.take().unwrap());
        writeln!(
            child.stdin.as_mut().unwrap(),
            "{}",
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05", "capabilities": {},
                    "clientInfo": { "name": "attach-live", "version": "0" }
                }
            })
        )
        .unwrap();
        child.stdin.as_mut().unwrap().flush().unwrap();
        (child, responses)
    };

    let (mut first, mut first_responses) = spawn_session();
    let response = first_responses.read_json(&mut first);
    assert!(
        response.get("result").is_some(),
        "session 1 must serve: {response}"
    );
    writeln!(
        first.stdin.as_mut().unwrap(),
        "{}",
        serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} })
    )
    .unwrap();
    first.stdin.as_mut().unwrap().flush().unwrap();

    let (mut second, mut second_responses) = spawn_session();
    let response = second_responses.read_json(&mut second);
    assert!(
        response.get("result").is_some(),
        "session 2 must ATTACH and serve, not fail on the data-dir lock: {response}"
    );
    writeln!(
        second.stdin.as_mut().unwrap(),
        "{}",
        serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} })
    )
    .unwrap();
    second.stdin.as_mut().unwrap().flush().unwrap();

    // Exactly ONE store process serves the data dir — counted by DISTINCT
    // PORT (redis rewrites its title; the watchdog shells duplicate the
    // store's argv), the same measure `--verify` uses.
    let ps = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .unwrap();
    // R14 (Cl7): the SAME measure --verify uses, not a re-implementation.
    let ports = exocortex_client::verify_checks::store_ports_on_dir(
        &String::from_utf8_lossy(&ps.stdout),
        dir,
    );
    assert_eq!(
        ports.len(),
        1,
        "one store must serve both sessions: {ports:?}"
    );

    // Cross-session visibility: write through session 1, read through 2.
    let call = |child: &mut std::process::Child,
                reader: &mut support::BoundedLineReader,
                id: i64,
                method: &str,
                params: serde_json::Value| {
        writeln!(
            child.stdin.as_mut().unwrap(),
            "{}",
            serde_json::json!({
                "jsonrpc": "2.0", "id": id, "method": method, "params": params
            })
        )
        .unwrap();
        child.stdin.as_mut().unwrap().flush().unwrap();
        let out = reader.read_json(child);
        assert!(out.get("result").is_some(), "call {method} failed: {out}");
        out
    };
    let ack = call(
        &mut first,
        &mut first_responses,
        2,
        "tools/call",
        serde_json::json!({
            "name": "exocortex.end_session",
            "arguments": {
                "project_id": "attach-live",
                "edges": [],
                "memories": [{
                    "draft_key": "d1",
                    "memory_type": "Fix",
                    "title": "attach live shared row",
                    "content": "written by session one",
                    "visibility": "org"
                }]
            }
        }),
    );
    let ack_text = ack["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        ack_text.contains("\"accepted\":1"),
        "write acked: {ack_text}"
    );

    // Give session 2's SSE a moment, then search through it.
    std::thread::sleep(std::time::Duration::from_millis(750));
    let hit = call(
        &mut second,
        &mut second_responses,
        3,
        "tools/call",
        serde_json::json!({
            "name": "exocortex.search_memories",
            "arguments": { "query": "attach live shared row", "limit": 5 }
        }),
    );
    let hit_text = hit["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        hit_text.contains("written by session one"),
        "session 2 must read session 1's write through the shared store: {hit_text}"
    );

    let _ = first.kill();
    let _ = first.wait();
    let _ = second.kill();
    let _ = second.wait();
}

/// R14 (T7): the wrapper's attach fail-closed branches — a bad marker,
/// non-hex credentials, or a wrong-width key must refuse the session
/// (nonzero exit) WITHOUT ever spawning the MCP client.
#[test]
#[cfg(unix)]
fn attach_runtime_failures_refuse_before_the_client_spawns() {
    use std::os::unix::fs::PermissionsExt as _;
    let cases: &[(&str, String)] = &[
        ("bad marker", format!(
            "EXOCORTEX_BACKEND='http://127.0.0.1:43119'\nEXOCORTEX_SSE_KEY='{zeros}'\nEXOCORTEX_AUTH_TOKEN='{zeros}'\nEXOCORTEX_HMAC_KEY='{zeros}'\nEXOCORTEX_ATTACHED='2'\n",
            zeros = "0".repeat(64),
        )),
        ("non-hex token", format!(
            "EXOCORTEX_BACKEND='http://127.0.0.1:43119'\nEXOCORTEX_SSE_KEY='{zeros}'\nEXOCORTEX_AUTH_TOKEN='{bad}'\nEXOCORTEX_HMAC_KEY='{zeros}'\nEXOCORTEX_ATTACHED='1'\n",
            zeros = "0".repeat(64),
            bad = "z".repeat(64),
        )),
        ("short key", format!(
            "EXOCORTEX_BACKEND='http://127.0.0.1:43119'\nEXOCORTEX_SSE_KEY='{zeros}'\nEXOCORTEX_AUTH_TOKEN='{zeros}'\nEXOCORTEX_HMAC_KEY='short'\nEXOCORTEX_ATTACHED='1'\n",
            zeros = "0".repeat(64),
        )),
    ];
    for (label, runtime_env) in cases {
        let dir =
            std::env::temp_dir().join(format!("exo-attach-refuse-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let client_marker = dir.join("client-ran");
        let fake_node = dir.join("exocortex-node");
        let fake_client = dir.join("exocortex-mcp-client");
        // A quoted heredoc: the env payload carries single quotes that
        // would break a printf-with-quotes stub (an earlier draft did
        // exactly that and the test passed for the WRONG reason — an
        // empty runtime file, a 10s readiness timeout, not a refusal).
        std::fs::write(
            &fake_node,
            format!(
                "#!/bin/sh\nruntime=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = --standalone-runtime-file ]; then runtime=$2; shift 2; else shift; fi\ndone\ncat > \"$runtime\" <<'EXO_ENV'\n{env}EXO_ENV\n",
                env = runtime_env,
            ),
        )
        .unwrap();
        std::fs::write(
            &fake_client,
            format!(
                "#!/bin/sh\ntouch '{}'\nIFS= read -r request\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{}}}}'\n",
                client_marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&fake_client, std::fs::Permissions::from_mode(0o700)).unwrap();

        let wrapper =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/exocortex");
        let mut child = Command::new(wrapper)
            .args(["--mode", "mcp-standalone", "--org", "refuse", "--user", "t"])
            .env("EXOCORTEX_BIN_DIR", &dir)
            .env_remove("EXOCORTEX_REDIS_SERVER")
            .env_remove("EXOCORTEX_FALKORDB_MODULE")
            .env("EXOCORTEX_STANDALONE_NODE_BIN", &fake_node)
            .env("EXOCORTEX_STANDALONE_CLIENT_BIN", &fake_client)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let _ = child.stdin.take();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success(), "{label}: the wrapper must refuse");
        assert!(
            !client_marker.exists(),
            "{label}: the client must never spawn"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
#[cfg(unix)]
fn installed_wrapper_rule_probe_enters_standalone_topology() {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-standalone-rule-probe-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("supervisor-started");
    let fake_node = dir.join("exocortex-node");
    std::fs::write(
        &fake_node,
        format!(
            "#!/bin/sh\nruntime=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = --standalone-runtime-file ]; then runtime=$2; shift 2; else shift; fi\ndone\nprintf \"EXOCORTEX_BACKEND='http://127.0.0.1:43119'\\nEXOCORTEX_SSE_KEY='0000000000000000000000000000000000000000000000000000000000000000'\\n\" > \"$runtime\"\ntouch '{}'\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o700)).unwrap();

    let bin_dir = std::path::Path::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
        .parent()
        .unwrap();
    let wrapper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/exocortex");
    let output = Command::new(wrapper)
        .args([
            "--mode",
            "mcp-standalone",
            "--verify-rules",
            "--org",
            "standalone-rule-probe",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ])
        .env("EXOCORTEX_BIN_DIR", bin_dir)
        .env_remove("EXOCORTEX_REDIS_SERVER")
        .env_remove("EXOCORTEX_FALKORDB_MODULE")
        .env("EXOCORTEX_STANDALONE_NODE_BIN", &fake_node)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "standalone rule probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(marker.exists(), "standalone supervisor was not started");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("rules-ok mode=mcp-standalone count=9")
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn archive_live_harness_leaves_sibling_runtime_resolution_to_wrapper() {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-archive-live-harness-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let runtime = dir.join("standalone-runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    for path in [
        dir.join("exocortex-node"),
        dir.join("exocortex-mcp-client"),
        runtime.join("redis-server"),
    ] {
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(runtime.join("falkordb.so"), "fixture").unwrap();
    let wrapper = dir.join("exocortex");
    std::fs::write(
        &wrapper,
        "#!/bin/sh\n[ -z \"${EXOCORTEX_REDIS_SERVER:-}\" ]\n[ -z \"${EXOCORTEX_FALKORDB_MODULE:-}\" ]\nprintf '%s\\n' '{\\\"accepted\\\":1,\\\"rejected\\\":0}'\nprintf '%s\\n' 'standalone live durable marker'\n",
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new("sh")
        .arg(root.join("scripts/test-standalone-live.sh"))
        .env("EXOCORTEX_BIN_DIR", &dir)
        .env("EXOCORTEX_WRAPPER", &wrapper)
        .env_remove("EXOCORTEX_REDIS_SERVER")
        .env_remove("EXOCORTEX_FALKORDB_MODULE")
        .current_dir(&root)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "the release workflow environment must exercise wrapper-owned sibling runtime discovery"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
