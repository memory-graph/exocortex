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
    let args = std::fs::read_to_string(client_args).unwrap();
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
        "#!/bin/sh\n[ -z \"${EXOCORTEX_REDIS_SERVER:-}\" ]\n[ -z \"${EXOCORTEX_FALKORDB_MODULE:-}\" ]\nprintf '%s\\n' 'standalone live durable marker'\n",
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
