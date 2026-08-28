use std::io::{BufRead as _, BufReader, Write as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, Stdio};

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
    let fake_node = dir.join("exocortex-node");
    std::fs::write(
        &fake_node,
        format!(
            "#!/bin/sh\ntouch '{}'\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o700)).unwrap();

    let bin_dir = std::path::Path::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
        .parent()
        .unwrap();
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
        .env("EXOCORTEX_BIN_DIR", bin_dir)
        .env("EXOCORTEX_STANDALONE_NODE_BIN", &fake_node)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
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
    let mut response = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut response)
        .unwrap();
    if response.is_empty() {
        let mut stderr = String::new();
        std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut stderr).unwrap();
        panic!("installed wrapper closed before MCP response: {stderr}");
    }
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert!(response.get("result").is_some(), "{response}");
    assert!(marker.exists(), "standalone supervisor was not started");
    assert!(Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap()
        .success());
    child.wait().unwrap();
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
            "#!/bin/sh\ntouch '{}'\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
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
