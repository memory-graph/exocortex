//! H1: `exocortex-worker --adapter noop` boots and idles without a live
//! backend (M6 AC; the lazy channel must not dial on startup).
use std::process::Command;

#[test]
fn noop_adapter_boots_without_backend() {
    let bin = env!("CARGO_BIN_EXE_exocortex-worker");
    let mut child = Command::new(bin)
        .args(["--adapter", "noop", "--backend", "http://127.0.0.1:1"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn worker");
    std::thread::sleep(std::time::Duration::from_millis(750));
    let exited = child.try_wait().expect("child is alive or exited cleanly");
    assert!(
        exited.is_none(),
        "worker must idle, not exit: {exited:?}"
    );
    child.kill().expect("kill");
    child.wait().expect("reaped");
}
