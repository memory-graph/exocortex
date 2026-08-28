use std::io::Read as _;
use std::process::{Child, ChildStdout};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BYTES: usize = exocortex_wire::limits::MAX_MCP_REQUEST_BYTES;

pub struct BoundedLineReader {
    lines: Receiver<Result<String, String>>,
}

impl BoundedLineReader {
    pub fn new(mut stdout: ChildStdout) -> Self {
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || loop {
            let mut line = Vec::new();
            let outcome = loop {
                let mut byte = [0_u8; 1];
                match stdout.read(&mut byte) {
                    Ok(0) if line.is_empty() => break None,
                    Ok(0) => break Some(Err("child closed stdout mid-response".into())),
                    Ok(_) if byte[0] == b'\n' => match String::from_utf8(line) {
                        Ok(line) => break Some(Ok(line)),
                        Err(_) => break Some(Err("child response was not UTF-8".into())),
                    },
                    Ok(_) if line.len() == MAX_RESPONSE_BYTES => {
                        break Some(Err(format!(
                            "child response exceeded {MAX_RESPONSE_BYTES} bytes"
                        )))
                    }
                    Ok(_) => line.push(byte[0]),
                    Err(error) => break Some(Err(format!("child stdout read failed: {error}"))),
                }
            };
            let Some(outcome) = outcome else {
                return;
            };
            if tx.send(outcome).is_err() {
                return;
            }
        });
        Self { lines }
    }

    pub fn read_json(&self, child: &mut Child) -> serde_json::Value {
        self.read_json_with_timeout(child, RESPONSE_TIMEOUT)
            .unwrap_or_else(|error| panic!("MCP child response failed: {error}"))
    }

    fn read_json_with_timeout(
        &self,
        child: &mut Child,
        timeout: Duration,
    ) -> Result<serde_json::Value, String> {
        let result = match self.lines.recv_timeout(timeout) {
            Ok(Ok(line)) => serde_json::from_str(&line)
                .map_err(|error| format!("invalid JSON-RPC line: {error}")),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("no answer within {timeout:?}: {error}")),
        };
        if result.is_err() {
            terminate(child);
        }
        result
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn silent_child_response_wait_is_bounded_and_reaped() {
        let mut child = Command::new("sh")
            .args(["-c", "sleep 30"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let reader = BoundedLineReader::new(child.stdout.take().unwrap());
        let started = std::time::Instant::now();
        let error = reader
            .read_json_with_timeout(&mut child, Duration::from_millis(20))
            .unwrap_err();
        assert!(error.contains("no answer"));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(
            child.try_wait().unwrap().is_some(),
            "silent child was reaped"
        );
    }
}

fn terminate(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}
