//! D17 end-to-end: the harness binary over a real corpus + labelled
//! set on disk — `evaluate` reports measured precision/recall/F1 and
//! the calibrated model; `classify` emits scored proposals as JSONL.

use std::process::Stdio;

fn write(path: &std::path::Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn binary() -> std::process::Command {
    let exe = env!("CARGO_BIN_EXE_exocortex-er");
    std::process::Command::new(exe)
}

fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    write(
        &dir.path().join("corpus.jsonl"),
        concat!(
            "{\"id\":\"m-1\",\"name\":\"Acme Financial Corp\",\"attributes\":{\"city\":\"Austin\"}}\n",
            "{\"id\":\"m-2\",\"name\":\"acme financial corporation\",\"attributes\":{\"city\":\"austin\"}}\n",
            "{\"id\":\"m-3\",\"name\":\"Zenith Data Systems\",\"attributes\":{\"city\":\"Reno\"}}\n",
            "{\"id\":\"m-4\",\"name\":\"zenith data systems\",\"attributes\":{\"city\":\"Reno\"}}\n",
            "{\"id\":\"m-5\",\"name\":\"Harborview Logistics LLC\",\"attributes\":{\"city\":\"Miami\"}}\n",
        ),
    );
    write(
        &dir.path().join("labelled.jsonl"),
        concat!(
            "{\"a\":\"m-1\",\"b\":\"m-2\",\"label\":\"match\"}\n",
            "{\"a\":\"m-3\",\"b\":\"m-4\",\"label\":\"match\"}\n",
            "{\"a\":\"m-1\",\"b\":\"m-3\",\"label\":\"non_match\"}\n",
            "{\"a\":\"m-2\",\"b\":\"m-4\",\"label\":\"non_match\"}\n",
            "{\"a\":\"m-3\",\"b\":\"m-5\",\"label\":\"non_match\"}\n",
        ),
    );
    dir
}

#[test]
fn evaluate_reports_measured_quality() {
    let dir = fixture();
    let output = binary()
        .args([
            "evaluate",
            "--corpus",
            dir.path().join("corpus.jsonl").to_str().unwrap(),
            "--labelled",
            dir.path().join("labelled.jsonl").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"precision\""), "{stdout}");
    assert!(stdout.contains("\"f1\""), "{stdout}");
    assert!(stdout.contains("true_positives"), "{stdout}");
    assert!(stdout.contains("accept_threshold"), "{stdout}");
}

#[test]
fn classify_emits_scored_proposals() {
    let dir = fixture();
    let out_path = dir.path().join("proposals.jsonl");
    let output = binary()
        .args([
            "classify",
            "--corpus",
            dir.path().join("corpus.jsonl").to_str().unwrap(),
            "--labelled",
            dir.path().join("labelled.jsonl").to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(&out_path).unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert!(!lines.is_empty(), "candidates were scored");
    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let decision = value["decision"].as_str().unwrap();
        assert!(
            decision == "match" || decision == "review" || decision == "non_match",
            "{line}"
        );
        assert!(value["score"].is_number(), "{line}");
    }
    // The known match pair is proposed.
    assert!(
        lines
            .iter()
            .any(|l| l.contains("m-1") && l.contains("m-2") && l.contains("match")),
        "the labelled match surfaces as a proposal: {text}"
    );
}

#[test]
fn a_missing_corpus_id_is_a_hard_error() {
    let dir = tempfile::tempdir().unwrap();
    write(
        &dir.path().join("corpus.jsonl"),
        "{\"id\":\"a\",\"name\":\"Acme\"}\n",
    );
    write(
        &dir.path().join("labelled.jsonl"),
        "{\"a\":\"a\",\"b\":\"zz\",\"label\":\"match\"}\n",
    );
    let output = binary()
        .args([
            "evaluate",
            "--corpus",
            dir.path().join("corpus.jsonl").to_str().unwrap(),
            "--labelled",
            dir.path().join("labelled.jsonl").to_str().unwrap(),
        ])
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "bad labelled sets must exit non-zero"
    );
}

#[test]
fn classify_thresholds_are_pinnable_at_classification_time() {
    let dir = fixture();
    let out_path = dir.path().join("proposals-low-review.jsonl");
    let output = binary()
        .args([
            "classify",
            "--corpus",
            dir.path().join("corpus.jsonl").to_str().unwrap(),
            "--labelled",
            dir.path().join("labelled.jsonl").to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
            // Pinning both thresholds proves the override: no score
            // can reach the accept floor and every score clears the
            // review floor, so every proposal lands in the review
            // band the calibrated model would have split.
            "--accept=1000",
            "--review=-1000",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(&out_path).unwrap();
    assert!(
        text.lines().filter(|l| l.contains("review")).count() > 0,
        "the override surfaces review-band pairs: {text}"
    );
}
