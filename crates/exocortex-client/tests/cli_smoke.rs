//! CLI1 smoke: the real `exocortex-cli` binary against an in-process
//! backend node — `add` through the signed submit path, then `search`
//! and `get` through the node's HTTP operations. One process per
//! command, exactly as a human runs it.

use std::sync::Arc;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;

const CLUSTER_KEY: [u8; 32] = [21u8; 32];
const PRODUCER_KEY: [u8; 32] = [22u8; 32];
const TOKEN: &str = "3031323334353637383961626364656630313233343536373839616263646566";

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

async fn boot_node() -> (
    exocortex_server::backend::BackendNode<InMemoryStorage>,
    std::net::SocketAddr,
) {
    let ontology = Arc::new(
        Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
            exocortex_pack_study_v1::pack_def(),
        ])
        .expect("composed pack set assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let node = exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: "org".into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: "cli-smoke-node".into(),
            cluster_secret: CLUSTER_KEY,
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(
                    TOKEN.into(),
                    exocortex_ops::operations::ops_vc("org", "cliu", Visibility::Org),
                )
                .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: Default::default(),
            admin_source_policies: vec![(
                (
                    "org".into(),
                    "session://cli".into(),
                    "session-wrapup".into(),
                ),
                exocortex_ingest::service::AdminSourcePolicy {
                    ceiling: Visibility::Org,
                    kind: exocortex_kernel::ProducerKind::CodingAgent,
                    signing_key: PRODUCER_KEY,
                },
            )],
        },
    )
    .await
    .expect("boot in-process backend node");
    let addr = node.local_addr;
    (node, addr)
}

fn run_cli(addr: std::net::SocketAddr, args: &[&str]) -> (bool, String, String) {
    let bin = env!("CARGO_BIN_EXE_exocortex-cli");
    let output = std::process::Command::new(bin)
        .args(args)
        .env("EXOCORTEX_BACKEND", format!("http://{addr}"))
        .env("EXOCORTEX_ORG", "org")
        .env("EXOCORTEX_AUTH_TOKEN", TOKEN)
        .env("EXOCORTEX_HMAC_KEY", hex(&PRODUCER_KEY))
        .output()
        .expect("spawn exocortex-cli");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

// multi_thread: the in-process node serves on this runtime while the
// test thread blocks on the child process; the default current-thread
// flavor would freeze the node and deadlock the CLI's submit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_search_get_round_trip_through_the_real_binary() {
    let (_node, addr) = boot_node().await;

    // add: the signed submit path (session://cli, admin-pinned key).
    let (ok, out, err) = run_cli(
        addr,
        &[
            "add",
            "Insight",
            "CLI smoke insight about rust closures",
            "--content",
            "Closures capture by reference by default.",
            "--visibility",
            "org",
            "--tags",
            "cli,smoke",
        ],
    );
    assert!(ok, "add exited clean: {err}");
    assert!(out.contains("accepted 1"), "add ack: {out} / {err}");

    // search: the HTTP op face, human rendering.
    let (ok, out, err) = run_cli(addr, &["search", "closures", "--limit", "5"]);
    assert!(ok, "search exited clean: {err}");
    assert!(
        out.contains("CLI smoke insight about rust closures"),
        "search finds the written row: {out}"
    );
    assert!(out.contains("[Insight]"), "search renders the type: {out}");

    // get: parse the id out of the JSON search, then fetch it raw.
    let (ok, out, err) = run_cli(addr, &["search", "closures", "--json"]);
    assert!(ok, "{err}");
    let json: serde_json::Value = serde_json::from_str(&out).expect("search --json parses");
    let id = json["memories"][0]["id"]
        .as_str()
        .expect("hit carries an id")
        .to_string();
    let (ok, out, err) = run_cli(addr, &["get", &id, "--json"]);
    assert!(ok, "{err}");
    let json: serde_json::Value = serde_json::from_str(&out).expect("get --json parses");
    assert_eq!(
        json["memory"]["title"]
            .as_str()
            .expect("memory title present"),
        "CLI smoke insight about rust closures"
    );

    // Edge round trip: a second row linked to the first via --link
    // (the section 4.5 cross-batch shape), then the neighborhood
    // carries it.
    let (ok, out, err) = run_cli(
        addr,
        &[
            "add",
            "Topic",
            "Rust closures topic row",
            "--content",
            "The topic the smoke insight is about.",
            "--visibility",
            "org",
            "--link",
            &format!("RelatedTo:{id}"),
        ],
    );
    assert!(ok, "linked add exited clean: {err}");
    // One memory row + one edge row.
    assert!(out.contains("accepted 2"), "linked add ack: {out} / {err}");
    let (ok, out, err) = run_cli(addr, &["related", &id]);
    assert!(ok, "{err}");
    assert!(
        out.contains("Rust closures topic row"),
        "the neighborhood carries the linked row: {out}"
    );

    // --draft: the file carries its own draft_key, type must agree,
    // and --link targets THAT key (round-11 R11 coverage).
    let draft = serde_json::json!({
        "draft_key": "file-key-7",
        "memory_type": "Insight",
        "title": "Draft-file insight rows link by their own key",
        "content": "Written via --draft, linked via --link.",
        "visibility": "org",
        "tags": ["cli", "draft"]
    });
    // Unique per run: two concurrent `cargo test` invocations (or a
    // stale file from a crashed earlier run) must not race on one
    // shared temp path.
    let draft_dir = std::env::temp_dir().join(format!(
        "exocortex-cli-smoke-draft-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&draft_dir).unwrap();
    let draft_path = draft_dir.join("draft.json");
    std::fs::write(&draft_path, serde_json::to_string(&draft).unwrap()).unwrap();
    let (ok, out, err) = run_cli(
        addr,
        &[
            "add",
            "Insight",
            "placeholder-title-ignored",
            "--draft",
            draft_path.to_str().unwrap(),
            "--link",
            &format!("RelatedTo:{id}"),
        ],
    );
    assert!(ok, "draft add exited clean: {err}");
    assert!(
        out.contains("accepted 2"),
        "draft add ack (memory + edge): {out} / {err}"
    );
    let (ok, out, err) = run_cli(addr, &["related", &id]);
    assert!(ok, "{err}");
    assert!(
        out.contains("link by their own key"),
        "the draft row's edge landed: {out}"
    );

    // The draft contract's other two clauses: no title needed under
    // --draft, and a disagreeing type is rejected with the named cause.
    let (ok, out, err) = run_cli(
        addr,
        &["add", "Insight", "--draft", draft_path.to_str().unwrap()],
    );
    assert!(ok, "title is optional under --draft: {err}");
    assert!(out.contains("accepted"), "{out} / {err}");
    let (ok, out, err) = run_cli(
        addr,
        &["add", "Topic", "--draft", draft_path.to_str().unwrap()],
    );
    assert!(!ok, "a disagreeing type must fail the run");
    assert!(
        out.contains("disagrees") || err.contains("disagrees"),
        "the rejection names the disagreement: {out} / {err}"
    );
}

// D67-1: a dead MCP session leaves NO runtime env — the CLI must find
// the live node's credentials itself, from the attach record in the
// data home (the D55 re-resolution source), and refuse a foreign org.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_discovers_credentials_from_the_attach_record_without_env() {
    let (_node, addr) = boot_node().await;

    // The attach record a standalone owner publishes: loopback backend,
    // the org it serves, and the owner's secrets.
    let home = std::env::temp_dir().join(format!(
        "exocortex-cli-smoke-attach-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let record = serde_json::json!({
        "backend": format!("http://{addr}"),
        "org": "org",
        "auth_token": TOKEN,
        "hmac_key": hex(&PRODUCER_KEY),
        "sse_key": "ef".repeat(32),
    });
    std::fs::write(home.join("attach.json"), record.to_string()).unwrap();

    // Every credential env removed: discovery is the ONLY path.
    let run = |args: &[&str]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-cli"))
            .args(args)
            .env_remove("EXOCORTEX_BACKEND")
            .env_remove("EXOCORTEX_ORG")
            .env_remove("EXOCORTEX_AUTH_TOKEN")
            .env_remove("EXOCORTEX_HMAC_KEY")
            .env("EXOCORTEX_STANDALONE_DATA_HOME", &home)
            .output()
            .expect("spawn exocortex-cli");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };

    // Write through discovery (the submit needs the record's hmac too).
    let (ok, out, err) = run(&[
        "add",
        "Insight",
        "Attach-discovered insight written with no env",
        "--content",
        "Credentials came from attach.json, not the environment.",
        "--visibility",
        "org",
    ]);
    assert!(ok, "discovered add exited clean: {err}");
    assert!(
        out.contains("accepted 1"),
        "discovered add ack: {out} / {err}"
    );
    assert!(
        err.contains("credentials from"),
        "the discovery is ANNOUNCED on stderr: {err}"
    );

    // Read through discovery.
    let (ok, out, err) = run(&["search", "attach-discovered", "--limit", "5"]);
    assert!(ok, "discovered search exited clean: {err}");
    assert!(
        out.contains("Attach-discovered insight written with no env"),
        "search through discovery finds the row: {out}"
    );

    // The R14-final rule: an explicit foreign org is refused, never
    // adopted — the record serves "org", "loanlight" must not ride it.
    let (ok, _out, err) = run(&["--org", "loanlight", "search", "anything"]);
    assert!(!ok, "a foreign org must fail the run");
    assert!(
        err.contains("refusing foreign-org credentials"),
        "the refusal names the rule: {err}"
    );

    // No record at all: fail closed with the env requirement, never a
    // guess.
    let empty = home.join("elsewhere");
    std::fs::create_dir_all(&empty).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-cli"))
        .args(["search", "anything"])
        .env_remove("EXOCORTEX_BACKEND")
        .env_remove("EXOCORTEX_ORG")
        .env_remove("EXOCORTEX_AUTH_TOKEN")
        .env_remove("EXOCORTEX_HMAC_KEY")
        .env("EXOCORTEX_STANDALONE_DATA_HOME", &empty)
        .output()
        .expect("spawn exocortex-cli");
    assert!(!output.status.success(), "no record + no env must fail");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("--backend or EXOCORTEX_BACKEND is required"),
        "names the missing prerequisite: {err}"
    );

    std::fs::remove_dir_all(&home).ok();
}

// D67-2: `add --batch` lands a full wrapup — several memories plus
// intra-batch and cross-batch edges — in ONE call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_add_lands_memories_and_edges_in_one_call() {
    let (_node, addr) = boot_node().await;

    // Seed one row as the cross-batch edge target.
    let (ok, _out, err) = run_cli(
        addr,
        &[
            "add",
            "Problem",
            "Batch smoke target problem row",
            "--content",
            "The cross-batch edge lands on this row.",
            "--visibility",
            "org",
        ],
    );
    assert!(ok, "seed add: {err}");
    let (ok, out, err) = run_cli(addr, &["search", "target problem", "--json"]);
    assert!(ok, "{err}");
    let json: serde_json::Value = serde_json::from_str(&out).expect("search --json parses");
    let target = json["memories"][0]["id"].as_str().expect("id").to_string();

    let wrapup = serde_json::json!({
        "session_id": "cli",
        "project_id": "cli",
        "memories": [
            {
                "draft_key": "prob",
                "memory_type": "Problem",
                "title": "Batch smoke second problem",
                "content": "Solved by the batch fix row.",
                "visibility": "org",
                "tags": ["cli", "batch"]
            },
            {
                "draft_key": "fix",
                "memory_type": "Fix",
                "title": "Batch smoke fix solves both problems",
                "content": "One call, two memories, two edges.",
                "visibility": "org"
            }
        ],
        "edges": [
            { "from_draft_key": "fix", "to_draft_key": "prob", "kind": "Solves" },
            { "from_draft_key": "fix", "to_memory_id": target, "kind": "RelatedTo" }
        ]
    });
    let dir = std::env::temp_dir().join(format!(
        "exocortex-cli-smoke-batch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wrapup.json");
    std::fs::write(&path, wrapup.to_string()).unwrap();

    let (ok, out, err) = run_cli(addr, &["add", "--batch", path.to_str().unwrap()]);
    assert!(ok, "batch add exited clean: {err}");
    assert!(
        out.contains("accepted 4"),
        "two memories + two edges in one ack: {out} / {err}"
    );

    // Both rows land; the neighborhood of the seeded target carries the
    // fix row through the cross-batch edge.
    let (ok, out, err) = run_cli(addr, &["related", &target]);
    assert!(ok, "{err}");
    assert!(
        out.contains("Batch smoke fix solves both problems"),
        "cross-batch edge surfaced in the neighborhood: {out}"
    );
    let (ok, out, err) = run_cli(addr, &["search", "second problem", "--json"]);
    assert!(ok, "{err}");
    assert!(out.contains("Batch smoke second problem"), "{out}");

    // An empty batch is refused up front, not submitted as nothing.
    std::fs::write(&path, r#"{ "memories": [] }"#).unwrap();
    let (ok, out, err) = run_cli(addr, &["add", "--batch", path.to_str().unwrap()]);
    assert!(!ok, "an empty batch must fail the run");
    assert!(
        out.contains("no memories") || err.contains("no memories"),
        "the refusal names the empty batch: {out} / {err}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

// D67-4b: the `--verify` CLI row's machinery actually exercises the
// fallback binary read-only against a live node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_cli_probe_answers_against_a_live_node() {
    let (_node, addr) = boot_node().await;
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_exocortex-cli"));
    let install_dir = bin.parent().expect("bin sits in a dir").to_path_buf();
    assert!(
        exocortex_client::verify_checks::cli_sibling(&install_dir).is_some(),
        "the CLI is located beside the client in the install"
    );
    // The probe child inherits THIS process's env: set the backend
    // credentials around the call (contained — every other test in
    // this binary drives the CLI child with explicit per-child env).
    std::env::set_var("EXOCORTEX_BACKEND", format!("http://{addr}"));
    std::env::set_var("EXOCORTEX_ORG", "org");
    std::env::set_var("EXOCORTEX_AUTH_TOKEN", TOKEN);
    let probe = exocortex_client::verify_checks::run_cli_search_probe(
        &bin,
        std::time::Duration::from_secs(20),
    );
    std::env::remove_var("EXOCORTEX_BACKEND");
    std::env::remove_var("EXOCORTEX_ORG");
    std::env::remove_var("EXOCORTEX_AUTH_TOKEN");
    let output = probe.expect("the probe runs");
    assert!(
        output.status.success(),
        "read-only search answers over the live node: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
