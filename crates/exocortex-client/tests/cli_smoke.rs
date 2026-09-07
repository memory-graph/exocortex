//! CLI1 smoke: the real `exocortex-cli` binary against an in-process
//! backend node — `add` through the signed submit path, then `search`
//! and `get` through the node's HTTP operations. One process per
//! command, exactly as a human runs it.

use std::sync::Arc;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;

const CLUSTER_KEY: [u8; 32] = [21u8; 32];
const PRODUCER_KEY: [u8; 32] = [22u8; 32];
const TOKEN: &str = "test-only-cli-smoke-bearer-token-0000";

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
    let draft_path = std::env::temp_dir().join("exocortex-cli-smoke-draft.json");
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
    let (ok, out, err) = run_cli(addr, &["search", "link by their own key", "--json"]);
    assert!(ok, "{err}");
    let json: serde_json::Value = serde_json::from_str(&out).expect("search --json parses");
    let draft_id = json["memories"][0]["id"]
        .as_str()
        .expect("hit id")
        .to_string();
    let (ok, out, err) = run_cli(addr, &["related", &id]);
    assert!(ok, "{err}");
    assert!(
        out.contains("link by their own key"),
        "the draft row's edge landed: {out}"
    );
    let _ = draft_id;
}
