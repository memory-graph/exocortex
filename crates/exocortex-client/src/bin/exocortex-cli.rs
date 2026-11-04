//! CLI1 (master plan): the human command line beside the MCP surface.
//!
//! A third face over the ONE operation registry (CR-9). Reads ride the
//! node's HTTP operations with bearer auth — byte-identical to the MCP
//! answers by construction, because both dispatch the same registered
//! handler. `add` reuses [`EndSessionTool`] verbatim: the same signed
//! submit path every agent harness uses, not a re-implementation.
//!
//! No MCP handshake, no long-lived server: one process, one command,
//! exit. Credentials follow the house pattern — env, never argv:
//! `EXOCORTEX_AUTH_TOKEN` (bearer) and, for `add`, `EXOCORTEX_HMAC_KEY`
//! (64 hex chars). When `EXOCORTEX_BACKEND` is unset (a dead MCP
//! session leaves no runtime env to source), the CLI falls back to the
//! standalone attach record in the data home (D67) — the same
//! re-resolution source the long-lived client consults (D55). `--json`
//! prints the raw operation output; the default is plain text a human
//! reads in a terminal.

use clap::{Parser, Subcommand};
use exocortex_api_client::ApiClient;
use exocortex_client::tools::end_session::{EndSessionArgs, EndSessionTool, MemoryDraftInput};
use exocortex_kernel::Visibility;
use exocortex_ops::VisibilityContext;

/// Query and write an exocortex graph from the command line.
#[derive(Debug, Parser)]
#[command(name = "exocortex-cli", version)]
struct Args {
    /// Backend node base URL (http://host:port). Env: EXOCORTEX_BACKEND.
    #[arg(long, global = true)]
    backend: Option<String>,
    /// Owning org. Env: EXOCORTEX_ORG.
    #[arg(long, global = true)]
    org: Option<String>,
    /// Data home whose attach.json supplies credentials when the
    /// backend env is unset (default: EXOCORTEX_STANDALONE_DATA_HOME,
    /// then the platform data home).
    #[arg(long, global = true, value_name = "DIR")]
    data_dir: Option<std::path::PathBuf>,
    /// Print raw operation JSON instead of plain text.
    #[arg(long, global = true)]
    json: bool,
    /// Print the instruction block for non-MCP client harnesses
    /// (Perplexity-class: they run shell commands, not MCP) and exit.
    /// Paste the output into the client's custom instructions.
    #[arg(long, global = true)]
    dump_block: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Free-text search over memories.
    Search {
        /// The query.
        query: String,
        /// Result cap.
        #[arg(long, default_value = "20")]
        limit: u32,
    },
    /// One memory by 32-hex id.
    Get {
        /// 32-hex memory id.
        id: String,
    },
    /// k-hop neighborhood around a memory.
    Related {
        /// 32-hex anchor memory id.
        anchor: String,
        /// Hop bound (server caps at 4).
        #[arg(long, default_value = "2")]
        k: u8,
    },
    /// Derived-evidence chain behind a memory.
    Chain {
        /// 32-hex memory id.
        memory: String,
        /// Depth bound (server caps at 4).
        #[arg(long, default_value = "4")]
        depth: u8,
    },
    /// Write one memory through the signed submit path.
    Add {
        /// Memory type (a registered label, e.g. Task, Insight, Topic).
        /// Optional only with --draft/--batch, which carry their own.
        #[arg(required_unless_present_any = ["draft_file", "batch_file"])]
        memory_type: Option<String>,
        /// Title (1..=200 chars). Optional only with --draft/--batch,
        /// which carry their own.
        #[arg(required_unless_present_any = ["draft_file", "batch_file"])]
        title: Option<String>,
        /// Content ('-' reads stdin).
        #[arg(long, default_value = "-")]
        content: String,
        /// Comma-separated lowercase tags.
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,
        /// private|project|team|org.
        #[arg(long, default_value = "project")]
        visibility: String,
        /// Edge to an EXISTING memory: Kind:<32-hex-id> (repeatable).
        /// The new memory is the FROM side; the kind must be a
        /// registered label the triple table accepts.
        #[arg(long = "link", value_name = "KIND:TO_ID")]
        links: Vec<String>,
        /// Read the draft from a JSON file (PreflightMemoryDraft shape)
        /// instead of the positional title/content arguments.
        #[arg(long = "draft", value_name = "PATH", conflicts_with = "batch_file")]
        draft_file: Option<String>,
        /// Read a full wrapup batch from a JSON file (the `end_session`
        /// shape: session_id?, project_id?, team_id?, 1-5 memories,
        /// edges) — one call, the fallback a dead-MCP agent needs.
        #[arg(
            long = "batch",
            value_name = "PATH",
            conflicts_with_all = ["draft_file", "links", "content", "tags", "visibility"]
        )]
        batch_file: Option<String>,
    },
}

/// The `--batch` file shape (D67-2): a full `end_session` submit — 1-5
/// memory drafts plus intra-batch/cross-batch edges — so a fallback
/// wrapup is ONE CLI call, not one call per memory.
#[derive(Debug, serde::Deserialize)]
struct BatchWrap {
    #[serde(default)]
    session_id: Option<String>,
    /// Defaults to EXOCORTEX_PROJECT, then "cli" (the single-draft
    /// path's rule).
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    memories: Vec<MemoryDraftInput>,
    #[serde(default)]
    edges: Vec<exocortex_client::tools::end_session::EdgeHintInput>,
}

async fn op(
    client: &ApiClient,
    token: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let (_status, _rate, json) = client.post_json(token, &body).await?;
    Ok(json)
}

fn render_memory(m: &serde_json::Value, type_names: &[String]) -> String {
    let id = m.get("id").and_then(|v| v.as_str()).unwrap_or("?");
    // The ops payload carries the memory-type ID; the label comes from
    // the same composed ontology every other face renders from.
    let kind = m
        .get("memory_type")
        .and_then(|v| v.as_u64())
        .and_then(|id| type_names.get(id as usize))
        .map(|name| name.as_str())
        .unwrap_or("?");
    let title = m.get("title").and_then(|v| v.as_str()).unwrap_or("");
    format!("{id}  [{kind}]  {title}")
}

fn render_search(out: &serde_json::Value, type_names: &[String]) {
    let Some(memories) = out.get("memories").and_then(|v| v.as_array()) else {
        return;
    };
    let scores = out.get("scores").and_then(|v| v.as_array());
    if memories.is_empty() {
        println!("no hits");
        return;
    }
    for (index, memory) in memories.iter().enumerate() {
        let score = scores
            .and_then(|s| s.get(index))
            .and_then(|s| s.as_f64())
            .map(|s| format!("  ({s:.3})"))
            .unwrap_or_default();
        println!("{}{score}", render_memory(memory, type_names));
    }
}

fn render_one(out: &serde_json::Value, type_names: &[String]) {
    match out.get("memory") {
        Some(memory) if !memory.is_null() => {
            println!("{}", render_memory(memory, type_names));
            if let Some(content) = memory.get("content").and_then(|c| c.as_str()) {
                println!();
                println!("{content}");
            }
        }
        _ => println!("not found (or not visible at this scope)"),
    }
}

fn render_related(out: &serde_json::Value, type_names: &[String]) {
    // find_related output shape: a flat `memories` array (the registry
    // pins FindRelatedOutput; CR-9 parity means the HTTP answer is the
    // registry's own serialization).
    if let Some(rows) = out.get("memories").and_then(|v| v.as_array()) {
        if rows.is_empty() {
            println!("nothing related");
        }
        for row in rows {
            println!("{}", render_memory(row, type_names));
        }
        return;
    }
    println!("(unrecognized shape; use --json)");
}

fn render_chain(out: &serde_json::Value) {
    // get_chain output shape: hex memory ids, origin first (the
    // registry pins GetChainOutput { chain: Vec<String> }).
    if let Some(steps) = out.get("chain").and_then(|v| v.as_array()) {
        for (index, step) in steps.iter().enumerate() {
            let id = step.as_str().unwrap_or("(unparseable id)");
            println!("{:>2}. {}", index + 1, id);
        }
        return;
    }
    println!("(unrecognized shape; use --json)");
}

/// The pasteable block for non-MCP client harnesses (CLI2): the
/// `exocortex-mcp-client --dump-block` equivalent, adapted to CLI
/// verbs. Hand-authored with a bound + content test; folding it into
/// the gen-playbook generator is the recorded follow-up if it drifts.
const INSTRUCTION_BLOCK: &str = r#"# Exocortex memory (CLI bridge - for harnesses without MCP)

You have a shared memory graph, reached by RUNNING SHELL COMMANDS.
`exocortex-cli` is on PATH. Credentials come from the environment
(EXOCORTEX_BACKEND/ORG/AUTH_TOKEN, and EXOCORTEX_HMAC_KEY for writes);
when those are unset, the CLI auto-finds the live standalone node's
credentials from its attach record — no setup needed.

Read at the start of a task, and whenever stuck:
  exocortex-cli search "<terms>"          ranked hits: id [Type] title (score)
  exocortex-cli get <id>                  one memory, full content
  exocortex-cli related <id>              the k-hop neighborhood
  exocortex-cli chain <id>                the derivation chain
Append --json to any command for machine-parseable output.

Write at the end of a turn when ANY of these fire:
  - you made an edit the user accepted
  - you ran a non-obvious command and it worked
  - you answered a why/how question about the codebase
  - you decided against an alternative for a stated reason
  - the user said "remember this"
  - you found a problem, solved or not
Then run:
  exocortex-cli add <Type> "<specific title>" --content "<what and why>"       --tags tag1,tag2 --visibility project
  exocortex-cli add --batch wrapup.json   (the end_session shape: several
   memories plus edges, one call)

Usual types: Fix, Solution, Problem, Error, CodePattern, Command, Technology;
for learning: Topic, Insight, Question, Resource, StudySession, LearningGoal.
Titles are subject-verb-object and specific. One memory per distinct fact.
Never invent confidence scores. Default to project visibility; escalate to
org only for cross-project knowledge.
"#;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The PX1/D27 lesson: a pack crate nothing references is
    // dead-stripped and its inventory registration never runs — the
    // CLI would validate against dev-v1 alone and reject every study
    // or mortgage row locally. Force-link every shipped pack.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let _ = std::hint::black_box(exocortex_pack_mortgage_v1::pack_def().name.clone());
    let _ = std::hint::black_box(exocortex_pack_study_v1::pack_def().name.clone());
    let args = Args::parse();
    if args.dump_block {
        print!("{INSTRUCTION_BLOCK}");
        return Ok(());
    }
    let command = match args.command {
        Some(command) => command,
        None => {
            use clap::CommandFactory as _;
            Args::command().print_help()?;
            return Ok(());
        }
    };
    // Credential resolution (D67): --backend flag > EXOCORTEX_BACKEND >
    // the standalone attach record. The record is the LIVE owner's
    // endpoint and secrets, valid exactly while a node owns the data
    // home (it is removed on owner exit) — the same D55 re-resolution
    // source the long-lived client consults. A dead MCP session has no
    // runtime env; this is what makes the CLI fallback reachable.
    let mut attach: Option<exocortex_client::standalone_creds::Discovered> = None;
    let backend = match args.backend.clone().filter(|v| !v.is_empty()).or_else(|| {
        std::env::var("EXOCORTEX_BACKEND")
            .ok()
            .filter(|v| !v.is_empty())
    }) {
        Some(backend) => backend,
        None => {
            let explicit_org = args.org.clone().filter(|v| !v.is_empty()).or_else(|| {
                std::env::var("EXOCORTEX_ORG")
                    .ok()
                    .filter(|v| !v.is_empty())
            });
            let home =
                exocortex_client::standalone_creds::standalone_data_home(args.data_dir.clone());
            match exocortex_client::standalone_creds::discover(&home, explicit_org.as_deref()) {
                Ok(found) => {
                    eprintln!(
                        "exocortex-cli: credentials from {} (standalone node at {})",
                        found.data_home.join("attach.json").display(),
                        found.info.backend
                    );
                    let backend = found.info.backend.clone();
                    attach = Some(found);
                    backend
                }
                Err(reason) => {
                    return Err(anyhow::anyhow!(
                        "--backend or EXOCORTEX_BACKEND is required (no usable standalone attach record under {}: {reason})",
                        home.display()
                    ));
                }
            }
        }
    };
    let org = match args
        .org
        .clone()
        .filter(|v| !v.is_empty())
        .or_else(|| {
            std::env::var("EXOCORTEX_ORG")
                .ok()
                .filter(|v| !v.is_empty())
        })
        .or_else(|| attach.as_ref().map(|d| d.info.org.clone()))
    {
        Some(org) => org,
        None => return Err(anyhow::anyhow!("--org or EXOCORTEX_ORG is required")),
    };
    let token = std::env::var("EXOCORTEX_AUTH_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| attach.as_ref().map(|d| d.info.auth_token.clone()))
        .ok_or_else(|| anyhow::anyhow!("EXOCORTEX_AUTH_TOKEN is required"))?;

    let base = backend.trim_end_matches('/');

    let type_names: Vec<String> = exocortex_kernel::pack::load_registered_packs()?
        .memory_type_names
        .iter()
        .map(|n| n.to_string())
        .collect();
    let (path, body): (&str, serde_json::Value) = match &command {
        Command::Search { query, limit } => (
            "/v1/search_memories",
            serde_json::json!({ "query": query, "limit": limit }),
        ),
        Command::Get { id } => ("/v1/get_memory", serde_json::json!({ "id": id })),
        Command::Related { anchor, k } => (
            "/v1/find_related",
            serde_json::json!({ "anchor": anchor, "k": k }),
        ),
        Command::Chain { memory, depth } => (
            "/v1/get_chain",
            serde_json::json!({ "memory": memory, "max_depth": depth }),
        ),
        Command::Add { .. } => ("", serde_json::Value::Null),
    };

    if let Command::Add {
        memory_type,
        title,
        content,
        tags,
        visibility,
        links,
        draft_file,
        batch_file,
    } = &command
    {
        // The signing key: env first, then the discovered attach record
        // (a dead-MCP session leaves no runtime env — D67).
        let hmac_key = exocortex_wire::signing::decode_hex32(
            &std::env::var("EXOCORTEX_HMAC_KEY")
                .ok()
                .filter(|v| !v.is_empty())
                .or_else(|| attach.as_ref().map(|d| d.info.hmac_key.clone()))
                .ok_or_else(|| {
                    anyhow::anyhow!("EXOCORTEX_HMAC_KEY is required for add (64 hex chars)")
                })?,
        )
        .map_err(anyhow::Error::msg)?;

        // '-' expands to stdin per draft, at most once per submit.
        fn expand_stdin(content: String) -> anyhow::Result<String> {
            if content != "-" {
                return Ok(content);
            }
            use std::io::Read as _;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            Ok(buf)
        }

        let submit: EndSessionArgs = if let Some(path) = batch_file {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("--batch {}: {e}", path))?;
            let batch: BatchWrap = serde_json::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("batch file must carry the end_session shape: {e}"))?;
            anyhow::ensure!(!batch.memories.is_empty(), "batch file carries no memories");
            let mut memories = Vec::with_capacity(batch.memories.len());
            for mut draft in batch.memories {
                draft.content = expand_stdin(draft.content)?;
                memories.push(draft);
            }
            EndSessionArgs {
                session_id: batch.session_id,
                project_id: batch.project_id.unwrap_or_else(|| {
                    std::env::var("EXOCORTEX_PROJECT").unwrap_or_else(|_| "cli".into())
                }),
                team_id: batch.team_id,
                memories,
                edges: batch.edges,
            }
        } else {
            let mut edges: Vec<(String, String)> = Vec::new();
            for link in links {
                let (kind, to_id) = link.split_once(':').ok_or_else(|| {
                    anyhow::anyhow!("--link takes Kind:<32-hex-id>, got {link:?}")
                })?;
                anyhow::ensure!(
                    to_id.len() == 32
                        && to_id.chars().all(|c| c.is_ascii_hexdigit())
                        && kind.chars().all(|c| c.is_alphabetic()),
                    "--link takes Kind:<32-hex-id>, got {link:?}"
                );
                // Resolved to a real EdgeHintInput after the draft loads:
                // a --draft file carries its own draft_key and --link must
                // target it (round-10 R10-5 found the hardcoded "cli-1"
                // dangling under --draft).
                edges.push((kind.to_string(), to_id.to_ascii_lowercase()));
            }
            let draft = if let Some(path) = draft_file {
                let raw = std::fs::read_to_string(path)?;
                let value: serde_json::Value = serde_json::from_str(&raw)?;
                let draft: MemoryDraftInput = serde_json::from_value(value)
                    .map_err(|e| anyhow::anyhow!("draft file must carry the draft shape: {e}"))?;
                anyhow::ensure!(
                    Some(&draft.memory_type) == memory_type.as_ref(),
                    "draft file type {} disagrees with the positional {} (they must match)",
                    draft.memory_type,
                    memory_type.as_deref().unwrap_or("(absent)")
                );
                draft
            } else {
                MemoryDraftInput {
                    draft_key: "cli-1".into(),
                    memory_type: memory_type
                        .clone()
                        .expect("clap requires the type unless --draft/--batch"),
                    title: title
                        .clone()
                        .expect("clap requires title unless --draft/--batch"),
                    content: content.clone(),
                    visibility: visibility.clone(),
                    tags: tags.clone(),
                }
            };
            let edges: Vec<exocortex_client::tools::end_session::EdgeHintInput> = edges
                .into_iter()
                .map(
                    |(kind, to_id)| exocortex_client::tools::end_session::EdgeHintInput {
                        from_draft_key: draft.draft_key.clone(),
                        to_draft_key: String::new(),
                        to_memory_id: to_id,
                        kind,
                        strength: 0.0,
                    },
                )
                .collect();
            let content = expand_stdin(draft.content.clone())?;
            // One deterministic session for all single CLI writes: the
            // backend groups them (and admin source policy can pin
            // session://cli with its own ceiling and signing key).
            // The requested project/team scopes default to "cli",
            // overridable by env so an authenticated backend's
            // project/team membership checks can pass for the
            // operator's REAL scopes. A --batch file may carry its own
            // session/project/team per wrapup.
            EndSessionArgs {
                session_id: Some("cli".into()),
                project_id: std::env::var("EXOCORTEX_PROJECT").unwrap_or_else(|_| "cli".into()),
                team_id: std::env::var("EXOCORTEX_TEAM")
                    .ok()
                    .filter(|t| !t.is_empty()),
                memories: vec![MemoryDraftInput { content, ..draft }],
                edges,
            }
        };

        let channel = tonic::transport::Endpoint::new(format!("{base}/"))?
            .connect()
            .await?;
        let mut ingest =
            exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::new(channel);
        use exocortex_wire::ingest::v1::FingerprintRequest;
        let mut req = tonic::Request::new(FingerprintRequest {});
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let fingerprint: [u8; 32] = ingest
            .fingerprint(req)
            .await?
            .into_inner()
            .fingerprint
            .try_into()
            .map_err(|v: Vec<u8>| {
                anyhow::anyhow!("server fingerprint is {} bytes, expected 32", v.len())
            })?;

        let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);
        let tool = EndSessionTool {
            client: ingest,
            org_id: org.clone(),
            fingerprint,
            hmac_key,
            node_id: format!("exocortex-cli-{}", std::process::id()),
            agent_id: whoami(),
            auth_token: Some(token.clone()),
            ontology,
            cache: None,
            vc: VisibilityContext {
                user_id: whoami().into(),
                org_id: org.into(),
                project_ids: Default::default(),
                team_ids: Default::default(),
                max_visibility: Visibility::Org,
            },
            live: None,
            timeouts: Default::default(),
        };
        let ack = tool
            .handle(submit)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&ack)?);
        } else {
            println!(
                "accepted {} rejected {} (lsn {})",
                ack.accepted, ack.rejected, ack.assigned_lsn
            );
            for rejection in &ack.rejections {
                eprintln!(
                    "REJECTED [{}] {}: {}",
                    rejection.code, rejection.draft_key, rejection.detail
                );
            }
        }
        // A write that persisted nothing must not exit 0 — shell
        // automation cannot detect silent loss otherwise.
        if ack.rejected > 0 || ack.accepted == 0 {
            eprintln!("write rejected; nothing persisted");
            std::process::exit(3);
        }
        return Ok(());
    }

    // Reads: dispatch through the node's HTTP ops (the registry's own
    // HTTP face — CR-9 parity with MCP by construction).
    let url = format!("{base}{path}");
    let client =
        ApiClient::new(&url)?.with_user_agent(concat!("exocortex-cli/", env!("CARGO_PKG_VERSION")));
    let out = op(&client, &token, body).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }
    match &command {
        Command::Search { .. } => render_search(&out, &type_names),
        Command::Get { .. } => render_one(&out, &type_names),
        Command::Related { .. } => render_related(&out, &type_names),
        Command::Chain { .. } => render_chain(&out),
        Command::Add { .. } => unreachable!("add handled above"),
    }
    Ok(())
}

fn whoami() -> String {
    std::env::var("USER")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "cli-user".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_search_rows_with_scores() {
        let out = serde_json::json!({
            "memories": [
                { "id": "a".repeat(32), "memory_type": "Insight", "title": "lifetimes are regions" },
                { "id": "b".repeat(32), "memory_type": "Topic", "title": "tokio runtime" }
            ],
            "scores": [0.91, 0.42]
        });
        // Renders without panicking; the shape is the contract.
        render_search(&out, &[]);
    }

    #[test]
    fn renders_empty_search_honestly() {
        render_search(&serde_json::json!({ "memories": [], "scores": [] }), &[]);
    }

    #[test]
    fn missing_memory_renders_not_found() {
        render_one(&serde_json::json!({ "memory": null }), &[]);
    }

    #[test]
    fn instruction_block_is_bounded_and_names_the_surface() {
        let words = INSTRUCTION_BLOCK.split_whitespace().count();
        assert!(
            words <= 300,
            "the CLI block must stay under 300 words (like the MCP block); it is {words}"
        );
        for command in ["search", "get", "related", "chain", "add"] {
            assert!(INSTRUCTION_BLOCK.contains(command), "names {command}");
        }
        for trigger in ["edit the user accepted", "remember this", "why/how"] {
            assert!(
                INSTRUCTION_BLOCK.contains(trigger),
                "carries the {trigger} trigger"
            );
        }
        assert!(INSTRUCTION_BLOCK.contains("--json"));
    }

    #[test]
    fn instruction_block_names_the_standalone_credential_fallback() {
        // D67-4a: the block must not promise env an unwired shell never
        // had — the CLI auto-finds the live node when env is unset.
        assert!(
            INSTRUCTION_BLOCK.contains("auto-finds"),
            "the CLI block teaches the attach-record fallback: {INSTRUCTION_BLOCK}"
        );
    }
}
