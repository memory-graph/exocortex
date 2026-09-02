//! D17: the entity-resolution harness binary. Two modes:
//!
//! - `evaluate` — estimate the Fellegi-Sunter model from a labelled
//!   set, evaluate it (precision/recall/F1 + confusion table), and
//!   print both; the operator iterates the labelled set or thresholds
//!   against MEASURED numbers.
//! - `classify` — score every blocked candidate pair of the corpus
//!   under the model and emit scored proposals as JSONL (match /
//!   review / non_match). Output is PROPOSALS for an operator; the
//!   harness never writes a graph.

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "exocortex-er", version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Estimate the model from the labelled set and report its measured quality.
    Evaluate {
        /// Corpus JSONL ({id, name, attributes}).
        #[arg(long)]
        corpus: std::path::PathBuf,
        /// Labelled pairs JSONL ({a, b, label}).
        #[arg(long)]
        labelled: std::path::PathBuf,
    },
    /// Score every blocked candidate pair under the model; emit JSONL proposals.
    Classify {
        /// Corpus JSONL ({id, name, attributes}).
        #[arg(long)]
        corpus: std::path::PathBuf,
        /// Labelled pairs JSONL the model is estimated from.
        #[arg(long)]
        labelled: std::path::PathBuf,
        /// Output JSONL path (default stdout).
        #[arg(long)]
        out: Option<std::path::PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let corpus = exocortex_entity_resolution::load_corpus(match &args.command {
        Command::Evaluate { corpus, .. } | Command::Classify { corpus, .. } => corpus,
    })?;
    let labelled_path = match &args.command {
        Command::Evaluate { labelled, .. } | Command::Classify { labelled, .. } => labelled,
    };
    let labelled = exocortex_entity_resolution::load_labelled(labelled_path, &corpus)?;
    let model = exocortex_entity_resolution::estimate(&corpus, &labelled)?;

    match &args.command {
        Command::Evaluate { .. } => {
            let evaluation = exocortex_entity_resolution::evaluate(&corpus, &labelled, &model)?;
            println!("{}", serde_json::to_string_pretty(&evaluation)?);
            println!("model: {}", serde_json::to_string_pretty(&model)?);
            Ok(())
        }
        Command::Classify { out, .. } => {
            let scored = exocortex_entity_resolution::score_candidates(&corpus, &model);
            let mut lines = String::new();
            for pair in &scored {
                lines.push_str(&serde_json::to_string(pair)?);
                lines.push('\n');
            }
            match out {
                Some(path) => std::fs::write(path, lines)?,
                None => print!("{lines}"),
            }
            Ok(())
        }
    }
}
