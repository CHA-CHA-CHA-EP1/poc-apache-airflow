mod compare;
mod copy_encrypt;
mod count_document;
mod insert_missing;
mod read_control_file;
mod xcom;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "reconcile-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    CopyEncrypt {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        date: String,
    },
    ReadControlFile {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        date: String,
    },
    CountDocument {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        date: String,
    },
    Compare {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        date: String,
        #[arg(long)]
        control_count: i64,
        #[arg(long)]
        document_count: i64,
    },
    InsertMissing {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        date: String,
        #[arg(long)]
        missing_records: i64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::CopyEncrypt { channel, date } => copy_encrypt::run(&channel, &date).await,
        Command::ReadControlFile { channel, date } => read_control_file::run(&channel, &date).await,
        Command::CountDocument { channel, date } => count_document::run(&channel, &date).await,
        Command::Compare {
            channel,
            date,
            control_count,
            document_count,
        } => compare::run(&channel, &date, control_count, document_count).await,
        Command::InsertMissing {
            channel,
            date,
            missing_records,
        } => insert_missing::run(&channel, &date, missing_records).await,
    }
}
