use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::path::{PathBuf};

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Ignore {
        #[clap(short, long, value_delimiter = ' ', required = true, num_args = 1..)]
        tables: Option<Vec<String>>,
    },
    Filter
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
    #[clap(value_name = "FILE", required=true)]
    input: Option<PathBuf>,
}

pub fn parse_options() -> (PathBuf, HashSet<String>) {
    let mut all_tables: HashSet<String> = HashSet::new();
    let cli = Cli::parse();
    match cli.cmd {
        Commands::Ignore { tables, .. } => {
            for table in tables.unwrap() {
                all_tables.insert(table.to_string());
            }
        }
        Commands::Filter => {

        }
    }
    return (cli.input.unwrap(), all_tables);
}
