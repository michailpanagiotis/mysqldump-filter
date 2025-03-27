use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::path::PathBuf;
use std::fs::create_dir_all;

mod reader;
mod splitter;

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Split {
        #[clap(short, long, value_delimiter = ' ', required = true, num_args = 1..)]
        keep_table_data: Option<Vec<String>>,
        #[clap(short, long, required = true, num_args = 1..)]
        output_dir: PathBuf,
    },
    Filter {
        #[clap(short, long, required = true, num_args = 1..)]
        query: String,
        #[clap(short, long, required = true, num_args = 1..)]
        output: PathBuf,
    },
    Ids
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
    #[clap(value_name = "FILE", required=true)]
    input: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let input_path = cli.input.unwrap();
    match cli.cmd {
        Commands::Split { keep_table_data, output_dir, .. } => {
            let mut requested_tables: HashSet<String> = HashSet::new();
            for table in keep_table_data.unwrap() {
                requested_tables.insert(table.to_string());
            }
            create_dir_all(&output_dir).ok();
            let schema_file = String::from("schema.sql");
            let _exported_tables = splitter::split(&input_path, &output_dir, &schema_file, &requested_tables);
        }
        Commands::Ids => {
            let table_file = String::from("dim_stripe_events.test.sql");
            reader::read_ids(&table_file);
        }
        Commands::Filter { query, output, } => {
            let (_, parsed) = reader::parse_query(&query).expect("cannot parse query");
            let (field, value) = parsed;
            splitter::filter_inserts(&input_path, &field, &value, &output);
        }
    }

    // let schema_file = String::from("schema.sql");
    // let _exported_tables = splitter::split(&input_path, &schema_file, &requested_tables);
    //
    // let table_file = String::from("sequelize_meta.sql");
    // reader::read_ids(&table_file);
    // let table_file = String::from("dim_stripe_events.test.sql");
    // reader::read_ids(&table_file);

    // let mut tables: Vec<String> = requested_tables.into_iter().collect();
    // tables.sort();
    //
    // for table in tables {
    //     let table_file = format!("{table}.sql");
    //     reader::read_ids(&table_file);
    // }
}
