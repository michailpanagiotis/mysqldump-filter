use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::iter;
use tempdir::TempDir;

mod reader;
mod splitter;
mod config;

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Filter {
        #[clap(short, long, required = true, num_args = 1..)]
        query: String,
        #[clap(short, long, required = true, num_args = 1..)]
        output: PathBuf,
    },
    Ids,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Commands>,
    #[clap(value_name = "FILE", required=true)]
    input: PathBuf,
    #[clap(short, long, required = true, num_args = 1..)]
    config: PathBuf,
    #[clap(short, long, required = true, num_args = 1..)]
    output: PathBuf,
}

fn append_to_file(input_path: &PathBuf, mut output_file: &File) {
    let mut input = File::open(input_path).expect("cannot open file");
    io::copy(&mut input, &mut output_file).expect("cannot copy file");
}

fn combine_files<'a, I: Iterator<Item = &'a PathBuf>>(schema_file: &'a PathBuf, data_files: I, output: PathBuf) {
    let all_files = iter::once(schema_file).chain(data_files);
    let output_file = File::create(output).expect("cannot create output file");
    for f in all_files {
        append_to_file(f, &output_file);
    }
}

fn main() {
    let cli = Cli::parse();
    let input_path = cli.input;
    dbg!(&input_path);
    // println!(
    //     "{:?}",
    //     settings
    //         .try_deserialize::<HashMap<String, String>>()
    //         .unwrap()
    // );
    match cli.cmd {
        Some(cmd) => {
            match cmd {
                Commands::Ids => {
                    let table_file = String::from("dim_stripe_events.test.sql");
                    reader::read_ids(&table_file);
                }
                Commands::Filter { query, output, } => {
                    let (_, parsed) = reader::parse_query(&query).expect("cannot parse query");
                    let (field, value) = parsed;
                    splitter::filter_inserts(&input_path, field, value, &output);
                }
            }
        },
        None => {
            let config = config::parse(cli.config.to_str().unwrap());

            dbg!(&config);
            let working_dir = TempDir::new("splitter").expect("cannot create temporary dir");
            let schema_file = working_dir.path().join("schema.sql");
            let (_, data_files) = splitter::split(&input_path, working_dir.path(), &schema_file, config);

            println!("Combining files");
            combine_files(&schema_file, data_files.iter(), cli.output);
            _ = working_dir.close();
        },
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
