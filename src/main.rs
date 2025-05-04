use clap::Parser;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempdir::TempDir;

mod expression_parser;
mod filters;
mod io_utils;
mod references;
mod sql;

use io_utils::read_config;
use sql::{get_data_types, read_sql_file, write_sql_file};
use references::References;
use filters::{filter_statements, Filters};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(value_name = "FILE", required=true)]
    input: PathBuf,
    #[clap(short, long, required = true)]
    config: PathBuf,
    #[clap(short, long, required = true)]
    output: PathBuf,
    #[clap(short, long, required = false)]
    working_dir: Option<PathBuf>,
}

fn process_file(input_file: &Path, output_file: &Path, allow_data_on_tables: &HashSet<String>, filters: &mut Filters, references: &mut References) {
    let all_statements = read_sql_file(input_file, allow_data_on_tables);
    let filtered = filter_statements(filters, references, all_statements);
    write_sql_file(output_file, filtered);
}

fn main() {
    let cli = Cli::parse();
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(cli.input);
    let output_file = std::env::current_dir().unwrap().to_path_buf().join(cli.output);
    let config_file = std::env::current_dir().unwrap().to_path_buf().join(cli.config);
    let temp_dir = if cli.working_dir.is_none() { Some(TempDir::new("sql_parser").expect("cannot create temporary dir")) } else { None };

    let working_dir_path = match temp_dir {
        Some(ref dir) => dir.path().to_path_buf(),
        None => cli.working_dir.unwrap(),
    };

    let working_file_path = working_dir_path.join("INTERIM").with_extension("sql");
    let data_types = get_data_types(input_file.as_path());

    println!("Read data types!");

    let config = read_config(config_file.as_path());
    let conditions = &config.get_conditions(&data_types);

    let mut filters = Filters::from_iter(conditions);
    let mut references = References::from_iter(conditions);

    println!("First pass...");
    process_file(input_file.as_path(), output_file.as_path(), &config.allow_data_on_tables, &mut filters, &mut references);

    println!("Second pass...");
    fs::rename(output_file.as_path(), &working_file_path).expect("cannot rename");
    process_file(&working_file_path, output_file.as_path(), &config.allow_data_on_tables, &mut filters, &mut references);

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }
}
