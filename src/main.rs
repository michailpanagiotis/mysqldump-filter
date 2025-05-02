use std::fs;
use clap::Parser;
use std::path::PathBuf;
use tempdir::TempDir;

mod expression_parser;
mod filters;
mod io_utils;
mod references;

use io_utils::{Configuration, read_sql_file, write_sql_file};
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

    let config = Configuration::from(&config_file);

    let (all_statements, data_types) = read_sql_file(input_file.as_path(), &config.allowed_tables);

    let mut references = References::from_iter(config.get_foreign_keys());
    let mut filters = Filters::new(&config.get_conditions());

    println!("First pass...");
    let filtered = filter_statements(&mut filters, &mut references, None, all_statements);
    write_sql_file(&working_file_path, filtered);

    fs::rename(working_file_path, output_file.as_path()).expect("cannot rename output file");

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }

}
