use clap::Parser;
use std::path::PathBuf;
use tempdir::TempDir;

mod expression_parser;
mod filters;
mod io_utils;
mod references;

use io_utils::{get_data_types, read_sql_file, write_sql_file, Configuration};
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
    let data_types = get_data_types(input_file.as_path());

    println!("Read data types!");

    let config = Configuration::from(&config_file);
    let conditions = &config.get_conditions(&data_types);

    let mut filters = Filters::from_iter(conditions);
    let mut references = References::from_iter(conditions);

    println!("First pass...");
    let all_statements = read_sql_file(input_file.as_path(), &config.allowed_tables);
    let filtered = filter_statements(&mut filters, &mut references, all_statements);
    write_sql_file(&working_file_path, filtered);

    println!("Second pass...");
    let all_statements = read_sql_file(&working_file_path, &config.allowed_tables);
    let filtered = filter_statements(&mut filters, &mut references, all_statements);
    write_sql_file(&output_file, filtered);

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }
}
