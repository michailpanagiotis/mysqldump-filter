use clap::Parser;
use std::path::PathBuf;
use tempdir::TempDir;

mod expression_parser;
mod filters;
mod io_utils;
mod sql_parser;

use io_utils::read_config;
use sql_parser::parse_input_file;

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

    let config = read_config(
        &config_file,
        input_file.as_path(),
        output_file.as_path(),
        &working_dir_path,
    );

    parse_input_file(&config);

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }
}
