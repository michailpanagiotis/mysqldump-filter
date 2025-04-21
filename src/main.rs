use clap::Parser;
use std::path::PathBuf;
use tempdir::TempDir;

mod sql_statement;
mod io_utils;
mod sql_parser;
mod config;
mod trackers;

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
    println!("HELLO");
    let cli = Cli::parse();
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(cli.input);
    let output_file = std::env::current_dir().unwrap().to_path_buf().join(cli.output);
    let config_file = std::env::current_dir().unwrap().to_path_buf().join(cli.config);
    let temp_dir = if cli.working_dir.is_none() { Some(TempDir::new("sql_parser").expect("cannot create temporary dir")) } else { None };

    let working_dir_path = match temp_dir {
        Some(ref dir) => dir.path().to_path_buf(),
        None => cli.working_dir.unwrap(),
    };

    let config = config::Config::new(&config_file, &working_dir_path);

    parse_input_file(&config, input_file.as_path(), output_file.as_path());

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }
}
