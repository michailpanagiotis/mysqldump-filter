use clap::Parser;
use std::path::PathBuf;
use tempdir::TempDir;

mod reader;
mod io_utils;
mod sql_parser;
mod config;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(value_name = "FILE", required=true)]
    input: PathBuf,
    #[clap(short, long, required = true, num_args = 1..)]
    config: PathBuf,
    #[clap(short, long, required = true, num_args = 1..)]
    output: PathBuf,
}


fn main() {
    let cli = Cli::parse();
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(cli.input);
    let output_file = std::env::current_dir().unwrap().to_path_buf().join(cli.output);
    let config_file = std::env::current_dir().unwrap().to_path_buf().join(cli.config);
    let working_dir = TempDir::new("sql_parser").expect("cannot create temporary dir");
    let config = config::Config::new(
        &config_file,
        &input_file,
        &output_file,
        working_dir.path(),
    );

    sql_parser::truncate(config);

    _ = working_dir.close();
}
