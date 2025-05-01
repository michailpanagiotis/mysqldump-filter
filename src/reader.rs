use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::io::{self, BufRead};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;
use tempdir::TempDir;

mod io_utils;
mod expression_parser;

use crate::io_utils::{read_config, read_sql_file, write_file_lines};

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

    let second_pass_temp_dir = TempDir::new("sql_parser_intermediate").expect("cannot create temporary dir");

    let config = read_config(
        &config_file,
        input_file.as_path(),
        output_file.as_path(),
        &working_dir_path,
        second_pass_temp_dir.path(),
    );

    let (schema, all_statements) = read_sql_file(&input_file, &config.requested_tables);

    write_file_lines(&output_file, schema.iter().cloned().chain(all_statements.map(|(_, line)| line)));
}
