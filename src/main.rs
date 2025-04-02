use clap::Parser;
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::iter;
use tempdir::TempDir;

mod reader;
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
    let working_dir = TempDir::new("sql_parser").expect("cannot create temporary dir");
    let schema_file = working_dir.path().join("schema.sql");
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(&input_path);
    let config = config::parse(cli.config.to_str().unwrap(), &input_file, working_dir.path(), &schema_file);

    dbg!(&config);
    let (_, data_files) = sql_parser::split(config);

    println!("Combining files");
    combine_files(&schema_file, data_files.iter(), cli.output);
    _ = working_dir.close();
}
