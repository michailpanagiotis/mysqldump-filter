use clap::Parser;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use tempdir::TempDir;

mod checks;
mod filters;
mod sql;
mod traits;

use traits::{ColumnMeta, NoDataTypeError};
use checks::from_config;
use filters::FilterConditions;
use sql::{get_data_types, read_sql_file, write_sql_file};

#[derive(Deserialize)]
#[serde(rename = "name")]
pub struct Config {
    pub allow_data_on_tables: HashSet<String>,
    pub cascades: HashMap<String, Vec<String>>,
    filters: HashMap<String, Vec<String>>
}

impl Config {
    fn from_file(config_file: &Path) -> Self {
        let file = config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json);
        let settings = config::Config::builder().add_source(file).build().expect("cannot read config file");
        settings.try_deserialize::<Config>().expect("malformed config")
    }
}

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

fn process_file(input_file: &Path, output_file: &Path, allow_data_on_tables: &HashSet<String>, filters: &mut FilterConditions) {
    let filtered = filters.filter(read_sql_file(input_file, allow_data_on_tables));
    write_sql_file(output_file, filtered);
}

fn main() -> Result<(), NoDataTypeError> {
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

    let config = Config::from_file(config_file.as_path());
    let mut per_table = from_config(config.filters.iter().chain(&config.cascades), &data_types)?;

    let deps: Vec<ColumnMeta> = per_table.values().flat_map(|f| f.borrow().get_column_dependencies()).collect();

    let mut fc = FilterConditions::new(&mut per_table);

    println!("First pass...");
    process_file(input_file.as_path(), output_file.as_path(), &config.allow_data_on_tables, &mut fc);

    println!("Second pass...");
    fs::rename(output_file.as_path(), &working_file_path).expect("cannot rename");
    process_file(&working_file_path, output_file.as_path(), &config.allow_data_on_tables, &mut fc);

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }

    Ok(())
}
