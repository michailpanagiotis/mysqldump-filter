use clap::Parser;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tempdir::TempDir;

mod checks;
mod dependencies;
mod table;
mod scanner;

use table::{process_checks};
use checks::{get_passes};
use scanner::{explode_to_files, gather};

#[derive(Debug)]
#[derive(Deserialize)]
#[serde(rename = "name")]
pub struct Config {
    pub allow_data_on_tables: Option<HashSet<String>>,
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

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(cli.input);
    let output_file = std::env::current_dir().unwrap().to_path_buf().join(cli.output);
    let config_file = std::env::current_dir().unwrap().to_path_buf().join(cli.config);
    let temp_dir = if cli.working_dir.is_none() { Some(TempDir::new("sql_parser").expect("cannot create temporary dir")) } else { None };
    let config = Config::from_file(config_file.as_path());

    let working_dir_path = match temp_dir {
        Some(ref dir) => dir.path().to_path_buf(),
        None => cli.working_dir.unwrap(),
    };
    let working_file_path = working_dir_path.join("INTERIM").with_extension("sql");

    // explode_to_files(
    //     working_file_path.as_path(),
    //     input_file.as_path(),
    //     |statement| {
    //         if let Some(allowed) = &config.allow_data_on_tables {
    //             if !allowed.contains(statement.get_table()) {
    //                 return Ok(None);
    //             }
    //         }
    //         Ok(Some(()))
    //     }
    // ).unwrap_or_else(|e| {
    //     panic!("Problem exploding to files: {e:?}");
    // });

    let passes = get_passes(config.filters.iter().chain(&config.cascades))?;
    process_checks(passes, working_file_path.as_path())?;
    // gather(&working_file_path, &output_file)?;
    //
    // dbg!(collection);

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }

    Ok(())
}
