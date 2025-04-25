use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::io_utils::read_config;

#[derive(Debug)]
pub struct Config {
    pub input_file: PathBuf,
    pub output_file: PathBuf,
    working_dir_path: PathBuf,
    requested_tables: HashSet<String>,
    filter_conditions: Vec<(String, String)>,
}

impl Config {
    pub fn new(
        config_file: &Path,
        input_file: &Path,
        output_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let (requested_tables, filter_conditions) = read_config(config_file);

        Config {
            input_file: input_file.to_path_buf(),
            output_file: output_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filter_conditions,
        }
    }

    pub fn get_requested_tables(&self) -> &HashSet<String> {
        &self.requested_tables
    }

    pub fn get_filepath(&self, table: &Option<String>) -> PathBuf {
        match table {
            Some(x) => self.working_dir_path.join(x).with_extension("sql"),
            None => self.working_dir_path.join("INFORMATION_SCHEMA").with_extension("sql"),
        }
    }

    pub fn get_filter_conditions(&self) -> &Vec<(String, String)> {
        &self.filter_conditions
    }
}
