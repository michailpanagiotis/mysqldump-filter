use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::io_utils::read_config;
use crate::filters::{Filters, TableFilters};

#[derive(Debug)]
pub struct Config {
    working_dir_path: PathBuf,
    requested_tables: HashSet<String>,
    filter_conditions: Vec<(String, String)>,
}

impl Config {
    pub fn new(
        config_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let (requested_tables, filter_conditions) = read_config(config_file);

        Config {
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

    pub fn get_filters(&self, table: &Option<String>) -> TableFilters {
        let Some(t) = table else { return TableFilters::default() };
        let filters = Filters::from_iter(self.filter_conditions.iter());
        dbg!(&filters);
        filters.get_filters_of_table(t).unwrap_or_default()
    }
}
