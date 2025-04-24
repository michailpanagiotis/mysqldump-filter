use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::{read_config, Writer};
use crate::sql_statement::{Statement, TableStatementsIterator};
use crate::filters::{FilterCondition, Filters, TableFilters};

#[derive(Debug)]
pub struct Config {
    working_dir_path: PathBuf,
    schema_file: PathBuf,
    requested_tables: HashSet<String>,
    filters: Filters,
}

impl Config {
    pub fn new(
        config_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let (requested_tables, filter_conditions) = read_config(config_file);

        let filters = Filters::from_iter(filter_conditions.iter().map(|(table, condition)| FilterCondition::new(table, condition)));

        dbg!(&filters);

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filters,
        }
    }

    pub fn get_requested_tables(&self) -> &HashSet<String> {
        &self.requested_tables
    }

    pub fn get_table_config(&self, table: &Option<String>) -> TableConfig {
        TableConfig::new(
            table,
            &self.get_table_filepath(table),
            &self.get_filters(table),
        )
    }

    fn get_table_filepath(&self, table: &Option<String>) -> PathBuf {
        match table {
            Some(x) => self.working_dir_path.join(x).with_extension("sql"),
            None => self.schema_file.to_path_buf()
        }
    }

    fn get_filters(&self, table: &Option<String>) -> TableFilters {
        let Some(t) = table else { return TableFilters::default() };
        self.filters.get_filters_of_table(t).unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct TableConfig {
    table: Option<String>,
    filepath: PathBuf,
    pub filters: TableFilters,
}

impl TableConfig {
    pub fn new(
        table: &Option<String>,
        filepath: &Path,
        filters: &TableFilters,
    ) -> TableConfig
    {
        TableConfig {
            table: table.clone(),
            filepath: filepath.to_path_buf(),
            filters: filters.clone(),
        }
    }

    pub fn get_writer(&self) -> Writer {
        Writer::new(&self.filepath)
    }

    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    pub fn filter_statements<I: Iterator<Item=Statement>>(
        &self,
        statements: I,
        filters: &mut TableFilters,
        references: Option<&HashMap<String, HashSet<String>>>,
    ) -> impl Iterator<Item=Statement> {
        TableStatementsIterator::new(filters, references, statements)
    }
}
