use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::{read_config, Writer};
use crate::trackers::{InsertTracker, ReferenceTracker};
use crate::sql_statement::{Statement, TableStatementsIterator};
use crate::filters::{Filters, TableFilters, FilterCondition};

#[derive(Debug)]
pub struct Config {
    working_dir_path: PathBuf,
    schema_file: PathBuf,
    requested_tables: HashSet<String>,
    filter_conditions: Vec<(String, String)>,
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
            filter_conditions,
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
            &self.get_referenced_fields(table),
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
        let filters = Filters::from_iter(self.filter_conditions.iter().map(|(table, condition)| FilterCondition::new(table, condition)));
        filters.get_filters_of_table(t).unwrap_or(TableFilters::default())
    }

    fn get_referenced_fields(&self, table: &Option<String>) -> HashSet<String> {
        let Some(t) = table else { return HashSet::new() };
        let filters = Filters::from_iter(self.filter_conditions.iter().map(|(table, condition)| FilterCondition::new(table, condition)));
        filters.get_references_of_table(t)
    }
}

#[derive(Debug)]
pub struct TableConfig {
    table: Option<String>,
    filepath: PathBuf,
    filters: TableFilters,
    referenced_fields: HashSet<String>,
}

impl TableConfig {
    pub fn new(
        table: &Option<String>,
        filepath: &Path,
        filters: &TableFilters,
        referenced_fields: &HashSet<String>,
    ) -> TableConfig
    {
        TableConfig {
            table: table.clone(),
            filepath: filepath.to_path_buf(),
            filters: filters.clone(),
            referenced_fields: referenced_fields.clone(),
        }
    }

    pub fn get_writer(&self) -> Writer {
        Writer::new(&self.filepath)
    }

    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn get_insert_tracker<'a>(&self, references: Option<&'a HashMap<String, HashSet<String>>>) -> Option<InsertTracker<'a>> {
        self.table.clone().map(|t| InsertTracker::new(
            &t,
            &self.filters,
            references,
        ))
    }

    pub fn get_reference_tracker(&self) -> Option<ReferenceTracker> {
        let ref_tracker = match self.table.is_some() && !self.referenced_fields.is_empty() {
            true => Some(ReferenceTracker::new(self.table.as_ref().unwrap(), &self.referenced_fields)),
            false => None,
        };
        ref_tracker
    }

    pub fn filter_statements<I: Iterator<Item=Statement>>(
        &self,
        statements: I,
        references: Option<&HashMap<String, HashSet<String>>>,
    ) -> impl Iterator<Item=Statement> {
        TableStatementsIterator::new(self.get_insert_tracker(references), statements)
    }
}
