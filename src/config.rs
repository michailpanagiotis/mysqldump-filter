use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::{read_settings, Writer};
use crate::trackers::{InsertTracker, ReferenceTracker};
use crate::sql_statement::{Statement, TableStatementsIterator};
use crate::filters::{Filters, TableFilters, FilterCondition};

#[derive(Debug)]
pub struct Config {
    pub working_dir_path: PathBuf,
    pub schema_file: PathBuf,
    pub requested_tables: HashSet<String>,
    pub filters: Filters,
}

impl Config {
    pub fn new(
        config_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let (requested_tables, filter_conditions) = read_settings(config_file);

        let it = filter_conditions.into_iter().map(|(table, condition)| FilterCondition::new(&table, &condition));
        let filters = Filters::from_iter(it);

        dbg!(&filters);

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filters,
        }
    }

    fn get_filters(&self, table: &Option<String>) -> TableFilters {
        let Some(t) = table else { return TableFilters::empty() };
        self.filters.get_filters_of_table(t).unwrap_or(TableFilters::empty())
    }

    pub fn get_referenced_fields(&self, table: &Option<String>) -> HashSet<String> {
        match table {
            None => HashSet::new(),
            Some(t) => self.filters.get_references_of_table(t),
        }
    }

    pub fn get_table_config(&self, table: &Option<String>) -> TableConfig {
        TableConfig::new(
            &self.working_dir_path,
            &self.schema_file,
            table,
            &self.get_filters(table),
            &self.get_referenced_fields(table),
        )
    }

    pub fn read_statements(&self, input_file: &Path) -> impl Iterator<Item=Statement> {
        Statement::from_file(input_file, &self.requested_tables)
    }
}

#[derive(Debug)]
pub struct TableConfig {
    working_dir: PathBuf,
    default_file: PathBuf,
    table: Option<String>,
    filters: TableFilters,
    referenced_fields: HashSet<String>,
}

impl TableConfig {
    pub fn new(
        working_dir: &Path,
        default_file: &Path,
        table: &Option<String>,
        filters: &TableFilters,
        referenced_fields: &HashSet<String>,
    ) -> TableConfig
    {
        TableConfig {
            working_dir: working_dir.to_path_buf(),
            default_file: default_file.to_path_buf(),
            table: table.clone(),
            filters: filters.clone(),
            referenced_fields: referenced_fields.clone(),
        }
    }

    pub fn get_writer(&self) -> Writer {
        Writer::new( &self.table, &self.working_dir, &self.default_file)
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
