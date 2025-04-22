use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::SQLWriter;
use crate::trackers::{InsertTracker, ReferenceTracker};
use crate::sql_statement::{Statement, TableStatementsIterator};
use crate::filters::{DatabaseFilters, TableFilters};

#[derive(Debug)]
pub struct Config {
    pub working_dir_path: PathBuf,
    pub schema_file: PathBuf,
    pub requested_tables: HashSet<String>,
    pub filters_per_table: DatabaseFilters,
}

impl Config {
    pub fn new(
        config_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let settings = config::Config::builder()
            .add_source(config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json))
            .add_source(config::Environment::with_prefix("MYSQLDUMP_FILTER"))
            .build()
            .unwrap();
        let requested_tables: HashSet<_> = settings
            .get_array("allow_data_on_tables")
            .expect("no key 'allow_data_on_tables' in config")
            .iter().map(|x| x.to_string()).collect();

        let filters_per_table = DatabaseFilters::from_config_value(
            &settings.get_table("filter_inserts").expect("no key 'filter_inserts' in config"),
        );

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filters_per_table,
        }
    }

    pub fn get_filters_of_table(&self, table: &Option<String>) -> Option<TableFilters> {
        let Some(t) = table else { return None };
        self.filters_per_table.get_filters_of_table(t)
    }

    pub fn get_referenced_fields(&self, table: &Option<String>) -> HashSet<String> {
        match table {
            None => HashSet::new(),
            Some(t) => self.filters_per_table.get_references_of_table(t),
        }
    }

    pub fn get_table_config(&self, table: &Option<String>) -> TableConfig {
        let referenced_fields = &self.get_referenced_fields(table);
        let filters = &self.get_filters_of_table(table);
        TableConfig::new(&self.working_dir_path, &self.schema_file, table, filters, referenced_fields)
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
    filters: Option<TableFilters>,
    referenced_fields: HashSet<String>,
}

impl TableConfig {
    pub fn new(
        working_dir: &Path,
        default_file: &Path,
        table: &Option<String>,
        filters: &Option<TableFilters>,
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

    pub fn get_writer(&self) -> SQLWriter {
        SQLWriter::new( &self.table, &self.working_dir, &self.default_file)
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
