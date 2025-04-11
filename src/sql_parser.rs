use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::{FieldPositions, Statement, TableStatements};
use crate::io_utils::SQLWriter;
use crate::config::{Config, FilterMap, TableFilters};

#[derive(Debug)]
struct ReferenceTracker {
    references: HashMap<String, HashSet<String>>,
    is_complete: bool,
}

impl ReferenceTracker {
    fn new() -> Self {
        ReferenceTracker {
            references: HashMap::new(),
            is_complete: false,
        }
    }

    fn get_key(&mut self, table: &String, field: &str) -> String {
        table.to_owned() + "." + field
    }

    fn has_completed(&self) -> bool {
        self.is_complete
    }

    fn insert(&mut self, table: &String, field: &str, value: &String) {
        let key: String = self.get_key(table, field);
        match self.references.get_mut(&key) {
            Some(x) => {
                x.insert(value.to_string());
            },
            None => {
                self.references.insert(key, HashSet::from([value.to_string()]));
            }
        }
    }
}

#[derive(Debug)]
struct InsertTracker {
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    references: HashMap<String, HashSet<String>>,
    field_positions: FieldPositions,
}

impl InsertTracker {
    fn new(
        table: &String,
        filters_per_table: &FilterMap,
        references_per_table: &HashMap<String, Vec<String>>,
        statement: &Statement,
    ) -> Self {
        let field_positions = statement.get_field_positions().expect("cannot find positions");

        let filters = filters_per_table.get(table);
        let references = match references_per_table.get(table) {
            Some(x) => x.clone(),
            None => Vec::new(),
        };
        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashMap::from_iter(references.iter().map(|r| (r.clone(), HashSet::new()))),
            field_positions,
        }
    }

    fn capture_references(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) {
        if let Some(ref table) = statement.get_table() {
            for (field, set) in self.references.iter_mut() {
                let value = self.field_positions.get_value(statement, field);
                set.insert(value.clone());
                reference_tracker.insert(table, field, &value);
            }
        }
    }

    fn should_keep_statement(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) -> bool {
        if !statement.is_insert() {
            return true;
        }

        let value_per_field = self.field_positions.get_values(
            statement,
            self.direct_filters.get_filtered_fields(),
        );

        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        if reference_tracker.has_completed() && !self.reference_filters.test(&value_per_field) {
            return false;
        }

        self.capture_references(statement, reference_tracker);
        true
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    insert_tracker_per_table: HashMap<String, InsertTracker>,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            insert_tracker_per_table: HashMap::new(),
        }
    }

    fn register_table(&mut self, table: &String, statement: &Statement) {
        self.insert_tracker_per_table.insert(table.to_string(), InsertTracker::new(
            table,
            &self.config.filters_per_table,
            &self.config.references_per_table,
            statement,
        ));
    }

    fn should_keep_statement(
        &mut self,
        statement: &Statement,
        requested_tables: &HashSet<String>,
        reference_tracker: &mut ReferenceTracker,
    ) -> bool {
        match statement.get_table() {
            None => {
                return true;
            },
            Some(table) => {
                if !statement.is_insert() {
                    return false;
                }
                if !requested_tables.contains(&table) {
                    return false;
                }
                if !self.insert_tracker_per_table.contains_key(&table) {
                    self.register_table(&table, statement);
                }
                let info = self.insert_tracker_per_table.get_mut(&table).expect("Cannot find table info");
                if info.should_keep_statement(statement, reference_tracker) {
                    return true;
                }
            },
        };
        false
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        let mut filepaths: Vec<PathBuf> = Vec::new();
        let reference_tracker = &mut ReferenceTracker::new();
        for (table, statements) in Statement::from_file(input_file).chunk_by(Statement::get_table).into_iter() {
            let st = TableStatements::new(&table, statements);
            let mut writer = st.get_writer(&self.config.working_dir_path, &self.config.schema_file);

            for statement in st.filter(|statement| self.should_keep_statement(
                statement,
                &self.config.requested_tables,
                reference_tracker,
            )) {
                writer.write_statement(&statement).expect("Unable to write data");
            }

            writer.flush().expect("Cannot flush buffer");

            filepaths.push(writer.get_filepath());
        }

        dbg!(&reference_tracker);

        SQLWriter::combine_files(filepaths.iter(), output_file);
    }
}
