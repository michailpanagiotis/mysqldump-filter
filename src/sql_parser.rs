use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::{FieldPositions, Statement, TableStatements};
use crate::io_utils::SQLWriter;
use crate::trackers::{InsertTracker, ReferenceTracker, TableReferences};
use crate::config::{Config, FilterMap, TableFilters};

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
        let field_positions = statement.get_field_positions().expect("cannot find positions");
        self.insert_tracker_per_table.insert(table.to_string(), InsertTracker::new(
            table,
            &self.config.filters_per_table,
            &self.config.references_per_table,
            &field_positions,
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
                true
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
                    info.capture_references(statement);
                    true
                } else {
                    false
                }
            },
        }
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        let mut filepaths: Vec<PathBuf> = Vec::new();
        let reference_tracker = &mut ReferenceTracker::new(&HashSet::new());
        let mut reference_trackers: Vec<TableReferences> = Vec::new();
        for (table, statements) in Statement::from_file(input_file).chunk_by(Statement::get_table).into_iter() {
            let st = TableStatements::new(&table, statements);
            let working_dir_path = &self.config.working_dir_path.clone();
            let schema_file = &self.config.schema_file.clone();

            let referenced_fields = match table {
                None => HashSet::new(),
                Some(ref t) => {
                    match self.config.references_per_table.get(t) {
                        Some(x) => HashSet::from_iter(x.iter().cloned()),
                        None => HashSet::new(),
                    }
                }
            };
            if table.is_some() {
                println!("Parsing table {}", &table.unwrap());
            }

            let (ref_tracker, filepath) = st.scan(
                |statement| self.should_keep_statement(
                    statement,
                    &self.config.requested_tables,
                    reference_tracker,
                ),
                working_dir_path,
                schema_file,
                &referenced_fields,
            );

            filepaths.push(filepath);
            if let Some(tracker) = ref_tracker {
                reference_trackers.push(tracker);
            }
        }

        let ref_trackers = ReferenceTracker::from_iter(reference_trackers.iter());

        dbg!(&ref_trackers);

        SQLWriter::combine_files(filepaths.iter(), output_file);
    }
}
