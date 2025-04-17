use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::{Statement, TableStatements};
use crate::io_utils::SQLWriter;
use crate::trackers::{InsertTracker, ReferenceTracker, TableReferences};
use crate::config::Config;

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
        }
    }

    fn should_keep_statement(statement: &Statement, insert_tracker: &mut Option<InsertTracker>) -> bool {
        if let Some(info) = insert_tracker {
            info.should_keep_statement(statement)
        } else {
            true
        }
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        let mut filepaths: Vec<PathBuf> = Vec::new();
        let mut reference_trackers: Vec<TableReferences> = Vec::new();
        for (table, statements) in Statement::from_file(input_file, &self.config.requested_tables).chunk_by(Statement::get_table).into_iter() {
            let filters = table.clone().map(|t| self.config.filters_per_table.get(&t));
            let working_dir_path = &self.config.working_dir_path.clone();
            let schema_file = &self.config.schema_file.clone();

            let st = TableStatements::new(&table, &filters, statements);

            let referenced_fields = match table {
                None => HashSet::new(),
                Some(ref t) => {
                    match self.config.references_per_table.get(t) {
                        Some(x) => HashSet::from_iter(x.iter().cloned()),
                        None => HashSet::new(),
                    }
                }
            };
            let mut insert_tracker = table.clone().map(|t| InsertTracker::new(
                &t,
                &filters,
            ));

            let (ref_tracker, filepath) = st.scan(
                |statement| {
                    if let Some(info) = &mut insert_tracker {
                        info.should_keep_statement(statement)
                    } else {
                        true
                    }
                },
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
