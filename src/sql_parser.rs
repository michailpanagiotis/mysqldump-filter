use std::collections::HashSet;
use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::{Statement, TableStatements};
use crate::io_utils::SQLWriter;
use crate::trackers::{ReferenceTracker, TableReferences};
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

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        let mut filepaths: Vec<PathBuf> = Vec::new();
        let mut reference_trackers: Vec<TableReferences> = Vec::new();
        for (table, statements) in Statement::from_file(input_file, &self.config.requested_tables).chunk_by(Statement::get_table).into_iter() {
            let working_dir_path = &self.config.working_dir_path.clone();
            let schema_file = &self.config.schema_file.clone();
            let referenced_fields = &self.config.get_referenced_fields(&table);
            let filters = &self.config.get_filters(&table);

            let st = TableStatements::new(&table, &filters, referenced_fields, statements);

            let (ref_tracker, filepath) = st.scan(
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
