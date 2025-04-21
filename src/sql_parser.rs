use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::{scan_statements, Statement};
use crate::io_utils::SQLWriter;
use crate::trackers::ReferenceTracker;
use crate::config::Config;

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut reference_trackers: Vec<ReferenceTracker> = Vec::new();
    for (table, statements) in Statement::from_file(input_file, &config.requested_tables).chunk_by(Statement::get_table).into_iter() {
        let working_dir_path = &config.working_dir_path.clone();
        let schema_file = &config.schema_file.clone();
        let table_config = config.get_table_config(&table);

        let (ref_tracker, filepath) = scan_statements(
            &table_config,
            working_dir_path,
            schema_file,
            statements,
        );

        filepaths.push(filepath);
        if let Some(tracker) = ref_tracker {
            reference_trackers.push(tracker);
        }
    }

    let ref_trackers = ReferenceTracker::merge(reference_trackers.iter());

    dbg!(&ref_trackers);

    SQLWriter::combine_files(filepaths.iter(), output_file);
}
