use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::Statement;
use crate::io_utils::SQLWriter;
use crate::trackers::ReferenceTracker;
use crate::config::{Config, TableConfig};

pub fn process_table_statements<I: Iterator<Item=Statement>>(config: &TableConfig, statements: I) -> (PathBuf, Option<ReferenceTracker>) {
    if let Some(table) = &config.get_table() {
        println!("Processing table {}", &table);
    }

    let mut writer = config.get_writer();
    let mut ref_tracker = config.get_reference_tracker();

    for statement in statements {
        if let Some(ref mut tracker) = ref_tracker {
            tracker.capture(&statement);
        }
        writer.write_statement(&statement).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), ref_tracker)
}

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut reference_trackers: Vec<ReferenceTracker> = Vec::new();
    for (table, statements) in Statement::from_file(input_file, &config.requested_tables).chunk_by(Statement::get_table).into_iter() {
        let (filepath, ref_tracker) = process_table_statements(&config.get_table_config(&table), statements);

        filepaths.push(filepath);
        if let Some(tracker) = ref_tracker {
            reference_trackers.push(tracker);
        }
    }

    let references = ReferenceTracker::merge(reference_trackers.iter());

    dbg!(&references);

    SQLWriter::combine_files(filepaths.iter(), output_file);
}
