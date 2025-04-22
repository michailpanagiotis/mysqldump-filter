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

    for statement in config.filter_statements(statements) {
        if let Some(ref mut tracker) = ref_tracker {
            tracker.capture(&statement);
        }
        writer.write_statement(&statement).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), ref_tracker)
}

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    // let mut filepaths: Vec<PathBuf> = Vec::new();
    // let mut reference_trackers: Vec<Option<ReferenceTracker>> = Vec::new();
    let all_statements = config.read_statements(input_file);

    type ParseResult = (Vec<PathBuf>, Vec<Option<ReferenceTracker>>);

    let (filepaths, reference_trackers): ParseResult = all_statements.chunk_by(Statement::get_table).into_iter().map(|(table, statements)| {
        process_table_statements(&config.get_table_config(&table), statements)
    }).unzip();

    let references = ReferenceTracker::merge(reference_trackers.iter().flatten());

    dbg!(&references);

    SQLWriter::combine_files(filepaths.iter(), output_file);
}
