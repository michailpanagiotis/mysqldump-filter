use std::path::{Path, PathBuf};
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::sql_statement::Statement;
use crate::io_utils::Writer;
use crate::trackers::ReferenceTracker;
use crate::config::{Config, TableConfig};

pub fn process_table_statements<I: Iterator<Item=Statement>>(
    config: &TableConfig,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> (PathBuf, Option<ReferenceTracker>) {
    if let Some(table) = &config.get_table() {
        println!("Processing table {}", &table);
    }

    let mut writer = config.get_writer();
    let mut ref_tracker = config.get_reference_tracker();

    for statement in config.filter_statements(statements, references) {
        if let Some(ref mut tracker) = ref_tracker {
            tracker.capture(&statement);
        }
        writer.write_line(statement.as_bytes()).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), ref_tracker)
}

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let all_statements = config.read_statements(input_file);

    type ParseResult = (Vec<PathBuf>, Vec<Option<ReferenceTracker>>);

    println!("First pass...");
    let (filepaths, reference_trackers): ParseResult = all_statements.chunk_by(Statement::get_table).into_iter().map(|(table, statements)| {
        process_table_statements(&config.get_table_config(&table), statements, None)
    }).unzip();

    let references = ReferenceTracker::merge(reference_trackers.iter().flatten());

    dbg!(&references);

    println!("Second pass...");

    Writer::combine_files(filepaths.iter(), output_file);
}
