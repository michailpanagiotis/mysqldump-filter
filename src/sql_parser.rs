use std::path::{Path, PathBuf};
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::sql_statement::Statement;
use crate::io_utils::Writer;
use crate::filters::{References, TableReferences};
use crate::config::{Config, TableConfig};

pub fn process_table_statements<I: Iterator<Item=Statement>>(
    config: &TableConfig,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> (PathBuf, Option<TableReferences>) {
    if let Some(table) = &config.get_table() {
        println!("Processing table {}", &table);
    }

    let mut writer = config.get_writer();
    let mut ref_tracker = config.get_reference_tracker();

    for statement in config.filter_statements(statements, references) {
        if let Some(ref mut tracker) = ref_tracker {
            if statement.is_insert() {
                tracker.capture(statement.as_str());
            }
        }
        writer.write_line(statement.as_bytes()).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), ref_tracker)
}

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let all_statements = Statement::from_file(input_file, config.get_requested_tables());

    type ParseResult = (Vec<PathBuf>, Vec<Option<TableReferences>>);

    println!("First pass...");
    let (filepaths, reference_trackers): ParseResult = all_statements.chunk_by(Statement::get_table).into_iter().map(|(table, statements)| {
        process_table_statements(&config.get_table_config(&table), statements, None)
    }).unzip();

    let references = HashMap::from(
        References{ inner: reference_trackers.into_iter().flatten().collect() },
    );

    dbg!(&references);

    println!("Second pass...");

    Writer::combine_files(filepaths.iter(), output_file);
}
