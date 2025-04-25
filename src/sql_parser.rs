use std::path::{Path, PathBuf};
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::sql_statement::{Statement, TableStatementsIterator};
use crate::io_utils::Writer;
use crate::filters::{References, TableReferences, TableFilters};
use crate::config::Config;

fn filter_statements<I: Iterator<Item=Statement>>(
    statements: I,
    filters: &mut TableFilters,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> impl Iterator<Item=Statement> {
    TableStatementsIterator::new(filters, references, statements)
}

fn process_table_statements<I: Iterator<Item=Statement>>(
    config: &Config,
    table_option: &Option<String>,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> (PathBuf, TableReferences) {
    if let Some(table) = table_option {
        println!("Processing table {}", &table);
    }

    let mut writer = Writer::new(&config.get_filepath(table_option));
    let mut filters = config.get_filters(table_option);

    for statement in filter_statements(statements, &mut filters, references) {
        writer.write_line(statement.as_bytes()).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), filters.references)
}

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let all_statements = Statement::from_file(input_file, config.get_requested_tables());

    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut reference_trackers: Vec<TableReferences> = Vec::new();

    println!("First pass...");
    for (table, statements) in all_statements.chunk_by(Statement::get_table).into_iter() {
        let (filepath, ref_tracker) = process_table_statements(&config, &table, statements, None);
        filepaths.push(filepath);
        reference_trackers.push(ref_tracker.clone());
    }

    let refs: References = References::from_iter(reference_trackers);


    println!("Second pass...");
    dbg!(&refs);

    Writer::combine_files(filepaths.iter(), output_file);
}
