use std::path::PathBuf;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::io_utils::{read_statements, Writer};
use crate::filters::{Filters, References, TableReferences};
use crate::config::Config;

fn process_table_statements<I: Iterator<Item=String>>(
    config: &Config,
    filters: &Filters,
    table_option: &Option<String>,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> (PathBuf, TableReferences) {
    if let Some(table) = table_option {
        println!("Processing table {}", &table);
    }

    let mut writer = Writer::new(&config.get_filepath(table_option));
    let mut table_filters = filters.get_filters_of_table(table_option);

    for statement in statements.filter(|st| table_filters.test_insert_statement(st, &references)) {
        writer.write_line(statement.as_bytes()).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (writer.get_filepath(), table_filters.references)
}

pub fn parse_input_file(config: &Config) {
    let all_statements = read_statements(&config.input_file, config.get_requested_tables());

    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut reference_trackers: Vec<TableReferences> = Vec::new();

    let filters = Filters::from_iter(config.get_filter_conditions().iter());

    println!("First pass...");
    for (table, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        let (filepath, ref_tracker) = process_table_statements(config, &filters, &table, statements.map(|(_, line)| line), None);
        filepaths.push(filepath);
        reference_trackers.push(ref_tracker.clone());
    }

    let refs: References = References::from_iter(reference_trackers);


    println!("Second pass...");
    dbg!(&refs);

    Writer::combine_files(filepaths.iter(), &config.output_file);
}
