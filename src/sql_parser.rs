use std::path::PathBuf;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::io_utils::{Configuration, read_statements, TableWriter};
use crate::filters::Filters;

fn process_table_statements<I: Iterator<Item=String>>(
    config: &Configuration,
    filters: &mut Filters,
    table_option: &Option<String>,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> PathBuf {
    if let Some(table) = table_option {
        println!("Processing table {}", &table);
    }

    let mut writer = TableWriter::new(&config.working_dir_path, table_option);

    for statement in statements.filter(|st| filters.test_insert_statement(st, table_option, &references)) {
        writer.write_line(statement.as_bytes()).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    writer.get_filepath()
}

pub fn parse_input_file(config: &Configuration) {
    let all_statements = read_statements(&config.input_file, &config.requested_tables);

    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut filters = Filters::from_iter(config.filter_conditions.iter());

    println!("First pass...");
    for (table, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        let filepath = process_table_statements(config, &mut filters, &table, statements.map(|(_, line)| line), None);
        filepaths.push(filepath);
    }

    filters.consolidate();
    dbg!(&filters);

    println!("Second pass...");

    TableWriter::combine_files(filepaths.iter(), &config.output_file);
}
