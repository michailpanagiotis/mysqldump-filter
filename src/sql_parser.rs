use std::path::PathBuf;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::io_utils::{Configuration, combine_files, read_sql_file, write_sql_file};
use crate::filters::Filters;

fn process_table_statements<I: Iterator<Item=String>>(
    config: &Configuration,
    filters: &mut Filters,
    table: &Option<String>,
    statements: I,
    references: Option<&HashMap<String, HashSet<String>>>,
) -> PathBuf {
    write_sql_file(
        table,
        &config.working_dir_path,
        statements.filter(|st| filters.test_insert_statement(st, table, &references))
    )
}

pub fn parse_input_file(config: &Configuration) {
    let all_statements = read_sql_file(&config.input_file, &config.requested_tables);

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

    combine_files(filepaths.iter(), &config.output_file);
}
