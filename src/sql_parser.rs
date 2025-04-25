use std::path::PathBuf;
use itertools::Itertools;

use crate::io_utils::{Configuration, combine_files, read_sql_file, write_sql_file};
use crate::filters::{filter_sql_lines, Filters};

pub fn parse_input_file(config: &Configuration) {
    let all_statements = read_sql_file(&config.input_file, &config.requested_tables);

    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut filters = Filters::from_iter(config.filter_conditions.iter());

    println!("First pass...");
    for (table, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        let lines = filter_sql_lines(&mut filters, None, &table, statements.map(|(_, line)| line));
        let filepath = write_sql_file(&table, &config.working_dir_path, lines);
        filepaths.push(filepath);
    }

    filters.consolidate();
    dbg!(&filters);

    println!("Second pass...");

    combine_files(filepaths.iter(), &config.output_file);
}
