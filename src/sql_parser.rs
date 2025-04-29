use std::collections::HashMap;
use std::path::PathBuf;
use itertools::Itertools;
use tempdir::TempDir;

use crate::io_utils::{Configuration, combine_files, read_sql_file, write_sql_file};
use crate::references::References;
use crate::filters::{filter_sql_lines, Filters};

pub fn parse_input_file(config: &Configuration) {
    let mut filters = Filters::new(&config.get_conditions());

    dbg!(&filters);

    let mut references = References::from_iter(config.get_foreign_keys());

    let temp_dir = TempDir::new("sql_parser_intermediate").expect("cannot create temporary dir");

    println!("First pass...");
    let all_statements = read_sql_file(&config.input_file, &config.requested_tables);
    let mut filepaths: HashMap<Option<String>, PathBuf> = HashMap::new();
    for (table, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        let lines = filter_sql_lines(&mut filters, &mut references, None, table.clone(), statements.map(|(_, line)| line));
        let working_dir_path = config.get_working_dir_for_table(&table);

        let filepath = write_sql_file(&table, working_dir_path, lines);
        filepaths.insert(table, filepath);
    }

    println!("Second pass...");
    for table in config.second_pass_tables.iter() {
        let input_file = &filepaths[&Some(table.clone())];
        let statements = read_sql_file(input_file, &config.requested_tables);
        let lines = filter_sql_lines(&mut filters, &mut references, None, Some(table.clone()), statements.map(|(_, line)| line));
        let filepath = write_sql_file(&Some(table.clone()), &config.working_dir_path, lines);
        filepaths.insert(Some(table.clone()), filepath);
    }

    dbg!(&filepaths);

    dbg!(&references);

    combine_files(filepaths.values(), &config.output_file);

    let _ = temp_dir.close();
}
