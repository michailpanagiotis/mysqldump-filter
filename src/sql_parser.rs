use std::collections::HashMap;
use std::path::PathBuf;
use itertools::Itertools;
use tempdir::TempDir;

use crate::io_utils::{Configuration, combine_files, read_sql_file, write_sql_file};
use crate::references::References;
use crate::filters::{filter_sql_lines, Filters};

pub fn parse_input_file(config: &Configuration) {
    let mut filters = Filters::new(&config.get_conditions());
    let second_pass_tables = filters.get_foreign_tables();

    dbg!(&filters);

    let mut references = References::from_iter(config.get_foreign_keys());

    let temp_dir = TempDir::new("sql_parser_intermediate").expect("cannot create temporary dir");
    let temp_dir_path = temp_dir.path().to_path_buf();

    println!("First pass...");
    let all_statements = read_sql_file(&config.input_file, &config.requested_tables);
    let mut filepaths: HashMap<Option<String>, PathBuf> = HashMap::new();
    for (table, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        let lines = filter_sql_lines(&mut filters, &mut references, None, table.clone(), statements.map(|(_, line)| line));
        let working_dir_path = match table {
            None => &config.working_dir_path,
            Some(ref t) => {
                match second_pass_tables.contains(t) {
                    false => &config.working_dir_path,
                    true => &temp_dir_path,
                }
            }
        };

        let filepath = write_sql_file(&table, working_dir_path, lines);
        filepaths.insert(table, filepath);
    }

    println!("Second pass...");
    for table in second_pass_tables.into_iter() {
        let input_file = &filepaths[&Some(table.clone())];
        let statements = read_sql_file(input_file, &config.requested_tables);
        let lines = filter_sql_lines(&mut filters, &mut references, None, Some(table.clone()), statements.map(|(_, line)| line));
        let filepath = write_sql_file(&Some(table.clone()), &config.working_dir_path, lines);
        filepaths.insert(Some(table), filepath);
    }

    dbg!(&filepaths);

    dbg!(&references);

    combine_files(filepaths.values(), &config.output_file);

    let _ = temp_dir.close();
}
