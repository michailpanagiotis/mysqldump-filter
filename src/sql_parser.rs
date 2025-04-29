use std::path::PathBuf;
use itertools::Itertools;

use crate::expression_parser::get_data_types;
use crate::io_utils::{Configuration, combine_files, read_sql_file, read_file_lines, write_sql_file};
use crate::references::References;
use crate::filters::{filter_insert_statements, Filters};

pub fn parse_input_file(config: &Configuration) {
    let mut filters = Filters::new(&config.get_conditions());

    dbg!(&filters);

    let mut references = References::from_iter(config.get_foreign_keys());

    println!("First pass...");
    let all_statements = read_sql_file(&config.input_file, &config.requested_tables);
    let mut first_pass_filepaths: Vec<(Option<String>, PathBuf)> = Vec::new();
    for (group, statements) in all_statements.chunk_by(|(table, _)| table.clone()).into_iter() {
        match group {
            None => {
                let schema: Vec<String> = statements.map(|(_, line)| line).collect();
                let str: String = schema.iter().cloned().collect();
                let filepath = write_sql_file(&group, config.get_working_dir_for_table(&group), schema.into_iter());
                first_pass_filepaths.push((group, filepath));
            },
            Some(ref table) => {
                let lines = filter_insert_statements(&mut filters, &mut references, None, table, statements.map(|(_, line)| line));
                let filepath = write_sql_file(&group, config.get_working_dir_for_table(&group), lines);
                first_pass_filepaths.push((group, filepath));
            }
        }
    }

    println!("Second pass...");

    let second_pass_filepaths: Vec<PathBuf> = first_pass_filepaths.iter().map(|(table, path)| {
        match table {
            None => path.clone(),
            Some(t) => {
                if !config.second_pass_tables.contains(t) {
                    return path.clone();
                }
                let statements = read_file_lines(path);
                let lines = filter_insert_statements(&mut filters, &mut references, None, t, statements);
                write_sql_file(table, &config.working_dir_path, lines)
            }
        }
    }).collect();

    dbg!(&second_pass_filepaths);

    combine_files(second_pass_filepaths.iter(), &config.output_file);
}
