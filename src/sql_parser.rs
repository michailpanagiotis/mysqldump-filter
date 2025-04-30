use std::path::PathBuf;
use itertools::Itertools;

use crate::expression_parser::get_data_types;
use crate::io_utils::{combine_files, read_file_lines, read_sql_file, write_file_lines, write_sql_file, Configuration};
use crate::references::References;
use crate::filters::{filter_statements, filter_insert_statements, Filters};

pub fn parse_input_file(config: &Configuration) {
    println!("Capturing schema...");
    let (schema, all_statements) = read_sql_file(&config.input_file, &config.requested_tables);

    let schema_path = config.get_schema_path();
    write_file_lines(&schema_path, schema.iter().cloned());

    let data_types = get_data_types(&schema);
    let mut references = References::from_iter(config.get_foreign_keys());
    let mut filters = Filters::new(&config.get_conditions());

    println!("First pass...");
    let mut first_pass_filepaths: Vec<(Option<String>, PathBuf)> = Vec::new();

    let filtered = filter_statements(&mut filters, &mut references, None, all_statements.map(|(_, table, field)| (table, field)));
    write_file_lines(&config.output_file, filtered.map(|(_, line)| line));
}

// pub fn parse_input_file(config: &Configuration) {
//     println!("Capturing schema...");
//     let (schema, all_statements) = read_sql_file(&config.input_file, &config.requested_tables);
//
//     let schema_path = config.get_schema_path();
//     write_file_lines(&schema_path, schema.iter().cloned());
//
//
//     let data_types = get_data_types(&schema);
//     let mut references = References::from_iter(config.get_foreign_keys());
//     let mut filters = Filters::new(&config.get_conditions());
//
//     println!("First pass...");
//     let mut first_pass_filepaths: Vec<(Option<String>, PathBuf)> = Vec::new();
//     for (group, statements) in all_statements.chunk_by(|(_, table, _)| table.clone()).into_iter() {
//         if let Some(ref table) = group {
//             let lines = filter_insert_statements(&mut filters, &mut references, None, table, statements.map(|(_, _, line)| line));
//             let filepath = write_sql_file(&group, config.get_working_dir_for_table(&group), lines);
//             first_pass_filepaths.push((group, filepath));
//         } else {
//             println!("OUT");
//         }
//     }
//
//     println!("Second pass...");
//     let second_pass_filepaths: Vec<PathBuf> = std::iter::once(schema_path).chain(first_pass_filepaths.iter().map(|(table, path)| {
//         match table {
//             None => path.clone(),
//             Some(t) => {
//                 if !config.second_pass_tables.contains(t) {
//                     return path.clone();
//                 }
//                 let statements = read_file_lines(path);
//                 let lines = filter_insert_statements(&mut filters, &mut references, None, t, statements);
//                 write_sql_file(table, &config.working_dir_path, lines)
//             }
//         }
//     })).collect();
//
//     dbg!(&second_pass_filepaths);
//
//     combine_files(second_pass_filepaths.iter(), &config.output_file);
// }
