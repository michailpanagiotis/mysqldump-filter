use std::fs;
use crate::expression_parser::get_data_types;
use crate::io_utils::{read_sql_file, write_file_lines, Configuration};
use crate::references::References;
use crate::filters::{filter_statements, Filters};

pub fn parse_input_file(config: &Configuration) {
    println!("Capturing schema...");
    let (all_statements, data_types) = read_sql_file(&config.input_file, &config.allowed_tables);

    let mut references = References::from_iter(config.get_foreign_keys());
    let mut filters = Filters::new(&config.get_conditions());

    println!("First pass...");
    let filtered = filter_statements(&mut filters, &mut references, None, all_statements);
    write_file_lines(&config.get_working_file_path(), filtered.map(|(_, line)| line));

    fs::rename(config.get_working_file_path(), &config.output_file).expect("cannot rename output file");
}
