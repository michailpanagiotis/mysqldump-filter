use lazy_static::lazy_static;
use regex::Regex;
use std::io::Write;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::reader;
use crate::io_utils;
use crate::config::{Config, FilterCondition};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Debug)]
struct TableDataWriter {
    value_position_per_field: Option<HashMap<String, usize>>,
    filepath: PathBuf,
    writer: io_utils::Writer,
    filters: Option<Vec<FilterCondition>>,
}

impl TableDataWriter {
    fn new(table: &String, working_dir: &Path, filter_per_table: &HashMap<String, Vec<FilterCondition>>) -> TableDataWriter {
        let filepath = working_dir.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, filepath.display());
        let writer = io_utils::get_file_writer(&filepath);
        TableDataWriter {
            value_position_per_field: None,
            filepath,
            writer,
            filters: filter_per_table.get(table).cloned(),
        }
    }

    fn try_determine_field_positions(&mut self, statement: &reader::Statement) {
        if self.filters.is_some() && self.value_position_per_field.is_none() {
            self.value_position_per_field = statement.get_field_positions();
            let Some(ref value_position_per_field) = self.value_position_per_field else { return };
            assert_eq!(value_position_per_field.len(), 44);
        }
    }

    fn should_drop_statement(&self, statement: &reader::Statement) -> bool {
        if !statement.is_insert(){ return false };

        let Some(ref filters) = self.filters else { return false };
        let Some(ref value_position_per_field) = self.value_position_per_field else { return false };

        let values = statement.get_values();

        let failed_filters = filters.iter().filter(|f| {
            let position = value_position_per_field[&f.field];
            !f.test(&values[position])
        });

        failed_filters.count() > 0
    }

    fn on_new_statement(&mut self, statement: &reader::Statement) {
        if statement.is_insert() {
            self.try_determine_field_positions(statement);
        }
        if !self.should_drop_statement(statement) {
            self.writer.write_all(statement.as_bytes()).expect("Unable to write data");
            self.writer.write_all(b"\n").expect("Unable to write data");
        }
    }

    fn flush(&mut self) {
        self.writer.flush().expect("Cannot flush buffer");
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    writer_per_table: HashMap<String, TableDataWriter>,
    schema_writer: io_utils::Writer,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            writer_per_table: HashMap::new(),
            schema_writer: io_utils::get_file_writer(&config.schema_file),
        }
    }

    fn register_table(&mut self, table: &String) {
        self.writer_per_table.insert(table.to_string(), TableDataWriter::new(
            table,
            &self.config.working_dir_path,
            &self.config.filter_per_table,
        ));
    }

    fn on_new_statement(&mut self, statement: &reader::Statement) {
        match statement.get_table() {
            None => {
                self.schema_writer.write_all(statement.as_bytes()).expect("Unable to write data");
                self.schema_writer.write_all(b"\n").expect("Unable to write data");
            },
            Some(table) => {
                if !self.writer_per_table.contains_key(table) {
                    self.register_table(table);
                }
                let info = self.writer_per_table.get_mut(table).expect("Cannot find table info");
                info.on_new_statement(statement);
            },
        };
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for info in self.writer_per_table.values_mut() {
            info.flush();
        }
    }

    fn get_data_files(&mut self) -> Vec<&Path> {
        self.writer_per_table.values().map(|x| x.filepath.as_path()).collect::<Vec<&Path>>()
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        for statement in reader::read_statements(input_file, &self.config.requested_tables, true) {
            self.on_new_statement(&statement);
        }
        self.on_input_end();
        io_utils::combine_files(
            &self.config.schema_file,
            self.get_data_files().into_iter(),
            output_file,
        );
    }
}
