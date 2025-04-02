use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};

use crate::reader;
use crate::config::{Config, FilterCondition};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_writer(filename: &PathBuf) -> BufWriter<File> {
    File::create(filename).expect("Unable to create file");
    let file = OpenOptions::new()
        .append(true)
        .open(filename)
        .expect("Unable to open file");

    BufWriter::new(file)
}


#[derive(Debug)]
struct TableDataWriter {
    value_position_per_field: Option<HashMap<String, usize>>,
    filepath: PathBuf,
    writer: io::BufWriter<File>,
    filters: Option<Vec<FilterCondition>>,
}

impl TableDataWriter {
    fn new(table: &String, output_dir: &Path, filter_per_table: &HashMap<String, Vec<FilterCondition>>) -> TableDataWriter {
        let path = output_dir.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, path.display());
        TableDataWriter {
            value_position_per_field: None,
            filepath: path.clone(),
            writer: get_writer(&path),
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
struct Parser {
    config: Config,
    writer_per_table: HashMap<String, TableDataWriter>,
    schema_writer: io::BufWriter<File>,
}

impl Parser {
    fn new(config: Config, output_dir: &Path, schema_file: &PathBuf) -> Parser {
        Parser{
            config,
            writer_per_table: HashMap::new(),
            schema_writer: get_writer(&PathBuf::from(output_dir).join(schema_file)),
        }
    }

    fn register_table(&mut self, table: &String) {
        self.writer_per_table.insert(table.to_string(), TableDataWriter::new(
            table,
            &self.config.output_dir,
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

    fn get_data_files(&mut self) -> Vec<PathBuf> {
        let filepaths: Vec<PathBuf> = self.writer_per_table.values().map(|x| x.filepath.clone()).collect();
        filepaths
    }

    fn get_exported_tables(&mut self) -> HashSet<String> {
        let filepaths = HashSet::from_iter(self.writer_per_table.keys().cloned());
        filepaths
    }
}

pub fn split(config: Config) -> (HashSet<String>, Vec<PathBuf>) {
    dbg!(&config);
    let mut table_info = Parser::new(config.clone(), &config.output_dir, &config.schema_file);
    for statement in reader::read_statements(&config.input_file, &config.requested_tables, true) {
        table_info.on_new_statement(&statement);
    }

    table_info.on_input_end();

    (table_info.get_exported_tables(), table_info.get_data_files())
}
