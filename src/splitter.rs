use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};

use crate::reader;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_table_name_from_comment(comment: String) -> String {
    let table = TABLE_DUMP_RE.captures(&comment).unwrap().get(1).unwrap().as_str().to_string();
    table
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
enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
struct Statement {
    line: String,
    table: Option<String>,
    r#type: StatementType,
}

#[derive(Debug)]
struct TableInfo {
    writer_per_table: HashMap<String, io::BufWriter<File>>,
    filepath_per_table: HashMap<String, PathBuf>,
    schema_writer: io::BufWriter<File>,
    output_dir: PathBuf,
    current_table: Option<String>,
}

impl TableInfo {
    fn new(output_dir: &Path, schema_file: &PathBuf) -> TableInfo {
        let mut schema_path = PathBuf::from(output_dir);
        schema_path.push(schema_file);
        TableInfo{
            writer_per_table: HashMap::new(),
            filepath_per_table: HashMap::new(),
            schema_writer: get_writer(&schema_path),
            output_dir: PathBuf::from(output_dir),
            current_table: None,
        }
    }

    fn add_writer(&mut self, table: &String) {
        if !self.writer_per_table.contains_key(table) {
            let path = self.output_dir.join(table).with_extension("sql");
            println!("Reading table {} into {}", table, path.display());
            self.filepath_per_table.insert(table.to_string(), path.clone());
            self.writer_per_table.insert(table.to_string(), get_writer(&path));
        }
    }

    fn get_writer(&mut self, current_table: &Option<String>) -> Option<&mut BufWriter<File>>{
        if let Some(table) = current_table {
            self.writer_per_table.get_mut(table)
        } else {
            Some(&mut self.schema_writer)
        }
    }

    fn on_new_statement(&mut self, statement: &Statement) {
        if let Some(ref mut writer) = self.get_writer(&statement.table) {
            writer.write_all(statement.line.as_bytes()).expect("Unable to write data");
            writer.write_all(b"\n").expect("Unable to write data");
        }
    }

    fn on_table_end(&mut self, current_table: &Option<String>) {
        if let Some(ref mut writer) = self.get_writer(current_table) {
            writer.flush().expect("Cannot flush buffer");
        }
    }

    fn on_input_end(&mut self, current_table: &Option<String>) {
        if let Some(ref mut writer) = self.get_writer(current_table) {
            writer.flush().expect("Cannot flush buffer");
        }
    }

    fn get_data_files(&mut self) -> Vec<PathBuf> {
        let filepaths: Vec<PathBuf> = self.filepath_per_table.values().cloned().collect();
        filepaths
    }

    fn get_exported_tables(&mut self) -> HashSet<String> {
        let filepaths = HashSet::from_iter(self.filepath_per_table.keys().cloned());
        filepaths
    }

    fn set_current_table(&mut self, table: &str) {
        self.current_table = Some(table.to_owned());
    }
}

fn read_statements(sqldump_filepath: &PathBuf, requested_tables: &HashSet<String>) -> impl Iterator<Item = Statement> {
    let mut current_table: Option<String> = None;
    let annotate_with_table = move |line: String| {
        if line.starts_with("-- Dumping data for table") {
            let table = get_table_name_from_comment(line.clone());
            current_table = Some(table.to_string());
        }
        Statement { line, r#type: StatementType::Insert, table: current_table.clone() }
    };
    reader::read_lines(sqldump_filepath)
        .map_while(Result::ok)
        .map(annotate_with_table)
        .filter(|st| st.table.is_some() && requested_tables.contains(st.table.as_ref().unwrap()))
}

pub fn split(sqldump_filepath: &PathBuf, output_dir: &Path, schema_file: &PathBuf, requested_tables: &HashSet<String>) -> (HashSet<String>, Vec<PathBuf>) {
    let mut current_table: Option<String> = None;

    let annotate_with_table = |line: String| {
        if line.starts_with("-- Dumping data for table") {
            let table = get_table_name_from_comment(line.clone());
            current_table = Some(table.to_string());
        }
        Statement {
            line,
            r#type: StatementType::Insert,
            table: current_table.clone(),
        }
    };

    let mut table_info = TableInfo::new(output_dir, schema_file);
    for statement in read_statements(sqldump_filepath, requested_tables) {
        if let Some(ref table) = statement.table {
            // if requested_tables.contains(table) {
                table_info.add_writer(table);
            // }
        }
        table_info.on_new_statement(&statement);
    }

    (table_info.get_exported_tables(), table_info.get_data_files())
}

pub fn filter_inserts(sqldump_filepath: &PathBuf, field: &str, value: &str, output: &PathBuf) {
    let lines = reader::read_lines(sqldump_filepath);
    let mut writer: io::BufWriter<File> = get_writer(output);
    let mut field_position: Option<usize> = None;

    println!("Filtering table {} with {}={}", sqldump_filepath.display(), field, value);

    for line in lines.map_while(Result::ok) {
        if !line.starts_with("INSERT INTO") {
            writer.write_all(line.as_bytes()).expect("Unable to write data");
            writer.write_all(b"\n").expect("Unable to write data");
        } else {
            if field_position.is_none() {
                let (_, fields) = reader::parse_fields(line.as_str()).unwrap();
                field_position = fields.iter().position(|x| x == &field);
            }

            let (_, values) = reader::parse_values(field_position.unwrap(), line.as_str()).unwrap();
            let current_value = String::from(values.into_iter().nth(field_position.unwrap()).unwrap());
            if current_value == value {
                writer.write_all(line.as_bytes()).expect("Unable to write data");
                writer.write_all(b"\n").expect("Unable to write data");
            }
        }
    }
}
