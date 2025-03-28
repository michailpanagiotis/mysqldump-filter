use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};
use std::thread::current;

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

pub fn split(sqldump_filepath: &PathBuf, output_dir: &Path, schema_file: &PathBuf, requested_tables: &HashSet<String>) -> (HashSet<String>, Vec<PathBuf>) {
    let mut exported_tables: HashSet<String> = HashSet::new();
    let mut writer_per_table: HashMap<String, io::BufWriter<File>> = HashMap::new();
    let mut data_files: Vec<PathBuf> = Vec::new();
    let mut current_table: Option<String> = None;

    let lines = reader::read_lines(sqldump_filepath);

    let mut schema_path = PathBuf::from(output_dir);
    schema_path.push(schema_file);

    let mut cwriter: Option<io::BufWriter<File>> = Some(get_writer(&schema_path));

    for line in lines.map_while(Result::ok) {
        if line.starts_with("-- Dumping data for table") {
            if let Some(ref mut writer) = cwriter {
                writer.flush().expect("Cannot flush buffer");
            }
            let table = get_table_name_from_comment(line.clone());
            if requested_tables.contains(&table) {
                let path = PathBuf::from(output_dir).join(&table).with_extension("sql");
                data_files.push(path.to_owned());
                println!("Reading table {} into {}", table, path.display());
                writer_per_table.insert(table.to_string(), get_writer(&path));
                cwriter = Some(get_writer(&path));
                current_table = Some(table.to_string());
                exported_tables.insert(table.to_string());
            } else {
                cwriter = None;
                current_table = None;
            }
        }

        if let Some(ref mut table) = current_table {
            let current_writer = writer_per_table.get_mut(table).expect("unknown writer");
            current_writer.write_all(line.as_bytes()).expect("Unable to write data");
            current_writer.write_all(b"\n").expect("Unable to write data");
        }
        //
        //
        // if let Some(ref mut writer) = cwriter {
        //     writer.write_all(line.as_bytes()).expect("Unable to write data");
        //     writer.write_all(b"\n").expect("Unable to write data");
        // }
    }

    if let Some(ref mut writer) = cwriter {
        writer.flush().expect("Cannot flush buffer");
    }
    (exported_tables, data_files)
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
