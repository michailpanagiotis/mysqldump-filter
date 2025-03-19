use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;

mod options;
mod reader;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_table_name_from_comment(comment: String) -> (String, String) {
    let caps = TABLE_DUMP_RE.captures(&comment).unwrap();
    let table = caps.get(1).unwrap().as_str().to_string();
    let filename = format!("{table}.sql");
    (table, filename)
}

fn get_writer(filename: &String) -> BufWriter<File> {
    File::create(filename).expect("Unable to create file");
    let file = OpenOptions::new()
        .append(true)
        .open(filename)
        .expect("Unable to open file");

    BufWriter::new(file)
}

fn get_writers(requested_tables: &HashSet<String>) -> HashMap<String, BufWriter<File>> {
    let mut writers: HashMap<String, io::BufWriter<File>> = HashMap::new();
    for table in requested_tables.iter() {
        let filename = format!("{table}.sql");
        writers.insert(table.clone(), get_writer(&filename));
    }
    writers
}

fn split(sqldump_filepath: &PathBuf, requested_tables: &HashSet<String>) -> HashSet<String> {
    let exported_tables: HashSet<String> = HashSet::new();
    let lines = reader::read_lines(sqldump_filepath).expect("Cannot open file");
    let mut writers = get_writers(requested_tables);
    let mut base_writer: BufWriter<File> = get_writer(&"schema.sql".to_string());
    let mut current_writer: Option<&mut io::BufWriter<File>> = Some(&mut base_writer);

    for line in lines.map_while(Result::ok) {
        if line.starts_with("-- Dumping data for table") {
            let (table, filename) = get_table_name_from_comment(line.clone());
            if let Some(value) = current_writer {
                value.flush().expect("Cannot flush buffer");
            }
            if requested_tables.contains(&table) {
                println!("Reading table {} into {}", table, filename);
                current_writer = writers.get_mut(&table);
            } else {
                current_writer = None;
            }
        }

        if let Some(ref mut writer) = current_writer {
            writer.write_all(line.as_bytes()).expect("Unable to write data");
            writer.write_all(b"\n").expect("Unable to write data");
        }
    }

    if let Some(value) = current_writer {
        value.flush().expect("Cannot flush buffer");
    }
    exported_tables
}

fn main() {
    let (input_path, requested_tables) = options::parse_options();
    let _exported_tables = split(&input_path, &requested_tables);
}
