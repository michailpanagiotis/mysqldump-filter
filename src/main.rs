use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};

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

fn get_files(all_tables: &HashSet<String>) -> HashMap<String, File> {
    let mut files: HashMap<String, File> = HashMap::new();
    for table in all_tables.iter() {
        let filename = format!("{table}.sql");
        println!("{} {}", table, filename);

        File::create(&filename).expect("Unable to create file");
        let file = OpenOptions::new()
            .append(true)
            .open(&filename)
            .expect("Unable to open file");

        files.insert(table.clone(), file);
    }
    files
}

fn get_writers(all_tables: &HashSet<String>) -> HashMap<String, BufWriter<File>> {
    let mut writers: HashMap<String, io::BufWriter<File>> = HashMap::new();
    for table in all_tables.iter() {
        let filename = format!("{table}.sql");
        println!("{} {}", table, filename);

        File::create(&filename).expect("Unable to create file");
        let file = OpenOptions::new()
            .append(true)
            .open(&filename)
            .expect("Unable to open file");

        writers.insert(table.clone(), BufWriter::new(file));
    }
    writers
}

fn main() {
    let (all_tables, input_path) = options::parse_options();
    if let Ok(lines) = reader::read_lines(&input_path) {
        let mut files = get_files(&all_tables);
        let mut writers = get_writers(&all_tables);
        let mut current_writer: Option<&mut io::BufWriter<File>> = None;

        for line in lines.map_while(Result::ok) {
            if line.starts_with("-- Dumping data for table") {
                let (table, filename) = get_table_name_from_comment(line.clone());
                if all_tables.contains(&table) {
                    println!("Reading table {} into {}", table, filename);
                    current_writer = writers.get_mut(&table);
                } else {
                    println!("Omitting table {}", table);
                    current_writer = None;
                }
            }

            if let Some(ref mut writer) = current_writer {
                writer.write_all(line.as_bytes()).expect("Unable to write data");
                writer.write_all(b"\n").expect("Unable to write data");
            }
        }
    }
}
