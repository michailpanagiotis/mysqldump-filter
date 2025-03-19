use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::HashSet;
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

fn split(sqldump_filepath: &PathBuf, schema_file: &String, requested_tables: &HashSet<String>) -> HashSet<String> {
    let exported_tables: HashSet<String> = HashSet::new();
    let lines = reader::read_lines(sqldump_filepath).expect("Cannot open file");
    let mut cwriter: Option<io::BufWriter<File>> = Some(get_writer(schema_file));

    for line in lines.map_while(Result::ok) {
        if line.starts_with("-- Dumping data for table") {
            let (table, filename) = get_table_name_from_comment(line.clone());
            if let Some(ref mut writer) = cwriter {
                writer.flush().expect("Cannot flush buffer");
            }
            if requested_tables.contains(&table) {
                println!("Reading table {} into {}", table, filename);
                cwriter = Some(get_writer(&format!("{table}.sql")));
            } else {
                cwriter = None;
            }
        }

        if let Some(ref mut writer) = cwriter {
            writer.write_all(line.as_bytes()).expect("Unable to write data");
            writer.write_all(b"\n").expect("Unable to write data");
        }
    }

    if let Some(ref mut writer) = cwriter {
        writer.flush().expect("Cannot flush buffer");
    }
    exported_tables
}

fn main() {
    let (input_path, requested_tables) = options::parse_options();
    let _exported_tables = split(&input_path, &"schema.sql".to_string(), &requested_tables);
}
