use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::HashSet;
use std::path::PathBuf;

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

pub fn split(sqldump_filepath: &PathBuf, output_dir: &PathBuf, schema_file: &String, requested_tables: &HashSet<String>) -> HashSet<String> {
    let exported_tables: HashSet<String> = HashSet::new();
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
                let mut path = PathBuf::from(output_dir);
                path.push(format!("{table}.sql"));
                println!("Reading table {} into {}", table, path.display());
                cwriter = Some(get_writer(&path));
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
