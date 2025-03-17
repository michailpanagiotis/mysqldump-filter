use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::env;
use tempfile::tempfile;
use std::collections::HashMap;
use lazy_static::lazy_static;
use regex::Regex;

mod reader;

type Writer = io::BufWriter<File>;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_write_buffer<P: AsRef<Path>>(filename: P) -> io::BufWriter<File> {
    // let file = tempfile().expect("Unable to open temporary file");

    File::create(&filename).expect("Unable to create file");
    let file = OpenOptions::new()
        .append(true)
        .open(&filename)
        .expect("Unable to open file");


    return BufWriter::new(file);
}

fn get_table_name_from_comment(comment: &String) -> (String, String) {
    let caps = TABLE_DUMP_RE.captures(&comment).unwrap();
    let table = caps.get(1).unwrap().as_str().to_string();
    let filename = format!("{table}.sql");
    return (table, filename);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    dbg!(file_path);

    let mut writers: HashMap<String, File> = HashMap::new();
    let mut wbuffers: HashMap<String, Writer> = HashMap::new();

    if let Ok(lines) = reader::read_lines(file_path) {
        let mut buf = get_write_buffer("schema.sql");
        // Consumes the iterator, returns an (Optional) String
        for line in lines.map_while(Result::ok) {
            if line.starts_with("-- Dumping data for table") {
                let (table, filename) = get_table_name_from_comment(&line);
                println!("Reading table {} into {}", table, filename);
                let file = tempfile().expect("Unable to open temporary file");
                // writers.insert(table.clone(), file);
                wbuffers.insert(table.clone(), BufWriter::new(file));
                dbg!(&wbuffers[&table]);

                buf = get_write_buffer(&filename);
            }
            // buf.write_all(line.as_bytes()).expect("Unable to write data");
            // buf.write_all(b"\n").expect("Unable to write data");
        }
    }
}
