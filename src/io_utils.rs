use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::io::Write;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter};
use std::path::{Path, PathBuf};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub type WriterType = io::BufWriter<File>;

pub trait LineWriter {
    fn new(filepath: &Path) -> Self;
    fn write_line(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> ;
}

impl LineWriter for WriterType {
    fn new(filename: &Path) -> Self {
        File::create(filename).expect("Unable to create file");
        let file = OpenOptions::new()
            .append(true)
            .open(filename)
            .expect("Unable to open file");

        BufWriter::new(file)
    }

    fn write_line(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        self.write_all(bytes)?;
        self.write_all(b"\n")?;
        Ok(())
    }
}

pub fn combine_files<'a, I: Iterator<Item=&'a PathBuf>>(all_files: I, output: &Path) {
    println!("Combining files");
    let mut output_file = File::create(output).expect("cannot create output file");
    for f in all_files {
        let mut input = File::open(f).expect("cannot open file");
        io::copy(&mut input, &mut output_file).expect("cannot copy file");
    }
}

pub fn read_sql(sqldump_filepath: &Path, requested_tables: &HashSet<String>) -> impl Iterator<Item = (Option<String>, String)> {
    let mut current_table: Option<String> = None;
    let annotate_with_table = move |line: String| {
        if line.starts_with("-- Dumping data for table") {
            let table = TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
            current_table = Some(table);
        }
        (current_table.clone(), line)
    };
    let file = File::open(sqldump_filepath).expect("Cannot open file");
    io::BufReader::new(file).lines()
        .map_while(Result::ok)
        .map(annotate_with_table)
        .filter(|(table, _)| table.is_none() || requested_tables.contains(table.as_ref().unwrap()))
}
