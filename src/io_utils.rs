use lazy_static::lazy_static;
use regex::Regex;
use std::io::Write;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter};
use std::iter;
use std::path::Path;

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

pub fn combine_files<'a, I: Iterator<Item = &'a Path>>(schema_file: &'a Path, data_files: I, output: &Path) {
    println!("Combining files");
    let all_files = iter::once(schema_file).chain(data_files);
    let mut output_file = File::create(output).expect("cannot create output file");
    for f in all_files {
        let mut input = File::open(f).expect("cannot open file");
        io::copy(&mut input, &mut output_file).expect("cannot copy file");
    }
}
