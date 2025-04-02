use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter};
use std::iter;
use std::path::Path;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub type Writer = io::BufWriter<File>;

pub fn get_file_writer(filename: &Path) -> BufWriter<File> {
    File::create(filename).expect("Unable to create file");
    let file = OpenOptions::new()
        .append(true)
        .open(filename)
        .expect("Unable to open file");

    BufWriter::new(file)
}

fn append_to_file(input_path: &Path, mut output_file: &File) {
    let mut input = File::open(input_path).expect("cannot open file");
    io::copy(&mut input, &mut output_file).expect("cannot copy file");
}

pub fn combine_files<'a, I: Iterator<Item = &'a Path>>(schema_file: &'a Path, data_files: I, output: &Path) {
    let all_files = iter::once(schema_file).chain(data_files);
    let output_file = File::create(output).expect("cannot create output file");
    for f in all_files {
        append_to_file(f, &output_file);
    }
}
