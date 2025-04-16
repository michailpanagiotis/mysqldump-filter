use lazy_static::lazy_static;
use regex::Regex;
use std::io::Write;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};

use crate::sql_statement::Statement;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub struct SQLWriter {
    filepath: PathBuf,
    inner: io::BufWriter<File>
}

impl SQLWriter {
    pub fn new(table: &Option<String>, working_dir: &Path, default: &Path) -> Self {
        let filepath = match table {
            Some(x) => working_dir.join(x).with_extension("sql"),
            None => working_dir.join(default)
        };

        File::create(&filepath).expect(format!("Unable to create file {}", &filepath.display()).as_str());
        let file = OpenOptions::new()
            .append(true)
            .open(&filepath)
            .expect("Unable to open file");

        SQLWriter {
            filepath: filepath.to_path_buf(),
            inner: BufWriter::new(file)
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

    pub fn write_statement(&mut self, statement: &Statement) -> Result<(), std::io::Error> {
        self.inner.write_all(statement.as_bytes())?;
        self.inner.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.inner.flush()?;
        Ok(())
    }

    pub fn get_filepath(&self) -> PathBuf {
        self.filepath.clone()
    }
}
