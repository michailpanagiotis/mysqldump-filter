use config::{Config, Environment, File, FileFormat};
use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

pub fn read_settings(config_file: &Path) -> (HashSet<String>, Vec<(String, String)>) {
    let settings = Config::builder()
        .add_source(File::new(config_file.to_str().expect("invalid config path"), FileFormat::Json))
        .add_source(Environment::with_prefix("MYSQLDUMP_FILTER"))
        .build()
        .unwrap();

    let requested_tables: HashSet<_> = settings
        .get_array("allow_data_on_tables")
        .expect("no key 'allow_data_on_tables' in config")
        .iter().map(|x| x.to_string()).collect();

    let filter_inserts = settings
            .get_table("filter_inserts")
            .expect("no key 'filter_inserts' in config");

    let filter_conditions: Vec<_> = filter_inserts
        .iter()
        .flat_map(|(table, conditions)| {
            conditions.clone().into_array().expect("cannot parse config array").into_iter().map(move |x| {
                (table.clone(), x.to_string())
            })
        }).collect();

    (requested_tables, filter_conditions)
}

pub fn read_file(filepath: &Path) -> impl Iterator<Item=String> + use<> {
    let file = fs::File::open(filepath).expect("Cannot open file");
    io::BufReader::new(file).lines()
        .map_while(Result::ok)
}

pub struct Writer {
    filepath: PathBuf,
    inner: io::BufWriter<fs::File>
}

impl Writer {
    pub fn new(table: &Option<String>, working_dir: &Path, default: &Path) -> Self {
        let filepath = match table {
            Some(x) => working_dir.join(x).with_extension("sql"),
            None => default.to_path_buf()
        };

        fs::File::create(&filepath).unwrap_or_else(|_| panic!("Unable to create file {}", &filepath.display()));
        let file = fs::OpenOptions::new()
            .append(true)
            .open(&filepath)
            .expect("Unable to open file");

        Writer {
            filepath: filepath.to_path_buf(),
            inner: BufWriter::new(file)
        }
    }

    pub fn combine_files<'a, I: Iterator<Item=&'a PathBuf>>(all_files: I, output: &Path) {
        println!("Combining files");
        let mut output_file = fs::File::create(output).expect("cannot create output file");
        for f in all_files {
            let mut input = fs::File::open(f).expect("cannot open file");
            io::copy(&mut input, &mut output_file).expect("cannot copy file");
        }
    }

    pub fn write_line(&mut self, line: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write_all(line)?;
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
