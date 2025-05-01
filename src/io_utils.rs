use config::{Config, Environment, File, FileFormat};
use core::panic;
use itertools::Itertools;
use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::expression_parser::{extract_table, FilterCondition};

#[derive(Debug)]
pub struct Configuration {
    pub input_file: PathBuf,
    pub output_file: PathBuf,
    pub working_dir_path: PathBuf,
    pub temp_dir_path: PathBuf,
    pub requested_tables: HashSet<String>,
    pub second_pass_tables: HashSet<String>,
    pub filter_conditions: Vec<FilterCondition>,
}

impl Configuration {
    pub fn new(
        settings: Config,
        input_file: &Path,
        output_file: &Path,
        working_dir_path: &Path,
        temp_dir_path: &Path,
    ) -> Self {
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

        let filter_conditions: Vec<FilterCondition> = filter_conditions.iter().map(|(table, definition)| FilterCondition::new(table, definition)).collect();

        Configuration {
            input_file: input_file.to_path_buf(),
            output_file: output_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            temp_dir_path: temp_dir_path.to_path_buf(),
            requested_tables,
            second_pass_tables: filter_conditions.iter().filter(|fc| fc.is_foreign_filter()).map(|fc| fc.table.clone()).collect(),
            filter_conditions,
        }
    }

    pub fn get_conditions(&self) -> Vec<&FilterCondition> {
        self.filter_conditions.iter().collect()
    }

    pub fn get_foreign_keys(&self) -> impl Iterator<Item=(String, String)> {
        self.filter_conditions.iter().filter(|fc| fc.is_foreign_filter()).map(|fc| fc.get_foreign_key() )
    }

    pub fn get_schema_path(&self) -> PathBuf {
        self.working_dir_path.join("INFORMATION_SCHEMA").with_extension("sql")
    }

    pub fn get_working_file_path(&self) -> PathBuf {
        self.temp_dir_path.join("INTERIM").with_extension("sql")
    }

    pub fn get_working_dir_for_table(&self, table: &Option<String>) -> &PathBuf {
        match table {
            None => &self.working_dir_path,
            Some(t) => {
                match &self.second_pass_tables.contains(t) {
                    false => &self.working_dir_path,
                    true => &self.temp_dir_path,
                }
            }
        }
    }
}

pub fn read_config(
    config_file: &Path,
    input_file: &Path,
    output_file: &Path,
    working_dir_path: &Path,
    temp_dir_path: &Path,
) -> Configuration {
    let settings = Config::builder()
        .add_source(File::new(config_file.to_str().expect("invalid config path"), FileFormat::Json))
        .add_source(Environment::with_prefix("MYSQLDUMP_FILTER"))
        .build()
        .unwrap();

    Configuration::new(settings, input_file, output_file, working_dir_path, temp_dir_path)
}

#[derive(Clone)]
pub enum StatementSection {
    Header,
    Insert,
    Footer,
}

struct Statements<B> {
    buf: B,
    cur_table: Option<String>,
    last_statement: Option<String>,
    section: StatementSection,
}

impl<B: BufRead> Statements<B> {
    fn capture_table(&mut self, cur_statement: &str) {
        if self.last_statement.as_ref().is_some_and(|x| x.starts_with("UNLOCK TABLES;")) {
            self.cur_table = None;
            self.section = StatementSection::Footer;
        }
        if cur_statement.starts_with("--\n-- Dumping data for table") {
            let table = extract_table(cur_statement);
            if self.cur_table.is_none() {
                self.section = StatementSection::Insert;
            }
            println!("reading table {}", &table);
            self.cur_table = Some(table);
        }
        self.last_statement = Some(cur_statement.to_string());
    }
}

impl<B: BufRead> Iterator for Statements<B> {
    type Item = (StatementSection, Option<String>, String);
    fn next(&mut self) -> Option<(StatementSection, Option<String>, String)> {
        let mut buf8 = vec![];
        while {
            let first_read_bytes = self.buf.read_until(b';', &mut buf8).ok()?;
            let second_read_bytes = if first_read_bytes > 0 { self.buf.read_until(b'\n', &mut buf8).ok()? } else { 0 };
            second_read_bytes > 1
        } {}
        match buf8.is_empty() {
            true => None,
            false => {
                let statement = String::from_utf8(buf8).ok()?.split('\n').filter(|x| !x.is_empty()).map(|x| x.trim()).join("\n") + "\n";
                self.capture_table(&statement);
                Some((self.section.clone(), self.cur_table.clone(), statement))
            }
        }
    }
}

impl<B: BufRead> itertools::PeekingNext for Statements<B> {
    fn peeking_next<F>(&mut self, accept: F) -> Option<Self::Item>
      where Self: Sized,
            F: FnOnce(&Self::Item) -> bool
    {
        let last_statement = self.next();
        let st = last_statement.as_ref()?;
        if !accept(st) {
            return None;
        }
        last_statement
    }
}

pub fn read_file_lines(filepath: &Path) -> impl Iterator<Item=String> + use<> {
    let file = fs::File::open(filepath).expect("Cannot open file");
    io::BufReader::new(file).lines()
        .map_while(Result::ok)
}

pub fn read_sql_file(sqldump_filepath: &Path, requested_tables: &HashSet<String>) -> (Vec<String>, impl Iterator<Item = (StatementSection, Option<String>, String)>) {
    let f1 = fs::File::open(sqldump_filepath).expect("Cannot open file");
    let mut iter = Statements {
        buf: io::BufReader::new(f1),
        cur_table: None,
        last_statement: None,
        section: StatementSection::Header,
    };
    let schema: Vec<String> = iter.by_ref().peeking_take_while(|(_, table,_)| table.is_none()).map(|(_, _, line)| line).collect();
    let inserts = iter.filter(|(_, table, _)| {
        table.is_none() || table.as_ref().is_some_and(|t| requested_tables.contains(t))
    });

    (schema, inserts)
}

pub fn write_file_lines<I: Iterator<Item=String>>(filepath: &PathBuf, lines: I) -> PathBuf {
    fs::File::create(filepath).unwrap_or_else(|_| panic!("Unable to create file {}", &filepath.display()));
    let file = fs::OpenOptions::new()
        .append(true)
        .open(filepath)
        .expect("Unable to open file");

    let mut writer = BufWriter::new(file);

    println!("Writing to {}", &filepath.display());

    for line in lines {
        writer.write_all(line.as_bytes()).expect("Cannot write to file");
    };

    writer.flush().expect("Cannot flush buffer");
    filepath.clone()
}

pub fn write_sql_file<I: Iterator<Item=String>>(table: &Option<String>, working_dir_path: &Path, lines: I) -> PathBuf {
    let filepath = match table {
        Some(x) => working_dir_path.join(x).with_extension("sql"),
        None => working_dir_path.join("INFORMATION_SCHEMA").with_extension("sql"),
    };
    write_file_lines(&filepath, lines);
    filepath
}

pub fn combine_files<'a, I: Iterator<Item=&'a PathBuf>>(all_files: I, output: &Path) {
    println!("Combining files");
    let mut output_file = fs::File::create(output).expect("cannot create output file");
    for f in all_files {
        dbg!(f);
        let mut input = fs::File::open(f).expect("cannot open file");
        io::copy(&mut input, &mut output_file).expect("cannot copy file");
    }
}
