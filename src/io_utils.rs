use config::{Config, Environment, File, FileFormat};
use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::expression_parser::{get_table_from_comment, FilterCondition};

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

pub fn read_file_lines(filepath: &Path) -> impl Iterator<Item=String> + use<> {
    let file = fs::File::open(filepath).expect("Cannot open file");
    io::BufReader::new(file).lines()
        .map_while(Result::ok)
}

pub fn read_sql_file(sqldump_filepath: &Path, requested_tables: &HashSet<String>) -> (Vec<String>, impl Iterator<Item = (Option<String>, String)>) {
    let mut cur_table: Option<String> = None;
    let mut iter = read_file_lines(sqldump_filepath)
        .map(move |line| {
            if let Some(t) = get_table_from_comment(&line) {
                cur_table = Some(t);
            }
            (cur_table.clone(), line)
        })
        .filter(|(table, _)| table.is_none() || table.as_ref().is_some_and(|t| requested_tables.contains(t)));

    let schema: Vec<String> = iter.by_ref().take_while(|(table,_)| table.is_none()).map(|(_, line)| line + "\n").collect();
    (schema, iter)
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
        writer.write_all(b"\n").expect("Cannot write to file");
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
