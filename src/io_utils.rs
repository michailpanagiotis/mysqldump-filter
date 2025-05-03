use config::{Config, Environment, File, FileFormat};
use core::panic;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::expression_parser::{extract_table, FilterCondition};

#[derive(Debug)]
pub struct Configuration {
    pub allowed_tables: HashSet<String>,
    filter_config: Vec<(String, String)>,
}

impl Configuration {
    pub fn new(
        settings: Config,
    ) -> Self {
        let allowed_tables: HashSet<_> = settings
            .get_array("allow_data_on_tables")
            .expect("no key 'allow_data_on_tables' in config")
            .iter().map(|x| x.to_string()).collect();

        let filter_inserts = settings
                .get_table("filter_inserts")
                .expect("no key 'filter_inserts' in config");

        let filter_config: Vec<_> = filter_inserts
            .iter()
            .flat_map(|(table, conditions)| {
                conditions.clone().into_array().expect("cannot parse config array").into_iter().map(move |x| {
                    (table.clone(), x.to_string())
                })
            }).collect();

        Configuration {
            allowed_tables,
            filter_config,
        }
    }

    pub fn get_conditions(&self, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<FilterCondition> {
        self.filter_config.iter().map(|(table, definition)| FilterCondition::new(table, definition, data_types)).collect()
    }
}

impl From<&PathBuf> for Configuration {
    fn from(config_file: &PathBuf) -> Self {
        let settings = Config::builder()
            .add_source(File::new(config_file.to_str().expect("invalid config path"), FileFormat::Json))
            .add_source(Environment::with_prefix("MYSQLDUMP_FILTER"))
            .build()
            .unwrap();

        Configuration::new(settings)
    }
}

struct SqlStatements {
    buf: io::BufReader<fs::File>,
    cur_table: Option<String>,
    last_statement: Option<String>,
    allowed_tables: HashSet<String>,
}

impl SqlStatements {
    pub fn from_file(sqldump_filepath: &Path, allowed_tables: &HashSet<String>) -> Self {
        let file = fs::File::open(sqldump_filepath).expect("Cannot open file");
        SqlStatements {
            buf: io::BufReader::new(file),
            cur_table: None,
            last_statement: None,
            allowed_tables: allowed_tables.clone(),
        }
    }

    fn capture_table(&mut self, cur_statement: &str) {
        if self.last_statement.as_ref().is_some_and(|x| x.starts_with("UNLOCK TABLES;")) {
            self.cur_table = None;
        }
        if cur_statement.starts_with("--\n-- Dumping data for table") {
            let table = extract_table(cur_statement);
            println!("reading table {}", &table);
            self.cur_table = Some(table);
        }
        self.last_statement = Some(cur_statement.to_string());
    }

    fn next_statement(&mut self) -> Option<(Option<String>, String)> {
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
                Some((self.cur_table.clone(), statement))
            }
        }
    }
}

impl Iterator for SqlStatements {
    type Item = (Option<String>, String);
    fn next(&mut self) -> Option<(Option<String>, String)> {
        let (mut table, mut line) = self.next_statement()?;
        while table.as_ref().is_some_and(|t| !self.allowed_tables.contains(t)) {
            (table, line) = self.next_statement()?;
        }
        Some((table, line))
    }
}

pub fn get_data_types<I: Iterator<Item=(Option<String>, String)>>(statements: I) -> HashMap<String, sqlparser::ast::DataType> {
    let mut data_types = HashMap::new();
    let dialect = MySqlDialect {};
    let sql: String = statements
        .take_while(|(table,_)| table.is_none())
        .map(|(_, line)| line)
        .filter(|x| !x.starts_with("--"))
        .map(|x| x.replace('\n', " "))
        .collect();
    let ast = SqlParser::parse_sql(&dialect, &sql).unwrap();
    for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
        if let sqlparser::ast::Statement::CreateTable(ct) = st {
            for column in ct.columns.into_iter() {
                data_types.insert(ct.name.0[0].as_ident().unwrap().value.to_string() + "." + column.name.value.as_str(), column.data_type);
            }
        }
    }
    data_types
}

pub fn read_sql_file(sqldump_filepath: &Path, allowed_tables: &HashSet<String>) -> (impl Iterator<Item = (Option<String>, String)>, HashMap<String, sqlparser::ast::DataType>) {
    let iter = SqlStatements::from_file(sqldump_filepath, allowed_tables);
    let data_types = get_data_types(SqlStatements::from_file(sqldump_filepath, allowed_tables));

    (iter, data_types)
}

pub fn write_sql_file<I: Iterator<Item=(Option<String>, String)>>(filepath: &PathBuf, lines: I) -> PathBuf {
    fs::File::create(filepath).unwrap_or_else(|_| panic!("Unable to create file {}", &filepath.display()));
    let file = fs::OpenOptions::new()
        .append(true)
        .open(filepath)
        .expect("Unable to open file");

    let mut writer = BufWriter::new(file);

    println!("Writing to {}", &filepath.display());

    for line in lines.map(|(_, line)| line) {
        writer.write_all(line.as_bytes()).expect("Cannot write to file");
    };

    writer.flush().expect("Cannot flush buffer");
    filepath.clone()
}
