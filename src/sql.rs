use itertools::Itertools;
use lazy_static::lazy_static;
use nom::{
  IResult,
  Parser,
  branch::alt,
  bytes::complete::{escaped, is_not, take_until, tag, take_till},
  character::complete::{char, one_of, none_of},
  multi::{separated_list0, separated_list1},
  sequence::{delimited, preceded},
};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"--\n-- Dumping data for table `([^`]*)`").unwrap();
}

pub fn extract_table(sql_comment: &str) -> String {
    TABLE_DUMP_RE.captures(sql_comment).unwrap().get(1).unwrap().as_str().to_string()
}

pub fn get_field_positions(insert_statement: &str) -> HashMap<String, usize> {
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, insert_statement).unwrap();

    let st = ast.first().unwrap();
    let sqlparser::ast::Statement::Insert(x) = st else { panic!("stop") };

    let positions: HashMap<String, usize> = HashMap::from_iter(
        x.columns.iter().enumerate().map(|(idx, c)| (c.value.to_owned(), idx))
    );

    positions
}

pub fn get_values(insert_statement: &str) -> Vec<&str> {
    let mut parser = preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
        separated_list1(
            one_of(",)"),
            alt((
                // quoted value
                delimited(
                    tag("'"),
                    // escaped or empty
                    alt((
                        escaped(none_of("\\\'"), '\\', tag("'")),
                        tag("")
                    )),
                    tag("'")
                ),
                // unquoted value
                take_till(|c| c == ','),
            )),
        )
    );
    let res: IResult<&str, Vec<&str>> = parser.parse(insert_statement);
    let (_, values) = res.unwrap_or_else(|_| panic!("cannot parse values for {}", &insert_statement));
    values
}

pub fn parse_insert_fields(insert_statement: &str) -> HashMap<String, usize> {
    let mut parser = preceded(
        take_until("("), preceded(take_until("`"), take_until(")"))
    ).and_then(
      separated_list0(
          tag(", "),
          delimited(char('`'), is_not("`"), char('`')),
      )
    );
    let res: IResult<&str, Vec<&str>> = parser.parse(insert_statement);
    let (_, fields) = res.expect("cannot parse fields");
    HashMap::from_iter(
        fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
    )
}

pub struct SqlStatements {
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
                let statement: String = String::from_utf8(buf8).ok()?.split('\n').filter(|x| !x.is_empty()).map(|x| x.trim()).map(|x| x.to_owned() + "\n").collect();
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

pub fn get_data_types(sqldump_filepath: &Path) -> HashMap<String, sqlparser::ast::DataType> {
    let file = fs::File::open(sqldump_filepath).expect("Cannot open file");
    let sql = io::BufReader::new(file).lines().map_while(|x| x.ok()).filter(|x| !x.starts_with("--")).take_while(|x| !x.starts_with("INSERT")).join("\n");
    let mut data_types = HashMap::new();
    let dialect = MySqlDialect {};
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

pub fn read_sql_file(sqldump_filepath: &Path, allowed_tables: &HashSet<String>) -> impl Iterator<Item = (Option<String>, String)> {
    SqlStatements::from_file(sqldump_filepath, allowed_tables)
}

pub fn write_sql_file<I: Iterator<Item=(Option<String>, String)>>(filepath: &Path, lines: I) -> PathBuf {
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
    filepath.to_path_buf()
}
