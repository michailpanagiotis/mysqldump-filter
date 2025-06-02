use itertools::Itertools;
use lazy_static::lazy_static;
use nom::{
  branch::alt, bytes::complete::{escaped, is_not, tag, take_till, take_until}, character::complete::{char, none_of, one_of}, multi::{separated_list0, separated_list1}, sequence::{delimited, preceded}, IResult, Parser
};
use regex::Regex;
use std::{collections::{HashMap, HashSet}, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub fn extract_table(sql_comment: &str) -> String {
    TABLE_DUMP_RE.captures(sql_comment).unwrap().get(1).unwrap().as_str().to_string()
}

pub fn get_columns(insert_statement: &str) -> Vec<String> {
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, insert_statement).unwrap();

    let st = ast.first().unwrap();
    let sqlparser::ast::Statement::Insert(x) = st else { panic!("stop") };

    x.columns.iter().map(|x| x.value.to_owned()).collect()
}

pub fn get_column_positions(insert_statement: &str) -> HashMap<String, usize> {
    get_columns(insert_statement).iter().enumerate().map(|(idx, c)| (c.to_owned(), idx)).collect()
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

pub struct PlainStatements {
    buf: io::BufReader<fs::File>,
}

impl PlainStatements {
    pub fn from_file(sqldump_filepath: &Path) -> Result<Self, anyhow::Error> {
        let file = fs::File::open(sqldump_filepath)?;
        Ok(PlainStatements {
            buf: io::BufReader::new(file),
        })
    }

    pub fn is_full_line(line: &str) -> bool {
        if line.ends_with(";\n") {
            return true;
        }

        if line.starts_with("\n") {
            return true;
        }

        if line.starts_with("--") {
            return true;
        }

        false
    }
}

impl Iterator for PlainStatements {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        let mut buf: String = String::new();

        while {
            let read_bytes = self.buf.read_line(&mut buf).ok()?;
            read_bytes > 0 && !PlainStatements::is_full_line(&buf)
        } {}

        match buf.is_empty() {
            true => None,
            false => {
                Some(buf)
            }
        }
    }
}

pub struct SqlStatementsWithTable {
    buf: PlainStatements,
    cur_table: Option<String>,
    last_statement: Option<String>,
    allowed_tables: Option<HashSet<String>>,
}

impl SqlStatementsWithTable {
    pub fn from_file(sqldump_filepath: &Path, allowed_tables: &Option<HashSet<String>>, curr_table: &Option<String>) -> Self {
        let buf = PlainStatements::from_file(sqldump_filepath).expect("Cannot open file");
        SqlStatementsWithTable {
            buf,
            cur_table: curr_table.clone(),
            last_statement: None,
            allowed_tables: allowed_tables.clone(),
        }
    }

    fn capture_table(&mut self, cur_statement: &str) {
        if self.last_statement.as_ref().is_some_and(|x| x.starts_with("UNLOCK TABLES;")) {
            self.cur_table = None;
        }
        if cur_statement.starts_with("-- Dumping data for table") {
            let table = extract_table(cur_statement);
            println!("reading table {}", &table);
            self.cur_table = Some(table);
        }
        self.last_statement = Some(cur_statement.to_string());
    }

    fn next_item(&mut self) -> Option<(Option<String>, String)> {
        match self.buf.next() {
            None => None,
            Some(ref s) => {
                self.capture_table(s);
                Some((self.cur_table.clone(), s.to_owned()))
            }
        }
    }
}

impl Iterator for SqlStatementsWithTable {
    type Item = (Option<String>, String);
    fn next(&mut self) -> Option<(Option<String>, String)> {
        let (mut table, mut line) = self.next_item()?;
        while table.as_ref().is_some_and(|t| !self.allowed_tables.as_ref().is_none_or(|at| at.contains(t))) {
            (table, line) = self.next_item()?;
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

pub fn read_table_data_file(table: &str, sqldump_filepath: &Path) -> impl Iterator<Item = (Option<String>, String)> {
    SqlStatementsWithTable::from_file(sqldump_filepath, &Some(HashSet::from([table.to_owned()])), &Some(table.to_string()))
}
