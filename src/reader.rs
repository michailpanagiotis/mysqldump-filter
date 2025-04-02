use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use lazy_static::lazy_static;
use nom::multi::separated_list1;
use regex::Regex;
use nom::{
  IResult,
  Parser,
  character::complete::{char, one_of, none_of},
  branch::alt,
  multi::separated_list0,
  bytes::complete::{escaped, is_not, take_until, tag, take_till},
  sequence::{delimited, preceded},
};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
    static ref INSERT_RE: Regex = Regex::new(r"INSERT[^(]*\(([^)]+)\)").unwrap();
    static ref INSERT_VALUES_RE: Regex = Regex::new(r"INSERT.*\(([^)]+)\)").unwrap();
    static ref SPLIT_VALUES_RE: Regex = Regex::new(r"(?U)'[^']+'|[^,]+").unwrap();
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
pub struct Statement {
    pub line: String,
    pub table: Option<String>,
    pub r#type: StatementType,
}

impl Statement {
    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.line.as_bytes()
    }

    pub fn get_field_positions(&self) -> Option<HashMap::<String, usize>> {
        if !self.is_insert() {
            return None;
        }
        let mut parser = preceded(
            take_until("("), preceded(take_until("`"), take_until(")"))
        ).and_then(
          separated_list0(
              tag(", "),
              delimited(char('`'), is_not("`"), char('`')),
          )
        );
        let res: IResult<&str, Vec<&str>> = parser.parse(&self.line);
        let (_, fields) = res.expect("cannot parse fields");
        Some(HashMap::from_iter(
            fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
        ))
    }

    pub fn get_values(&self) -> Vec<String> {
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
        let res: IResult<&str, Vec<&str>> = parser.parse(&self.line);
        let (_, values) = res.expect("cannot parse values");
        values.iter().map(|item| item.to_string()).collect()
    }
}

pub fn read_statements(sqldump_filepath: &PathBuf, requested_tables: &HashSet<String>, use_running_table: bool) -> impl Iterator<Item = Statement> {
    let mut current_table: Option<String> = None;
    let annotate_with_table = move |line: String| {
        if line.starts_with("-- Dumping data for table") {
            let table = TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
            current_table = Some(table);
        }
        let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
        if !use_running_table {
            if let StatementType::Insert = statement_type {
                let table: String = line.chars().skip(13).take_while(|x| x != &'`').collect();
                current_table = Some(table);
            }
        }
        Statement { line, r#type: statement_type, table: current_table.clone() }
    };
    let file = File::open(sqldump_filepath).expect("Cannot open file");
    io::BufReader::new(file).lines()
        .map_while(Result::ok)
        .map(annotate_with_table)
        .filter(|st| st.table.is_none() || requested_tables.contains(st.table.as_ref().unwrap()))
}
