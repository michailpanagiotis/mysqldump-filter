use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use lazy_static::lazy_static;
use regex::Regex;
use fastbloom::BloomFilter;
use nom::{
  IResult,
  Parser,
  character::complete::char,
  multi::many_m_n,
  branch::alt,
  multi::separated_list0,
  combinator::eof,
  bytes::complete::{is_not, take_until, tag},
  sequence::{delimited, preceded, terminated, separated_pair},
};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
    static ref INSERT_RE: Regex = Regex::new(r"INSERT[^(]*\(([^)]+)\)").unwrap();
    static ref INSERT_VALUES_RE: Regex = Regex::new(r"INSERT.*\(([^)]+)\)").unwrap();
    static ref SPLIT_VALUES_RE: Regex = Regex::new(r"(?U)'[^']+'|[^,]+").unwrap();
}

#[derive(Debug)]
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

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
pub fn read_lines<P>(filename: P) -> io::Lines<io::BufReader<File>>
where P: AsRef<Path>, {
    let file = File::open(filename).expect("Cannot open file");
    io::BufReader::new(file).lines()
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
            match statement_type {
                StatementType::Insert => {
                    let table: String = line.chars().skip(13).take_while(|x| x != &'`').collect();
                    current_table = Some(table);
                }
                _ => {

                }
            }
        }
        Statement { line, r#type: statement_type, table: current_table.clone() }
    };
    read_lines(sqldump_filepath)
        .map_while(Result::ok)
        .map(annotate_with_table)
        .filter(|st| st.table.is_none() || requested_tables.contains(st.table.as_ref().unwrap()))
}

pub fn parse_table_name(input: &str) -> IResult<&str, &str> {
    preceded(
        tag("INSERT INTO `"),
        is_not("`"),
    ).parse(input)
}

pub fn parse_fields(input: &str) -> IResult<&str, Vec<&str>> {
    preceded(take_until("("), preceded(take_until("`"), take_until(")"))).and_then(
      separated_list0(
          tag(", "),
          delimited(char('`'), is_not("`"), char('`')),
      )
    ).parse(input)
}

pub fn parse_query(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(
        is_not("="),
        tag("="),
        is_not("=")
    ).parse(input)
}

pub fn parse_values(id_index: usize, input: &str) -> IResult<&str, Vec<&str>> {
    preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
        // VALUES list
        many_m_n(1, id_index + 1, terminated(
            alt((
                tag("''"),
                // quoted value
                delimited(char('\''), is_not("'"), char('\'')),
                // unquoted value
                take_until(",")
            )),
            // delimiter
            alt((tag(","), eof)),
        ))
    ).parse(input)
}

pub fn read_ids(filename: &String) -> (HashSet<String>, BloomFilter) {
    let lines = read_lines(filename);
    let mut id_position: Option<usize> = None;
    let mut ids: HashSet<String> = HashSet::new();
    println!("Reading ids of {}", filename);
    for line in lines.map_while(Result::ok) {
        if !line.starts_with("INSERT") {
            continue
        }

        if id_position.is_none() {
            let (_, fields) = parse_fields(line.as_str()).unwrap();
            id_position = fields.iter().position(|x| x == &"id");
            if id_position.is_none() {
                id_position = fields.iter().position(|x| x == &"name");
            }
        }

        let (_, values) = parse_values(id_position.unwrap(), line.as_str()).unwrap();
        let id = String::from(values.into_iter().nth(id_position.unwrap()).unwrap());
        ids.insert(id);
    }

    let ids_lookup = BloomFilter::with_false_pos(0.001).items(ids.iter());
    (ids, ids_lookup)
}
