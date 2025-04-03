use std::collections::HashMap;
use nom::multi::separated_list1;
use nom::{
  IResult,
  Parser,
  character::complete::{char, one_of, none_of},
  branch::alt,
  multi::separated_list0,
  bytes::complete::{escaped, is_not, take_until, tag, take_till},
  sequence::{delimited, preceded},
};

#[derive(Debug)]
#[derive(PartialEq)]
enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
pub struct Statement {
    pub line: String,
    pub table: Option<String>,
    r#type: StatementType,
}

impl Statement {
    pub fn new(table: &Option<String>, line: &str) -> Self {
       let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
       Statement {
        line: line.to_string(),
        r#type: statement_type,
        table: table.clone(),
       }
    }

    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn get_table(&self) -> Option<&String> {
        self.table.as_ref()
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
