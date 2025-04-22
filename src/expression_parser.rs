use lazy_static::lazy_static;
use nom::{
  IResult,
  Parser,
  branch::alt,
  bytes::complete::{escaped, is_not, take_until, tag, take_till},
  character::complete::{char, one_of, none_of},
  combinator::rest,
  multi::{separated_list0, separated_list1},
  sequence::{delimited, preceded},
};
use regex::Regex;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub fn get_table_from_comment(sql_comment: &str) -> Option<String> {
    if sql_comment.starts_with("-- Dumping data for table") {
        return Some(
            TABLE_DUMP_RE.captures(sql_comment).unwrap().get(1).unwrap().as_str().to_string()
        );
    }
    None
}

pub fn parse_filter(filter_definition: &str) -> (&str, &str, &str) {
    let mut parser = (
        is_not("!=-"),
        alt((tag("=="), tag("!="), tag("->"))),
        rest
    );
    let res: IResult<&str, (&str, &str, &str)> = parser.parse(filter_definition);
    let (_, parsed) = res.expect("cannot parse filter condition");
    parsed
}

pub fn parse_insert_fields(insert_statement: &str) -> Vec<&str> {
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
    fields
}

pub fn parse_insert_values(insert_statement: &str) -> Vec<&str> {
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
    let (_, values) = res.expect("cannot parse values");
    values
}
