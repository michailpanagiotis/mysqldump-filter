use lazy_static::lazy_static;
use cel_interpreter::{Context, Program};
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
use std::collections::{HashMap, HashSet};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

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

pub fn get_data_types(schema: &[String]) -> HashMap<String, sqlparser::ast::DataType> {
    let mut data_types = HashMap::new();

    let sql: String = schema.iter().filter(|x| !x.starts_with("--")).cloned().collect();

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

#[derive(Debug)]
enum FilterOperator {
    Equals,
    NotEquals,
    ForeignKey,
    Cel(Program),
    Unknown,
}

#[derive(Debug)]
pub struct FilterCondition {
    pub table: String,
    pub field: String,
    operator: FilterOperator,
    value: String,
}

impl FilterCondition {
    pub fn new(table: &str, definition: &str) -> FilterCondition {
        if definition.starts_with("cel:") {
            let Some(end) = definition.strip_prefix("cel:") else { panic!("cannot parse cel expression") };
            let program = Program::compile(end).unwrap();
            let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
            return FilterCondition {
                table: table.to_string(),
                field: variables[0].to_string(),
                operator: FilterOperator::Cel(program),
                value: definition.to_string(),
            }
        }
        let (field, operator, value) = parse_filter(definition);
        FilterCondition {
            table: table.to_string(),
            field: field.to_string(),
            operator: match operator {
                "==" => FilterOperator::Equals,
                "!=" => FilterOperator::NotEquals,
                "->" => FilterOperator::ForeignKey,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    pub fn test(&self, other_value: &str) -> bool {
        match &self.operator {
            FilterOperator::Equals => self.value == other_value,
            FilterOperator::NotEquals => self.value != other_value,
            FilterOperator::ForeignKey => true,
            FilterOperator::Cel(program) => {
                let mut context = Context::default();
                let val: u64 = other_value.parse().unwrap();
                context.add_variable(self.field.clone(), val).unwrap();

                let value = program.execute(&context).unwrap();
                let res = match value {
                    cel_interpreter::objects::Value::Bool(v) => v,
                    _ => false,
                };
                println!("testing {} -> {}", &val, &res);
                res
            },
            FilterOperator::Unknown => true
        }
    }

    pub fn test_foreign(&self, value:&str, foreign_values: &Option<&HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = foreign_values else { return true };
        let Some(set) = fvs.get(self.value.as_str()) else { return false };
        set.contains(value)
    }

    pub fn is_foreign_filter(&self) -> bool {
        matches!(self.operator, FilterOperator::ForeignKey)
    }

    pub fn get_foreign_key(&self) -> (String, String) {
        let mut split = self.value.split('.');
        let (Some(table), Some(field), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {}", self.value);
        };
        (table.to_string(), field.to_string())
    }
}
