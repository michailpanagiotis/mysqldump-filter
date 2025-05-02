use lazy_static::lazy_static;
use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
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
use chrono::NaiveDateTime;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"--\n-- Dumping data for table `([^`]*)`").unwrap();
}

pub fn extract_table(sql_comment: &str) -> String {
    TABLE_DUMP_RE.captures(sql_comment).unwrap().get(1).unwrap().as_str().to_string()
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
    let (_, values) = res.expect(&format!("cannot parse values for {}", &insert_statement));
    values
}

pub fn get_data_types(sql: &str) -> HashMap<String, sqlparser::ast::DataType> {
    let mut data_types = HashMap::new();
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, sql).unwrap();
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
    data_type: sqlparser::ast::DataType,
}

fn get_data_type(data_types: &HashMap<String, sqlparser::ast::DataType>, table: &str, field: &str) -> sqlparser::ast::DataType {
    match data_types.get(&(table.to_owned() + "." + field)) {
        None => panic!("{}", format!("cannot find data type for {table}.{field}")),
        Some(data_type) => data_type.to_owned()
    }
}

fn parse_date(s: &str) -> i64 {
    let to_parse = if s.len() == 10 { s.to_owned() + " 00:00:00" } else { s.to_owned() };
    let val = NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S").unwrap_or_else(|_| panic!("cannot parse timestamp {s}"));
    let timestamp: i64 = val.and_utc().timestamp();
    timestamp
}

fn parse_int(s: &str) -> i64 {
    s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
}

fn timestamp(This(s): This<Arc<String>>) -> i64 {
    parse_date(&s)
}

impl FilterCondition {
    pub fn new(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> FilterCondition {
        if definition.starts_with("cel:") {
            let Some(end) = definition.strip_prefix("cel:") else { panic!("cannot parse cel expression") };
            let program = Program::compile(end).unwrap();
            let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
            let field = &variables[0];
            return FilterCondition {
                table: table.to_string(),
                field: field.to_string(),
                operator: FilterOperator::Cel(program),
                value: definition.to_string(),
                data_type: get_data_type(data_types, table, field),
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
            data_type: get_data_type(data_types, table, field),
        }
    }

    pub fn build_context(&self, other_value: &str) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", timestamp);

        if other_value == "NULL" {
            context.add_variable(self.field.clone(), false).unwrap();
            return context;
        }

        context.add_variable(self.field.clone(), match self.data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                parse_int(other_value)
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                parse_date(other_value)
            },
            _ => panic!("{}", format!("cannot parse {} for {}", other_value, self.data_type))
        }).unwrap();

        context
    }

    pub fn test(&self, other_value: &str) -> bool {
        match &self.operator {
            FilterOperator::Equals => self.value == other_value,
            FilterOperator::NotEquals => self.value != other_value,
            FilterOperator::ForeignKey => true,
            FilterOperator::Cel(program) => {
                let context = self.build_context(other_value);

                let value = program.execute(&context).unwrap();
                let res = match value {
                    cel_interpreter::objects::Value::Bool(v) => v,
                    _ => false,
                };
                println!("testing {}.{} {} -> {}", self.table, self.field, &other_value, &res);
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

    pub fn get_foreign_keys<'a, I: Iterator<Item=&'a FilterCondition>>(conditions: I) -> impl Iterator<Item=(String, String)> {
        conditions.filter(|fc| fc.is_foreign_filter()).map(|fc| fc.get_foreign_key() )

    }
}
