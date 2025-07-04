use nom::{IResult, Parser};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take, take_until};
use nom::character::complete::multispace0;
use nom::combinator::{opt, recognize};
use nom::multi::many0;
use nom::sequence::{delimited, preceded};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;
use std::collections::HashMap;

pub type TableDataTypes = HashMap<String, sqlparser::ast::DataType>;
pub type TableColumnPositions = HashMap<String, usize>;

fn quoted(i: &str) -> IResult<&str, &str> {
    recognize(delimited(
        tag("\'"),
        many0(
            // from https://github.com/ms705/nom-sql
            alt((
                is_not("\\\'"),
                tag("\'\'"),
                tag("\\\\"),
                tag("\\b"),
                tag("\\r"),
                tag("\\n"),
                tag("\\t"),
                tag("\\0"),
                tag("\\Z"),
                preceded(tag("\\"), take(1usize)),
            )),
        ),
        tag("\'"),
    )).parse(i)
}

pub fn values(i: &str) -> IResult<&str, Vec<&str>> {
    many0(
        delimited(
            // space
            multispace0,
            // value
            alt((
                // quoted value
                quoted,
                // unquoted value
                is_not(","),
            )),
            // comma
            opt(delimited(multispace0, tag(","), multispace0)),
        ),
    ).parse(i)
}

pub fn insert_parts(insert_statement: &str) -> Result<(String, String, String), anyhow::Error> {
    let mut parser = (
        // table
        preceded(tag("INSERT INTO `"), take_until("` (")),
        // columns
        preceded(tag("` ("), take_until(") VALUES (")),
        // values
        preceded(tag(") VALUES ("), take_until(");\n"))
    );
    let res: IResult<&str, (&str, &str, &str)> = parser.parse(insert_statement);
    match res {
        Ok(r) => {
            let (_, (table, columns, values)) = r;
            Ok((table.to_string(), columns.to_string(), values.to_string()))
        },
        Err(_) => Err(anyhow::anyhow!("cannot parse"))
    }
}

pub fn is_insert(statement: &str) -> bool {
    statement.starts_with("INSERT")
}

pub fn is_create_table(statement: &str) -> bool {
    statement.starts_with("CREATE TABLE")
}

pub fn get_data_types(create_statement: &str) -> Result<Option<(String, TableDataTypes)>, anyhow::Error> {
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, create_statement)?;
    for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
        if let sqlparser::ast::Statement::CreateTable(ct) = st {
            let table = ct.name.0[0].as_ident().unwrap().value.to_string();
            let data_types = HashMap::from_iter(
                ct.columns.iter().map(|column| (column.name.value.to_string(), column.data_type.to_owned())),
            );
            return Ok(Some((table, data_types)));
        }
    }
    Ok(None)
}

pub fn get_column_positions(insert_statement: &str) -> Result<HashMap<String, usize>, anyhow::Error> {
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, insert_statement)?;

    let st = ast.first().unwrap();
    let sqlparser::ast::Statement::Insert(x) = st else { return Err(anyhow::anyhow!("cannot get positions of insert statement")) };

    Ok(x.columns.iter().enumerate().map(|(idx, x)| (x.value.to_owned(), idx)).collect())
}
