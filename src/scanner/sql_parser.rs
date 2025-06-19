use nom::{IResult, Parser};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take, take_until};
use nom::character::complete::multispace0;
use nom::combinator::{opt, recognize};
use nom::multi::many0;
use nom::sequence::{delimited, preceded};

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
