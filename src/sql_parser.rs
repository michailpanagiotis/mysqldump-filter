use nom::{IResult, Parser};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take};
use nom::character::complete::{digit1, multispace0};
use nom::combinator::{opt, recognize};
use nom::multi::many0;
use nom::sequence::{delimited, pair, preceded};

fn raw_string_quoted(i: &str) -> IResult<&str, &str> {
    recognize(delimited(
        tag("\'"),
        many0(
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

pub fn literal(i: &str) -> IResult<&str, &str> {
    alt((
        // float
        recognize((opt(tag("-")), digit1, tag("."), digit1)),
        // integer
        recognize(pair(opt(tag("-")), digit1)),
        raw_string_quoted,
        tag_no_case("null"),
    )).parse(i)
}

pub fn ws_sep_comma(i: &str) -> IResult<&str, &str> {
    delimited(multispace0, tag(","), multispace0).parse(i)
}

pub fn values(i: &str) -> IResult<&str, Vec<&str>> {
    many0(delimited(multispace0, literal, opt(ws_sep_comma))).parse(i)
}
