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
                is_not(" \t\r\n,"),
            )),
            // comma
            opt(delimited(multispace0, tag(","), multispace0)),
        ),
    ).parse(i)
}

pub fn split_insert_parts(insert_statement: &str) -> Result<(String, String, String), anyhow::Error> {
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
        Err(e) => Err(e.to_owned().into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quoted_simple() {
        let input = "'hello world'";
        let result = quoted(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed, "'hello world'");
    }

    #[test]
    fn test_quoted_with_escaped_quotes() {
        let input = "'hello ''world'''";
        let result = quoted(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed, "'hello ''world'''");
    }

    #[test]
    fn test_quoted_with_backslashes() {
        let input = "'hello\\nworld\\t'";
        let result = quoted(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed, "'hello\\nworld\\t'");
    }

    #[test]
    fn test_quoted_invalid() {
        let input = "hello world"; // No quotes
        let result = quoted(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_values_simple() {
        let input = "1, 'hello', 3";
        let result = values(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], "1");
        assert_eq!(parsed[1], "'hello'");
        assert_eq!(parsed[2], "3");
    }

    #[test]
    fn test_values_with_spaces() {
        let input = " 1 , 'hello world' , 3 ";
        let result = values(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        dbg!(&parsed);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], "1");
        assert_eq!(parsed[1], "'hello world'");
        assert_eq!(parsed[2], "3");
    }

    #[test]
    fn test_values_empty() {
        let input = "";
        let result = values(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_values_single_value() {
        let input = "'single'";
        let result = values(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0], "'single'");
    }

    #[test]
    fn test_values_with_null() {
        let input = "1, NULL, 'test'";
        let result = values(input);
        assert!(result.is_ok());
        let (_, parsed) = result.unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], "1");
        assert_eq!(parsed[1], "NULL");
        assert_eq!(parsed[2], "'test'");
    }

    #[test]
    fn test_split_insert_parts_valid() {
        let input = "INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'John', 'john@example.com');\n";
        let result = split_insert_parts(input);
        assert!(result.is_ok());
        let (table, columns, values) = result.unwrap();
        assert_eq!(table, "users");
        assert_eq!(columns, "`id`, `name`, `email`");
        assert_eq!(values, "1, 'John', 'john@example.com'");
    }

    #[test]
    fn test_split_insert_parts_simple() {
        let input = "INSERT INTO `test` (`id`) VALUES (1);\n";
        let result = split_insert_parts(input);
        assert!(result.is_ok());
        let (table, columns, values) = result.unwrap();
        assert_eq!(table, "test");
        assert_eq!(columns, "`id`");
        assert_eq!(values, "1");
    }

    #[test]
    fn test_split_insert_parts_invalid_format() {
        let input = "SELECT * FROM users;";
        let result = split_insert_parts(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_insert_parts_malformed() {
        let input = "INSERT INTO users VALUES (1);\n"; // Missing backticks and columns
        let result = split_insert_parts(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_insert_true() {
        assert!(is_insert("INSERT INTO users VALUES (1);"));
        assert!(is_insert("INSERT IGNORE INTO users VALUES (1);"));
        assert!(is_insert("INSERT LOW_PRIORITY INTO users VALUES (1);"));
    }

    #[test]
    fn test_is_insert_false() {
        assert!(!is_insert("SELECT * FROM users;"));
        assert!(!is_insert("CREATE TABLE users;"));
        assert!(!is_insert("UPDATE users SET name = 'John';"));
        assert!(!is_insert("DELETE FROM users;"));
        assert!(!is_insert(""));
        assert!(!is_insert("-- INSERT comment"));
    }

    #[test]
    fn test_is_create_table_true() {
        assert!(is_create_table("CREATE TABLE users (id INT);"));
        assert!(is_create_table("CREATE TABLE IF NOT EXISTS users (id INT);"));
    }

    #[test]
    fn test_is_create_table_false() {
        assert!(!is_create_table("CREATE TEMPORARY TABLE users (id INT);"));
        assert!(!is_create_table("INSERT INTO users VALUES (1);"));
        assert!(!is_create_table("SELECT * FROM users;"));
        assert!(!is_create_table("DROP TABLE users;"));
        assert!(!is_create_table("ALTER TABLE users ADD COLUMN name VARCHAR(255);"));
        assert!(!is_create_table(""));
        assert!(!is_create_table("-- CREATE TABLE comment"));
    }

    #[test]
    fn test_get_data_types_valid() {
        let create_statement = r#"
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"#;

        let result = get_data_types(create_statement);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        let (table_name, data_types) = parsed.unwrap();
        assert_eq!(table_name, "users");
        assert_eq!(data_types.len(), 5);
        assert!(data_types.contains_key("id"));
        assert!(data_types.contains_key("name"));
        assert!(data_types.contains_key("email"));
        assert!(data_types.contains_key("created_at"));
        assert!(data_types.contains_key("is_active"));

        // Check specific data types
        match &data_types["id"] {
            sqlparser::ast::DataType::Int(_) => {},
            _ => panic!("Expected Int data type for id column"),
        }

        match &data_types["name"] {
            sqlparser::ast::DataType::Varchar(_) => {},
            _ => panic!("Expected Varchar data type for name column"),
        }

        match &data_types["created_at"] {
            sqlparser::ast::DataType::Datetime(_) => {},
            _ => panic!("Expected Datetime data type for created_at column"),
        }
    }

    #[test]
    fn test_get_data_types_simple() {
        let create_statement = "CREATE TABLE `test` (`id` int, `name` varchar(100));";
        let result = get_data_types(create_statement);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());

        let (table_name, data_types) = parsed.unwrap();
        assert_eq!(table_name, "test");
        assert_eq!(data_types.len(), 2);
        assert!(data_types.contains_key("id"));
        assert!(data_types.contains_key("name"));
    }

    #[test]
    fn test_get_data_types_non_create_table() {
        let statement = "INSERT INTO users VALUES (1, 'John');";
        let result = get_data_types(statement);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_none());
    }

    #[test]
    fn test_get_data_types_invalid_sql() {
        let statement = "INVALID SQL STATEMENT";
        let result = get_data_types(statement);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_column_positions_valid() {
        let insert_statement = "INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'John', 'john@example.com');";
        let result = get_column_positions(insert_statement);
        assert!(result.is_ok());
        let positions = result.unwrap();

        assert_eq!(positions.len(), 3);
        assert_eq!(positions["id"], 0);
        assert_eq!(positions["name"], 1);
        assert_eq!(positions["email"], 2);
    }

    #[test]
    fn test_get_column_positions_single_column() {
        let insert_statement = "INSERT INTO `test` (`id`) VALUES (1);";
        let result = get_column_positions(insert_statement);
        assert!(result.is_ok());
        let positions = result.unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions["id"], 0);
    }

    #[test]
    fn test_get_column_positions_multiple_values() {
        let insert_statement = "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John'), (2, 'Jane');";
        let result = get_column_positions(insert_statement);
        assert!(result.is_ok());
        let positions = result.unwrap();

        assert_eq!(positions.len(), 2);
        assert_eq!(positions["id"], 0);
        assert_eq!(positions["name"], 1);
    }

    #[test]
    fn test_get_column_positions_non_insert() {
        let statement = "SELECT * FROM users;";
        let result = get_column_positions(statement);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_column_positions_invalid_sql() {
        let statement = "INVALID SQL";
        let result = get_column_positions(statement);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_data_types_alias() {
        let data_types: TableDataTypes = HashMap::new();
        assert!(data_types.is_empty());

        let mut data_types: TableDataTypes = HashMap::new();
        data_types.insert("id".to_string(), sqlparser::ast::DataType::Int(None));
        data_types.insert("name".to_string(), sqlparser::ast::DataType::Varchar(Some(sqlparser::ast::CharacterLength::IntegerLength { length: 255, unit: None })));

        assert_eq!(data_types.len(), 2);
        assert!(data_types.contains_key("id"));
        assert!(data_types.contains_key("name"));
    }

    #[test]
    fn test_table_column_positions_alias() {
        let positions: TableColumnPositions = HashMap::new();
        assert!(positions.is_empty());

        let mut positions: TableColumnPositions = HashMap::new();
        positions.insert("id".to_string(), 0);
        positions.insert("name".to_string(), 1);
        positions.insert("email".to_string(), 2);

        assert_eq!(positions.len(), 3);
        assert_eq!(positions["id"], 0);
        assert_eq!(positions["name"], 1);
        assert_eq!(positions["email"], 2);
    }
}
