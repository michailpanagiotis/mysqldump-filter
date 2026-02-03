mod sql_parser;
mod writers;

use lazy_static::lazy_static;
use regex::Regex;
use core::panic;
use std::cell::RefCell;
use std::panic::panic_any;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::scanner::sql_parser::{TableColumnPositions, TableDataTypes, get_column_positions, get_data_types, split_insert_parts, is_create_table, is_insert, values};
use crate::scanner::writers::{Writers, get_table_file};

type DBMetaCell = Rc<RefCell<DBMeta>>;

type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type IteratorItem = SqlStatementResult;
type EmptyResult = Result<(), anyhow::Error>;

type ValuesMap = HashMap<String, (String, sqlparser::ast::DataType)>;

pub trait AbstractTransformFn<Iv>: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>
where
    Iv: IntoIterator + Clone + for<'a> Extend<(&'a String, &'a String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

impl<Iv, T: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>> AbstractTransformFn<Iv> for T
where
    Iv: IntoIterator + Clone + for<'a> Extend<(&'a String, &'a String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

pub trait TransformFn: AbstractTransformFn<SqlStatement> {}
impl<T: AbstractTransformFn<SqlStatement>> TransformFn for T {}

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

/// Represents a parsed SQL statement from a MySQL dump file
///
/// Contains the raw SQL text along with metadata about which table it operates on
/// and database schema information for processing column values.
#[derive(Clone)]
#[derive(Debug)]
pub struct SqlStatement {
    /// The raw SQL statement text
    text: String,
    /// The table name this statement operates on (if applicable)
    table: Option<String>,
    /// Reference to shared database metadata for column type information
    db_meta: Option<DBMetaCell>,
}

impl SqlStatement {
    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn set_meta(&mut self, db_meta_cell: &DBMetaCell) {
        self.db_meta = Some(Rc::clone(db_meta_cell));
    }

    fn get_insert_parts(&self) -> Option<(String, String, Vec<String>)> {
        if !is_insert(&self.text) {
            return None;
        }

        match split_insert_parts(&self.text) {
            Ok((table, columns_part, values_part)) => {

            },
            Err(e) => {
                dbg!(&e);
                panic!("cannot split insert parts");
            },
        }

        let Ok((table, columns_part, values_part)) = split_insert_parts(&self.text) else {
            panic!("cannot split insert parts");
        };

        let Ok((_, value_array)) = values(&values_part) else {
            panic!("cannot parse values");
        };

        Some((table, columns_part, value_array.iter().map(|x| x.to_string()).collect()))
    }
}

impl IntoIterator for SqlStatement {
    type Item = <ValuesMap as IntoIterator>::Item;
    type IntoIter = <ValuesMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let Some((table, _, value_array)) = self.get_insert_parts() else {
            return ValuesMap::default().into_iter();
        };

        let Some(ref meta) = self.db_meta else {
            panic!("statement with no meta");
        };
        let binding = meta.borrow();
        let Some(data_types) = binding.data_types.get(&table) else {
            panic!("statement with no data types");
        };

        let Some(positions) = binding.column_positions.get(&table) else {
            panic!("statement with no positions");
        };

        let values: ValuesMap = positions
            .iter()
            .map(|(column_name, position)| {
                (column_name.to_owned(), (value_array[*position].to_string(), data_types[column_name].to_owned()))
            })
            .collect();
        values.into_iter()
    }
}

impl<'a> Extend<(&'a String, &'a String)> for SqlStatement {
    fn extend<T: IntoIterator<Item=(&'a String, &'a String)>>(&mut self, iter: T) {
        if let Some((table, columns_part, mut values)) = self.get_insert_parts() {
            let Some(ref meta) = self.db_meta else {
                panic!("statement with no meta");
            };
            let binding = meta.borrow();
            let Some(positions) = binding.column_positions.get(&table) else {
                panic!("statement with no positions");
            };

            for (field, value) in iter {
                values[positions[field]] = value.to_string();
            }
            self.text = format!("INSERT INTO `{}` ({}) VALUES ({});\n", table, columns_part, values.join(","));
        }
    }
}

/// Database metadata container
///
/// Stores information about table schemas including column data types
/// and column positions within INSERT statements.
#[derive(Debug)]
pub struct DBMeta {
    /// Maps table names to their column data type information
    data_types: HashMap<String, Rc<TableDataTypes>>,
    /// Maps table names to column position mappings for INSERT statements
    column_positions: HashMap<String, Rc<TableColumnPositions>>,
}

impl DBMeta {
    fn from_file(filename: &Path) -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        let db_meta = DBMeta::new()?;
        let statements = TrackedStatements::from_file(filename, Some(&db_meta))?;
        // consume iterator to populate db_meta
        statements.for_each(drop);
        Ok(db_meta)
    }

    fn new() -> Result<DBMetaCell, anyhow::Error> {
        Ok(Rc::new(RefCell::new(DBMeta {
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
        })))
    }

    fn capture(&mut self, statement: &SqlStatement) -> EmptyResult {
        if is_create_table(&statement.text) {
            if let Some((table, data_types)) = get_data_types(&statement.text)? {
                self.data_types.insert(table.to_string(), Rc::new(data_types));
            }
        }
        if let Some(ref table) = statement.table {
            if !self.column_positions.contains_key(table) && is_insert(&statement.text) {
                self.column_positions.insert(table.to_string(), Rc::new(get_column_positions(&statement.text)?));
            };
        }
        Ok(())
    }
}

struct PlainStatements {
    buf: io::BufReader<fs::File>,
}

impl PlainStatements {
    fn from_file(sqldump_filepath: &Path) -> Result<Self, anyhow::Error> {
        let file = fs::File::open(sqldump_filepath)?;
        Ok(PlainStatements {
            buf: io::BufReader::new(file),
        })
    }

    fn is_full_line(line: &str) -> bool {
        if line.ends_with(";\n") {
            return true;
        }

        if line.starts_with("\n") {
            return true;
        }

        if line.starts_with("--") {
            return true;
        }

        false
    }
}

impl Iterator for PlainStatements {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        let mut buf: String = String::new();

        while {
            let read_bytes = self.buf.read_line(&mut buf).ok()?;
            read_bytes > 0 && !PlainStatements::is_full_line(&buf)
        } {}

        match buf.is_empty() {
            true => None,
            false => Some(buf),
        }
    }
}

struct TrackedStatements {
    iter: PlainStatements,
    current_table: Option<String>,
    unlock_next: bool,
    db_meta: DBMetaCell,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, db_meta: Option<&DBMetaCell>) -> Result<Self, anyhow::Error> {
        let db_meta = if let Some(db_meta) = db_meta { Rc::clone(db_meta) } else { DBMeta::new()? };
        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            current_table: None,
            unlock_next: false,
            db_meta,
        })
    }

    fn extract_table(statement: &str) -> Result<&str, anyhow::Error> {
        let Some(captures) = TABLE_DUMP_RE.captures(statement) else {
            return Err(anyhow::anyhow!("cannot extract table"));
        };

        let Some(captured) = captures.get(1) else {
            return Err(anyhow::anyhow!("cannot extract table"));
        };

        Ok(captured.as_str())
    }

    fn read_statement(&mut self) -> Option<SqlStatementResult> {
        let next = self.iter.next()?;

        if self.unlock_next {
            self.current_table = None;
            self.unlock_next = false;
        } else if next.starts_with("-- Dumping data for table") {
            let Ok(table) = TrackedStatements::extract_table(&next) else {
                return Some(Err(anyhow::anyhow!("cannot extract table")));
            };
            println!("Processing table {table}");
            self.current_table = Some(table.to_owned());
        }

        if next.starts_with("UNLOCK TABLES;") {
            self.unlock_next = true;
        }

        Some(Ok(SqlStatement{ text: next.to_string(), table: self.current_table.to_owned(), db_meta: None }))
    }
}

impl Iterator for TrackedStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;

        if let Ok(st) = &mut statement {
            if let Err(e) = self.db_meta.borrow_mut().capture(st) {
                return Some(Err(e));
            }
        }

        Some(statement)
    }
}

struct TransformedStatements<F: TransformFn> {
    iter: TrackedStatements,
    transform: F,
}

impl<F: TransformFn> TransformedStatements<F> {
    fn from_file(sqldump_filepath: &Path, transform: F, db_meta: Option<&DBMetaCell>) -> Result<Self, anyhow::Error> {
        Ok(TransformedStatements {
            iter: TrackedStatements::from_file(sqldump_filepath, db_meta)?,
            transform,
        })
    }

    fn transform_iteration_item(&mut self, statement_result: SqlStatementResult) -> Option<SqlStatementResult> {
        let Ok(mut statement) = statement_result else { return Some(statement_result); };
        statement.set_meta(&self.iter.db_meta);
        let tr: Option<SqlStatement> = (self.transform)(statement).expect("err");
        tr.map(Ok)
    }
}

impl<F: TransformFn> Iterator for TransformedStatements<F> {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut transformed;

        while {
            let input_statement = self.iter.next()?;
            transformed = self.transform_iteration_item(input_statement);
            transformed.is_none()
        } {}

        transformed
    }
}

pub fn process<F>(working_file_path: &Path, input_filepath: &Path, transform: F, db_meta: Option<DBMetaCell>,) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    let mut writers = Writers::new(working_file_path)?;
    for st in TransformedStatements::from_file(input_filepath, transform, db_meta.as_ref())? {
        let statement = st?;
        writers.write_statement(&statement.table, statement.text.as_bytes())?;
    };
    writers.flush()?;

    Ok(())
}

pub fn explode_to_files<F>(
    working_file_path: &Path,
    input_filepath: &Path,
    transform: F,
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    process(working_file_path, input_filepath, transform, None)
}

/// Process INSERT statements for a specific table with transformation function
///
/// # Arguments
/// * `working_file_path` - Path to working directory containing intermediate files
/// * `table` - Name of the table to process
/// * `transform` - Function to apply to each SQL statement for filtering/transformation
///
/// # Returns
/// * `Ok(())` on successful processing
/// * `Err(anyhow::Error)` if processing fails
pub fn process_table_inserts<F>(
    working_file_path: &Path,
    table: &str,
    transform: F,
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    println!("Processing records of table {table}");
    let input_filepath = &get_table_file(working_file_path, table)?;

    process(working_file_path, input_filepath, transform, Some(DBMeta::from_file(working_file_path)?))
}

/// Gather all processed SQL statements from working files into final output
///
/// Reads the main working file and inlines any referenced table-specific files
/// to create the final filtered MySQL dump output.
///
/// # Arguments
/// * `working_file_path` - Path to the main working file containing inline references
/// * `output_path` - Path where the final output file should be written
///
/// # Returns
/// * `Ok(())` on successful gathering
/// * `Err(anyhow::Error)` if file operations fail
#[allow(dead_code)]
pub fn gather(working_file_path: &Path, output_path: &Path) -> EmptyResult {
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);

    let file = File::open(working_file_path)?;

    for res in io::BufReader::new(file).lines() {
        let line = res?;
        if line.starts_with("--- INLINE ") {
            let st = line.replace("--- INLINE ", "").to_string();
            let mut split = st.split(" ");
            let filename = split.next().ok_or(anyhow::anyhow!("cannot parse filename"))?;
            println!("INLINING {filename}");
            let inline_file = File::open(PathBuf::from(filename))?;
            for inline_line in io::BufReader::new(inline_file).lines() {
                writer.write_all(inline_line?.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        } else {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use std::fs::File;
    use std::io::Write;

    fn create_test_sql_dump(content: &str) -> Result<TempDir, anyhow::Error> {
        let temp_dir = TempDir::new("sql_test")?;
        let sql_path = temp_dir.path().join("test.sql");
        let mut file = File::create(&sql_path)?;
        file.write_all(content.as_bytes())?;
        Ok(temp_dir)
    }

    #[test]
    fn test_sql_statement_creation() {
        let statement = SqlStatement {
            text: "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John');".to_string(),
            table: Some("users".to_string()),
            db_meta: None,
        };

        assert_eq!(statement.get_table(), &Some("users".to_string()));
        assert_eq!(statement.text, "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John');");
    }

    #[test]
    fn test_sql_statement_get_insert_parts() {
        let statement = SqlStatement {
            text: "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John');\n".to_string(),
            table: Some("users".to_string()),
            db_meta: None,
        };

        let parts = statement.get_insert_parts();
        assert!(parts.is_some());
        let (table, columns, values) = parts.unwrap();
        assert_eq!(table, "users");
        assert_eq!(columns, "`id`, `name`");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_sql_statement_get_insert_parts_non_insert() {
        let statement = SqlStatement {
            text: "CREATE TABLE `users` (`id` int, `name` varchar(255));".to_string(),
            table: None,
            db_meta: None,
        };

        let parts = statement.get_insert_parts();
        assert!(parts.is_none());
    }

    #[test]
    fn test_db_meta_new() {
        let db_meta = DBMeta::new().unwrap();
        let borrowed = db_meta.borrow();
        assert!(borrowed.data_types.is_empty());
        assert!(borrowed.column_positions.is_empty());
    }

    #[test]
    fn test_db_meta_from_file() {
        let sql_content = r#"
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- Dumping data for table `users`
LOCK TABLES `users` WRITE;
INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'John', 'john@example.com');
UNLOCK TABLES;
"#;

        let temp_dir = create_test_sql_dump(sql_content).unwrap();
        let sql_path = temp_dir.path().join("test.sql");

        let db_meta = DBMeta::from_file(&sql_path).unwrap();
        let borrowed = db_meta.borrow();

        assert!(borrowed.data_types.contains_key("users"));
        assert!(borrowed.column_positions.contains_key("users"));

        let positions = borrowed.column_positions.get("users").unwrap();
        assert_eq!(positions.len(), 3);
        assert_eq!(positions["id"], 0);
        assert_eq!(positions["name"], 1);
        assert_eq!(positions["email"], 2);
    }

    #[test]
    fn test_plain_statements_from_file() {
        let sql_content = r#"CREATE TABLE test;
INSERT INTO test VALUES (1);
-- This is a comment
SELECT * FROM test;"#;

        let temp_dir = create_test_sql_dump(sql_content).unwrap();
        let sql_path = temp_dir.path().join("test.sql");

        let statements = PlainStatements::from_file(&sql_path).unwrap();
        let collected: Vec<String> = statements.collect();

        assert_eq!(collected.len(), 4);
        assert!(collected[0].starts_with("CREATE TABLE"));
        assert!(collected[1].starts_with("INSERT INTO"));
        assert!(collected[2].starts_with("-- This is"));
        assert!(collected[3].starts_with("SELECT"));
    }

    #[test]
    fn test_plain_statements_is_full_line() {
        assert!(PlainStatements::is_full_line("SELECT * FROM test;\n"));
        assert!(PlainStatements::is_full_line("-- Comment\n"));
        assert!(PlainStatements::is_full_line("\n"));
        assert!(!PlainStatements::is_full_line("SELECT * FROM"));
        assert!(!PlainStatements::is_full_line("test"));
    }

    #[test]
    fn test_tracked_statements_extract_table() {
        let comment = "-- Dumping data for table `users`";
        let result = TrackedStatements::extract_table(comment);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "users");

        let invalid_comment = "-- Some other comment";
        let result = TrackedStatements::extract_table(invalid_comment);
        assert!(result.is_err());
    }

    #[test]
    fn test_tracked_statements_from_file() {
        let sql_content = r#"-- MySQL dump 10.13
--
-- Table structure for table `users`
--
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- Dumping data for table `users`
LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'John');
INSERT INTO `users` VALUES (2,'Jane');
UNLOCK TABLES;
"#;

        let temp_dir = create_test_sql_dump(sql_content).unwrap();
        let sql_path = temp_dir.path().join("test.sql");

        let statements = TrackedStatements::from_file(&sql_path, None).unwrap();
        let results: Vec<Result<SqlStatement, anyhow::Error>> = statements.collect();

        assert!(!results.is_empty());

        // Find INSERT statements and verify they have table context
        let insert_statements: Vec<_> = results.into_iter()
            .filter_map(|r| r.ok())
            .filter(|stmt| stmt.text.starts_with("INSERT"))
            .collect();

        assert_eq!(insert_statements.len(), 2);
        for stmt in insert_statements {
            assert_eq!(stmt.table, Some("users".to_string()));
        }
    }

    #[test]
    fn test_process_basic_transform() {
        let sql_content = r#"-- Dumping data for table `users`
LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'John');
INSERT INTO `users` VALUES (2,'Jane');
UNLOCK TABLES;
"#;

        let temp_dir = create_test_sql_dump(sql_content).unwrap();
        let input_path = temp_dir.path().join("test.sql");
        let working_path = temp_dir.path().join("working.sql");

        // Transform that keeps only statements containing "John"
        let transform = |stmt: SqlStatement| -> Result<Option<SqlStatement>, anyhow::Error> {
            if stmt.text.contains("John") {
                Ok(Some(stmt))
            } else {
                Ok(None)
            }
        };

        let result = process(&working_path, &input_path, transform, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_gather_simple() {
        let working_content = r#"CREATE TABLE test;
INSERT INTO test VALUES (1);
"#;

        let temp_dir = create_test_sql_dump(working_content).unwrap();
        let working_path = temp_dir.path().join("test.sql");
        let output_path = temp_dir.path().join("output.sql");

        let result = gather(&working_path, &output_path);
        assert!(result.is_ok());

        // Verify output file exists and has content
        assert!(output_path.exists());
        let output_content = std::fs::read_to_string(&output_path).unwrap();
        assert!(output_content.contains("CREATE TABLE"));
        assert!(output_content.contains("INSERT INTO"));
    }

    #[test]
    fn test_gather_with_inline_references() {
        let working_content = "--- INLINE /path/to/users.sql users\n";
        let inline_content = "INSERT INTO users VALUES (1, 'John');\n";

        let temp_dir = TempDir::new("gather_test").unwrap();
        let working_path = temp_dir.path().join("working.sql");
        let inline_path = temp_dir.path().join("users.sql");
        let output_path = temp_dir.path().join("output.sql");

        // Create working file with inline reference to absolute path
        let mut working_file = File::create(&working_path).unwrap();
        let inline_ref = format!("--- INLINE {} users\n", inline_path.display());
        working_file.write_all(inline_ref.as_bytes()).unwrap();

        // Create the inline file
        let mut inline_file = File::create(&inline_path).unwrap();
        inline_file.write_all(inline_content.as_bytes()).unwrap();

        let result = gather(&working_path, &output_path);
        assert!(result.is_ok());

        let output_content = std::fs::read_to_string(&output_path).unwrap();
        assert!(output_content.contains("INSERT INTO users"));
        assert!(output_content.contains("John"));
    }

    #[test]
    fn test_explode_to_files() {
        let sql_content = r#"CREATE TABLE `users` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL
) ENGINE=InnoDB;

-- Dumping data for table `users`
INSERT INTO `users` VALUES (1,'John');
INSERT INTO `users` VALUES (2,'Jane');
"#;

        let temp_dir = create_test_sql_dump(sql_content).unwrap();
        let input_path = temp_dir.path().join("test.sql");
        let working_path = temp_dir.path().join("working.sql");

        let transform = |stmt: SqlStatement| -> Result<Option<SqlStatement>, anyhow::Error> {
            Ok(Some(stmt)) // Keep all statements
        };

        let result = explode_to_files(&working_path, &input_path, transform);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_statement_into_iter_empty() {
        let statement = SqlStatement {
            text: "CREATE TABLE test;".to_string(),
            table: None,
            db_meta: None,
        };

        let values: ValuesMap = statement.into_iter().collect();
        assert!(values.is_empty());
    }

    #[test]
    fn test_sql_statement_extend_empty() {
        let mut statement = SqlStatement {
            text: "CREATE TABLE test;".to_string(),
            table: None,
            db_meta: None,
        };

        let updates = HashMap::new();
        statement.extend(updates.iter());

        // Should remain unchanged since it's not an INSERT
        assert_eq!(statement.text, "CREATE TABLE test;");
    }
}
