use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

type EmptyResult = Result<(), anyhow::Error>;

pub fn get_table_file(working_file_path: &Path, table: &str) -> Result<PathBuf, anyhow::Error> {
    let working_dir_path = working_file_path.parent().ok_or(anyhow::anyhow!("cannot find parent directory"))?;
    Ok(std::path::absolute(working_dir_path.join(table).with_extension("sql"))?)
}

#[derive(Debug)]
struct Writer {
    table: Option<String>,
    filepath: PathBuf,
    tmp_filepath: PathBuf,
    buf_writer: Option<BufWriter<File>>,
}

impl Writer {
    fn new(filepath: &Path, table: &Option<String>) -> Result<Self, anyhow::Error> {
        let tmp_filepath = filepath.with_extension("proc").to_owned();
        Ok(Self {
            table: table.to_owned(),
            filepath: filepath.to_owned(),
            tmp_filepath,
            buf_writer: None,
        })
    }

    fn write_statement(&mut self, statement: &[u8]) -> EmptyResult {
        if self.buf_writer.is_none() {
            fs::File::create(&self.tmp_filepath)?;
            let file = fs::OpenOptions::new().append(true).open(&self.tmp_filepath)?;
            self.buf_writer = Some(BufWriter::new(file));
        }

        self.buf_writer.as_mut().unwrap().write_all(statement)?;

        Ok(())
    }

    fn flush(&mut self) -> EmptyResult {
        if let Some(ref mut writer) = self.buf_writer {
            writer.flush()?;
            dbg!("RENAMING", &self.tmp_filepath, &self.filepath);
            fs::rename(&self.tmp_filepath, &self.filepath)?;
            self.buf_writer = None;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Writers {
    working_file_path: PathBuf,
    writer_per_table: HashMap<Option<String>, Writer>,
}

impl Writers {
    pub fn new(working_file_path: &Path) -> Result<Self, anyhow::Error> {
        Ok(Writers {
            working_file_path: working_file_path.to_owned(),
            writer_per_table: HashMap::new(),
        })
    }

    fn get_table_file(&self, table: &str) -> Result<PathBuf, anyhow::Error> {
        get_table_file(&self.working_file_path, table)
    }

    fn get_writer<'a>(&'a mut self, table_option: &Option<String>) -> Result<&'a mut Writer, anyhow::Error> {
        if !self.writer_per_table.contains_key(table_option) {
            let filepath = match table_option {
                Some(t) => self.get_table_file(t)?,
                None => std::path::absolute(&self.working_file_path)?,
            };
            self.writer_per_table.insert(table_option.to_owned(), Writer::new(&filepath, table_option)?);
        }
        Ok(self.writer_per_table.get_mut(table_option).unwrap())
    }

    pub fn write_statement(&mut self, table_option: &Option<String>, statement: &[u8]) -> EmptyResult {
        if let Some(table) = table_option {
            if self.writer_per_table.contains_key(&None) && !self.writer_per_table.contains_key(table_option) {
                let filepath = self.get_table_file(table)?;
                let working_file_writer = self.get_writer(&None)?;
                working_file_writer.write_statement(format!("--- INLINE {} {}\n", filepath.display(), table).as_bytes())?;
            }
        }
        let writer = self.get_writer(table_option)?;
        writer.write_statement(statement)?;
        Ok(())
    }

    pub fn flush(&mut self) -> EmptyResult {
        for (_, writer) in self.writer_per_table.iter_mut() {
            writer.flush()?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use std::fs::File;
    use std::io::Read;

    #[test]
    fn test_get_table_file() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let result = get_table_file(&working_file_path, "users");
        assert!(result.is_ok());
        
        let table_file = result.unwrap();
        assert!(table_file.to_string_lossy().contains("users.sql"));
        assert!(table_file.is_absolute());
    }

    #[test]
    fn test_get_table_file_with_special_chars() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let result = get_table_file(&working_file_path, "user_profiles");
        assert!(result.is_ok());
        
        let table_file = result.unwrap();
        assert!(table_file.to_string_lossy().contains("user_profiles.sql"));
    }

    #[test]
    fn test_writer_new() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let filepath = temp_dir.path().join("test.sql");
        let table = Some("users".to_string());
        
        let result = Writer::new(&filepath, &table);
        assert!(result.is_ok());
        
        let writer = result.unwrap();
        assert_eq!(writer.table, table);
        assert_eq!(writer.filepath, filepath);
        assert!(writer.buf_writer.is_none());
    }

    #[test]
    fn test_writer_new_no_table() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let filepath = temp_dir.path().join("test.sql");
        let table = None;
        
        let result = Writer::new(&filepath, &table);
        assert!(result.is_ok());
        
        let writer = result.unwrap();
        assert_eq!(writer.table, None);
        assert_eq!(writer.filepath, filepath);
    }

    #[test]
    fn test_writer_write_and_flush() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let filepath = temp_dir.path().join("test.sql");
        let table = Some("users".to_string());
        
        let mut writer = Writer::new(&filepath, &table).unwrap();
        
        // Write some data
        let statement = b"INSERT INTO users VALUES (1, 'John');\n";
        let result = writer.write_statement(statement);
        assert!(result.is_ok());
        
        // Flush to finalize the file
        let result = writer.flush();
        assert!(result.is_ok());
        
        // Verify the file exists and contains the data
        assert!(filepath.exists());
        let mut file_contents = String::new();
        File::open(&filepath).unwrap().read_to_string(&mut file_contents).unwrap();
        assert_eq!(file_contents, "INSERT INTO users VALUES (1, 'John');\n");
    }

    #[test]
    fn test_writer_multiple_writes() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let filepath = temp_dir.path().join("test.sql");
        let table = Some("users".to_string());
        
        let mut writer = Writer::new(&filepath, &table).unwrap();
        
        // Write multiple statements
        writer.write_statement(b"INSERT INTO users VALUES (1, 'John');\n").unwrap();
        writer.write_statement(b"INSERT INTO users VALUES (2, 'Jane');\n").unwrap();
        writer.write_statement(b"INSERT INTO users VALUES (3, 'Bob');\n").unwrap();
        
        writer.flush().unwrap();
        
        let mut file_contents = String::new();
        File::open(&filepath).unwrap().read_to_string(&mut file_contents).unwrap();
        
        assert!(file_contents.contains("INSERT INTO users VALUES (1, 'John');"));
        assert!(file_contents.contains("INSERT INTO users VALUES (2, 'Jane');"));
        assert!(file_contents.contains("INSERT INTO users VALUES (3, 'Bob');"));
    }

    #[test]
    fn test_writer_flush_without_write() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let filepath = temp_dir.path().join("test.sql");
        let table = Some("users".to_string());
        
        let mut writer = Writer::new(&filepath, &table).unwrap();
        
        // Flush without writing anything
        let result = writer.flush();
        assert!(result.is_ok());
        
        // File should not exist since nothing was written
        assert!(!filepath.exists());
    }

    #[test]
    fn test_writers_new() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let result = Writers::new(&working_file_path);
        assert!(result.is_ok());
        
        let writers = result.unwrap();
        assert_eq!(writers.working_file_path, working_file_path);
        assert!(writers.writer_per_table.is_empty());
    }

    #[test]
    fn test_writers_get_table_file() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let writers = Writers::new(&working_file_path).unwrap();
        let result = writers.get_table_file("users");
        assert!(result.is_ok());
        
        let table_file = result.unwrap();
        assert!(table_file.to_string_lossy().contains("users.sql"));
    }

    #[test]
    fn test_writers_write_statement_no_table() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let mut writers = Writers::new(&working_file_path).unwrap();
        
        let table = None;
        let statement = b"CREATE TABLE users (id INT);\n";
        
        let result = writers.write_statement(&table, statement);
        assert!(result.is_ok());
        
        let result = writers.flush();
        assert!(result.is_ok());
        
        // Verify main working file was created
        assert!(working_file_path.exists());
        let mut contents = String::new();
        File::open(&working_file_path).unwrap().read_to_string(&mut contents).unwrap();
        assert!(contents.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_writers_write_statement_with_table() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let mut writers = Writers::new(&working_file_path).unwrap();
        
        let table = Some("users".to_string());
        let statement = b"INSERT INTO users VALUES (1, 'John');\n";
        
        let result = writers.write_statement(&table, statement);
        assert!(result.is_ok());
        
        let result = writers.flush();
        assert!(result.is_ok());
        
        // Verify table-specific file was created
        let table_file = writers.get_table_file("users").unwrap();
        assert!(table_file.exists());
        
        let mut contents = String::new();
        File::open(&table_file).unwrap().read_to_string(&mut contents).unwrap();
        assert!(contents.contains("INSERT INTO users VALUES (1, 'John');"));
    }

    #[test]
    fn test_writers_mixed_statements() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let mut writers = Writers::new(&working_file_path).unwrap();
        
        // Write a CREATE TABLE statement (no table)
        writers.write_statement(&None, b"CREATE TABLE users (id INT);\n").unwrap();
        
        // Write table-specific INSERT statements
        let users_table = Some("users".to_string());
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (1, 'John');\n").unwrap();
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (2, 'Jane');\n").unwrap();
        
        // Write to another table
        let profiles_table = Some("profiles".to_string());
        writers.write_statement(&profiles_table, b"INSERT INTO profiles VALUES (1, 'Bio');\n").unwrap();
        
        writers.flush().unwrap();
        
        // Verify main working file contains CREATE and inline references
        assert!(working_file_path.exists());
        let mut working_contents = String::new();
        File::open(&working_file_path).unwrap().read_to_string(&mut working_contents).unwrap();
        assert!(working_contents.contains("CREATE TABLE users"));
        assert!(working_contents.contains("--- INLINE"));
        assert!(working_contents.contains("users"));
        assert!(working_contents.contains("profiles"));
        
        // Verify table files exist and contain correct data
        let users_file = writers.get_table_file("users").unwrap();
        assert!(users_file.exists());
        let mut users_contents = String::new();
        File::open(&users_file).unwrap().read_to_string(&mut users_contents).unwrap();
        assert!(users_contents.contains("INSERT INTO users VALUES (1, 'John');"));
        assert!(users_contents.contains("INSERT INTO users VALUES (2, 'Jane');"));
        
        let profiles_file = writers.get_table_file("profiles").unwrap();
        assert!(profiles_file.exists());
        let mut profiles_contents = String::new();
        File::open(&profiles_file).unwrap().read_to_string(&mut profiles_contents).unwrap();
        assert!(profiles_contents.contains("INSERT INTO profiles VALUES (1, 'Bio');"));
    }

    #[test]
    fn test_writers_table_inline_reference_order() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let mut writers = Writers::new(&working_file_path).unwrap();
        
        // Write to main file first
        writers.write_statement(&None, b"-- Start of dump\n").unwrap();
        
        // First table write should create inline reference
        let users_table = Some("users".to_string());
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (1);\n").unwrap();
        
        // More main file writes
        writers.write_statement(&None, b"-- Middle of dump\n").unwrap();
        
        // Another table
        let orders_table = Some("orders".to_string());
        writers.write_statement(&orders_table, b"INSERT INTO orders VALUES (1);\n").unwrap();
        
        writers.flush().unwrap();
        
        let mut working_contents = String::new();
        File::open(&working_file_path).unwrap().read_to_string(&mut working_contents).unwrap();
        
        // Verify structure: start -> inline users -> middle -> inline orders
        let lines: Vec<&str> = working_contents.lines().collect();
        assert_eq!(lines[0], "-- Start of dump");
        assert!(lines[1].starts_with("--- INLINE") && lines[1].contains("users"));
        assert_eq!(lines[2], "-- Middle of dump");
        assert!(lines[3].starts_with("--- INLINE") && lines[3].contains("orders"));
    }

    #[test]
    fn test_writers_same_table_multiple_times() {
        let temp_dir = TempDir::new("writers_test").unwrap();
        let working_file_path = temp_dir.path().join("working.sql");
        
        let mut writers = Writers::new(&working_file_path).unwrap();
        
        let users_table = Some("users".to_string());
        
        // Write to same table multiple times
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (1);\n").unwrap();
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (2);\n").unwrap();
        writers.write_statement(&users_table, b"INSERT INTO users VALUES (3);\n").unwrap();
        
        writers.flush().unwrap();
        
        let users_file = writers.get_table_file("users").unwrap();
        let mut users_contents = String::new();
        File::open(&users_file).unwrap().read_to_string(&mut users_contents).unwrap();
        
        // Should contain all three inserts
        assert!(users_contents.contains("INSERT INTO users VALUES (1);"));
        assert!(users_contents.contains("INSERT INTO users VALUES (2);"));
        assert!(users_contents.contains("INSERT INTO users VALUES (3);"));
        
        // Count the number of INSERT statements
        let insert_count = users_contents.matches("INSERT INTO").count();
        assert_eq!(insert_count, 3);
    }
}
