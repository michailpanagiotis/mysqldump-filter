use std::collections::HashSet;
use std::fs::File;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

type EmptyResult = Result<(), anyhow::Error>;

#[derive(Debug)]
pub struct Writers {
    working_dir_path: PathBuf,
    working_file_path: PathBuf,
    in_place: bool,
    written_files: HashSet<PathBuf>,
    working_file_writer: Option<BufWriter<File>>,
    current_table: Option<String>,
    current_writer: Option<BufWriter<File>>,
    current_file: Option<PathBuf>,
}

impl Writers {
    pub fn new(working_file_path: &Path, in_place: bool) -> Result<Self, anyhow::Error> {
        let working_dir_path = working_file_path.parent().ok_or(anyhow::anyhow!("cannot find parent directory"))?;
        Ok(Writers {
            working_dir_path: working_dir_path.to_owned(),
            working_file_path: working_file_path.to_owned(),
            in_place,
            written_files: HashSet::new(),
            working_file_writer: None,
            current_table: None,
            current_writer: None,
            current_file: None,
        })
    }

    pub fn get_table_file(&self, table: &str) -> Result<PathBuf, io::Error> {
        std::path::absolute(self.working_dir_path.join(table).with_extension("sql"))
    }

    fn get_processed_table_file(&self, table: &str) -> Result<PathBuf, io::Error> {
        std::path::absolute(self.working_dir_path.join(table).with_extension("proc"))
    }

    fn determine_output_file(&self, table: &str, in_place: bool) -> Result<PathBuf, io::Error> {
        if in_place {
            self.get_processed_table_file(table)
        } else {
            self.get_table_file(table)
        }
    }

    fn determine_writer(&mut self, table: &str) -> EmptyResult {
        if self.current_writer.is_none() || Some(table) != self.current_table.as_deref() {
            self.current_table = Some(table.to_owned());
            dbg!(self.in_place);
            let filepath = self.determine_output_file(table, self.in_place)?;
            self.current_file = Some(filepath.to_owned());
            if !self.written_files.contains(&filepath) {
                println!("creating file {}", &filepath.display());
                self.written_files.insert(filepath.to_owned());
                fs::File::create(&filepath)?;
            } else {
                println!("appending to file {}", &filepath.display());
            }
            let file = fs::OpenOptions::new().append(true).open(&filepath)?;
            if let Some(ref mut writer) = self.current_writer {
                writer.flush()?;
            }
            self.current_writer = Some(BufWriter::new(file));
        }
        Ok(())
    }

    fn try_write_inline_file(&mut self, table: &str) -> EmptyResult {
        let filepath = self.get_table_file(table)?;
        let Some(ref mut working_file_writer) = self.working_file_writer else {
            return Err(anyhow::anyhow!("cannot find output file"));
        };
        working_file_writer.write_all(format!("--- INLINE {} {}\n", filepath.display(), table).as_bytes())?;
        Ok(())
    }

    pub fn write_statement(&mut self, table_option: &Option<String>, statement: &[u8]) -> EmptyResult {
        match table_option {
            Some(table) => {
                self.determine_writer(table)?;
                let Some(writer) = &mut self.current_writer else {
                    return Err(anyhow::anyhow!("cannot find writer"));
                };
                writer.write_all(statement)?;

                if !self.in_place && let Some(table) = table_option {
                    self.try_write_inline_file(table)?;
                }
            },
            None => {
                if self.working_file_writer.is_none() {
                    println!("determining working file writer");
                    fs::File::create(&self.working_file_path)?;
                    let file = fs::OpenOptions::new().append(true).open(&self.working_file_path)?;
                    self.working_file_writer = Some(BufWriter::new(file));
                }

                let Some(writer) = &mut self.working_file_writer else {
                    return Err(anyhow::anyhow!("cannot find working file writer"));
                };
                writer.write_all(statement)?;
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> EmptyResult {
        if let Some(ref mut w) = self.current_writer {
            w.flush()?;
            if self.in_place && let Some(ref table) = self.current_table {
                let processsed_file = self.get_processed_table_file(table)?;
                let table_file = self.get_table_file(table)?;
                fs::rename(processsed_file, table_file)?;
            }
        }
        if let Some(ref mut w) = self.working_file_writer {
            w.flush()?;
        }
        Ok(())
    }
}
