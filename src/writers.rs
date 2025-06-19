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
    inline_files: HashSet<PathBuf>,
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
            inline_files: HashSet::new(),
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

    fn determine_output_file(&self, table_option: &Option<String>) -> Result<PathBuf, anyhow::Error> {
        match table_option {
            None => {
                if self.in_place {
                    return Err(anyhow::anyhow!("cannot write to working file in place"));
                }
                Ok(self.working_file_path.to_owned())
            }
            Some(table) => {
                let table_file = std::path::absolute(
                    if self.in_place {
                        self.working_dir_path.join(table).with_extension("proc")
                    } else {
                        self.working_dir_path.join(table).with_extension("sql")
                    }
                )?;
                Ok(table_file)
            }
        }
    }

    fn determine_writer(&mut self, table_option: &Option<String>) -> EmptyResult {
        if self.current_writer.is_none() || table_option != &self.current_table {
            println!("determining writer");
            self.current_table = table_option.clone();
            let filepath = self.determine_output_file(table_option)?;
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
        if self.current_table.is_none() && self.working_file_writer.is_none() {
            println!("determining working file writer");
            let file = fs::OpenOptions::new().append(true).open(&self.working_file_path)?;
            self.working_file_writer = Some(BufWriter::new(file));
        }
        Ok(())
    }

    fn try_write_inline_file(&mut self, table_option: &Option<String>, filepath: &Path) -> EmptyResult {
        if !self.inline_files.contains(filepath) {
            self.inline_files.insert(filepath.to_owned());
            println!("inlining file {}", &filepath.display());
            if let Some(table) = table_option {
                let Some(ref mut working_file_writer) = self.working_file_writer else {
                    return Err(anyhow::anyhow!("cannot find output file"));
                };
                working_file_writer.write_all(format!("--- INLINE {} {}\n", filepath.display(), table).as_bytes())?;
            };
        }
        Ok(())
    }

    pub fn write_statement(&mut self, table_option: &Option<String>, statement: &[u8]) -> EmptyResult {
        self.determine_writer(table_option)?;
        let filepath_option = self.current_file.to_owned();
        let Some(writer) = &mut self.current_writer else {
            return Err(anyhow::anyhow!("cannot find writer"));
        };
        let Some(filepath) = &filepath_option else {
            return Err(anyhow::anyhow!("cannot find output file"));
        };

        writer.write_all(statement)?;

        if !self.in_place {
            self.try_write_inline_file(table_option, filepath)?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> EmptyResult {
        if let Some(ref mut w) = self.current_writer {
            w.flush()?;
        }
        if let Some(ref mut w) = self.working_file_writer {
            w.flush()?;
        }
        Ok(())
    }
}
