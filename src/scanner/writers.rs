use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

type EmptyResult = Result<(), anyhow::Error>;

#[derive(Debug)]
struct Writer {
    table: Option<String>,
    filepath: PathBuf,
    tmp_filepath: PathBuf,
    buf_writer: Option<BufWriter<File>>,
}

impl Writer {
    fn new(working_file_path: &Path, table: &Option<String>) -> Result<Self, anyhow::Error> {
        let working_dir_path = working_file_path.parent().ok_or(anyhow::anyhow!("cannot find parent directory"))?;
        let filepath = match table {
            Some(t) => std::path::absolute(working_dir_path.join(t).with_extension("sql"))?,
            None => std::path::absolute(working_file_path)?,
        };
        let tmp_filepath = filepath.clone().with_extension("proc").to_owned();
        Ok(Self {
            table: table.to_owned(),
            filepath,
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
    working_dir_path: PathBuf,
    working_file_path: PathBuf,
    writer_per_table: HashMap<Option<String>, Writer>,
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
            writer_per_table: HashMap::new(),
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

    fn determine_output_file(&self, table: &str, in_place: bool) -> Result<PathBuf, io::Error> {
        let filepath = self.get_table_file(table)?;
        Ok(if in_place { filepath.with_extension("proc") } else { filepath })
    }

    fn get_processed_table_file(&self, table: &str) -> Result<PathBuf, io::Error> {
        self.determine_output_file(table, true)
    }

    fn get_writer<'a>(&'a mut self, table_option: &Option<String>) -> Result<&'a mut Writer, anyhow::Error> {
        if !self.writer_per_table.contains_key(table_option) {
            self.writer_per_table.insert(table_option.to_owned(), Writer::new(&self.working_file_path, table_option)?);
        }
        Ok(self.writer_per_table.get_mut(table_option).unwrap())
    }

    pub fn write_statement(&mut self, table_option: &Option<String>, statement: &[u8]) -> EmptyResult {
        if let Some(table) = table_option {
            if self.writer_per_table.contains_key(&None) && !self.writer_per_table.contains_key(table_option) {
                let filepath = std::path::absolute(self.working_dir_path.join(table).with_extension("sql"))?;
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
