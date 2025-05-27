use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::checks::{RowCheck, RowType};
use crate::traits::ReferenceTracker;

#[derive(Debug)]
pub struct FilterConditions<'a> {
    table_files: HashMap<String, PathBuf>,
    per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck<'a>>>>,
    pub current_pass: usize,
}

impl<'a> FilterConditions<'a> {
    pub fn new(table_files: &HashMap<String, PathBuf>, per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck<'a>>>>) -> Self {
        FilterConditions {
            table_files: table_files.clone(),
            per_table,
            current_pass: 0,
        }
    }

    fn get_current_loookup(&self) -> HashMap<String, HashSet<String>> {
        let mut lookup: HashMap<String, HashSet<String>> = HashMap::new();

        for (_, row_check) in self.per_table.iter() {
            lookup.extend(row_check.borrow().get_references().iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
        }

        lookup
    }

}

pub fn process_table<'a>(row_check: &'a mut RowType<'a>, current_pass: &usize, file: &Path, lookup_table: &HashMap<String, HashSet<String>>) -> Result<(), anyhow::Error> {
    row_check.borrow_mut().process_data_file(current_pass, file, lookup_table)
}
