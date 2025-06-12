use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};

use crate::traits::{ColumnPositions, ReferenceTracker, Dependency};
use crate::column::ColumnMeta;
use crate::sql::{get_values, read_table_data_file};
use crate::checks::{PlainCheckType, new_plain_test, parse_test_definition};
use crate::split::Tracker;

type ColumnType = ColumnMeta;
pub type TrackedColumnType = ColumnMeta;
type DependencyType = Weak<RefCell<dyn Dependency>>;

impl From<&ColumnMeta> for ColumnType {
    fn from(c: &ColumnMeta) -> Self {
        c.to_owned()
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    pub columns: HashMap<String, ColumnMeta>,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    references: HashMap<String, HashSet<String>>,
    checks: Vec<PlainCheckType>,
    dependencies: Vec<DependencyType>,
}

impl ColumnPositions for TableMeta {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>> {
        &self.column_positions
    }

    fn set_column_positions(&mut self, positions: HashMap<String, usize>) {
        self.column_positions = Some(positions.to_owned());
        for col in self.columns.values_mut() {
            col.capture_position(&positions);
        }
    }
}

impl ReferenceTracker for TableMeta {
    fn get_references(&self) -> &HashMap<String, HashSet<String>> {
        &self.references
    }

    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>> {
        &mut self.references
    }
}

impl Dependency for TableMeta {
    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>] {
        &self.dependencies
    }

    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.columns.values_mut().for_each(|v| v.set_fulfilled_at_depth(depth))
    }

    fn has_been_fulfilled(&self) -> bool {
        self.columns.values().all(|v| v.has_been_fulfilled())
    }
}

impl Extend<ColumnMeta> for Rc<RefCell<TableMeta>> {
    fn extend<T: IntoIterator<Item=ColumnMeta>>(&mut self, iter: T) {
        let mut borrowed_self = self.borrow_mut();
        for elem in iter {
            borrowed_self.add_column_meta(elem);
        }
    }
}

impl TableMeta {
    fn add_column_meta(&mut self, elem: ColumnMeta) {
        if self.table.is_empty() {
            self.table = elem.get_table_name().to_owned();
        } else if self.table != elem.get_table_name() {
            panic!("mismatched table names");
        }

        let key = elem.get_column_name().to_owned();

        for check in elem.get_checks() {
            self.checks.push(new_plain_test(&self.table, check).unwrap())
        }
        match self.columns.get_mut(&key) {
            None => {
                self.columns.insert(key.to_owned(), elem);
            },
            Some(cm) => {
                cm.extend(&elem);
            }
        }
        if self.columns[&key].is_referenced() {
            self.references.insert(self.columns[&key].get_column_key().to_owned(), HashSet::new());
        }
    }

    pub fn get_foreign_tables(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut tables: Vec<String> = Vec::new();
        for cm in self.columns.values() {
            tables.extend(cm.get_foreign_tables()?);
        }
        Ok(tables)
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<TableMeta>>) {
        let weak = Rc::downgrade(target);
        self.dependencies.push(weak);
    }

    fn get_checks(&self) -> impl Iterator<Item=&PlainCheckType> {
        self.checks.iter()
    }

    pub fn test(
        &mut self,
        pass: &usize,
        sql_statement: &str,
        lookup_table: &HashMap<String, HashSet<String>>,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
        tracker: &Tracker,
    ) -> Result<bool, anyhow::Error> {
        if !sql_statement.starts_with("INSERT") {
            return Ok(true);
        }

        if !self.has_fulfilled_dependencies() {
            return Ok(true);
        }

        self.fulfill_dependency(pass);

        let Some(positions) = self.resolve_column_positions(sql_statement) else { return Err(anyhow::anyhow!("unknown positions")) };

        let values = get_values(sql_statement);
        dbg!(&sql_statement);
        // dbg!(&positions);
        // dbg!(&values);
        let value_per_field: HashMap<String, &str> = positions.iter().map(|(column_name, position)| (column_name.to_owned(), values[*position])).collect();

        let all_checks_passed = self.get_checks().all(|t| {
            let column_meta = &self.columns[t.get_column_name()];
            t.test(
                column_meta,
                value_per_field[column_meta.get_column_name()],
                lookup_table,
                &data_types[column_meta.get_column_name()],
            )
        });

        if all_checks_passed {
            let keys: Vec<String> = self.get_references().keys().map(|k| k.to_owned()).collect();
            for key in keys {
                let (_, column) = ColumnMeta::get_components_from_key(&key)?;
                let value = value_per_field[&column];
                self.capture_reference(&key, value)?;
            }
        }

        Ok(all_checks_passed)
    }

    pub fn process_data_file(
        &mut self,
        current_pass: &usize,
        file: &Path,
        lookup_table: &HashMap<String, HashSet<String>>,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
        tracker: &Tracker,
    ) -> Result<(), anyhow::Error> {
        if !self.has_fulfilled_dependencies() {
            println!("Skipping table {} since it still has dependencies", &self.table);
            return Ok(());
        }
        println!("Processing table {}", self.table);
        let current_table = &self.table.clone();
        let table_file = file.to_path_buf();
        let input_file = &table_file.with_extension("proc");
        fs::rename(&table_file, input_file).expect("cannot rename");
        fs::File::create(&table_file)?;

        let mut writer = BufWriter::new(
            fs::OpenOptions::new()
            .append(true)
            .open(&table_file)?
        );

        let statements = read_table_data_file(current_table, input_file);
        for (table_option, sql_statement) in statements {
            let Some(ref table) = table_option else { return Err(anyhow::anyhow!("unknown table")) };
            if current_table != table {
                return Err(anyhow::anyhow!("wrong table {} != {}", current_table, table));
            }


            let passed = self.test(current_pass, &sql_statement, lookup_table, data_types, tracker)?;
            if passed {
                writer.write_all(sql_statement.as_bytes())?;
            }
        }

        writer.flush()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct CheckCollection {
    table_meta: HashMap<String, Rc<RefCell<TableMeta>>>,
}

impl CheckCollection {
    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
    ) -> Result<Self, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).collect();

        let mut tracked_cols: Vec<TrackedColumnType> = Vec::new();

        for (table, definition) in definitions.iter() {
            let (column_name, deps) = parse_test_definition(definition)?;
            let mut column_meta = ColumnMeta::new(table, &column_name, &deps)?;
            column_meta.add_check(definition);
            tracked_cols.push(column_meta);

            // track target columns
            for key in deps.iter() {
                tracked_cols.push(ColumnMeta::from_foreign_key(key)?);
            }
        }
        let grouped: HashMap<String, Rc<RefCell<TableMeta>>> = tracked_cols
            .into_iter()
            .into_grouping_map_by(|t| t.get_table_name().to_owned())
            .collect();

        for table_meta in grouped.values() {
            let mut table_borrow = table_meta.borrow_mut();
            let foreign_tables = table_borrow.get_foreign_tables()?;
            for target_table in foreign_tables.iter() {
                let target_table_meta = &grouped[target_table];
                table_borrow.add_dependency(target_table_meta);
            }
        }

        dbg!(&grouped);
        Ok(CheckCollection {
            table_meta: grouped,
        })
    }

    fn get_pending_tables(&self) -> Vec<String>{
        self.table_meta.values().filter(|v| !v.borrow().has_been_fulfilled()).map(|v| v.borrow().table.to_owned()).collect()
    }

    fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        let mut lookup_table: HashMap<String, HashSet<String>> = HashMap::new();

        for table_meta in self.table_meta.values() {
            lookup_table.extend(table_meta.borrow().get_references().iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
        }
        lookup_table
    }

    pub fn process(
        &mut self,
        table_files: &HashMap<String, PathBuf>,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
        tracker: &Tracker,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        while !self.get_pending_tables().is_empty() {
            let pending = self.get_pending_tables();
            let lookup_table = self.get_lookup_table();
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for table_meta in self.table_meta.values_mut().filter(|t| pending.iter().any(|p| p == &t.borrow().table)) {
                let file = table_files[&table_meta.borrow().table].to_path_buf();
                table_meta.borrow_mut().process_data_file(&current_pass, &file, &lookup_table, data_types, tracker)?;
            }
            current_pass += 1;
        }
        Ok(())
    }
}
