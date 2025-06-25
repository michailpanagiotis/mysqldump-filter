use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::rc::{Rc, Weak};


use crate::checks::{get_checks_per_table, test_checks, PlainCheckType, TableChecks};
use crate::scanner::process_table_inserts;

fn process_inserts(
    working_file_path: &Path,
    table: &str,
    checks: &[PlainCheckType],
    tracked_columns: &[&str],
    lookup_table: &HashMap<String, HashSet<String>>,
) -> Result<HashMap<String, HashSet<String>>, anyhow::Error> {
    let captured = process_table_inserts(working_file_path, table, tracked_columns, |statement| {
        let value_per_field = statement.get_values()?;

        match test_checks(checks, value_per_field, lookup_table)? {
            false => Ok(None),
            true => Ok(Some(()))
        }
    })?;
    Ok(captured)
}

type TableMetaCell = Rc<RefCell<TableMeta>>;
type DependencyCell = Rc<RefCell<Dependency>>;
type WeakDependencyRef = Weak<RefCell<Dependency>>;

#[derive(Debug)]
#[derive(Default)]
struct Dependency {
    dependencies: Vec<WeakDependencyRef>,
    tested_at_pass: Option<usize>,
}

impl Dependency {
    fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Dependency { dependencies: Vec::new(), tested_at_pass: None }))
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<Dependency>>) {
        let weak = Rc::downgrade(target);
        self.dependencies.push(weak);
    }

    fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    fn has_fulfilled_dependencies(&self) -> bool {
        self.dependencies.iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.tested_at_pass = Some(depth.to_owned());
        }
        assert!(self.has_been_fulfilled());
    }
}

#[derive(Debug)]
#[derive(Default)]
struct DependencyTree {
    nodes: HashMap<String, Rc<RefCell<Dependency>>>,
}

impl DependencyTree {
    fn new() -> Self {
        DependencyTree::default()
    }

    fn add_node(&mut self, key: &str) {
        if !self.nodes.contains_key(key) {
            self.nodes.insert(key.to_owned(), Dependency::new());
        }
    }

    fn add_dependency(&mut self, from: &str, to: &str) -> Result<(), anyhow::Error> {
        if !self.nodes.contains_key(from) {
            self.add_node(from);
        }
        if !self.nodes.contains_key(to) {
            self.add_node(to);
        }
        let target = Rc::downgrade(self.nodes.get(to).ok_or(anyhow::anyhow!("cannot get target node"))?);
        let mut source = self.nodes.get_mut(from).ok_or(anyhow::anyhow!("cannot get source node"))?.borrow_mut();
        source.dependencies.push(target);
        Ok(())
    }
}

#[derive(Debug)]
struct DependencyNode {
    key: String,
    dependents: Vec<DependencyNode>,
}

impl DependencyNode {
    fn new(key: &str) -> Self {
        DependencyNode {
            key: key.to_string(),
            dependents: Vec::new(),
        }
    }

    fn root() -> Self {
        DependencyNode {
            key: String::from("root"),
            dependents: Vec::new(),
        }
    }

    fn has_child(&self, key: &str) -> bool {
        if self.key == key {
            return true;
        }
        if self.dependents.iter().any(|d| d.has_child(key)) {
            return true;
        }
        false
    }

    fn add_child(&mut self, key: &str) {
        if !self.has_child(key) {
            self.dependents.push(DependencyNode::new(key));
        }
    }

    fn pop_child(&mut self, key: &str) -> Option<DependencyNode> {
        if let Some(index) = self.dependents.iter().position(|value| value.key == key) {
            Some(self.dependents.swap_remove(index))
        } else {
            for dep in self.dependents.iter_mut() {
                let child = dep.pop_child(key);
                if child.is_some() {
                    return child;
                }
            }
            None
        }
    }

    fn get_node_mut<'a>(&'a mut self, key: &str) -> Option<&'a mut DependencyNode> {
        if self.key == key {
            return Some(self);
        }
        for dep in self.dependents.iter_mut() {
            let child = dep.get_node_mut(key);
            if child.is_some() {
                return child;
            }
        }
        None
    }

    fn move_under(&mut self, parent_key: &str, child_key: &str) -> Result<(), anyhow::Error> {
        println!("Moving {child_key} under {parent_key}");
        let child = self.pop_child(child_key).unwrap_or(DependencyNode::new(child_key));
        if !self.has_child(parent_key) {
            self.add_child(parent_key);
        }
        self.get_node_mut(parent_key).ok_or(anyhow::anyhow!("cannot find parent node {parent_key}"))?.dependents.push(child);
        Ok(())
    }

    fn by_depth(&self) -> Vec<HashSet<String>> {
        let mut depths: Vec<HashSet<String>> = Vec::new();
        let mut dfs: Vec<(&DependencyNode, usize)> = Vec::new();
        for dep in self.dependents.iter() { dfs.push((dep, 0)) };

        let mut popped = dfs.pop();

        while popped.is_some() {
            let (node, depth) = popped.unwrap();
            if depths.len() == depth {
                depths.push(HashSet::new());
            }

            for dep in node.dependents.iter() {
                dfs.push((dep, depth + 1));
            }

            depths[depth].insert(node.key.to_owned());
            popped = dfs.pop();
        }

        depths
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    foreign_tables: Vec<String>,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
    dependency: DependencyCell,
}

impl TryFrom<&TableChecks> for TableMetaCell {
    type Error = anyhow::Error;
    fn try_from(table_checks: &TableChecks) -> Result<Self, Self::Error> {
        let checks = table_checks.get_checks()?;
        Ok(Rc::new(RefCell::new(TableMeta {
            table: table_checks.table.clone(),
            foreign_tables: table_checks.foreign_tables.clone(),
            references: table_checks.references.clone(),
            checks,
            dependency: Dependency::new(),
        })))
    }
}

impl TableMeta {
    pub fn get_foreign_tables(&self) -> Vec<String> {
        self.foreign_tables.clone()
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        self.references.iter().map(|x| x.as_str()).collect()
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<TableMeta>>) {
        self.dependency.borrow_mut().add_dependency(&target.borrow().dependency);
    }

    pub fn process_data_file(
        &mut self,
        current_pass: &usize,
        lookup_table: &HashMap<String, HashSet<String>>,
        working_file_path: &Path,
    ) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
        if !self.dependency.borrow().has_fulfilled_dependencies() {
            println!("Skipping table {} since it still has dependencies", &self.table);
            return Ok(None);
        }

        let captured = process_inserts(working_file_path, &self.table, &self.checks, &self.get_tracked_columns(), lookup_table)?;

        self.dependency.borrow_mut().fulfill_dependency(current_pass);

        Ok(Some(captured))
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

        let checks_per_table = get_checks_per_table(&definitions)?;

        let mut grouped: HashMap<String, Rc<RefCell<TableMeta>>> = HashMap::new();
        for (table, checks) in checks_per_table.iter() {
            grouped.insert(table.to_owned(), TableMetaCell::try_from(checks)?);
        }

        let mut root = DependencyNode::root();

        let mut tree = DependencyTree::new();
        for table_meta in grouped.values() {
            let source_table = &table_meta.borrow().table;
            tree.add_node(source_table);
            root.add_child(source_table);
            let foreign_tables = table_meta.borrow().get_foreign_tables();
            for target_table in foreign_tables.iter() {
                tree.add_dependency(source_table, target_table)?;
                root.move_under(target_table, source_table)?;
            }
        }

        // set dependencies
        for table_meta in grouped.values() {
            let foreign_tables = table_meta.borrow().get_foreign_tables();
            for target_table in foreign_tables.iter() {
                let target_table_meta = &grouped[target_table];
                table_meta.borrow_mut().add_dependency(target_table_meta);
            }
        }


        dbg!(&root);
        dbg!(&root.by_depth());
        panic!("stop");
        Ok(CheckCollection {
            table_meta: grouped,
        })
    }

    fn get_pending_tables(&self) -> Vec<String>{
        self.table_meta.values().filter(|v| !v.borrow().dependency.borrow().has_been_fulfilled()).map(|v| v.borrow().table.to_owned()).collect()
    }

    pub fn process(
        &mut self,
        working_file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        let mut lookup_table = HashMap::new();
        while !self.get_pending_tables().is_empty() {
            let pending = self.get_pending_tables();
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for table_meta in self.table_meta.values_mut().filter(|t| pending.iter().any(|p| p == &t.borrow().table)) {
                let captured_option = table_meta.borrow_mut().process_data_file(
                    &current_pass,
                    &lookup_table,
                    working_file_path,
                )?;
                if let Some(captured) = captured_option {
                    lookup_table.extend(captured);
                }
            }
            current_pass += 1;
        }
        dbg!(&lookup_table);
        Ok(())
    }
}
