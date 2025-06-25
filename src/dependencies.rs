use std::collections::HashSet;
use std::fmt::Debug;
use crate::checks::determine_target_tables;


#[derive(Debug)]
struct DependencyNode {
    key: String,
    dependents: Vec<DependencyNode>,
}

impl DependencyNode {
    fn new_node(key: &str) -> Self {
        DependencyNode {
            key: key.to_string(),
            dependents: Vec::new(),
        }
    }

    fn new() -> Self {
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
            self.dependents.push(DependencyNode::new_node(key));
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
        let child = self.pop_child(child_key).unwrap_or(DependencyNode::new_node(child_key));
        if !self.has_child(parent_key) {
            self.add_child(parent_key);
        }
        self.get_node_mut(parent_key).ok_or(anyhow::anyhow!("cannot find parent node {parent_key}"))?.dependents.push(child);
        Ok(())
    }

    fn group_by_depth(&self) -> Vec<HashSet<String>> {
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

pub fn get_dependency_order(definitions: &[(String, String)]) -> Result<Vec<HashSet<String>>, anyhow::Error> {
    let mut root = DependencyNode::new();
    for (table, definition) in definitions.iter() {
        root.add_child(table);
        let target_tables = determine_target_tables(definition)?;
        for target_table in target_tables {
            root.move_under(&target_table, table)?;
        }
    }
    dbg!(&root);
    dbg!(&root.group_by_depth());
    Ok(root.group_by_depth())
}
