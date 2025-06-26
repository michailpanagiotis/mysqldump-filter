use std::collections::HashSet;
use std::fmt::Debug;
use crate::checks::parse_test_definition;

#[derive(Debug)]
enum NodeType {
    Root,
    Node { payload: String },
}

#[derive(Debug)]
struct DependencyNode {
    node_type: NodeType,
    dependents: Vec<DependencyNode>,
}

impl DependencyNode {
    fn new_node(payload: &String) -> Self {
        DependencyNode {
            node_type: NodeType::Node { payload: payload.to_owned() },
            dependents: Vec::new(),
        }
    }

    fn new() -> Self {
        DependencyNode {
            node_type: NodeType::Root,
            dependents: Vec::new(),
        }
    }

    fn get_payload(&self) -> Option<&String> {
        let NodeType::Node { payload } = &self.node_type else { return None };
        Some(payload)
    }

    fn has_child(&self, payload: &String) -> bool {
        if self.get_payload().is_some_and(|p| p == payload) {
            return true;
        }
        if self.dependents.iter().any(|d| d.has_child(payload)) {
            return true;
        }
        false
    }

    fn add_child(&mut self, payload: &String) {
        if !self.has_child(payload) {
            self.dependents.push(DependencyNode::new_node(payload));
        }
    }

    fn pop_child(&mut self, payload: &String) -> Option<DependencyNode> {
        if let Some(index) = self.dependents.iter().position(|value| value.get_payload().is_some_and(|p| p == payload)) {
            Some(self.dependents.swap_remove(index))
        } else {
            for dep in self.dependents.iter_mut() {
                let child = dep.pop_child(payload);
                if child.is_some() {
                    return child;
                }
            }
            None
        }
    }

    fn get_node_mut<'a>(&'a mut self, payload: &String) -> Option<&'a mut DependencyNode> {
        if self.get_payload().is_some_and(|p| p == payload) {
            return Some(self);
        }
        for dep in self.dependents.iter_mut() {
            let child = dep.get_node_mut(payload);
            if child.is_some() {
                return child;
            }
        }
        None
    }

    fn move_under(&mut self, parent_payload: &String, child_payload: &String) -> Result<(), anyhow::Error> {
        println!("Moving {} under {}", child_payload, parent_payload);
        let child = self.pop_child(child_payload).unwrap_or(DependencyNode::new_node(child_payload));
        if !self.has_child(parent_payload) {
            self.add_child(parent_payload);
        }
        self.get_node_mut(parent_payload).ok_or(anyhow::anyhow!("cannot find parent node {parent_payload}"))?.dependents.push(child);
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

            if let NodeType::Node { payload } = &node.node_type {
                depths[depth].insert(payload.to_owned());
            }
            popped = dfs.pop();
        }

        depths
    }
}

pub fn get_dependency_order(definitions: &[(String, String)]) -> Result<Vec<HashSet<String>>, anyhow::Error> {
    let mut root = DependencyNode::new();
    for (source_table, definition) in definitions.iter() {
        let (_, foreign_keys) = parse_test_definition(definition)?;
        root.add_child(source_table);
        for target_key in foreign_keys {
            let mut split = target_key.split('.');
            let (Some(target_table), Some(_), None) = (split.next(), split.next(), split.next()) else {
                return Err(anyhow::anyhow!("malformed key {}", target_key));
            };
            root.move_under(&target_table.to_owned(), source_table)?;
        }
    }
    Ok(root.group_by_depth())
}
