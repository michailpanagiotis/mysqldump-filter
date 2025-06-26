use crate::checks::parse_test_definition;

pub trait NodeKey {
    fn get_key(&self) -> String;
}

#[derive(Debug)]
enum NodeType<T> {
    Root,
    Node { payload: T },
}

#[derive(Debug)]
struct DependencyNode<T> {
    node_type: NodeType<T>,
    dependents: Vec<DependencyNode<T>>,
}

impl<T> DependencyNode<T>
    where for<'a> &'a T: Into<&'a str>
{
    fn new_node(payload: T) -> Self {
        DependencyNode {
            node_type: NodeType::Node { payload },
            dependents: Vec::new(),
        }
    }

    fn new() -> Self {
        DependencyNode {
            node_type: NodeType::Root,
            dependents: Vec::new(),
        }
    }

    fn get_payload(&self) -> Option<&T> {
        let NodeType::Node { payload } = &self.node_type else { return None };
        Some(payload)
    }

    fn unwrap(self) -> (Vec<DependencyNode<T>>, Option<T>) {
        match self.node_type {
            NodeType::Root => (self.dependents, None),
            NodeType::Node { payload } => (self.dependents, Some(payload))
        }
    }

    fn pop_payload(self) -> Option<T> {
        let NodeType::Node { payload } = self.node_type else { return None };
        Some(payload)
    }

    fn has_child(&self, key: &str) -> bool {
        if self.get_payload().is_some_and(|p| p.into() == key) {
            return true;
        }
        if self.dependents.iter().any(|d| d.has_child(key)) {
            return true;
        }
        false
    }

    fn add_child(&mut self, payload: T) {
        if !self.has_child((&payload).into()) {
            self.dependents.push(DependencyNode::new_node(payload));
        }
    }

    fn pop_child(&mut self, key: &str) -> Option<DependencyNode<T>> {
        if let Some(index) = self.dependents.iter().position(|value| value.get_payload().is_some_and(|p| p.into() == key)) {
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

    fn get_node_mut<'a>(&'a mut self, key: &str) -> Option<&'a mut DependencyNode<T>> {
        if self.get_payload().is_some_and(|p| p.into() == key) {
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
        println!("Moving {} under {}", child_key, parent_key);
        let child = self.pop_child(child_key).ok_or(anyhow::anyhow!("child {child_key} does not exist"))?;
        self.get_node_mut(parent_key).ok_or(anyhow::anyhow!("parent {parent_key} does not exist"))?.dependents.push(child);
        Ok(())
    }

    fn group_by_depth(self) -> Vec<Vec<T>> {
        let mut depths: Vec<Vec<T>> = Vec::new();
        let mut dfs: Vec<(DependencyNode<T>, usize)> = Vec::new();
        for dep in self.dependents.into_iter() { dfs.push((dep, 0)) };

        let mut popped = dfs.pop();

        while popped.is_some() {
            let (node, depth) = popped.unwrap();
            if depths.len() == depth {
                depths.push(Vec::new());
            }

            let (dependents, payload_option) = node.unwrap();

            if let Some(payload) = payload_option {
                depths[depth].push(payload);
            }

            for dep in dependents.into_iter() {
                dfs.push((dep, depth + 1));
            }

            popped = dfs.pop();
        }

        depths
    }
}

#[derive(Debug)]
pub struct Test(String);

impl<'a> Into<&'a str> for &'a Test {
    fn into(self) -> &'a str {
        self.0.as_str()
    }
}

pub fn get_dependency_order(definitions: &[(String, String)]) -> Result<Vec<Vec<Test>>, anyhow::Error> {
    let mut root = DependencyNode::<Test>::new();
    for (source_table, definition) in definitions.iter() {
        let (_, foreign_keys) = parse_test_definition(definition)?;
        root.add_child(Test(source_table.to_string()));
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
