use crate::checks::parse_test_definition;
use lazy_static::lazy_static;

lazy_static! {
    static ref ROOT: String = String::from("root");
}

// trait alias for transform functions
pub trait DfsFn<T>: FnMut(usize, &DependencyNode<T>) {}
impl<T, A: FnMut(usize, &DependencyNode<T>)> DfsFn<T> for A {}

#[derive(Debug)]
pub enum NodeType<T> {
    Root,
    Node { payload: T },
    Group{ name: String, payloads: Vec<T> },
}

fn rmq(x: &[usize], i: usize, j: usize) -> Option<usize> {
    let y = &x[i..j];
    let min_val = y.iter().min()?;
    let pos = i + y.iter().position(|a| a == min_val)?;
    Some(pos)
}

#[derive(Debug)]
pub struct DependencyNode<T> {
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

    fn new_group(key: &str) -> Self {
        DependencyNode {
            node_type: NodeType::Group { name: key.to_string(), payloads: Vec::new() },
            dependents: Vec::new(),
        }
    }

    pub fn new() -> Self {
        DependencyNode {
            node_type: NodeType::Root,
            dependents: Vec::new(),
        }
    }

    fn get_key(&self) -> &str {
        match &self.node_type {
            NodeType::Root => ROOT.as_str(),
            NodeType::Node { payload } => payload.into(),
            NodeType::Group { name, .. } => name,
        }
    }

    fn unwrap(self) -> (Vec<DependencyNode<T>>, Option<Vec<T>>) {
        match self.node_type {
            NodeType::Root => (self.dependents, None),
            NodeType::Node { payload } => (self.dependents, Some(Vec::from([payload]))),
            NodeType::Group { payloads, .. } => (self.dependents, Some(payloads))
        }
    }

    fn pop_payload(self) -> Option<T> {
        let NodeType::Node { payload } = self.node_type else { return None };
        Some(payload)
    }

    fn has_child(&self, key: &str) -> bool {
        if self.get_key() == key {
            return true;
        }
        if self.dependents.iter().any(|d| d.has_child(key)) {
            return true;
        }
        false
    }

    pub fn add_child(&mut self, payload: T) {
        if !self.has_child((&payload).into()) {
            self.dependents.push(DependencyNode::new_node(payload));
        }
    }

    pub fn add_group(&mut self, key: &str) {
        if !self.has_child(key) {
            self.dependents.push(DependencyNode::new_group(key));
        }
    }

    fn pop_child(&mut self, key: &str) -> Option<DependencyNode<T>> {
        if let Some(index) = self.dependents.iter().position(|value| value.get_key() == key) {
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
        if self.get_key() == key {
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

    pub fn move_under(&mut self, parent_key: &str, child_key: &str) -> Result<(), anyhow::Error> {
        println!("Moving {} under {}", child_key, parent_key);
        let child = self.pop_child(child_key).ok_or(anyhow::anyhow!("child {child_key} does not exist"))?;
        self.get_node_mut(parent_key).ok_or(anyhow::anyhow!("parent {parent_key} does not exist"))?.dependents.push(child);
        Ok(())
    }

    pub fn move_into(&mut self, group_key: &str, child_key: &str) -> Result<(), anyhow::Error> {
        println!("Moving {child_key} into {group_key}");
        let child = self.pop_child(child_key).ok_or(anyhow::anyhow!("child {child_key} does not exist"))?;
        let parent = self.get_node_mut(group_key).ok_or(anyhow::anyhow!("parent {group_key} does not exist"))?;
        match &mut parent.node_type {
            NodeType::Group { payloads, .. } => {
                match child.node_type {
                    NodeType::Node { payload } => {
                        payloads.push(payload);
                    },
                    _ => Err(anyhow::anyhow!("can only move node type"))?
                };
            },
            _ => Err(anyhow::anyhow!("can only move into group node"))?
        };
        Ok(())
    }

    fn walk_recursive<F: DfsFn<T>>(&self, depth: usize, visit: &mut F)  {
        visit(depth, self);
        for dependent in self.dependents.iter() {
            dependent.walk_recursive(depth + 1, visit);
            visit(depth, self);
        }
    }

    fn dfs<F: DfsFn<T>>(&self, visit: &mut F)  {
        self.walk_recursive(0, visit);
    }

    pub fn lca(&self, first_node_key: &str, second_node_key: &str) -> Result<String, anyhow::Error>{
        let mut keys: Vec<String> = Vec::new();
        let mut depths: Vec<usize> = Vec::new();
        self.dfs(&mut |depth, node: &DependencyNode<T>| {
            keys.push(node.get_key().to_owned());
            depths.push(depth.to_owned());
        });

        let Some(first_index) = keys.iter().position(|k| k == first_node_key) else { return Err(anyhow::anyhow!("cannot find first index")) };
        let Some(second_index) = keys.iter().position(|k| k == second_node_key) else { return Err(anyhow::anyhow!("cannot find second_index index")) };
        let Some(lca_index) = rmq(
            &depths,
            std::cmp::min(first_index, second_index),
            std::cmp::max(first_index, second_index),
        ) else { return Err(anyhow::anyhow!("cannot find lca index")) };
        Ok(keys[lca_index].to_owned())
    }

    pub fn print(&self) {
        self.dfs(&mut |depth, node: &DependencyNode<T>| {
            println!("Walk: {} {}", depth, node.get_key())
        });
    }

    pub fn chunk_by_depth(self) -> Vec<Vec<NodeType<T>>> {
        let mut depths: Vec<Vec<NodeType<T>>> = Vec::new();
        let mut dfs: Vec<(DependencyNode<T>, usize)> = Vec::new();
        for dep in self.dependents.into_iter() { dfs.push((dep, 0)) };

        let mut popped = dfs.pop();

        while popped.is_some() {
            let (node, depth) = popped.unwrap();
            if depths.len() == depth {
                depths.push(Vec::new());
            }

            depths[depth].push(node.node_type);
            for dep in node.dependents.into_iter() {
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

pub fn get_dependency_order(definitions: &[(String, String)]) -> Result<Vec<Vec<NodeType<Test>>>, anyhow::Error> {
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
    Ok(root.chunk_by_depth())
}
