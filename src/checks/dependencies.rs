use lazy_static::lazy_static;

lazy_static! {
    static ref ROOT: String = String::from("root");
}

#[derive(Debug)]
pub enum NodeType<T> {
    Root,
    Node { payload: T },
    Group{ name: String, payloads: Vec<T> },
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

    fn has_child(&self, key: &str) -> bool {
        if self.get_key() == key {
            return true;
        }
        if self.dependents.iter().any(|d| d.has_child(key)) {
            return true;
        }
        false
    }

    pub fn add_child_to_group(&mut self, payload: T, group_key: &str) -> Result<(), anyhow::Error> {
        let key = (&payload).into().to_string();

        if !self.has_child(group_key) {
            self.dependents.push(DependencyNode::new_group(group_key));
        }

        if !self.has_child((&payload).into()) {
            self.dependents.push(DependencyNode::new_node(payload));
        }
        self.move_into(group_key, &key)?;
        Ok(())
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
                        let needle: &str = (&payload).into();
                        let found = payloads.iter().find(|x| {
                            let haystack: &str = (*x).into();
                            needle == haystack
                        });
                        if found.is_none() {
                            payloads.push(payload);
                        }
                    },
                    _ => Err(anyhow::anyhow!("can only move node type"))?
                };
            },
            _ => Err(anyhow::anyhow!("can only move into group node"))?
        };
        Ok(())
    }
}

pub fn chunk_by_depth<T>(node: DependencyNode<T>) -> Vec<Vec<Vec<T>>> {
    let mut depths: Vec<Vec<Vec<T>>> = Vec::new();
    let mut dfs: Vec<(DependencyNode<T>, usize)> = Vec::new();
    for dep in node.dependents.into_iter() { dfs.push((dep, 0)) };

    let mut popped = dfs.pop();

    while popped.is_some() {
        let (node, depth) = popped.unwrap();
        if depths.len() == depth {
            depths.push(Vec::new());
        }

        if let NodeType::Group { payloads, .. } = node.node_type {
            depths[depth].push(payloads);
        }

        for dep in node.dependents.into_iter() {
            dfs.push((dep, depth + 1));
        }

        popped = dfs.pop();
    }

    depths
}
