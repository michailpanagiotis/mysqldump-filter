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

#[cfg(test)]
mod tests {
    use super::*;

    // Mock type for testing that implements the required Into<&str> trait
    #[derive(Debug, Clone, PartialEq)]
    struct MockPayload {
        key: String,
    }

    impl<'a> From<&'a MockPayload> for &'a str {
        fn from(item: &'a MockPayload) -> Self {
            &item.key
        }
    }

    fn create_mock_payload(key: &str) -> MockPayload {
        MockPayload {
            key: key.to_string(),
        }
    }

    #[test]
    fn test_dependency_node_new() {
        let node: DependencyNode<MockPayload> = DependencyNode::new();
        match node.node_type {
            NodeType::Root => {},
            _ => panic!("Expected Root node type"),
        }
        assert!(node.dependents.is_empty());
        assert_eq!(node.get_key(), "root");
    }

    #[test]
    fn test_dependency_node_new_node() {
        let payload = create_mock_payload("test");
        let node = DependencyNode::new_node(payload);

        match node.node_type {
            NodeType::Node { ref payload } => {
                assert_eq!(payload.key, "test");
            },
            _ => panic!("Expected Node type"),
        }
        assert!(node.dependents.is_empty());
        assert_eq!(node.get_key(), "test");
    }

    #[test]
    fn test_dependency_node_new_group() {
        let node: DependencyNode<MockPayload> = DependencyNode::new_group("test_group");

        match node.node_type {
            NodeType::Group { ref name, ref payloads } => {
                assert_eq!(name, "test_group");
                assert!(payloads.is_empty());
            },
            _ => panic!("Expected Group type"),
        }
        assert!(node.dependents.is_empty());
        assert_eq!(node.get_key(), "test_group");
    }

    #[test]
    fn test_dependency_node_get_key() {
        let root: DependencyNode<MockPayload> = DependencyNode::new();
        assert_eq!(root.get_key(), "root");

        let payload = create_mock_payload("node1");
        let node = DependencyNode::new_node(payload);
        assert_eq!(node.get_key(), "node1");

        let group: DependencyNode<MockPayload> = DependencyNode::new_group("group1");
        assert_eq!(group.get_key(), "group1");
    }

    #[test]
    fn test_dependency_node_has_child_direct() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let payload = create_mock_payload("child1");
        root.dependents.push(DependencyNode::new_node(payload));

        assert!(root.has_child("child1"));
        assert!(!root.has_child("nonexistent"));
    }

    #[test]
    fn test_dependency_node_has_child_nested() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let mut intermediate = DependencyNode::new_group("intermediate");
        let leaf = DependencyNode::new_node(create_mock_payload("leaf"));

        intermediate.dependents.push(leaf);
        root.dependents.push(intermediate);

        assert!(root.has_child("leaf"));
        assert!(root.has_child("intermediate"));
        assert!(!root.has_child("nonexistent"));
    }

    #[test]
    fn test_dependency_node_has_child_self() {
        let node = DependencyNode::new_node(create_mock_payload("self"));
        assert!(node.has_child("self"));
    }

    #[test]
    fn test_dependency_node_add_child_to_group_new_group() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let payload = create_mock_payload("item1");

        let result = root.add_child_to_group(payload, "new_group");
        assert!(result.is_ok());

        dbg!(&root);

        // Check that only the group ended up as a child
        assert!(root.has_child("new_group"));
        assert!(!root.has_child("item1"));

        // Verify the item was moved into the group
        assert_eq!(root.dependents.len(), 1); // Only the group should remain at root level

        let group_node = root.dependents.iter().find(|n| n.get_key() == "new_group").unwrap();
        match &group_node.node_type {
            NodeType::Group { payloads, .. } => {
                assert_eq!(payloads.len(), 1);
                assert_eq!(payloads[0].key, "item1");
            },
            _ => panic!("Expected group node"),
        }
    }

    #[test]
    fn test_dependency_node_add_child_to_group_existing_group() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();

        // Add first item to create group
        let payload1 = create_mock_payload("item1");
        root.add_child_to_group(payload1, "existing_group").unwrap();

        // Add second item to same group
        let payload2 = create_mock_payload("item2");
        root.add_child_to_group(payload2, "existing_group").unwrap();

        assert_eq!(root.dependents.len(), 1); // Still only one group

        let group_node = &root.dependents[0];
        match &group_node.node_type {
            NodeType::Group { payloads, .. } => {
                assert_eq!(payloads.len(), 2);
                assert!(payloads.iter().any(|p| p.key == "item1"));
                assert!(payloads.iter().any(|p| p.key == "item2"));
            },
            _ => panic!("Expected group node"),
        }
    }

    #[test]
    fn test_dependency_node_pop_child_direct() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let payload = create_mock_payload("child1");
        root.dependents.push(DependencyNode::new_node(payload));

        let popped = root.pop_child("child1");
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().get_key(), "child1");
        assert!(root.dependents.is_empty());
    }

    #[test]
    fn test_dependency_node_pop_child_nested() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let mut intermediate = DependencyNode::new_group("intermediate");
        let leaf = DependencyNode::new_node(create_mock_payload("leaf"));

        intermediate.dependents.push(leaf);
        root.dependents.push(intermediate);

        let popped = root.pop_child("leaf");
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().get_key(), "leaf");

        // Intermediate should still exist but leaf should be gone
        assert!(root.has_child("intermediate"));
        assert!(!root.has_child("leaf"));
    }

    #[test]
    fn test_dependency_node_pop_child_nonexistent() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let popped = root.pop_child("nonexistent");
        assert!(popped.is_none());
    }

    #[test]
    fn test_dependency_node_get_node_mut() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_group("group1"));

        let node_mut = root.get_node_mut("group1");
        assert!(node_mut.is_some());
        assert_eq!(node_mut.unwrap().get_key(), "group1");

        let nonexistent = root.get_node_mut("nonexistent");
        assert!(nonexistent.is_none());
    }

    #[test]
    fn test_dependency_node_get_node_mut_self() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        let node_mut = root.get_node_mut("root");
        assert!(node_mut.is_some());
        assert_eq!(node_mut.unwrap().get_key(), "root");
    }

    #[test]
    fn test_dependency_node_move_under() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_group("parent"));
        root.dependents.push(DependencyNode::new_node(create_mock_payload("child")));

        let result = root.move_under("parent", "child");
        assert!(result.is_ok());

        // Child should no longer be at root level
        assert_eq!(root.dependents.len(), 1);
        assert_eq!(root.dependents[0].get_key(), "parent");

        // Child should be under parent
        assert_eq!(root.dependents[0].dependents.len(), 1);
        assert_eq!(root.dependents[0].dependents[0].get_key(), "child");
    }

    #[test]
    fn test_dependency_node_move_under_nonexistent_parent() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_node(create_mock_payload("child")));

        let result = root.move_under("nonexistent", "child");
        assert!(result.is_err());
    }

    #[test]
    fn test_dependency_node_move_under_nonexistent_child() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_group("parent"));

        let result = root.move_under("parent", "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_dependency_node_move_into_group() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_group("group1"));
        root.dependents.push(DependencyNode::new_node(create_mock_payload("item1")));

        let result = root.move_into("group1", "item1");
        assert!(result.is_ok());

        // Item should no longer be at root level
        assert_eq!(root.dependents.len(), 1);

        // Item should be in the group
        let group_node = &root.dependents[0];
        match &group_node.node_type {
            NodeType::Group { payloads, .. } => {
                assert_eq!(payloads.len(), 1);
                assert_eq!(payloads[0].key, "item1");
            },
            _ => panic!("Expected group node"),
        }
    }

    #[test]
    fn test_dependency_node_move_into_duplicate_item() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.add_child_to_group(create_mock_payload("item1"), "group1").unwrap();
        root.dependents.push(DependencyNode::new_node(create_mock_payload("item1")));

        let result = root.move_into("group1", "item1");
        assert!(result.is_ok());

        // Should still have only one copy in the group
        let group_node = &root.dependents[0];
        match &group_node.node_type {
            NodeType::Group { payloads, .. } => {
                assert_eq!(payloads.len(), 1);
                assert_eq!(payloads[0].key, "item1");
            },
            _ => panic!("Expected group node"),
        }
    }

    #[test]
    fn test_dependency_node_move_into_non_group() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_node(create_mock_payload("not_group")));
        root.dependents.push(DependencyNode::new_node(create_mock_payload("item1")));

        let result = root.move_into("not_group", "item1");
        assert!(result.is_err());
    }

    #[test]
    fn test_dependency_node_move_into_non_node_child() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.dependents.push(DependencyNode::new_group("group1"));
        root.dependents.push(DependencyNode::new_group("group2"));

        let result = root.move_into("group1", "group2");
        assert!(result.is_err());
    }

    #[test]
    fn test_chunk_by_depth_single_level() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();
        root.add_child_to_group(create_mock_payload("item1"), "group1").unwrap();
        root.add_child_to_group(create_mock_payload("item2"), "group1").unwrap();
        root.add_child_to_group(create_mock_payload("item3"), "group2").unwrap();

        let result = chunk_by_depth(root);

        assert_eq!(result.len(), 1); // Only one depth level
        assert_eq!(result[0].len(), 2); // Two groups

        // Verify contents
        let all_items: Vec<String> = result[0].iter()
            .flat_map(|group| group.iter().map(|item| item.key.clone()))
            .collect();

        assert!(all_items.contains(&"item1".to_string()));
        assert!(all_items.contains(&"item2".to_string()));
        assert!(all_items.contains(&"item3".to_string()));
    }

    #[test]
    fn test_chunk_by_depth_multiple_levels() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();

        // Level 0: parent group
        root.add_child_to_group(create_mock_payload("parent_item"), "parent_group").unwrap();

        // Level 1: child group under parent
        root.add_child_to_group(create_mock_payload("child_item"), "child_group").unwrap();
        root.move_under("parent_group", "child_group").unwrap();

        let result = chunk_by_depth(root);

        assert_eq!(result.len(), 2); // Two depth levels

        // Level 0 should have parent group
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0].len(), 1);
        assert_eq!(result[0][0][0].key, "parent_item");

        // Level 1 should have child group
        assert_eq!(result[1].len(), 1);
        assert_eq!(result[1][0].len(), 1);
        assert_eq!(result[1][0][0].key, "child_item");
    }

    #[test]
    fn test_chunk_by_depth_empty() {
        let root: DependencyNode<MockPayload> = DependencyNode::new();
        let result = chunk_by_depth(root);
        assert!(result.is_empty());
    }

    #[test]
    fn test_chunk_by_depth_complex_hierarchy() {
        let mut root: DependencyNode<MockPayload> = DependencyNode::new();

        // Create a more complex hierarchy
        // Level 0: two groups
        root.add_child_to_group(create_mock_payload("l0_item1"), "l0_group1").unwrap();
        root.add_child_to_group(create_mock_payload("l0_item2"), "l0_group2").unwrap();

        // Level 1: children of l0_group1
        root.add_child_to_group(create_mock_payload("l1_item1"), "l1_group1").unwrap();
        root.add_child_to_group(create_mock_payload("l1_item2"), "l1_group2").unwrap();
        root.move_under("l0_group1", "l1_group1").unwrap();
        root.move_under("l0_group1", "l1_group2").unwrap();

        // Level 2: child of l1_group1
        root.add_child_to_group(create_mock_payload("l2_item1"), "l2_group1").unwrap();
        root.move_under("l1_group1", "l2_group1").unwrap();

        let result = chunk_by_depth(root);

        assert_eq!(result.len(), 3); // Three depth levels

        // Level 0: 2 groups
        assert_eq!(result[0].len(), 2);

        // Level 1: 2 groups (both under l0_group1)
        assert_eq!(result[1].len(), 2);

        // Level 2: 1 group
        assert_eq!(result[2].len(), 1);
        assert_eq!(result[2][0].len(), 1);
        assert_eq!(result[2][0][0].key, "l2_item1");
    }

    #[test]
    fn test_node_type_enum_variants() {
        // Test Root variant
        let root_type: NodeType<MockPayload> = NodeType::Root;
        match root_type {
            NodeType::Root => {},
            _ => panic!("Expected Root variant"),
        }

        // Test Node variant
        let payload = create_mock_payload("test");
        let node_type = NodeType::Node { payload };
        match node_type {
            NodeType::Node { payload } => assert_eq!(payload.key, "test"),
            _ => panic!("Expected Node variant"),
        }

        // Test Group variant
        let group_type: NodeType<MockPayload> = NodeType::Group {
            name: "test_group".to_string(),
            payloads: vec![create_mock_payload("item1")],
        };
        match group_type {
            NodeType::Group { name, payloads } => {
                assert_eq!(name, "test_group");
                assert_eq!(payloads.len(), 1);
                assert_eq!(payloads[0].key, "item1");
            },
            _ => panic!("Expected Group variant"),
        }
    }

    #[test]
    fn test_lazy_static_root() {
        assert_eq!(*ROOT, "root");
    }
}
