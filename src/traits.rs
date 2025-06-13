use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use thiserror::Error;
use std::rc::Weak;
use std::cell::RefCell;

pub trait ReferenceTracker {
    fn get_references(&self) -> &HashMap<String, HashSet<String>>;
    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>>;

    fn capture_reference(&mut self, key: &str, value: &str) -> Result<(), anyhow::Error> {
        let Some(set) = self.get_references_mut().get_mut(key) else { return Err(anyhow::anyhow!("unknown reference key")) };
        set.insert(value.to_owned());
        Ok(())
    }
}

pub trait Dependency {
    fn set_fulfilled_at_depth(&mut self, depth: &usize);
    fn has_been_fulfilled(&self) -> bool;

    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>];

    fn has_fulfilled_dependencies(&self) -> bool {
        self.get_dependencies().iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.set_fulfilled_at_depth(depth);
        }
        assert!(self.has_been_fulfilled());
    }
}

#[derive(Debug)]
#[derive(Error)]
pub struct NoDataTypeError;

impl std::fmt::Display for NoDataTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "no data type")
    }
}
