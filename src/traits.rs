use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use thiserror::Error;
use std::rc::Weak;
use std::cell::RefCell;

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
