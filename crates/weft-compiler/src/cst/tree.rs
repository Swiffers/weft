//! A CST under edit.
//!
//! rowan trees are immutable: an edit builds a NEW tree that shares every
//! untouched subtree with the old one. One edit is a splice of one node's child
//! list, done the way rowan's own API and rust-analyzer's `SyntaxEditor` do it:
//! `GreenNodeData::splice_children` on the parent's green node, then
//! `SyntaxNode::replace_with` rebuilds the path up to a new root. Only the
//! spliced node and its ancestors are new; every other byte is carried over.
//!
//! A handle (`SyntaxNode`, `SyntaxToken`, a typed node) belongs to the version
//! it was read from and never changes. An op that collects handles and then
//! edits must find each one again in the newest version: `Tree::now` does it by
//! the handle's PATH (its child index at each level from the root), carried
//! through every splice made since, the way rust-analyzer's syntax editor maps
//! an element through the edits it applied. A handle whose element a splice
//! removed maps to `None`.

use std::cell::RefCell;
use std::ops::Range;

use rowan::{GreenNode, GreenToken, NodeOrToken};

use super::kind::{SyntaxElement, SyntaxNode, SyntaxToken};

/// An owned green element, the unit a splice inserts.
pub type GreenElement = NodeOrToken<GreenNode, GreenToken>;

/// Anything that names one element of the tree and can be found again in a
/// newer version: the raw node and token handles and every typed node.
pub trait Handle: Sized {
    fn element(&self) -> SyntaxElement;
    fn from_element(element: SyntaxElement) -> Option<Self>;
}

impl Handle for SyntaxNode {
    fn element(&self) -> SyntaxElement {
        NodeOrToken::Node(self.clone())
    }
    fn from_element(element: SyntaxElement) -> Option<Self> {
        element.into_node()
    }
}

impl Handle for SyntaxToken {
    fn element(&self) -> SyntaxElement {
        NodeOrToken::Token(self.clone())
    }
    fn from_element(element: SyntaxElement) -> Option<Self> {
        element.into_token()
    }
}

impl Handle for SyntaxElement {
    fn element(&self) -> SyntaxElement {
        self.clone()
    }
    fn from_element(element: SyntaxElement) -> Option<Self> {
        Some(element)
    }
}

/// The green form of an element, to splice it (or a copy of it) elsewhere.
pub fn green_of(element: &SyntaxElement) -> GreenElement {
    match element {
        NodeOrToken::Node(n) => NodeOrToken::Node(n.green().to_owned()),
        NodeOrToken::Token(t) => NodeOrToken::Token(t.green().to_owned()),
    }
}

/// Every version of the tree an edit produced, newest last, and the splice
/// between each consecutive pair.
pub struct Tree {
    state: RefCell<History>,
}

struct History {
    /// `versions[i]` is the root after `splices[..i]`.
    versions: Vec<SyntaxNode>,
    splices: Vec<Splice>,
}

/// One splice: the children `removed` of the node at `parent` replaced by
/// `inserted` new ones.
struct Splice {
    parent: Vec<usize>,
    removed: Range<usize>,
    inserted: usize,
}

impl Splice {
    /// Carry `path` across this splice. False when the splice removed the
    /// element `path` names (or one of its ancestors).
    fn carry(&self, path: &mut [usize]) -> bool {
        let depth = self.parent.len();
        if path.len() <= depth || path[..depth] != self.parent[..] {
            return true;
        }
        let index = path[depth];
        if index >= self.removed.end {
            path[depth] = index - self.removed.len() + self.inserted;
        } else if index >= self.removed.start {
            return false;
        }
        true
    }
}

/// The child index at each level from the root down to `element`.
fn path_of(element: &SyntaxElement) -> Vec<usize> {
    let mut path = Vec::new();
    let mut current = element.clone();
    while let Some(parent) = current.parent() {
        path.push(current.index());
        current = NodeOrToken::Node(parent);
    }
    path.reverse();
    path
}

/// The element at `path` under `root`.
fn resolve(root: &SyntaxNode, path: &[usize]) -> Option<SyntaxElement> {
    let mut element = NodeOrToken::Node(root.clone());
    for &index in path {
        element = element.into_node()?.children_with_tokens().nth(index)?;
    }
    Some(element)
}

impl Tree {
    pub fn new(root: SyntaxNode) -> Self {
        Self { state: RefCell::new(History { versions: vec![root], splices: Vec::new() }) }
    }

    /// The newest version's root.
    pub fn root(&self) -> SyntaxNode {
        self.state.borrow().versions.last().expect("a tree has a version").clone()
    }

    /// `handle` found again in the newest version, or `None` when an edit since
    /// the version it was read from removed it.
    pub fn now<H: Handle>(&self, handle: &H) -> Option<H> {
        let element = handle.element();
        let history = self.state.borrow();
        let top = element.tree_top();
        let version = history
            .versions
            .iter()
            .rposition(|root| *root == top)
            .expect("a handle read from another tree");
        let mut path = path_of(&element);
        for splice in &history.splices[version..] {
            if !splice.carry(&mut path) {
                return None;
            }
        }
        H::from_element(resolve(history.versions.last()?, &path)?)
    }

    /// Replace the children `range` of `parent` with `with`. `range` counts the
    /// children of `parent` as they are in the newest version.
    pub fn splice(&self, parent: &SyntaxNode, range: Range<usize>, with: Vec<GreenElement>) {
        let parent = self.now(parent).expect("an edit inside a node an earlier edit removed");
        let inserted = with.len();
        let green = parent.green().splice_children(range.clone(), with);
        let root = SyntaxNode::new_root(parent.replace_with(green));
        let mut history = self.state.borrow_mut();
        history.splices.push(Splice { parent: path_of(&parent.element()), removed: range, inserted });
        history.versions.push(root);
    }

    /// Remove `element`.
    pub fn remove<H: Handle>(&self, element: &H) {
        self.replace(element, Vec::new());
    }

    /// Replace `element` with `with`.
    pub fn replace<H: Handle>(&self, element: &H, with: Vec<GreenElement>) {
        let (parent, index) = self.anchor(element);
        self.splice(&parent, index..index + 1, with);
    }

    /// Insert `with` right before `element`.
    pub fn insert_before<H: Handle>(&self, element: &H, with: Vec<GreenElement>) {
        let (parent, index) = self.anchor(element);
        self.splice(&parent, index..index, with);
    }

    /// Insert `with` right after `element`.
    pub fn insert_after<H: Handle>(&self, element: &H, with: Vec<GreenElement>) {
        let (parent, index) = self.anchor(element);
        self.splice(&parent, index + 1..index + 1, with);
    }

    /// Append `with` after the last child of `parent`.
    pub fn append(&self, parent: &SyntaxNode, with: Vec<GreenElement>) {
        let parent = self.now(parent).expect("an edit inside a node an earlier edit removed");
        let end = parent.children_with_tokens().count();
        self.splice(&parent, end..end, with);
    }

    /// `element`'s parent and index in the newest version.
    fn anchor<H: Handle>(&self, element: &H) -> (SyntaxNode, usize) {
        let element = self
            .now(&element.element())
            .expect("an edit anchored on an element an earlier edit removed");
        let parent = element.parent().expect("an edit anchored on the root");
        (parent, element.index())
    }
}
