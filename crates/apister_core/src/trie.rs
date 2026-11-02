use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;

const MAX_TOPIC_BYTES: usize = 1024;

fn parse_topic(topic: &str, pattern: bool) -> PyResult<Vec<&str>> {
    if topic.is_empty() {
        return Err(PyValueError::new_err("topic must be a non-empty string"));
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(PyValueError::new_err(format!(
            "topic exceeds {} bytes",
            MAX_TOPIC_BYTES
        )));
    }
    if topic.starts_with('/') || topic.ends_with('/') || topic.contains("//") {
        return Err(PyValueError::new_err("topic must not have empty segments"));
    }
    let parts: Vec<&str> = topic.split('/').collect();
    let last = parts.len() - 1;
    for (index, part) in parts.iter().enumerate() {
        if *part == "#" {
            if !pattern || index != last {
                return Err(PyValueError::new_err(
                    "'#' is only valid as the final pattern segment",
                ));
            }
            continue;
        }
        if *part == "+" {
            if !pattern {
                return Err(PyValueError::new_err(
                    "'+' is only valid in subscription patterns",
                ));
            }
            continue;
        }
        if part.contains('+') || part.contains('#') {
            return Err(PyValueError::new_err("wildcards must occupy a whole segment"));
        }
    }
    Ok(parts)
}

struct Node {
    children: HashMap<String, Node>,
    subscribers: Vec<Py<PyAny>>,
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            subscribers: Vec::new(),
        }
    }
}

#[pyclass]
pub struct RustTopicTrie {
    root: Node,
}

impl Default for RustTopicTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RustTopicTrie {
    #[new]
    pub fn new() -> Self {
        Self { root: Node::new() }
    }

    pub fn add(&mut self, pattern: &str, subscription: Py<PyAny>) -> PyResult<()> {
        let parts = parse_topic(pattern, true)?;
        let mut curr = &mut self.root;
        for segment in parts {
            curr = curr
                .children
                .entry(segment.to_string())
                .or_insert_with(Node::new);
        }
        if !curr.subscribers.iter().any(|s| s.is(&subscription)) {
            curr.subscribers.push(subscription);
        }
        Ok(())
    }

    pub fn discard(&mut self, pattern: &str, subscription: &Bound<'_, PyAny>) -> PyResult<()> {
        let parts = parse_topic(pattern, true)?;
        let sub_py = subscription.as_unbound();

        fn clean_walk(node: &mut Node, parts: &[&str], idx: usize, sub: &Py<PyAny>) -> bool {
            if idx == parts.len() {
                node.subscribers.retain(|s| !s.is(sub));
            } else {
                let seg = parts[idx];
                if let Some(child) = node.children.get_mut(seg) {
                    let should_prune = clean_walk(child, parts, idx + 1, sub);
                    if should_prune {
                        node.children.remove(seg);
                    }
                }
            }
            node.subscribers.is_empty() && node.children.is_empty()
        }

        clean_walk(&mut self.root, &parts, 0, sub_py);
        Ok(())
    }

    pub fn match_topic(&self, py: Python<'_>, topic: &str) -> PyResult<Vec<Py<PyAny>>> {
        let segments = parse_topic(topic, false)?;
        let mut found: Vec<Py<PyAny>> = Vec::new();

        fn walk(
            node: &Node,
            segments: &[&str],
            index: usize,
            found: &mut Vec<Py<PyAny>>,
            py: Python<'_>,
        ) {
            if index == segments.len() {
                for sub in &node.subscribers {
                    found.push(sub.clone_ref(py));
                }
                if let Some(rest) = node.children.get("#") {
                    for sub in &rest.subscribers {
                        found.push(sub.clone_ref(py));
                    }
                }
                return;
            }

            if let Some(exact) = node.children.get(segments[index]) {
                walk(exact, segments, index + 1, found, py);
            }
            if let Some(plus) = node.children.get("+") {
                walk(plus, segments, index + 1, found, py);
            }
            if let Some(rest) = node.children.get("#") {
                for sub in &rest.subscribers {
                    found.push(sub.clone_ref(py));
                }
            }
        }

        walk(&self.root, &segments, 0, &mut found, py);
        Ok(found)
    }

    pub fn __len__(&self) -> usize {
        let mut total = 0;
        let mut stack = vec![&self.root];
        while let Some(node) = stack.pop() {
            total += node.subscribers.len();
            for child in node.children.values() {
                stack.push(child);
            }
        }
        total
    }
}
