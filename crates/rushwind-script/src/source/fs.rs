//! The static-tree script source.
//!
//! The source reads through [`StaticTree`], a
//! caller-supplied immutable name-to-content lookup —
//! Rust has
//! no standard virtual-filesystem trait, so the contract defines the
//! narrowest one that preserves the pattern — and the source
//! contributes the prefix joining. An embedded-asset source is a
//! `StaticTree` over `include_str!` maps; a zip-backed one is a
//! `StaticTree` over the caller's archive reader; the tree lives with
//! the caller and the source adds no lifecycle.
//!
//! There is no
//! watch capability — the trees are immutable, so the source is
//! reader-only.

use std::sync::Arc;

use crate::source::ScriptSource;
use crate::{BoxFuture, ScriptError};

/// A caller-supplied immutable name-to-content tree.
///
/// Implementations must be pure lookups: no watching, no mutation, no
/// side effects — the `fs.FS` read-only contract distilled to the one
/// method this source uses.
pub trait StaticTree: Send + Sync {
    /// Returns the content stored under `path`, or `None` when the
    /// tree holds nothing under that name.
    fn read(&self, path: &str) -> Option<String>;
}

/// A source serving scripts from a [`StaticTree`], optionally under a
/// prefix.
pub struct FileSystemSource {
    tree: Arc<dyn StaticTree>,
    prefix: String,
}

impl FileSystemSource {
    /// Creates the source over `tree`.
    pub fn new(tree: Arc<dyn StaticTree>) -> Self {
        Self {
            tree,
            prefix: String::new(),
        }
    }

    /// Sets the prefix prepended to every key before lookup. A leading
    /// slash is stripped from the prefix and a trailing slash appended
    /// when missing — so `with_prefix("scripts")` plus the key
    /// `main.lua` resolves `scripts/main.lua`.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        let p = prefix.strip_prefix('/').unwrap_or(prefix);
        self.prefix = if !p.is_empty() && !p.ends_with('/') {
            format!("{p}/")
        } else {
            p.to_string()
        };
        self
    }
}

impl ScriptSource for FileSystemSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        let result = {
            let key = key.strip_prefix('/').unwrap_or(key);
            let full = format!("{}{}", self.prefix, key);
            self.tree
                .read(&full)
                .ok_or_else(|| ScriptError::Failed(format!("fs source: read {full:?}: not found")))
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;
    use std::collections::HashMap;

    struct MapTree(HashMap<String, String>);

    impl StaticTree for MapTree {
        fn read(&self, path: &str) -> Option<String> {
            self.0.get(path).cloned()
        }
    }

    struct DirTree(std::path::PathBuf);

    impl StaticTree for DirTree {
        fn read(&self, path: &str) -> Option<String> {
            std::fs::read_to_string(self.0.join(path)).ok()
        }
    }

    fn map_tree(pairs: &[(&str, &str)]) -> Arc<dyn StaticTree> {
        Arc::new(MapTree(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn a_map_tree_serves_its_entries() {
        let source = FileSystemSource::new(map_tree(&[("scripts/a.lua", "map-content")]));
        assert_eq!(
            source
                .load("scripts/a.lua")
                .await
                .expect("load from map tree"),
            "map-content"
        );
    }

    #[tokio::test]
    async fn a_prefix_joins_the_lookup_path() {
        let source = FileSystemSource::new(map_tree(&[("scripts/b.lua", "prefixed")]))
            .with_prefix("scripts");
        assert_eq!(
            source.load("b.lua").await.expect("prefixed load"),
            "prefixed"
        );
    }

    #[tokio::test]
    async fn a_leading_slash_strips_before_the_prefix_joins() {
        let source =
            FileSystemSource::new(map_tree(&[("scripts/c.lua", "slashed")])).with_prefix("scripts");
        assert_eq!(
            source.load("/c.lua").await.expect("slash-stripped load"),
            "slashed"
        );
    }

    #[tokio::test]
    async fn a_missing_name_fails() {
        let source = FileSystemSource::new(map_tree(&[("x", "y")]));
        assert!(matches!(
            source.load("scripts/missing.lua").await,
            Err(ScriptError::Failed(msg)) if msg.contains("fs source")
        ));
    }

    #[tokio::test]
    async fn a_directory_tree_serves_real_files() {
        let dir = TempDir::new("fs-dirtree");
        dir.write("inner/d.lua", "dir-content");
        let source = FileSystemSource::new(Arc::new(DirTree(dir.path("inner"))));
        assert_eq!(
            source.load("d.lua").await.expect("load from dir tree"),
            "dir-content"
        );
    }

    #[tokio::test]
    async fn a_directory_tree_honors_a_prefix() {
        let dir = TempDir::new("fs-dirtree-prefix");
        dir.write("scripts/e.lua", "prefixed-dir-content");
        let source =
            FileSystemSource::new(Arc::new(DirTree(dir.path(".")))).with_prefix("/scripts/");
        assert_eq!(
            source.load("e.lua").await.expect("prefixed dir load"),
            "prefixed-dir-content"
        );
    }

    #[test]
    fn prefixes_normalize_predictably() {
        let tree = map_tree(&[]);
        let cases: [(&str, &str); 4] = [("/a/", "a/"), ("b", "b/"), ("c/", "c/"), ("/", "")];
        for (input, expected) in cases {
            let source = FileSystemSource::new(tree.clone()).with_prefix(input);
            assert_eq!(source.prefix, expected, "prefix {input:?}");
        }
    }

    #[tokio::test]
    async fn concurrent_loads_through_one_source_are_safe() {
        let source = Arc::new(FileSystemSource::new(map_tree(&[("k", "v")])));
        let loads: Vec<_> = (0..8)
            .map(|_| {
                let source = source.clone();
                async move { source.load("k").await }
            })
            .collect();
        let results = futures::future::join_all(loads).await;
        for result in results {
            assert_eq!(result.expect("load"), "v");
        }
    }
}
