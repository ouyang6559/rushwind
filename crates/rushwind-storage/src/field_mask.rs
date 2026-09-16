//! Field-level projection.

use std::collections::BTreeSet;

/// The set of fields a caller wants back — a field mask.
///
/// An empty mask means "return everything". When a mask is set, engines
/// project returned rows down to it; the conformance suite pins the
/// subset semantics (every returned field must be in the mask).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldMask(BTreeSet<String>);

impl FieldMask {
    /// A mask selecting nothing, i.e. return all fields.
    pub fn all() -> Self {
        Self(BTreeSet::new())
    }

    /// A mask over the given paths.
    pub fn of(paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(paths.into_iter().map(Into::into).collect())
    }

    /// Returns `true` when no projection is requested.
    pub fn is_all(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` when the named field may be returned. An all-fields
    /// mask allows everything.
    pub fn allows(&self, field: &str) -> bool {
        self.is_all() || self.0.contains(field)
    }

    /// The selected paths, in deterministic order.
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_mask_means_all() {
        let mask = FieldMask::all();
        assert!(mask.is_all());
        assert!(mask.allows("anything"));
    }

    #[test]
    fn selection_is_explicit() {
        let mask = FieldMask::of(["id", "name"]);
        assert!(!mask.is_all());
        assert!(mask.allows("name"));
        assert!(!mask.allows("age"));
        assert_eq!(mask.paths().collect::<Vec<_>>(), vec!["id", "name"]);
    }
}
