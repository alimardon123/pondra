//! The registry (designs/redesign/redesign.md §6): the one place a feature says what it adds.
//!
//! A section is a typed table (`objects::Kinds`, …), defined by the module that reads it, so this
//! module knows no section and depends on nothing. Every extension, ours built in or one installed,
//! adds its entries through `Extension::register`, and no other list names them. The registry is
//! built once, from the extensions the build has (`crate::built_in`), and read as a frozen snapshot
//! from then on: a hot path takes one atomic load and no lock.
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::OnceLock;

/// A section of the registry: one kind of entry (kinds of object, statements, functions, settings…).
pub trait Section: Any + Default + Send + Sync {
    /// Its name, for what is said when one is missing.
    const NAME: &'static str;
}

/// A unit of the product that adds entries: a door, a format, a kind of object, a set of functions.
/// Extensions never call each other's code; they meet in the registry and the catalog, and say which
/// others they build on in `requires`, checked when the registry is built.
pub trait Extension: Sync {
    fn name(&self) -> &'static str;
    /// The extensions this one builds on, by name.
    fn requires(&self) -> &'static [&'static str] {
        &[]
    }
    /// Adds its entries, and does nothing else: it reads no setting (a setting is read through the registry being built,
    /// so a read here would wait on itself) and starts nothing.
    fn register(&self, r: &mut Registry);
}

/// An extension compiled into the binary: its name, what it builds on, and what it adds. Each built-in one is a
/// `static` of this beside the code it adds (`objects::CATALOG`, `engine::ENGINE`, …).
pub struct BuiltIn {
    pub name: &'static str,
    pub requires: &'static [&'static str],
    pub register: fn(&mut Registry),
}

impl Extension for BuiltIn {
    fn name(&self) -> &'static str {
        self.name
    }
    fn requires(&self) -> &'static [&'static str] {
        self.requires
    }
    fn register(&self, r: &mut Registry) {
        (self.register)(r)
    }
}

/// Every section, by its type, and the extensions that filled them.
#[derive(Default)]
pub struct Registry {
    sections: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    extensions: Vec<&'static str>,
}

impl Registry {
    /// Built from these extensions, in order: each one's `requires` must come before it, and two
    /// extensions can't have one name.
    pub fn build(extensions: &[&dyn Extension]) -> Registry {
        let mut r = Registry::default();
        for x in extensions {
            assert!(!r.extensions.contains(&x.name()), "two extensions are named {}", x.name());
            for need in x.requires() {
                assert!(r.extensions.contains(need), "extension {} requires {need}, which this build doesn't have before it", x.name());
            }
            x.register(&mut r);
            r.extensions.push(x.name());
        }
        r
    }

    /// A section, as its extensions filled it.
    pub fn section<S: Section>(&self) -> &S {
        self.sections
            .get(&TypeId::of::<S>())
            .and_then(|s| s.downcast_ref())
            .unwrap_or_else(|| panic!("no extension of this build adds to the registry's {} section", S::NAME))
    }

    /// A section, or None when no extension of this build adds to it (a reader that has nothing to do without one).
    pub fn find<S: Section>(&self) -> Option<&S> {
        self.sections.get(&TypeId::of::<S>()).and_then(|s| s.downcast_ref())
    }

    /// A section to add to, made empty the first time.
    pub fn section_mut<S: Section>(&mut self) -> &mut S {
        self.sections.entry(TypeId::of::<S>()).or_insert_with(|| Box::new(S::default())).downcast_mut().expect("a section is kept under its own type")
    }

    /// The extensions registered, in the order they were.
    pub fn extensions(&self) -> &[&'static str] {
        &self.extensions
    }
}

/// The registry of this build's extensions, built on first use and never changed after.
pub fn get() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry::build(crate::built_in()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Words(Vec<&'static str>);
    impl Section for Words {
        const NAME: &'static str = "words";
    }
    struct Adds(&'static str, &'static [&'static str], &'static str);
    impl Extension for Adds {
        fn name(&self) -> &'static str {
            self.0
        }
        fn requires(&self) -> &'static [&'static str] {
            self.1
        }
        fn register(&self, r: &mut Registry) {
            r.section_mut::<Words>().0.push(self.2);
        }
    }

    #[test]
    fn extensions_fill_one_section_in_order() {
        let r = Registry::build(&[&Adds("a", &[], "one"), &Adds("b", &["a"], "two")]);
        assert_eq!(r.section::<Words>().0, ["one", "two"]);
        assert_eq!(r.extensions(), ["a", "b"]);
    }

    #[test]
    #[should_panic(expected = "requires a")]
    fn a_requirement_comes_first() {
        Registry::build(&[&Adds("b", &["a"], "two"), &Adds("a", &[], "one")]);
    }

    #[test]
    #[should_panic(expected = "two extensions are named a")]
    fn names_are_one_each() {
        Registry::build(&[&Adds("a", &[], "one"), &Adds("a", &[], "two")]);
    }

    #[test]
    #[should_panic(expected = "registry's words section")]
    fn a_missing_section_is_named() {
        Registry::build(&[]).section::<Words>();
    }

    #[test]
    fn a_missing_section_is_found_as_none() {
        assert!(Registry::build(&[]).find::<Words>().is_none());
        assert_eq!(Registry::build(&[&Adds("a", &[], "one")]).find::<Words>().map(|w| w.0.clone()), Some(vec!["one"]));
    }

}
