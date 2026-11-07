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
    /// What is wrong with its entries once every extension has added its own (two that clash, such as two catalog
    /// prefixes where one starts the other), read with the rest of the registry. The build stops on it.
    fn check(&self, _r: &Registry) -> Result<(), String> {
        Ok(())
    }
}

/// A section as the registry keeps it: its type forgotten, its name and check kept.
trait Stored: Any + Send + Sync {
    fn name(&self) -> &'static str;
    fn check(&self, r: &Registry) -> Result<(), String>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<S: Section> Stored for S {
    fn name(&self) -> &'static str {
        S::NAME
    }
    fn check(&self, r: &Registry) -> Result<(), String> {
        Section::check(self, r)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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
    sections: HashMap<TypeId, Box<dyn Stored>>,
    extensions: Vec<&'static str>,
}

impl Registry {
    /// Built from these extensions, in order: each one's `requires` must come before it, two
    /// extensions can't have one name, and each section's entries must pass its `check`.
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
        for s in r.sections.values() {
            if let Err(e) = s.check(&r) {
                panic!("the registry's {} section: {e}", s.name());
            }
        }
        r
    }

    /// A section, as its extensions filled it.
    pub fn section<S: Section>(&self) -> &S {
        self.sections
            .get(&TypeId::of::<S>())
            .and_then(|s| s.as_any().downcast_ref())
            .unwrap_or_else(|| panic!("no extension of this build adds to the registry's {} section", S::NAME))
    }

    /// A section, or None when no extension of this build adds to it (a reader that has nothing to do without one).
    pub fn find<S: Section>(&self) -> Option<&S> {
        self.sections.get(&TypeId::of::<S>()).and_then(|s| s.as_any().downcast_ref())
    }

    /// A section to add to, made empty the first time.
    pub fn section_mut<S: Section>(&mut self) -> &mut S {
        self.sections.entry(TypeId::of::<S>()).or_insert_with(|| Box::new(S::default())).as_any_mut().downcast_mut().expect("a section is kept under its own type")
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

    /// A section whose entries must differ: checked once every extension has added its own.
    #[derive(Default)]
    struct Different(Vec<&'static str>);
    impl Section for Different {
        const NAME: &'static str = "different";
        fn check(&self, r: &Registry) -> Result<(), String> {
            let words = r.find::<Words>().map_or(0, |w| w.0.len()); // (a check reads the rest of the registry)
            match self.0.iter().enumerate().find(|(i, w)| self.0[..*i].contains(w)) {
                Some((_, w)) => Err(format!("{w} twice, beside {words} words")),
                None => Ok(()),
            }
        }
    }
    struct AddsDifferent(&'static str, &'static str);
    impl Extension for AddsDifferent {
        fn name(&self) -> &'static str {
            self.0
        }
        fn register(&self, r: &mut Registry) {
            r.section_mut::<Different>().0.push(self.1);
        }
    }

    #[test]
    #[should_panic(expected = "the registry's different section: x twice, beside 1 words")]
    fn a_section_checks_its_entries_once_all_are_in() {
        Registry::build(&[&AddsDifferent("a", "x"), &Adds("w", &[], "one"), &AddsDifferent("b", "x")]);
    }

    #[test]
    fn a_section_that_passes_its_check_is_built() {
        let r = Registry::build(&[&AddsDifferent("a", "x"), &AddsDifferent("b", "y")]);
        assert_eq!(r.section::<Different>().0, ["x", "y"]);
    }

    #[test]
    fn a_missing_section_is_found_as_none() {
        assert!(Registry::build(&[]).find::<Words>().is_none());
        assert_eq!(Registry::build(&[&Adds("a", &[], "one")]).find::<Words>().map(|w| w.0.clone()), Some(vec!["one"]));
    }

}
