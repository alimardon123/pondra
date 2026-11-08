//! The catalog's record scheme (designs/redesign/redesign.md §4): which prefix each record of the catalog is kept under,
//! and whose it is.
//!
//! A kind of object's records are under its kind's prefix (`objects::Kind::prefix`: `t/` for a table). Every other
//! record's prefix is registered by the module that owns it, in the registry's `Prefixes` section, beside the code that
//! writes it (`store::PREFIXES`, `branch::PREFIXES`, …), and no other list names them. The registry refuses, when it is
//! built, a prefix that starts another (`s/` and `sq/` don't overlap: the slash ends a prefix), so two owners never read
//! each other's records. An installed extension's records are under `ex/<extension>/` (`of_extension`), where they can't
//! meet the core's. `pondra.prefixes` lists them all, the kinds' included.
use crate::registry::{Registry, Section};

/// A prefix of the catalog's keys and what is kept under it. One that ends in `/` is a prefix (`s/` holds `s/000…1`);
/// one that doesn't is a single key (`c`, the commit number).
#[derive(Clone, Copy, Debug)]
pub struct Prefix {
    pub prefix: &'static str,
    /// The module that writes it.
    pub owner: &'static str,
    /// One line: what is kept there.
    pub holds: &'static str,
}

/// Where an installed extension's records go: `ex/<extension>/…`.
pub const EXTENSIONS: &str = "ex/";

/// The prefix an installed extension keeps its records under.
pub fn of_extension(name: &str) -> String {
    format!("{EXTENSIONS}{name}/")
}

/// This module's own: the namespace it gives installed extensions.
pub const PREFIXES: &[Prefix] = &[Prefix { prefix: EXTENSIONS, owner: "records", holds: "installed extensions' records, each under ex/<extension>/" }];

/// The registry's section of prefixes that aren't a kind's. A kind's prefix is its own kind's (`objects::Kinds`).
#[derive(Default)]
pub struct Prefixes(Vec<Prefix>);

impl Section for Prefixes {
    const NAME: &'static str = "prefixes";

    /// No prefix may start another, a kind's included: a key would be two owners' (kinds of one family share theirs).
    fn check(&self, r: &Registry) -> Result<(), String> {
        let all = all_of(r);
        for (i, a) in all.iter().enumerate() {
            if a.prefix.is_empty() {
                return Err(format!("{} registers an empty prefix", a.owner));
            }
            for b in &all[..i] {
                if covers(a.prefix, b.prefix) || covers(b.prefix, a.prefix) {
                    return Err(format!("{}'s prefix {:?} overlaps {}'s {:?}: a key under one would be under both", a.owner, a.prefix, b.owner, b.prefix));
                }
            }
        }
        Ok(())
    }
}

impl Prefixes {
    /// A module's prefixes. One that overlaps another's is refused once every extension has added its own (`check`).
    pub fn add(&mut self, prefixes: &[Prefix]) {
        self.0.extend_from_slice(prefixes);
    }
}

/// The registry's section of the prefixes whose records say who may do what: a commit that writes under one moves the
/// catalog's rights version (`store::Catalog::rights_version`). Each owner marks its own as it registers them (the user
/// and role kinds' `u/` and the identity providers' `ip/`: `objects::RIGHTS`; the lake's keys: `users::RIGHTS`), and
/// the catalog reads the marks, so no list of them is kept anywhere else.
#[derive(Default)]
pub struct Rights(Vec<&'static str>);

impl Section for Rights {
    const NAME: &'static str = "rights";

    /// A mark is of a prefix a kind or a module registers: one nobody registers would cover no record its owner writes.
    fn check(&self, r: &Registry) -> Result<(), String> {
        let all = all_of(r);
        match self.0.iter().find(|m| !all.iter().any(|p| p.prefix == **m)) {
            Some(m) => Err(format!("{m:?} is marked as saying who may do what, but no kind or module registers it")),
            None => Ok(()),
        }
    }
}

impl Rights {
    /// A module's prefixes whose records say who may do what (each one it, or its kinds, registers).
    pub fn add(&mut self, prefixes: &[&'static str]) {
        self.0.extend_from_slice(prefixes);
    }

    /// Does a commit that writes `key` change who may do what?
    pub fn moves(&self, key: &str) -> bool {
        self.0.iter().any(|p| covers(p, key))
    }
}

/// A prefix as listed: a kind's names its kinds together (a family shares one prefix).
pub struct Listed {
    pub prefix: &'static str,
    pub owner: String,
    pub holds: &'static str,
}

/// Every prefix of this registry: the kinds' first, one entry per prefix, then the others in the order they were added.
fn all_of(r: &Registry) -> Vec<Listed> {
    let mut all: Vec<Listed> = Vec::new();
    for k in r.find::<crate::objects::Kinds>().map(|k| k.0.as_slice()).unwrap_or_default() {
        match all.iter_mut().find(|p| p.prefix == k.prefix) {
            Some(p) => p.owner = format!("{}, {}", p.owner, k.name),
            None => all.push(Listed { prefix: k.prefix, owner: format!("kind {}", k.name), holds: k.about }),
        }
    }
    let others = r.find::<Prefixes>().map(|p| p.0.as_slice()).unwrap_or_default();
    all.extend(others.iter().map(|p| Listed { prefix: p.prefix, owner: p.owner.to_string(), holds: p.holds }));
    all
}

/// Whose `key` is: the owner of the prefix it's under, or None when nobody registered one. (A walk of the registry's
/// prefixes, for a check or a tool: no hot path asks.)
pub fn owner(key: &str) -> Option<String> {
    all_of(crate::registry::get()).into_iter().find(|p| covers(p.prefix, key)).map(|p| p.owner)
}

/// Whether `key` is kept under `prefix`, or is that single key when it doesn't end in `/`.
fn covers(prefix: &str, key: &str) -> bool {
    match prefix.ends_with('/') {
        true => key.starts_with(prefix),
        false => key == prefix,
    }
}

/// `pondra.prefixes`: one row per prefix, a kind's or a module's.
pub const COLUMNS: &[(&str, crate::lists::Cell)] = {
    use crate::lists::Cell::Text;
    &[("prefix", Text), ("owner", Text), ("holds", Text)]
};

pub fn rows(r: &Registry) -> Vec<serde_json::Value> {
    all_of(r).iter().map(|p| serde_json::json!({"prefix": p.prefix, "owner": p.owner, "holds": p.holds})).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{Extension, Registry};

    struct Adds(&'static [Prefix]);
    impl Extension for Adds {
        fn name(&self) -> &'static str {
            "test"
        }
        fn register(&self, r: &mut Registry) {
            r.section_mut::<Prefixes>().add(self.0)
        }
    }

    #[test]
    fn this_builds_prefixes_are_one_owners_each() {
        let r = crate::registry::get();
        assert!(Prefixes::check(r.section::<Prefixes>(), r).is_ok());
        for (key, owner) in [("t/orders", "kind table"), ("q/eu", "kind view, external table"), ("s/00000000000000000001", "store"), ("c", "store"), ("pn/7", "branch"), ("ex/geo/cells", "records")] {
            assert_eq!(super::owner(key).as_deref(), Some(owner), "{key}");
        }
        assert_eq!(super::owner("cx"), None, "a single key covers only itself");
        assert_eq!(of_extension("geo"), "ex/geo/");
        assert!(rows(r).iter().any(|row| row["prefix"] == "fd/" && row["owner"] == "feeds"));
    }

    #[test]
    #[should_panic(expected = "overlaps")]
    fn a_prefix_that_starts_another_is_refused_when_the_registry_is_built() {
        Registry::build(&[&crate::objects::CATALOG, &Adds(&[Prefix { prefix: "t/hidden/", owner: "test", holds: "a table's shadow" }])]);
    }

    #[test]
    #[should_panic(expected = "test's prefix \"c/x\" overlaps test's \"c/\"")]
    fn a_single_key_under_a_prefix_is_refused() {
        Registry::build(&[&Adds(&[Prefix { prefix: "c/", owner: "test", holds: "c's" }, Prefix { prefix: "c/x", owner: "test", holds: "under it" }])]);
    }

    #[test]
    #[should_panic(expected = "test's prefix \"s/\" overlaps store's \"s/\"")]
    fn a_prefix_registered_twice_is_refused() {
        Registry::build(&[&crate::objects::CATALOG, &Adds(&[Prefix { prefix: "s/", owner: "test", holds: "segments again" }])]);
    }

    #[test]
    fn the_rights_are_marked_by_their_owners() {
        let r = crate::registry::get();
        let mut marked = r.section::<Rights>().0.clone();
        marked.sort();
        assert_eq!(marked, ["ip/", "u/", "z/auth", "z/signed"], "users and roles, identity providers, the lake's keys and its signed mark");
        let rights = r.section::<Rights>();
        for (key, moves) in [("u/ann", true), ("ip/acme", true), ("z/auth", true), ("z/signed", true), ("z/authx", false), ("t/orders", false), ("c", false)] {
            assert_eq!(rights.moves(key), moves, "{key}");
        }
    }

    struct Marks(&'static [&'static str]);
    impl Extension for Marks {
        fn name(&self) -> &'static str {
            "test"
        }
        fn register(&self, r: &mut Registry) {
            r.section_mut::<Rights>().add(self.0)
        }
    }

    #[test]
    #[should_panic(expected = "\"zz/\" is marked as saying who may do what, but no kind or module registers it")]
    fn a_mark_nobody_registers_is_refused() {
        Registry::build(&[&crate::objects::CATALOG, &Marks(&["zz/"])]);
    }

    #[test]
    fn prefixes_apart_by_their_slash_are_two() {
        Registry::build(&[&Adds(&[Prefix { prefix: "s/", owner: "test", holds: "one" }, Prefix { prefix: "sq/", owner: "test", holds: "two" }, Prefix { prefix: "sx", owner: "test", holds: "a key" }])]);
    }
}
