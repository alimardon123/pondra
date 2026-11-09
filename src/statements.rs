//! The registry's statements section (designs/redesign/redesign.md §6): a statement an extension adds, by its leading
//! words. Each entry says its words, its parser hook, what runs it, the right it needs, what a form must offer for it,
//! and one line on what it does. `write::parse` consults the section at one place, before the statements the core
//! reads by hand, so an extension's statement needs no edit anywhere else. Step 8's pipeline dispatches every statement
//! from here; until then only the extensions' own statements are entries (step 4, l1-wip/step4-plan.md).
use crate::registry::{Registry, Section};
use crate::store::Lake;
use anyhow::{Context, Result};
use futures::future::BoxFuture;
use serde_json::Value;

/// One statement an extension adds.
pub struct Statement {
    /// Its leading words, upper-cased (`["CREATE", "CONNECTION"]`), matched on the statement's tokens: where two
    /// entries' words both begin a statement, the longer is it. One of them after the first may be `*`, a name (`a`,
    /// `"A"`, `s.t`), for a form whose telling word comes after one (`CREATE TABLE * CLONE`).
    pub words: &'static [&'static str],
    /// Its one reader: what the statement says.
    pub read: Read,
    /// What carries it out.
    pub run: Run,
    /// What the caller must be allowed.
    pub right: Right,
    /// What a form must offer to run it: a form without one refuses the statement by name.
    pub needs: &'static [Need],
    /// One line: what it does.
    pub about: &'static str,
    /// The extension that adds it.
    pub extension: &'static str,
}

/// An entry's reader: one per family of statements, so no family is read in two places (step 8's PR 8,
/// l6/step8-pr8-plan.md). Core families join as `Core` (their words) and `Tree` (sqlparser's tree, where generic reads
/// every form of the family) in PR 8's next pieces.
pub enum Read {
    /// An extension's reader: the whole statement's text to what it says.
    Text(fn(&str) -> Result<Parsed>),
}

/// What a statement's parser hook read: the object it changes, as the catalog names it, and the rest of what it says,
/// which crosses to the leader as it is.
pub struct Parsed {
    pub object: String,
    pub tree: Value,
}

/// What carries a statement out.
pub enum Run {
    /// The leader, under the lake's lock, as it carries out the core's DDL: a change to what the lake holds.
    Leader(for<'a> fn(&'a Lake, Value) -> BoxFuture<'a, Result<Value>>),
}

/// The right a statement needs, read where every statement's is (`rights::asks`, which `rights::may` decides).
pub enum Right {
    /// An admin's: a superuser, or an admin token.
    Admin,
}

/// The role the extensions' statements in `ds` need (an admin's for one this build has no extension for: the leader
/// refuses it by name).
pub(crate) fn role(ds: &[crate::ddl::Ddl]) -> crate::auth::Role {
    let statements = crate::registry::get().find::<Statements>();
    let right = |words: &str| statements.and_then(|s| s.named(words)).map(|s| &s.right);
    let of = |d: &crate::ddl::Ddl| match d {
        crate::ddl::Ddl::Entry { words, .. } => match right(words) {
            Some(Right::Admin) | None => crate::auth::Role::Admin,
        },
        crate::ddl::Ddl::UndropKept { kind, .. } => match right(&dropping(kind)) {
            Some(Right::Admin) | None => crate::auth::Role::Admin,
        },
        _ => crate::auth::Role::Admin,
    };
    ds.iter().map(of).fold(crate::auth::Role::None, |most, r| if r > most { r } else { most })
}

/// The words of the statement that drops a `kind`.
fn dropping(kind: &str) -> String { format!("DROP {}", kind.to_uppercase()) }

/// Is `d` an UNDROP of a kind an extension's statement drops (`DROP CONNECTION`)? It needs the right that DROP needed
/// (`role`), as every UNDROP does.
pub(crate) fn undrops_entry(d: &crate::ddl::Ddl) -> bool {
    let statements = crate::registry::get().find::<Statements>();
    matches!(d, crate::ddl::Ddl::UndropKept { kind, .. } if statements.and_then(|s| s.named(&dropping(kind))).is_some())
}

/// Something a form offers or lacks (docs/forms.json's forms), from the facts a node has today; step 2's lifecycle facts
/// take this over.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Need {
    /// A port it listens on: a node that serves (`pondra serve`), not one in a program's process or a page.
    Listening,
    /// The network: every form reaches it.
    Network,
}

impl Need {
    /// What it is, and which node has it.
    fn word(self) -> &'static str {
        match self {
            Need::Listening => "a port to listen on, which this node doesn't have: a node started with pondra serve has one",
            Need::Network => "the network, which this node can't reach",
        }
    }
}

/// Whether this node offers `need`.
fn offered(need: Need) -> bool {
    match need {
        Need::Listening => *crate::store::SERVING.borrow(),
        Need::Network => true,
    }
}

/// The registry's statements section: every statement an extension adds, each one's words once, found by its first
/// word.
#[derive(Default)]
pub struct Statements {
    all: Vec<&'static Statement>,
    by_first: std::collections::HashMap<&'static str, Vec<&'static Statement>>,
}

/// What the extension of a core statement's entry is called (PR 8 adds the core's families as entries).
pub const CORE: &str = "core";

/// The leading words of the statements the core reads outside this section today (`write::parse`'s readers and its
/// match on sqlparser's tree), after a CREATE's `OR REPLACE`, `OR ALTER`, `TEMP` or `TEMPORARY`: an extension's entry
/// may not begin with one, so no extension takes a core statement over with longer words. Each goes as its family
/// becomes a core entry (PR 8b-8d), which the same check covers.
const CORE_WORDS: &[&[&str]] = &[
    &["SELECT"], &["WITH"], &["VALUES"], &["TABLE"], &["FROM"], &["INSERT"], &["UPDATE"], &["DELETE"], &["MERGE"], &["TRUNCATE"],
    &["GRANT"], &["REVOKE"], &["COMMENT"], &["COPY"], &["ATTACH"], &["DETACH"], &["UNDROP"], &["BACKUP"], &["RESTORE"],
    &["INSTALL"], &["LOAD"], &["UNINSTALL"], &["CALL"], &["DO"], &["SET"], &["RESET"], &["SHOW"], &["DESCRIBE"], &["USE"],
    &["EXPLAIN"], &["BEGIN"], &["START"], &["COMMIT"], &["ROLLBACK"], &["END"], &["CHECKPOINT"], &["EXPORT"], &["IMPORT"],
    &["CREATE", "TABLE"], &["CREATE", "EXTERNAL"], &["CREATE", "VIEW"], &["CREATE", "MATERIALIZED"], &["CREATE", "SCHEMA"],
    &["CREATE", "DATABASE"], &["CREATE", "FUNCTION"], &["CREATE", "PROCEDURE"], &["CREATE", "MACRO"], &["CREATE", "TASK"],
    &["CREATE", "PIPELINE"], &["CREATE", "USER"], &["CREATE", "ROLE"], &["CREATE", "TOKEN"], &["CREATE", "SHARE"],
    &["CREATE", "RECIPIENT"], &["CREATE", "IDENTITY"], &["CREATE", "SECRET"], &["CREATE", "PERSISTENT"],
    &["CREATE", "SEQUENCE"], &["CREATE", "INDEX"], &["CREATE", "UNIQUE"], &["CREATE", "TYPE"],
    &["ALTER", "TABLE"], &["ALTER", "VIEW"], &["ALTER", "MATERIALIZED"], &["ALTER", "SCHEMA"], &["ALTER", "DATABASE"],
    &["ALTER", "TASK"], &["ALTER", "USER"], &["ALTER", "ROLE"], &["ALTER", "SHARE"], &["ALTER", "RECIPIENT"],
    &["ALTER", "IDENTITY"], &["ALTER", "SEQUENCE"], &["ALTER", "INDEX"], &["ALTER", "TYPE"],
    &["DROP", "TABLE"], &["DROP", "VIEW"], &["DROP", "MATERIALIZED"], &["DROP", "SCHEMA"], &["DROP", "DATABASE"],
    &["DROP", "FUNCTION"], &["DROP", "PROCEDURE"], &["DROP", "MACRO"], &["DROP", "TASK"], &["DROP", "USER"],
    &["DROP", "ROLE"], &["DROP", "TOKEN"], &["DROP", "SHARE"], &["DROP", "RECIPIENT"], &["DROP", "IDENTITY"],
    &["DROP", "SECRET"], &["DROP", "PERSISTENT"], &["DROP", "TEMPORARY"], &["DROP", "SEQUENCE"], &["DROP", "INDEX"],
    &["DROP", "TYPE"],
];

/// `words` without a CREATE's `OR REPLACE`, `OR ALTER`, `TEMP` or `TEMPORARY` after its first word.
fn plain_words(words: &[&'static str]) -> Vec<&'static str> {
    let Some((&"CREATE", mut rest)) = words.split_first() else { return words.to_vec() };
    if let ["OR", "REPLACE" | "ALTER", more @ ..] = rest {
        rest = more;
    }
    if let ["TEMP" | "TEMPORARY", more @ ..] = rest {
        rest = more;
    }
    std::iter::once("CREATE").chain(rest.iter().copied()).collect()
}

/// Whether one entry's words begin with another's (`*` matches any one word in either).
fn begins_with(words: &[&str], first: &[&str]) -> bool {
    words.len() >= first.len() && words.iter().zip(first).all(|(a, b)| a == b || *a == "*" || *b == "*")
}

impl Section for Statements {
    const NAME: &'static str = "statements";
}

impl Statements {
    /// Adds an entry. Refused when the registry is built: words not upper-cased, a name first or twice, words another
    /// entry has, and an extension's words that begin with a core statement's.
    pub fn add(&mut self, s: &'static Statement) {
        let words = s.words.join(" ");
        assert!(!s.words.is_empty() && s.words.iter().all(|w| *w == w.to_uppercase()), "a statement's words are upper-cased: {words}");
        assert!(s.words[0] != "*" && s.words.iter().filter(|w| **w == "*").count() <= 1, "a statement's words hold at most one name, never first: {words}");
        assert!(self.all.iter().all(|o| o.words != s.words), "two statements are {words}");
        let (core, plain) = (s.extension == CORE, plain_words(s.words));
        if !core {
            let taken = CORE_WORDS.iter().copied().chain(self.all.iter().filter(|o| o.extension == CORE).map(|o| o.words));
            assert!(!taken.clone().any(|c| begins_with(&plain, &plain_words(c))), "{words} begins as a core statement does: an extension's statement has words of its own");
        } else {
            assert!(!self.all.iter().any(|o| o.extension != CORE && begins_with(&plain_words(o.words), &plain)), "{words}: an extension's statement begins so");
        }
        self.by_first.entry(s.words[0]).or_default().push(s);
        self.all.push(s);
    }

    /// The statement `sql` is, by its words: the entry whose words begin it, the longest where two do. A statement whose
    /// first word begins no entry is tokenized here not at all. The first word is its letters (`text::head`), as the
    /// tokenizer ends it, so `COPY(SELECT 1) TO …` is COPY's to read.
    fn of(&self, sql: &str) -> Option<&'static Statement> {
        let first = crate::text::head(sql).to_ascii_uppercase();
        let entries = self.by_first.get(first.as_str())?;
        let words = crate::words::Words::of(sql)?;
        entries.iter().filter(|s| words.begins(s.words)).max_by_key(|s| s.words.len()).copied()
    }

    /// The entry with these words, as the leader is sent them.
    fn named(&self, words: &str) -> Option<&'static Statement> {
        self.all.iter().find(|s| s.words.join(" ") == words).copied()
    }
}

/// `sql` as one of the extensions' statements, for `write::parse`: what the leader carries out, or why it is refused.
/// None: it is none of theirs.
pub(crate) fn matched(sql: &str) -> Option<crate::write::Stmt> {
    matched_in(crate::registry::get(), sql, offered)
}

fn matched_in(r: &Registry, sql: &str, offered: fn(Need) -> bool) -> Option<crate::write::Stmt> {
    use crate::write::Stmt;
    let s = r.find::<Statements>()?.of(sql)?;
    let words = s.words.join(" ");
    if let Some(need) = s.needs.iter().find(|n| !offered(**n)) {
        return Some(Stmt::Refused("0A000", format!("{words} needs {}", need.word())));
    }
    let read = match s.read {
        Read::Text(read) => read(sql).map(|Parsed { object, tree }| match s.run {
            Run::Leader(_) => Stmt::Ddl(vec![crate::ddl::Ddl::Entry { words, object, tree }]),
        }),
    };
    Some(read.unwrap_or_else(|e| Stmt::of_error(&e, format!("{e:#}"))))
}

/// Leader: carry out an extension's statement, as `write::parse` read it.
pub(crate) async fn carry_out(lake: &Lake, words: &str, tree: Value) -> Result<Value> {
    carry_out_in(crate::registry::get(), lake, words, tree).await
}

async fn carry_out_in(r: &Registry, lake: &Lake, words: &str, tree: Value) -> Result<Value> {
    // (a node of another build sent it: its extensions aren't all this one's)
    let s = r.find::<Statements>().and_then(|s| s.named(words)).with_context(|| format!("{words}: this node's build has no extension that adds it"))?;
    match s.run {
        Run::Leader(f) => f(lake, tree).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::BuiltIn;
    use crate::write::Stmt;
    use serde_json::json;

    fn parse_thing(sql: &str) -> Result<Parsed> {
        let words = crate::text::first_words(sql, 8);
        let name = words.last().filter(|w| !matches!(w.as_str(), "THING" | "BIG")).context("MAKE THING name")?;
        Ok(Parsed { object: name.to_lowercase(), tree: json!({"name": name.to_lowercase(), "big": words.get(1).is_some_and(|w| w == "BIG")}) })
    }
    fn made<'a>(_: &'a Lake, tree: Value) -> BoxFuture<'a, Result<Value>> {
        Box::pin(async move { Ok(json!({"made": tree["name"], "big": tree["big"]})) })
    }
    static THING: Statement = Statement { words: &["MAKE", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "makes a thing", extension: "things" };
    static BIG: Statement = Statement { words: &["MAKE", "BIG", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "makes a big thing", extension: "things" };
    static COPIED: Statement = Statement { words: &["MAKE", "*", "COPY"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "copies a thing", extension: "things" };
    static SERVED: Statement = Statement { words: &["SERVE", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[Need::Listening], about: "serves a thing", extension: "things" };
    static THINGS: BuiltIn = BuiltIn {
        name: "things",
        requires: &[],
        register: |r| {
            let s = r.section_mut::<Statements>();
            s.add(&THING);
            s.add(&BIG);
            s.add(&SERVED);
            s.add(&COPIED);
        },
    };

    fn entry(s: Option<Stmt>) -> Option<(String, String, Value)> {
        match s? {
            Stmt::Ddl(mut d) if d.len() == 1 => match d.pop()? {
                crate::ddl::Ddl::Entry { words, object, tree } => Some((words, object, tree)),
                _ => None,
            },
            _ => None,
        }
    }

    #[tokio::test]
    async fn an_extensions_statement_is_found_by_its_words_and_carried_out() -> Result<()> {
        let r = Registry::build(&[&THINGS]);
        let every = |_: Need| true;
        let (words, object, tree) = entry(matched_in(&r, "/* a */ make thing Box;", every)).context("found")?;
        assert_eq!((words.as_str(), object.as_str()), ("MAKE THING", "box"));
        let (words, ..) = entry(matched_in(&r, "-- the longer words win\nMAKE BIG THING crate", every)).context("found")?;
        assert_eq!(words, "MAKE BIG THING");
        assert!(matched_in(&r, "MAKE THINGS x", every).is_none(), "a word is whole");
        assert_eq!(entry(matched_in(&r, "MAKE/* a */THING t", every)).context("found")?.0, "MAKE THING", "a first word ends where the tokenizer ends it");
        assert!(matched_in(&r, "MAKER THING x", every).is_none(), "and is whole");
        // (words are tokens: a comment between them, a name where the words hold `*`, quoted or dotted)
        assert_eq!(entry(matched_in(&r, "make /* a */ big\n thing x", every)).context("found")?.0, "MAKE BIG THING");
        for sql in ["MAKE box COPY", "make s.\"Box\" copy", "MAKE lake.s.t COPY now"] {
            assert_eq!(entry(matched_in(&r, sql, every)).with_context(|| sql.to_string())?.0, "MAKE * COPY", "{sql}");
        }
        assert!(matched_in(&r, "MAKE COPY", every).is_none(), "`*` is a name");
        assert!(matched_in(&r, "MAKE 'box' COPY", every).is_none(), "a string is no name");
        assert!(matched_in(&r, "SELECT 1", every).is_none());
        assert!(matches!(matched_in(&r, "MAKE THING", every), Some(Stmt::Invalid(e)) if e == "MAKE THING name"), "its parser's refusal is the answer");
        let url = format!("memory://statements-{}", uuid::Uuid::new_v4().simple());
        let lake = Lake::open(&url, true, false, crate::node_state::NodeState::unhosted()).await?;
        assert_eq!(carry_out_in(&r, &lake, &words, tree).await?, json!({"made": "box", "big": false}));
        let e = carry_out_in(&r, &lake, "MAKE NOTHING", json!({})).await.unwrap_err();
        assert_eq!(e.to_string(), "MAKE NOTHING: this node's build has no extension that adds it");
        Ok(())
    }

    #[test]
    fn a_form_without_a_statements_need_refuses_it_by_name() {
        let r = Registry::build(&[&THINGS]);
        let page = |n: Need| n == Need::Network;
        match matched_in(&r, "SERVE THING x", page) {
            Some(Stmt::Refused("0A000", e)) => assert!(e.starts_with("SERVE THING needs a port to listen on, which this node doesn't have"), "{e}"),
            _ => panic!("refused"),
        }
        assert!(entry(matched_in(&r, "MAKE THING x", page)).is_some(), "a statement that needs nothing runs on every form");
    }

    /// An extension's statement has words of its own: none begins as one the core reads, whether the core reads it as
    /// an entry or elsewhere today, and a core entry may not begin as an extension's does.
    #[test]
    fn no_extension_takes_a_core_statement() {
        static GRANTED: Statement = Statement { words: &["GRANT", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "", extension: "things" };
        static TABLED: Statement = Statement { words: &["CREATE", "OR", "REPLACE", "TEMP", "TABLE", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "", extension: "things" };
        static CLONE: Statement = Statement { words: &["CREATE", "THING", "*", "CLONE"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "", extension: CORE };
        static LONGER: Statement = Statement { words: &["CREATE", "THING", "*", "CLONE", "NOW"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "", extension: "things" };
        static NAMED: Statement = Statement { words: &["*", "THING"], read: Read::Text(parse_thing), run: Run::Leader(made), right: Right::Admin, needs: &[], about: "", extension: "things" };
        let refused = |entries: &[&'static Statement]| {
            std::panic::catch_unwind(|| {
                let mut s = Statements::default();
                entries.iter().copied().for_each(|e| s.add(e));
            })
            .is_err()
        };
        assert!(refused(&[&GRANTED]), "GRANT is the core's");
        assert!(refused(&[&TABLED]), "CREATE TABLE is the core's, after OR REPLACE and TEMP too");
        assert!(refused(&[&CLONE, &LONGER]) && refused(&[&LONGER, &CLONE]), "a core entry's words, whichever is added first");
        assert!(refused(&[&NAMED]), "a name never begins an entry");
        assert!(!refused(&[&THING, &BIG, &COPIED, &CLONE]), "words of their own");
    }

    #[test]
    #[should_panic(expected = "two statements are MAKE THING")]
    fn a_statements_words_are_one_entrys() {
        let mut s = Statements::default();
        s.add(&THING);
        s.add(&THING);
    }
}
