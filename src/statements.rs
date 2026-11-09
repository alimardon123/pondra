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
    /// Its leading words, upper-cased (`["CREATE", "CONNECTION"]`): where two entries' words both begin a statement,
    /// the longer is it.
    pub words: &'static [&'static str],
    /// The parser hook: the whole statement's text to what it says.
    pub parse: fn(&str) -> Result<Parsed>,
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
        _ => crate::auth::Role::Admin,
    };
    ds.iter().map(of).fold(crate::auth::Role::None, |most, r| if r > most { r } else { most })
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

/// Whether the lake's node offers `need`: a port to listen on is its own, once it serves (`NodeState::serving`).
fn offered(lake: &Lake, need: Need) -> bool {
    match need {
        Need::Listening => *lake.node.serving.borrow(),
        Need::Network => true,
    }
}

/// The registry's statements section: every statement an extension adds, each one's words once.
#[derive(Default)]
pub struct Statements(Vec<&'static Statement>);

impl Section for Statements {
    const NAME: &'static str = "statements";
}

impl Statements {
    pub fn add(&mut self, s: &'static Statement) {
        assert!(!s.words.is_empty() && s.words.iter().all(|w| *w == w.to_uppercase()), "a statement's words are upper-cased: {:?}", s.words);
        assert!(self.0.iter().all(|o| o.words != s.words), "two statements are {}", s.words.join(" "));
        self.0.push(s);
    }

    /// The statement `sql` is, by its words: the entry whose words begin it, the longest where two do.
    fn of(&self, sql: &str) -> Option<&'static Statement> {
        let most = self.0.iter().map(|s| s.words.len()).max()?;
        let words = crate::text::first_words(sql, most);
        self.0.iter().filter(|s| crate::text::begins(&words, s.words)).max_by_key(|s| s.words.len()).copied()
    }

    /// The entry with these words, as the leader is sent them.
    fn named(&self, words: &str) -> Option<&'static Statement> {
        self.0.iter().find(|s| s.words.join(" ") == words).copied()
    }
}

/// `sql` as one of the extensions' statements, for `write::parse`: what the leader carries out, or why it is refused.
/// None: it is none of theirs.
pub(crate) fn matched(sql: &str) -> Option<crate::write::Stmt> {
    matched_in(crate::registry::get(), sql)
}

fn matched_in(r: &Registry, sql: &str) -> Option<crate::write::Stmt> {
    use crate::write::Stmt;
    let s = r.find::<Statements>()?.of(sql)?;
    let words = s.words.join(" ");
    Some(match (s.parse)(sql) {
        Ok(Parsed { object, tree }) => match s.run {
            Run::Leader(_) => Stmt::Ddl(vec![crate::ddl::Ddl::Entry { words, object, tree }]),
        },
        Err(e) => Stmt::of_error(&e, format!("{e:#}")),
    })
}

/// Leader: carry out an extension's statement, as `write::parse` read it.
pub(crate) async fn carry_out(lake: &Lake, words: &str, tree: Value) -> Result<Value> {
    carry_out_in(crate::registry::get(), lake, words, tree).await
}

async fn carry_out_in(r: &Registry, lake: &Lake, words: &str, tree: Value) -> Result<Value> {
    // (a node of another build sent it: its extensions aren't all this one's)
    let s = r.find::<Statements>().and_then(|s| s.named(words)).with_context(|| format!("{words}: this node's build has no extension that adds it"))?;
    // (the leader's lake's node: a statement that needs a port is refused where the node that would serve it is)
    if let Some(need) = s.needs.iter().find(|n| !offered(lake, **n)) {
        return Err(crate::codes::coded("0A000", format!("{words} needs {}", need.word())));
    }
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
    static THING: Statement = Statement { words: &["MAKE", "THING"], parse: parse_thing, run: Run::Leader(made), right: Right::Admin, needs: &[], about: "makes a thing", extension: "things" };
    static BIG: Statement = Statement { words: &["MAKE", "BIG", "THING"], parse: parse_thing, run: Run::Leader(made), right: Right::Admin, needs: &[], about: "makes a big thing", extension: "things" };
    static SERVED: Statement = Statement { words: &["SERVE", "THING"], parse: parse_thing, run: Run::Leader(made), right: Right::Admin, needs: &[Need::Listening], about: "serves a thing", extension: "things" };
    static THINGS: BuiltIn = BuiltIn {
        name: "things",
        requires: &[],
        register: |r| {
            let s = r.section_mut::<Statements>();
            s.add(&THING);
            s.add(&BIG);
            s.add(&SERVED);
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
        let (words, object, tree) = entry(matched_in(&r, "/* a */ make thing Box;")).context("found")?;
        assert_eq!((words.as_str(), object.as_str()), ("MAKE THING", "box"));
        let (words, ..) = entry(matched_in(&r, "-- the longer words win\nMAKE BIG THING crate")).context("found")?;
        assert_eq!(words, "MAKE BIG THING");
        assert!(matched_in(&r, "MAKE THINGS x").is_none(), "a word is whole");
        assert!(matched_in(&r, "SELECT 1").is_none());
        assert!(matches!(matched_in(&r, "MAKE THING"), Some(Stmt::Invalid(e)) if e == "MAKE THING name"), "its parser's refusal is the answer");
        let url = format!("memory://statements-{}", uuid::Uuid::new_v4().simple());
        let lake = Lake::open(&url, true, false, crate::node_state::NodeState::unhosted()).await?;
        assert_eq!(carry_out_in(&r, &lake, &words, tree).await?, json!({"made": "box", "big": false}));
        let e = carry_out_in(&r, &lake, "MAKE NOTHING", json!({})).await.unwrap_err();
        assert_eq!(e.to_string(), "MAKE NOTHING: this node's build has no extension that adds it");
        Ok(())
    }

    #[tokio::test]
    async fn a_statement_that_needs_a_port_is_refused_where_the_lake_has_none() -> Result<()> {
        let r = Registry::build(&[&THINGS]);
        let (words, _, tree) = entry(matched_in(&r, "SERVE THING x")).context("found")?;
        let url = format!("memory://statements-{}", uuid::Uuid::new_v4().simple());
        let lake = Lake::open(&url, true, false, crate::node_state::NodeState::unhosted()).await?;
        let e = carry_out_in(&r, &lake, &words, tree.clone()).await.unwrap_err();
        assert!(e.to_string().starts_with("SERVE THING needs a port to listen on, which this node doesn't have"), "{e}");
        lake.node.serving.send_replace(true);
        assert_eq!(carry_out_in(&r, &lake, &words, tree).await?, json!({"made": "x", "big": false}));
        Ok(())
    }

    #[test]
    #[should_panic(expected = "two statements are MAKE THING")]
    fn a_statements_words_are_one_entrys() {
        let mut s = Statements::default();
        s.add(&THING);
        s.add(&THING);
    }
}
