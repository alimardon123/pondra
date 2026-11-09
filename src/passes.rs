//! The registry's passes section (designs/redesign/redesign.md §5, the statement pipeline's front, and §6): each pass of
//! the front's text phase, what triggers it, and the one matcher that finds, in one scan of a statement's text, the passes
//! it may need. A pass runs only when its trigger is in the text, so a statement that triggers none pays one scan however
//! many passes the build's extensions add.
//!
//! A trigger is a superset of its pass's own look: its words, each where it stands as a whole word anywhere in the text
//! (in a string or a comment too), and its marks, each a run of symbols or letters anywhere. A pass whose trigger is not in
//! a text must have nothing to do there; the corpus test and the check (`checked`) hold every pass to that.
//!
//! The tree passes (`pattern::rewrite`, `pg_casts::rewrite`, `friendly::rewrite`, the measures) are still named in
//! `front::expand`: they take the host, whose futures aren't boxed yet (step 8's PR 9 brings them here).
use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use datafusion::sql::sqlparser::ast::{Expr, Statement};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use std::borrow::Cow;
use std::sync::OnceLock;

/// Where a pass runs in the front, in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// On the statement as it was sent: refuses one that is another database's, or 1.1's, with what to write.
    Refuse,
    /// Writes what the parser can't read as it is written into what it can, before anything is parsed.
    Text,
    /// Before the parse: whether one of the front's tree passes may have something to do in the text.
    Wants,
    /// In the parse: a hook of the fallback dialect (`dialect.rs`), which reads a form in a text GenericDialect refuses.
    Parse,
}

/// What a pass does: today's function of its module, unchanged.
#[derive(Clone, Copy)]
pub enum Run {
    Refuse(fn(&str) -> anyhow::Result<()>),
    Text(for<'a> fn(&'a str) -> anyhow::Result<Cow<'a, str>>),
    Wants(fn(&str) -> bool),
    Parse(Parse),
}

/// A hook of the fallback dialect: the token or keyword it claims (one hook a claim), the texts it reads (each one
/// GenericDialect refuses: a test holds it to that, so a sqlparser that starts reading one fails it, and the hook yields
/// or goes), and its parse.
#[derive(Clone, Copy, Debug)]
pub struct Parse {
    pub claims: &'static str,
    #[cfg_attr(not(test), allow(dead_code))] // (read by its test: `dialect::tests`)
    pub reads: &'static [&'static str],
    pub hook: Hook,
}

/// Where a hook reads, and its read: None leaves the parser where it was, for the dialect's own reading.
#[derive(Clone, Copy, Debug)]
pub enum Hook {
    /// At the start of an expression (sqlparser's `Dialect::parse_prefix`).
    Prefix(fn(&mut Parser) -> Option<Result<Expr, ParserError>>),
    /// At the start of a statement (sqlparser's `Dialect::parse_statement`), with the dialect, for a parse of its parts
    /// in which the other hooks read too.
    Statement(fn(&dyn Dialect, &mut Parser) -> Option<Result<Statement, ParserError>>),
}

/// A pass: its name (`module.look`), its place (its run's phase, and its order in it: no two passes of a phase share
/// one), its trigger, and what it runs.
pub struct Pass {
    pub name: &'static str,
    pub order: u16,
    /// Lower-case words, each a trigger where it stands whole: no ASCII letter, digit or `_` just before or after it.
    pub words: Vec<&'static str>,
    /// Marks, each a trigger anywhere (case aside): pieces in a row, with any whitespace (or none) between two pieces,
    /// as `&[")", "."]` is `)` then `.` (`) .x`). One piece is a plain run: `&["::"]`.
    pub marks: Vec<&'static [&'static str]>,
    pub run: Run,
}

impl Pass {
    pub fn phase(&self) -> Phase {
        match self.run {
            Run::Refuse(_) => Phase::Refuse,
            Run::Text(_) => Phase::Text,
            Run::Wants(_) => Phase::Wants,
            Run::Parse(_) => Phase::Parse,
        }
    }
}

/// The registry's passes section: every extension's passes, in their phases' order, and the one matcher over their triggers.
#[derive(Default)]
pub struct Passes {
    all: Vec<Pass>,
    matcher: OnceLock<Matcher>,
}

impl crate::registry::Section for Passes {
    const NAME: &'static str = "passes";
}

impl Passes {
    /// Adds a pass. Two passes of one phase can't share an order, nor two passes a name: an extension's pass never lands
    /// in a place no one chose.
    pub fn add(&mut self, pass: Pass) {
        assert!(pass.words.iter().all(|w| !w.is_empty() && *w == w.to_ascii_lowercase()), "pass {}: a trigger word is lower-case", pass.name);
        assert!(pass.marks.iter().all(|m| !m.is_empty() && m.iter().all(|p| !p.is_empty())), "pass {}: a mark has pieces", pass.name);
        assert!(!pass.words.is_empty() || !pass.marks.is_empty(), "pass {}: it has a trigger", pass.name);
        assert!(self.all.iter().all(|p| p.name != pass.name), "two passes are named {}", pass.name);
        if let Some(other) = self.all.iter().find(|p| p.phase() == pass.phase() && p.order == pass.order) {
            panic!("passes {} and {} both take order {} of {:?}", other.name, pass.name, pass.order, pass.phase());
        }
        if let Run::Parse(h) = pass.run {
            if let Some(other) = self.all.iter().find(|p| matches!(p.run, Run::Parse(o) if o.claims.eq_ignore_ascii_case(h.claims))) {
                panic!("hooks {} and {} both claim {}", other.name, pass.name, h.claims);
            }
        }
        assert!(self.all.len() < 128, "at most 128 passes: `Fired` is a u128");
        self.all.push(pass);
        self.all.sort_by_key(|p| (p.phase(), p.order));
    }

    /// The passes, in the order the front runs them.
    #[cfg(test)]
    pub fn all(&self) -> &[Pass] {
        &self.all
    }

    fn matcher(&self) -> &Matcher {
        self.matcher.get_or_init(|| Matcher::new(&self.all))
    }

    /// The passes whose trigger is in `sql`: one scan of it.
    pub fn fired(&self, sql: &str) -> Fired {
        self.matcher().fired(sql)
    }

    /// Runs the `Refuse` passes on `sql`, each whose trigger fired.
    pub fn refuse(&self, sql: &str) -> anyhow::Result<()> {
        let fired = self.fired(sql);
        for (i, p) in self.all.iter().enumerate() {
            if let Run::Refuse(f) = p.run {
                match fired.has(i) {
                    true => f(sql)?,
                    false if checked() => anyhow::ensure!(f(sql).is_ok(), "{}", missed(p, sql)),
                    false => {}
                }
            }
        }
        Ok(())
    }

    /// Runs the `Text` passes on `sql` in order, each whose trigger fired in the text as the passes before it left it
    /// (scanned again after a pass rewrote it, so a trigger an earlier pass wrote in is seen).
    pub fn text<'a>(&self, sql: &'a str) -> anyhow::Result<Cow<'a, str>> {
        let mut out = Cow::Borrowed(sql);
        let mut fired = self.fired(sql);
        for (i, p) in self.all.iter().enumerate() {
            let Run::Text(f) = p.run else { continue };
            if !fired.has(i) {
                if checked() {
                    anyhow::ensure!(f(&out).is_ok_and(|t| t == out.as_ref()), "{}", missed(p, &out));
                }
                continue;
            }
            if let Cow::Owned(t) = f(&out)? {
                if t != out.as_ref() {
                    fired = self.fired(&t);
                    out = Cow::Owned(t);
                }
            }
        }
        Ok(out)
    }

    /// Whether any `Wants` pass has something to do in `sql`, each asked only when its trigger fired; or, when a hook's
    /// trigger fired, whether GenericDialect refuses the text, so the front reads it with the fallback dialect.
    pub fn wants(&self, sql: &str) -> anyhow::Result<bool> {
        let fired = self.fired(sql);
        let mut wanted = false;
        for (i, p) in self.all.iter().enumerate() {
            match (p.run, fired.has(i)) {
                (Run::Wants(f), true) => wanted = wanted || f(sql),
                (Run::Wants(f), false) if checked() => anyhow::ensure!(!f(sql), "{}", missed(p, sql)),
                (Run::Parse(_), true) => wanted = wanted || crate::parsed::generic(sql).is_err(),
                _ => {}
            }
            if wanted && !checked() {
                return Ok(true);
            }
        }
        if checked() && !wanted && crate::parsed::generic(sql).is_err() {
            self.hooks_fired(sql, &crate::dialect::parse(sql).1)?; // (a hook that would read the text: its trigger fired)
        }
        Ok(wanted)
    }

    /// The fallback dialect's hooks, in their order.
    pub fn hooks(&self) -> impl Iterator<Item = (&'static str, Parse)> + '_ {
        self.all.iter().filter_map(|p| match p.run {
            Run::Parse(h) => Some((p.name, h)),
            _ => None,
        })
    }

    /// Each hook in `used` (the ones that read part of `sql`) had its trigger fired there: else the error that names it.
    pub fn hooks_fired(&self, sql: &str, used: &[&str]) -> anyhow::Result<()> {
        let fired = self.fired(sql);
        for (i, p) in self.all.iter().enumerate().filter(|(_, p)| used.contains(&p.name)) {
            anyhow::ensure!(fired.has(i), "{}", missed(p, sql));
        }
        Ok(())
    }
}

/// The section of this build's registry.
pub fn get() -> &'static Passes {
    crate::registry::get().section::<Passes>()
}

/// Whether a pass whose trigger didn't fire is run anyway, and its having something to do refused (`missed`):
/// `env::front_checked`.
pub(crate) fn checked() -> bool { crate::env::front_checked() }

fn missed(p: &Pass, sql: &str) -> String {
    format!("passes: {} had something to do in a text its trigger missed (its words and marks are short of its look): {sql}", p.name)
}

/// The passes a text triggered, by their place in the section.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fired(u128);

impl Fired {
    pub fn has(self, i: usize) -> bool {
        self.0 >> i & 1 == 1
    }
}

/// One automaton over every pass's words and marks' first pieces (ASCII case aside), each pattern with the passes it
/// triggers.
struct Matcher {
    ac: Option<AhoCorasick>,
    /// For each pattern: whether it is a word (bounded) or a mark's first piece, the rest of the mark, and the passes.
    what: Vec<(Kind, &'static [&'static str], u128)>,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Word,
    Mark,
}

impl Matcher {
    fn new(all: &[Pass]) -> Matcher {
        let mut patterns: Vec<&'static str> = vec![];
        let mut what: Vec<(Kind, &'static [&'static str], u128)> = vec![];
        let mut add = |kind: Kind, first: &'static str, rest: &'static [&'static str], bit: u128| {
            match patterns.iter().zip(&what).position(|(p, w)| p.eq_ignore_ascii_case(first) && w.0 == kind && w.1 == rest) {
                Some(k) => what[k].2 |= bit,
                None => {
                    patterns.push(first);
                    what.push((kind, rest, bit));
                }
            }
        };
        for (i, p) in all.iter().enumerate() {
            for w in &p.words {
                add(Kind::Word, w, &[], 1 << i);
            }
            for m in &p.marks {
                add(Kind::Mark, m[0], &m[1..], 1 << i);
            }
        }
        let ac = (!patterns.is_empty()).then(|| AhoCorasickBuilder::new().ascii_case_insensitive(true).match_kind(MatchKind::Standard).build(&patterns).expect("the passes' triggers"));
        Matcher { ac, what }
    }

    fn fired(&self, sql: &str) -> Fired {
        let Some(ac) = &self.ac else { return Fired(0) };
        // (a look's `(?i)` folds case as Unicode does, where `ſ` is an `s` and `K` (the Kelvin sign) a `k`: a text with
        // either fires every pass, so a trigger stays a superset of its look)
        if !sql.is_ascii() && sql.contains(['\u{17F}', '\u{212A}']) {
            return Fired(u128::MAX);
        }
        let b = sql.as_bytes();
        let word = |i: usize| b.get(i).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        let mut fired = 0u128;
        for m in ac.find_overlapping_iter(sql) {
            let (kind, rest, bits) = self.what[m.pattern().as_usize()];
            if fired & bits == bits {
                continue;
            }
            let hit = match kind {
                Kind::Word => !(m.start() > 0 && word(m.start() - 1)) && !word(m.end()),
                Kind::Mark => follows(sql, m.end(), rest),
            };
            if hit {
                fired |= bits;
            }
        }
        Fired(fired)
    }
}

/// Whether `rest`'s pieces follow `at` in `sql`, in a row, with any whitespace before each (case aside): whitespace as
/// the looks' `\s` reads it, U+00A0 too, so a trigger stays a superset of its look (11-08 audit L12).
fn follows(sql: &str, mut at: usize, rest: &[&str]) -> bool {
    for piece in rest {
        let tail = sql.get(at..).unwrap_or("");
        at += tail.len() - tail.trim_start().len();
        if !sql.get(at..at + piece.len()).is_some_and(|s| s.eq_ignore_ascii_case(piece)) {
            return false;
        }
        at += piece.len();
    }
    true
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn refuse_none(_: &str) -> anyhow::Result<()> { Ok(()) }
    fn text_none(s: &str) -> anyhow::Result<Cow<'_, str>> { Ok(Cow::Borrowed(s)) }
    fn wants_none(_: &str) -> bool { false }

    fn pass(name: &'static str, phase: Phase, order: u16, words: &[&'static str], marks: &[&'static [&'static str]]) -> Pass {
        let run = match phase {
            Phase::Refuse => Run::Refuse(refuse_none),
            Phase::Text => Run::Text(text_none),
            Phase::Wants => Run::Wants(wants_none),
            Phase::Parse => Run::Parse(Parse { claims: name, reads: &[], hook: Hook::Prefix(|_| None) }),
        };
        Pass { name, order, words: words.to_vec(), marks: marks.to_vec(), run }
    }

    /// A word triggers where it stands whole, ASCII case aside, and `_` and digits are a word's: `my_pivot` and `pivot2`
    /// aren't `pivot`; anything not ASCII stands between words, so the trigger stays a superset of a look's `\b`. A mark
    /// triggers anywhere, its pieces with any whitespace between them, Unicode's too.
    #[test]
    fn a_trigger_is_a_whole_word_or_a_mark() {
        let mut s = Passes::default();
        s.add(pass("a.pivot", Phase::Wants, 1, &["pivot"], &[]));
        s.add(pass("a.dot", Phase::Wants, 2, &[], &[&[")", "."], &["::"]]));
        s.add(pass("a.both", Phase::Wants, 3, &["pivot", "top"], &[]));
        let fired = |sql: &str| (0..3).filter(|&i| s.fired(sql).has(i)).collect::<Vec<_>>();
        assert_eq!(fired("PIVOT t ON g"), [0, 2]);
        assert_eq!(fired("SELECT my_pivot, pivot2, pivots FROM t"), Vec::<usize>::new());
        assert_eq!(fired("SELECT 'a pivot' -- top\n"), [0, 2], "in a string or a comment too");
        assert_eq!(fired("SELECT épivot"), [0, 2], "not ASCII: a boundary");
        assert_eq!(fired("SELECT (x) .a, (y).b"), [1]);
        assert_eq!(fired("SELECT (x)\n\t. a"), [1]);
        assert_eq!(fired("SELECT (x)\u{A0}.a, (y)\u{2003}. b"), [1], "whitespace as `\\s` reads it: U+00A0 too (11-08 audit L12)");
        assert_eq!(fired("SELECT (x) , .5"), Vec::<usize>::new());
        assert_eq!(fired("SELECT x::INT"), [1]);
        assert_eq!(fired("SELECT \u{17F}ample"), [0, 1, 2], "Unicode's case folding: every pass");
        assert_eq!(fired("SELECT TOP 1 x"), [2]);
        assert_eq!(s.fired("SELECT 1"), Fired(0));
    }

    /// Two passes of one phase can't share an order, so no extension's pass lands where no one put it.
    #[test]
    #[should_panic(expected = "both take order 5 of Text")]
    fn two_passes_of_a_phase_in_one_place_are_refused() {
        let mut s = Passes::default();
        s.add(pass("a.one", Phase::Text, 5, &["x"], &[]));
        s.add(pass("b.two", Phase::Wants, 5, &["x"], &[]));
        s.add(pass("c.three", Phase::Text, 5, &["y"], &[]));
    }

    /// Two hooks can't claim one token or keyword: the fallback dialect would ask only the first.
    #[test]
    #[should_panic(expected = "hooks a.one and b.two both claim [")]
    fn two_hooks_claiming_one_token_are_refused() {
        let hook = |claims| Run::Parse(Parse { claims, reads: &[], hook: Hook::Prefix(|_| None) });
        let mut s = Passes::default();
        s.add(Pass { name: "a.one", order: 1, words: vec!["x"], marks: vec![], run: hook("[") });
        s.add(Pass { name: "c.three", order: 3, words: vec!["x"], marks: vec![], run: hook("lambda") });
        s.add(Pass { name: "b.two", order: 2, words: vec!["x"], marks: vec![], run: hook("[") });
    }

    /// Every pass of this build is quiet where its trigger is not in the text: a refusal lets the text go, a text pass gives
    /// it back as it was, a look finds nothing. Over a corpus of the SQL this repository holds (its tests' strings, the
    /// harness's, the conformance files, the docs' examples, TPC-H, the answers recorded from Postgres), each pass on the
    /// text as the passes before it left it, as the front runs them.
    #[test]
    fn a_pass_whose_trigger_is_not_in_a_text_has_nothing_to_do_there() {
        let s = get();
        let quiet = |p: &Pass, sql: &str| match p.run {
            Run::Refuse(f) => f(sql).is_ok(),
            Run::Text(f) => f(sql).is_ok_and(|t| t == sql),
            Run::Wants(f) => !f(sql),
            Run::Parse(_) => true, // (a hook reads in the parse: `dialect::tests`)
        };
        let corpus = corpus();
        assert!(corpus.len() > 5000, "the corpus is the repository's SQL: {} texts", corpus.len());
        let mut missed: Vec<String> = vec![];
        let ran = std::cell::Cell::new(0);
        for sql in &corpus {
            let look = |text: &str, phase: Phase, missed: &mut Vec<String>| {
                let fired = s.fired(text);
                for (_, p) in s.all().iter().enumerate().filter(|(i, p)| p.phase() == phase && !fired.has(*i)) {
                    ran.set(ran.get() + 1);
                    if !quiet(p, text) {
                        missed.push(format!("{}: {text}", p.name));
                    }
                }
            };
            look(sql, Phase::Refuse, &mut missed);
            let mut text = sql.clone();
            for (i, p) in s.all().iter().enumerate().filter(|(_, p)| p.phase() == Phase::Text) {
                match s.fired(&text).has(i) {
                    true => {
                        if let Run::Text(f) = p.run {
                            if let Ok(t) = f(&text) {
                                text = t.into_owned();
                            }
                        }
                    }
                    false if !quiet(p, &text) => missed.push(format!("{}: {text}", p.name)),
                    false => ran.set(ran.get() + 1),
                }
            }
            look(&text, Phase::Wants, &mut missed);
        }
        assert!(missed.is_empty(), "{} misses over {} texts (a trigger short of its look), the first: {:#?}", missed.len(), corpus.len(), &missed[..missed.len().min(20)]);
        assert!(ran.get() > corpus.len(), "the passes ran where their triggers were not");
    }

    /// The repository's SQL: string literals that start as a statement does in src/ and tools/harness.py, the records of
    /// tests/forms/*.slt, the docs' sql blocks, TPC-H's queries and the texts recorded from Postgres (tools/pg16).
    pub(crate) fn corpus() -> Vec<String> {
        use std::collections::BTreeSet;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let read = |p: &std::path::Path| std::fs::read_to_string(p).unwrap_or_default();
        let files = |dir: &str, ext: &str| -> Vec<std::path::PathBuf> {
            let mut out = vec![];
            let mut todo = vec![root.join(dir)];
            while let Some(d) = todo.pop() {
                for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        todo.push(p);
                    } else if p.extension().is_some_and(|x| x == ext) {
                        out.push(p);
                    }
                }
            }
            out
        };
        let starts = regex::Regex::new(r"(?is)^\s*(?:--[^\n]*\n\s*)*(?:select|with|from|create|insert|update|delete|alter|drop|show|explain|pivot|unpivot|set|describe|desc|call|copy|summarize|values|table|merge|grant|revoke|pragma|upsert|replace|declare|prepare|execute|begin|comment|listen|notify|step|wait|parallel|use|attach|refresh|install|load)\b").unwrap();
        let strings = regex::Regex::new(r#"(?s)r#"(.*?)"[#]|"""(.*?)"""|"((?:[^"\\\n]|\\.)*)""#).unwrap();
        let unescape = |s: &str| s.replace("\\n", "\n").replace("\\t", "\t").replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\");
        let mut out = BTreeSet::new();
        let mut take = |t: String| {
            if starts.is_match(&t) && t.len() < crate::parsed::LONGEST {
                out.insert(t);
            }
        };
        for p in files("src", "rs").into_iter().chain([root.join("tools/harness.py")]) {
            for c in strings.captures_iter(&read(&p)) {
                match (c.get(1).or(c.get(2)), c.get(3)) {
                    (Some(raw), _) => take(raw.as_str().to_string()),
                    (None, Some(q)) => take(unescape(q.as_str())),
                    _ => {}
                }
            }
        }
        for p in files("tests/forms", "slt") {
            let text = read(&p);
            let mut record: Option<String> = None;
            for line in text.lines().chain([""]) {
                match &mut record {
                    Some(r) if line.trim().is_empty() || line.starts_with("----") => take(std::mem::take(r)),
                    Some(r) => {
                        r.push_str(line);
                        r.push('\n');
                    }
                    None => {}
                }
                if line.trim().is_empty() || line.starts_with("----") {
                    record = None;
                }
                if line.starts_with("statement ") || line.starts_with("query ") {
                    record = Some(String::new());
                }
            }
        }
        let sql_block = regex::Regex::new(r"(?s)```sql\n(.*?)```").unwrap();
        for p in files("docs", "mdx").into_iter().chain(files("docs", "md")).chain(files("site/src/content/docs", "mdx")) {
            for c in sql_block.captures_iter(&read(&p)) {
                take(c[1].to_string());
            }
        }
        for p in files("tools/bench/tpch-queries", "sql") {
            take(read(&p));
        }
        for p in files("tools/pg16", "json") {
            if let Ok(serde_json::Value::Object(m)) = serde_json::from_str::<serde_json::Value>(&read(&p)) {
                m.keys().for_each(|k| take(k.clone()));
            }
        }
        out.into_iter().collect()
    }

    /// This build's passes run in the order the front ran them when each was named in `front::expand`: the refusals, the
    /// text's rewrites, then the tree passes' looks. A new pass takes a place of its own (`Passes::add`).
    #[test]
    fn this_builds_passes_run_in_the_fronts_order() {
        let names = get().all().iter().map(|p| (p.phase(), p.name)).collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                (Phase::Refuse, "kept.refused"),
                (Phase::Refuse, "dialect_stmts.refused"),
                (Phase::Refuse, "pattern.refused"),
                (Phase::Text, "sparksql.inline"),
                (Phase::Text, "diff.diffs"),
                (Phase::Text, "past_names.syntax"),
                (Phase::Text, "friendly.text"),
                (Phase::Text, "pattern.text"),
                (Phase::Wants, "friendly.forms"),
                (Phase::Wants, "friendly.marked"),
                (Phase::Wants, "friendly.aliases"),
                (Phase::Wants, "pattern.wanted"),
                (Phase::Wants, "text.names_a_type"),
                (Phase::Parse, "friendly.comprehension"),
                (Phase::Parse, "friendly.lambda"),
                (Phase::Parse, "friendly.pivot"),
                (Phase::Parse, "friendly.unpivot"),
            ]
        );
    }

    /// The section's passes run in their phases' order, and a text pass sees the text the passes before it left: a
    /// trigger an earlier pass wrote in fires the later one.
    #[test]
    fn a_text_pass_sees_what_the_one_before_it_wrote() {
        fn one(s: &str) -> anyhow::Result<Cow<'_, str>> { Ok(match s.contains("one") { true => Cow::Owned(s.replace("one", "two")), false => Cow::Borrowed(s) }) }
        fn two(s: &str) -> anyhow::Result<Cow<'_, str>> { Ok(match s.contains("two") { true => Cow::Owned(s.replace("two", "three")), false => Cow::Borrowed(s) }) }
        let mut s = Passes::default();
        s.add(Pass { name: "b.two", order: 20, words: vec!["two"], marks: vec![], run: Run::Text(two) });
        s.add(Pass { name: "a.one", order: 10, words: vec!["one"], marks: vec![], run: Run::Text(one) });
        assert_eq!(s.all().iter().map(|p| p.name).collect::<Vec<_>>(), ["a.one", "b.two"]);
        assert_eq!(s.text("SELECT one").unwrap(), "SELECT three");
        assert!(matches!(s.text("SELECT 1").unwrap(), Cow::Borrowed(_)));
    }
}
