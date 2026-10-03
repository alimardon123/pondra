//! Scripts that decide (ADR-045): blocks with handlers, `IF`, `CASE`, `WHILE`, `REPEAT`, `LOOP`,
//! `FOR r IN (query)`, `LEAVE`, `ITERATE`, `RETURN`, `RAISE`, `PRINT`, `ASSERT`, `EXECUTE IMMEDIATE`,
//! `CALL … INTO` and `IDENTIFIER()`, the same at every door (`routines::one` hands them here).
//!
//! - **One entry per kind** (`KINDS`): the word that opens it and how its head is read; what it does
//!   is its `Step`. A new kind is an entry and a step, not a branch through the runner.
//! - **A statement in a block is a statement** (`routines::one`): a query spreads when that pays, a
//!   write goes to the leader. Conditions and values are SQL expressions, bound as variables are.
//! - **Blocks keep their own.** A `DECLARE` in a `BEGIN … END` ends with it; a loop's row (`$r.col`)
//!   and a handler's `$error` and `$sqlstate` last while they run (ADR-045 §2).
//! - **Exactly once:** each statement's part of the job is its place, its loops' passes counted
//!   (`{job}:3.2#5.0`), so a run again with the same job writes once.
//! - **Splitting** (`depth`, `joined`): a block is one statement to every door's splitter, the
//!   console's too (`sqlfile.js` `blocks`), by the same words.
use crate::routines::{Outcome, Who};
use crate::server::App;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::LazyLock;

// ---------------------------------------------------------------- the text's words

#[derive(Clone, Copy, PartialEq, Debug)]
enum K {
    Word,
    Var,
    Semi,
    Open,
    Close,
    Colon,
    Comma,
    Other,
}

#[derive(Clone, Copy, Debug)]
struct Tok {
    k: K,
    at: usize,
    end: usize,
}

/// The words of `text` outside comments; a string, a quoted name or a `$tag$` body is one token.
fn tokens(text: &str) -> Vec<Tok> {
    let (mut out, b, mut i) = (vec![], text.as_bytes(), 0);
    let word = |s: &str| s.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(s.len());
    while i < b.len() {
        let rest = &text[i..];
        let past = |end: &str, from: usize| rest[from..].find(end).map_or(rest.len(), |e| from + e + end.len());
        let (n, k) = match b[i] {
            b'-' if rest.starts_with("--") => (past("\n", 2), None),
            b'/' if rest.starts_with("/*") => (past("*/", 2), None),
            b'\'' => (past("'", 1), Some(K::Other)),
            b'"' => (past("\"", 1), Some(K::Other)),
            b'$' => match crate::routines::dollar_tag(rest) {
                Some(tag) => (past(tag, tag.len()), Some(K::Other)),
                None => (1 + word(&rest[1..]), Some(K::Var)),
            },
            b';' => (1, Some(K::Semi)),
            b'(' => (1, Some(K::Open)),
            b')' => (1, Some(K::Close)),
            b':' => (1, Some(K::Colon)),
            b',' => (1, Some(K::Comma)),
            c if c.is_ascii_whitespace() => (1, None),
            c if c.is_ascii_digit() => (word(rest).max(1), Some(K::Other)),
            _ => match word(rest) {
                0 => (rest.chars().next().map_or(1, char::len_utf8), Some(K::Other)),
                n => (n, Some(K::Word)),
            },
        };
        if let Some(k) = k {
            out.push(Tok { k, at: i, end: i + n });
        }
        i += n.max(1);
    }
    out
}

/// What a piece of a script (the text between two `;`s) does to how deep in blocks it is: one more
/// for each block it opens, one less for each `END`. A block's word opens one only where a statement
/// starts (the piece's start, after `THEN`, `ELSE`, `DO`, `LOOP`, `REPEAT`, `BEGIN` or a label), so
/// `DROP TABLE IF EXISTS` opens nothing; `CASE` always does, as an expression's ends with `END` too;
/// `BEGIN;` and `BEGIN TRANSACTION` are a transaction's.
pub fn depth(piece: &str) -> i32 {
    let t = tokens(piece);
    let word = |i: usize| t.get(i).filter(|x| x.k == K::Word).map(|x| piece[x.at..x.end].to_ascii_lowercase());
    let mut d = 0;
    for i in 0..t.len() {
        let Some(w) = word(i) else { continue };
        let before = i.checked_sub(1).and_then(word);
        if w == "end" {
            d -= 1;
            continue;
        }
        if before.as_deref() == Some("end") {
            continue; // (END IF)
        }
        let starts = i == 0 || matches!(before.as_deref(), Some("then" | "else" | "do" | "loop" | "repeat" | "begin")) || labelled(&t, i);
        d += match w.as_str() {
            "case" => 1,
            "begin" => (starts && opens(piece, &t, i)) as i32,
            "if" => (starts && (i == 0 || !called(piece, &t, i))) as i32,
            "repeat" => (starts && t.get(i + 1).is_none_or(|x| x.k != K::Open)) as i32,
            "while" | "loop" => starts as i32,
            "for" => (starts && matches!(word(i + 2).as_deref(), Some("in" | "as"))) as i32,
            _ => 0,
        };
    }
    d
}

/// Is the `IF` at `i` the function (`ELSE if(a, b, c) END`), not the statement (`THEN IF (a) THEN`)?
fn called(text: &str, t: &[Tok], i: usize) -> bool {
    if t.get(i + 1).is_none_or(|x| x.k != K::Open) {
        return false;
    }
    let mut depth = 0;
    for j in i + 1..t.len() {
        depth += (t[j].k == K::Open) as i32 - (t[j].k == K::Close) as i32;
        if depth == 0 {
            return t.get(j + 1).is_none_or(|x| !text[x.at..x.end].eq_ignore_ascii_case("then"));
        }
    }
    true
}

/// Is the word at `i` after a label (`outer: WHILE …`), not a cast (`x::int`)?
fn labelled(t: &[Tok], i: usize) -> bool { i >= 2 && t[i - 1].k == K::Colon && t[i - 2].k == K::Word && (i < 3 || t[i - 3].k != K::Colon) }

/// Does the `BEGIN` at `i` open a block (`BEGIN` then a statement), not a transaction?
fn opens(text: &str, t: &[Tok], i: usize) -> bool {
    match t.get(i + 1) {
        None => false,
        Some(x) if x.k == K::Semi => false,
        Some(x) => !(x.k == K::Word && matches!(text[x.at..x.end].to_ascii_lowercase().as_str(), "transaction" | "work" | "isolation" | "read" | "deferrable" | "not")),
    }
}

/// Pieces split at every `;` → statements, a block's pieces joined (`;` between them, as written);
/// and a block not closed yet, if the last one is.
pub fn joined(pieces: Vec<String>) -> (Vec<String>, Option<String>) {
    let (mut out, mut open, mut d) = (vec![], None::<String>, 0);
    for p in pieces {
        d = (d + depth(&p)).max(0);
        let whole = match open.take() {
            Some(o) => format!("{o};{p}"),
            None => p,
        };
        match d {
            0 => out.push(whole),
            _ => open = Some(whole),
        }
    }
    (out, open)
}

// ---------------------------------------------------------------- reading a script

/// A statement of a script: where it starts in the text, and what it does.
type Steps = Vec<(usize, Box<dyn Step>)>;

/// What a step says comes next.
enum Flow {
    Next,
    Leave(Option<String>),
    Iterate(Option<String>),
    Return,
}

trait Step: Send + Sync {
    /// Run it (`at`: where it starts; `path`: its place, for its statements' parts of the job).
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, at: usize, path: String) -> BoxFuture<'a, Result<Flow>>;
    /// A plain statement's text (None: a script's own kind).
    fn plain(&self) -> Option<&str> { None }
}

type Parse = fn(&mut Reader, Option<String>) -> Result<Option<Box<dyn Step>>>;

/// The kinds of statement a script has besides SQL's: the word that opens each, and how it is read
/// (`None`: not this kind after all, a plain statement: `BEGIN;`, `EXECUTE name`, `CALL` without `INTO`).
const KINDS: &[(&str, Parse)] = &[
    ("begin", block),
    ("if", branch),
    ("case", case),
    ("while", while_),
    ("repeat", repeat),
    ("loop", loop_),
    ("for", for_),
    ("leave", leave),
    ("iterate", iterate),
    ("return", return_),
    ("raise", raise),
    ("print", print),
    ("assert", assert),
    ("execute", execute),
    ("call", call_into),
];

struct Reader<'a> {
    text: &'a str,
    t: Vec<Tok>,
    i: usize,
}

impl<'a> Reader<'a> {
    fn new(text: &'a str) -> Self { Reader { text, t: tokens(text), i: 0 } }
    fn word_at(&self, i: usize) -> Option<String> { self.t.get(i).filter(|x| x.k == K::Word).map(|x| self.text[x.at..x.end].to_ascii_lowercase()) }
    fn is(&self, w: &str) -> bool { self.word_at(self.i).as_deref() == Some(w) }
    fn eat(&mut self, w: &str) -> bool {
        let yes = self.is(w);
        self.i += yes as usize;
        yes
    }
    fn at(&self) -> usize { self.t.get(self.i).map_or(self.text.len(), |x| x.at) }
    fn line(&self, at: usize) -> usize { self.text[..at.min(self.text.len())].matches('\n').count() + 1 }
    fn expect(&mut self, w: &str, after: &str) -> Result<()> {
        ensure!(self.eat(w), "line {}: {} after {after}", self.line(self.at()), w.to_uppercase());
        Ok(())
    }
    /// The text from here to a word of `stop` (or a `;`, or the end) outside parentheses and
    /// `CASE … END`; the stop not taken.
    fn until(&mut self, stop: &[&str]) -> String {
        let (from, mut depth, mut cases) = (self.at(), 0, 0);
        while let Some(x) = self.t.get(self.i) {
            match x.k {
                K::Open => depth += 1,
                K::Close if depth == 0 => break, // (the end of what it is inside: `IDENTIFIER(…)`)
                K::Close => depth -= 1,
                K::Semi if depth <= 0 => break,
                K::Word if depth <= 0 => match self.word_at(self.i).as_deref() {
                    Some("case") => cases += 1,
                    Some("end") if cases > 0 => cases -= 1,
                    Some(w) if cases == 0 && stop.contains(&w) => break,
                    _ => {}
                },
                _ => {}
            }
            self.i += 1;
        }
        self.text[from..self.at()].trim().to_string()
    }
    /// The text to the next `;` (taken) or the end.
    fn rest(&mut self) -> String {
        let s = self.until(&[]);
        self.eat_semi();
        s
    }
    fn eat_semi(&mut self) { self.i += self.t.get(self.i).is_some_and(|x| x.k == K::Semi) as usize; }
    /// `END [what] [label];`
    fn end(&mut self, what: &str, opened: usize) -> Result<()> {
        let open = |s: &Self| format!("{} on line {}", if what.is_empty() { "BEGIN".into() } else { what.to_uppercase() }, s.line(opened));
        ensure!(self.eat("end"), "line {}: the {} isn't closed (END{})", self.line(self.at()), open(self), if what.is_empty() { String::new() } else { format!(" {}", what.to_uppercase()) });
        if !what.is_empty() {
            let found = self.word_at(self.i).map_or("…".into(), |w| w.to_uppercase());
            ensure!(self.eat(what), "line {}: END {found} where the {} ends (it takes END {})", self.line(self.at()), open(self), what.to_uppercase());
        }
        let label = self.t.get(self.i).is_some_and(|x| x.k == K::Word) && self.t.get(self.i + 1).is_none_or(|x| x.k == K::Semi);
        self.i += label as usize;
        self.eat_semi();
        Ok(())
    }
    /// Statements up to a word of `stop` where a statement starts (not taken), or the end.
    fn steps(&mut self, stop: &[&str]) -> Result<Steps> {
        let mut out = vec![];
        loop {
            while self.t.get(self.i).is_some_and(|x| x.k == K::Semi) {
                self.i += 1;
            }
            if self.i >= self.t.len() || self.word_at(self.i).is_some_and(|w| stop.contains(&w.as_str())) {
                return Ok(out);
            }
            out.push((self.at(), self.step()?));
        }
    }
    fn step(&mut self) -> Result<Box<dyn Step>> {
        let start = self.i;
        let label = match (self.t.get(self.i), self.t.get(self.i + 1), self.t.get(self.i + 2)) {
            (Some(a), Some(b), c) if a.k == K::Word && b.k == K::Colon && c.is_none_or(|c| c.k != K::Colon) => {
                self.i += 2;
                self.word_at(start)
            }
            _ => None,
        };
        if let Some(parse) = self.word_at(self.i).and_then(|w| KINDS.iter().find(|(k, _)| *k == w)).map(|(_, p)| p) {
            let from = self.i;
            if let Some(step) = parse(self, label.clone())? {
                return Ok(step);
            }
            self.i = from;
        }
        ensure!(label.is_none(), "line {}: a label names a block or a loop", self.line(self.at()));
        Ok(Box::new(Plain(self.rest())))
    }
    fn expr(&mut self, stop: &[&str], what: &str) -> Result<String> {
        let line = self.line(self.at());
        let e = self.until(stop);
        ensure!(!e.is_empty(), "line {line}: {what} needs a condition or a value");
        Ok(e)
    }
}

/// A script's statements, its blocks read.
fn parse(text: &str) -> Result<Steps> {
    let mut r = Reader::new(text);
    let out = r.steps(&[])?;
    Ok(out)
}

static OPENER: LazyLock<regex::Regex> = LazyLock::new(|| {
    let words = KINDS.iter().map(|(w, _)| *w).collect::<Vec<_>>().join("|");
    regex::Regex::new(&format!(r"(?is)^(?:[a-z_]\w*\s*:\s*)?(?:{words})\b")).expect("a regex")
});

/// Is `sql` a statement only a script has (a block, a branch, a loop, `PRINT`, `RAISE`, …), or one
/// naming something by `IDENTIFIER(…)`? Then `run` takes it.
pub fn is(sql: &str) -> bool {
    let s = crate::write::first_word(sql);
    // (one that doesn't read is the runner's too: it says why)
    OPENER.is_match(s) && parse(s).map_or(true, |all| !(all.len() == 1 && all[0].1.plain().is_some())) || names_identifier(s)
}

fn names_identifier(s: &str) -> bool { s.as_bytes().windows(10).any(|w| w.eq_ignore_ascii_case(b"identifier")) && IDENTIFIER.is_match(s) }

static IDENTIFIER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bidentifier\s*\(").expect("a regex"));

/// The names a script sets for itself inside its blocks (a loop's row, `INTO`, a block's `DECLARE`,
/// an assignment, a handler's `$error`), which are never its parameters (`vars::parameters`).
pub fn binds(sql: &str) -> Vec<String> {
    let t = tokens(sql);
    let w = |i: usize| t.get(i).filter(|x| x.k == K::Word).map(|x| sql[x.at..x.end].to_ascii_lowercase());
    let v = |i: usize| t.get(i).filter(|x| x.k == K::Var).map(|x| sql[x.at + 1..x.end].to_string());
    let mut out = vec!["error".to_string(), "sqlstate".to_string()];
    for i in 0..t.len() {
        match w(i).as_deref() {
            Some("for") if matches!(w(i + 2).as_deref(), Some("in" | "as")) => out.extend(w(i + 1)),
            Some("into") => out.extend((i + 1..t.len()).step_by(2).map_while(|j| v(j).filter(|_| j == i + 1 || t[j - 1].k == K::Comma))),
            Some("declare") => out.extend(v(i + 1).or_else(|| v(i + 2))),
            _ if t[i].k == K::Var && t.get(i + 1).is_some_and(|x| &sql[x.at..x.end] == "=") => out.extend(v(i)),
            _ => {}
        }
    }
    out
}

// ---------------------------------------------------------------- running a script

/// A script being run: who for, its job, what it has said so far.
pub struct Runner<'a> {
    app: &'a App,
    who: Who,
    job: Option<String>,
    views: &'a HashMap<String, String>,
    /// Each block's variables as they were before its `DECLARE`s, to put back when it ends.
    blocks: Vec<Vec<(String, Option<crate::vars::Var>)>>,
    /// How deep in blocks and loops (a `DECLARE PARAMETER` only at the top).
    nested: usize,
    /// A loop's row variables in force (`$r.col`).
    rows: Vec<String>,
    /// The errors handlers are handling (`RAISE;` raises the last again).
    caught: Vec<(&'static str, String)>,
    /// Where the step running starts (the line an error names).
    at: usize,
    last: Outcome,
}

/// Run `sql`, a script: its statements in turn, blocks, branches and loops as they say. The answer
/// is its last statement's, or what `RETURN` gives.
pub async fn run(app: &App, sql: &str, views: &HashMap<String, String>, who: Who, job: Option<String>) -> Result<Outcome> {
    let steps = parse(sql)?;
    let one = steps.len() == 1;
    let go = async {
        let mut r = Runner { app, who, job, views, blocks: vec![], nested: 0, rows: vec![], caught: vec![], at: 0, last: Outcome::Done(serde_json::json!({})) };
        for (i, (at, s)) in steps.iter().enumerate() {
            let path = if one { String::new() } else { i.to_string() };
            let flow = match s.run(&mut r, *at, path).await {
                Err(e) if one && s.plain().is_some() => return Err(e),
                Err(e) if one => return Err(e.context(format!("line {}", line(sql, r.at)))),
                Err(e) if s.plain().is_some() => return Err(e.context(format!("statement {}: {}", i + 1, short(s.plain().unwrap_or_default())))),
                Err(e) => return Err(e.context(format!("line {}", line(sql, r.at))).context(format!("statement {}: {}", i + 1, short(&sql[*at..end_of(&steps, i, sql)])))),
                Ok(f) => f,
            };
            match flow {
                Flow::Next => {}
                Flow::Return => break,
                Flow::Leave(l) | Flow::Iterate(l) => bail!("LEAVE or ITERATE {} outside a loop or a block of that name", l.unwrap_or_default()),
            }
        }
        Ok(r.last)
    };
    match crate::vars::scoped() || one && steps[0].1.plain().is_some() {
        true => go.await, // (a lone DECLARE with no session is refused: nothing would read it)
        false => crate::vars::local(go).await, // (no session: the script's variables are its own)
    }
}

/// A one-row column's value as text (NULL: None), as SQL casts it.
fn text_of(col: &dyn Array) -> Result<Option<String>> {
    let t = datafusion::arrow::compute::cast(&datafusion::arrow::array::make_array(col.to_data()), &datafusion::arrow::datatypes::DataType::Utf8)?;
    Ok(t.as_string_opt::<i32>().filter(|a| a.is_valid(0)).map(|a| a.value(0).to_string()))
}

fn end_of(steps: &Steps, i: usize, sql: &str) -> usize { steps.get(i + 1).map_or(sql.len(), |(at, _)| *at) }
fn line(text: &str, at: usize) -> usize { text[..at.min(text.len())].matches('\n').count() + 1 }

fn short(s: &str) -> String {
    let s = s.trim().trim_end_matches(';').split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() > 80 { format!("{}…", s.chars().take(80).collect::<String>()) } else { s }
}

impl<'a> Runner<'a> {
    /// Steps in turn, until one says otherwise.
    fn list<'b>(&'b mut self, steps: &'b Steps, path: &'b str) -> BoxFuture<'b, Result<Flow>> {
        Box::pin(async move {
            for (j, (at, s)) in steps.iter().enumerate() {
                self.at = *at;
                let place = if path.is_empty() { j.to_string() } else { format!("{path}.{j}") };
                match s.run(self, *at, place).await? {
                    Flow::Next => {}
                    other => return Ok(other),
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Steps in a scope of their own: its `DECLARE`s, and anything kept in `keep`, put back after.
    async fn scoped(&mut self, steps: &Steps, path: &str) -> Result<Flow> {
        self.blocks.push(vec![]);
        self.nested += 1;
        let out = self.list(steps, path).await;
        self.nested -= 1;
        self.unwind();
        out
    }

    fn unwind(&mut self) {
        for (name, was) in self.blocks.pop().unwrap_or_default().into_iter().rev() {
            crate::vars::put(&name, was);
        }
    }

    fn keep(&mut self, name: &str) {
        let was = crate::vars::get(name);
        match self.blocks.last_mut() {
            Some(b) => b.push((name.to_string(), was)),
            None => {}
        }
    }

    /// A statement as written made ready: a loop's `$r.col` its variable, `IDENTIFIER(…)` a name.
    async fn ready(&self, s: &str) -> Result<String> {
        let mut s = self.dotted(s);
        while names_identifier(&s) {
            let m = IDENTIFIER.find(&s).expect("a match");
            let mut r = Reader::new(&s[m.end()..]);
            let inner = r.until(&[]);
            let close = m.end() + r.at();
            ensure!(s[close..].starts_with(')'), "IDENTIFIER( without its )");
            let row = self.select(&format!("SELECT ({inner})")).await?;
            let name = text_of(row.column(0).as_ref())?.with_context(|| format!("IDENTIFIER({inner}) is NULL"))?;
            let quoted = name.split('.').map(|p| format!("\"{}\"", p.replace('"', "\"\""))).collect::<Vec<_>>().join(".");
            s.replace_range(m.start()..close + 1, &quoted);
        }
        Ok(s)
    }

    /// `$r.col` → `$r__col`, for the loops' rows in force.
    fn dotted(&self, s: &str) -> String {
        let mut s = s.to_string();
        if self.rows.is_empty() || !s.contains('.') {
            return s;
        }
        let t = tokens(&s);
        for i in (0..t.len().saturating_sub(2)).rev() {
            let (v, dot, col) = (t[i], t[i + 1], t[i + 2]);
            let row = &s[v.at + 1..v.end];
            if v.k == K::Var && dot.at == v.end && &s[dot.at..dot.end] == "." && col.k == K::Word && col.at == dot.end && self.rows.iter().any(|r| r == row) {
                let to = format!("${row}__{}", s[col.at..col.end].to_lowercase());
                s.replace_range(v.at..col.end, &to);
            }
        }
        s
    }

    /// `sql` (a query, its loops' rows already in place), its `$name`s bound: one row.
    async fn select(&self, sql: &str) -> Result<RecordBatch> {
        let sql = crate::routines::prepare(&self.app.lake, &self.dotted(sql), &HashMap::new(), self.views).await?;
        let rows = crate::vars::answer(self.app, &sql).await?;
        ensure!(!rows.is_empty(), "no answer");
        let row = datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
        ensure!(row.num_rows() == 1, "one value, not {} rows", row.num_rows());
        Ok(row)
    }

    /// `SELECT {select}`, made ready and bound: one row.
    async fn row(&self, select: &str) -> Result<RecordBatch> { self.select(&self.ready(&format!("SELECT {select}")).await?).await.with_context(|| short(select)) }

    /// Is `cond` true (NULL is not)?
    async fn truth(&self, cond: &str) -> Result<bool> {
        // (in a WHERE, where DataFusion plans EXISTS and IN as joins: not in a SELECT's list)
        let sql = self.ready(&format!("SELECT 1 AS t WHERE ({cond})")).await?;
        let sql = crate::routines::prepare(&self.app.lake, &sql, &HashMap::new(), self.views).await?;
        let rows = crate::vars::answer(self.app, &sql).await.with_context(|| format!("the condition {}", short(cond)))?;
        Ok(rows.iter().any(|b| b.num_rows() > 0))
    }

    /// Values as text (NULL: None).
    async fn texts(&self, exprs: &[String]) -> Result<Vec<Option<String>>> {
        if exprs.is_empty() {
            return Ok(vec![]);
        }
        let row = self.row(&exprs.iter().map(|e| format!("({e})")).collect::<Vec<_>>().join(", ")).await?;
        row.columns().iter().map(|c| text_of(c.as_ref())).collect()
    }

    /// A plain statement, as `routines::one` runs one; a `DECLARE` in a block is the block's.
    async fn plain(&mut self, s: &str, path: &str) -> Result<()> {
        let s = self.ready(s).await?;
        let job = self.job.as_ref().map(|j| if path.is_empty() { j.clone() } else { format!("{j}:{path}") });
        let s = match crate::vars::change(&s) {
            Some(crate::vars::Change::Declare { name, parameter, .. }) if self.nested > 0 => {
                ensure!(!parameter, "DECLARE PARAMETER ${name}: a script's parameters are declared at its top, not in a block or a loop");
                self.keep(&name);
                s
            }
            Some(_) => s, // (`DECLARE $day …`, `$day = …`: worked out by `one`)
            None => crate::routines::prepare(&self.app.lake, &s, &HashMap::new(), self.views).await?,
        };
        self.last = Box::pin(crate::routines::one(self.app, &s, self.who, job)).await?;
        Ok(())
    }

    /// Set `$name` from a one-row column (`INTO`): cast to its declared type, if it has one.
    async fn set(&self, name: &str, col: &dyn Array) -> Result<()> {
        let v = crate::vars::of_column(col)?;
        Box::pin(crate::vars::apply(self.app, crate::vars::Change::Set { name: name.to_string(), value: v.sql })).await?;
        Ok(())
    }

    /// A loop's body, run as its pass says: `Some(flow)` ends the loop with that flow.
    async fn pass(&mut self, body: &Steps, path: &str, n: usize, label: &Option<String>) -> Result<Option<Flow>> {
        let ours = |l: &Option<String>| l.is_none() || l == label;
        Ok(match self.scoped(body, &format!("{path}#{n}")).await? {
            Flow::Next => None,
            Flow::Iterate(l) if ours(&l) => None,
            Flow::Leave(l) if ours(&l) => Some(Flow::Next),
            other => Some(other),
        })
    }
}

// ---------------------------------------------------------------- the kinds of statement

struct Plain(String);

impl Step for Plain {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, _: usize, path: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            r.plain(&self.0, &path).await?;
            Ok(Flow::Next)
        })
    }
    fn plain(&self) -> Option<&str> { Some(&self.0) }
}

/// `BEGIN … [EXCEPTION WHEN … THEN …] END`
struct Block {
    label: Option<String>,
    body: Steps,
    handlers: Vec<(Vec<String>, Steps)>,
}

fn block(r: &mut Reader, label: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    if !opens(r.text, &r.t, r.i) {
        return Ok(None); // (BEGIN; BEGIN TRANSACTION: a transaction's)
    }
    r.i += 1;
    let body = r.steps(&["exception", "end"])?;
    let mut handlers = vec![];
    if r.eat("exception") {
        while r.eat("when") {
            let conditions = r.until(&["then"]).split(|c: char| c.is_whitespace()).filter(|w| !w.is_empty() && !w.eq_ignore_ascii_case("or")).map(|w| w.trim_matches('\'').to_lowercase()).collect::<Vec<_>>();
            r.expect("then", "EXCEPTION WHEN …")?;
            handlers.push((conditions, r.steps(&["when", "end"])?));
        }
    }
    r.end("", opened)?;
    Ok(Some(Box::new(Block { label, body, handlers })))
}

/// The SQLSTATEs a handler's words name: a code (`'40001'`, `SQLSTATE '40001'`), one of Postgres's
/// names (`serialization_failure`), `OTHERS` or `SQLEXCEPTION` (every error).
fn catches(words: &[String], code: &str) -> bool {
    words.iter().any(|w| matches!(w.as_str(), "others" | "sqlexception") || w == &code.to_lowercase() || crate::codes::named(w) == Some(code))
}

impl Step for Block {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, at: usize, path: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            r.blocks.push(vec![]);
            r.nested += 1;
            let out = r.list(&self.body, &path).await;
            let out = match out {
                Err(e) => match self.handlers.iter().find(|(w, _)| catches(w, crate::codes::of(&e))) {
                    Some((_, handler)) => {
                        let (code, said) = (crate::codes::of(&e), crate::ext::said(&e));
                        r.at = at;
                        for (name, value) in [("error", said.clone()), ("sqlstate", code.to_string())] {
                            r.keep(name);
                            crate::vars::put(name, Some(crate::vars::text(&value)));
                        }
                        r.caught.push((code, said));
                        let out = r.list(handler, &format!("{path}!")).await;
                        r.caught.pop();
                        out
                    }
                    None => Err(e),
                },
                ok => ok,
            };
            r.nested -= 1;
            r.unwind();
            Ok(match out? {
                Flow::Leave(Some(l)) if Some(&l) == self.label.as_ref() => Flow::Next,
                f => f,
            })
        })
    }
}

/// `IF … THEN … ELSEIF … ELSE … END IF`, and `CASE` as one.
struct Branch {
    arms: Vec<(String, Steps)>,
    otherwise: Steps,
}

fn branch(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    r.i += 1;
    let mut arms = vec![];
    loop {
        let cond = r.expr(&["then"], "IF")?;
        r.expect("then", "IF …")?;
        arms.push((cond, r.steps(&["elseif", "elsif", "else", "end"])?));
        if !(r.eat("elseif") || r.eat("elsif")) {
            break;
        }
    }
    let otherwise = if r.eat("else") { r.steps(&["end"])? } else { vec![] };
    r.end("if", opened)?;
    Ok(Some(Box::new(Branch { arms, otherwise })))
}

/// `CASE [$x] WHEN … THEN … [ELSE …] END CASE`: a branch whose conditions are `$x = …` (or as written).
fn case(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    r.i += 1;
    let subject = Some(r.until(&["when"])).filter(|s| !s.is_empty());
    let mut arms = vec![];
    while r.eat("when") {
        let v = r.expr(&["then"], "WHEN")?;
        r.expect("then", "CASE … WHEN …")?;
        let cond = subject.as_ref().map_or(v.clone(), |s| format!("({s}) = ({v})"));
        arms.push((cond, r.steps(&["when", "else", "end"])?));
    }
    let otherwise = if r.eat("else") { r.steps(&["end"])? } else { vec![] };
    r.end("case", opened)?;
    Ok(Some(Box::new(Branch { arms, otherwise })))
}

impl Step for Branch {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, at: usize, path: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            for (i, (cond, steps)) in self.arms.iter().enumerate() {
                r.at = at;
                if r.truth(cond).await? {
                    return r.scoped(steps, &format!("{path}.{i}")).await;
                }
            }
            r.scoped(&self.otherwise, &format!("{path}.e")).await
        })
    }
}

/// `WHILE … DO … END WHILE`, `REPEAT … UNTIL … END REPEAT`, `LOOP … END LOOP`, `FOR r IN (…) DO … END FOR`.
struct Loop {
    label: Option<String>,
    kind: Kind,
    body: Steps,
}

enum Kind {
    While(String),
    Until(String),
    Ever,
    For(String, String),
}

fn while_(r: &mut Reader, label: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    r.i += 1;
    let cond = r.expr(&["do"], "WHILE")?;
    r.expect("do", "WHILE …")?;
    let body = r.steps(&["end"])?;
    r.end("while", opened)?;
    Ok(Some(Box::new(Loop { label, kind: Kind::While(cond), body })))
}

fn repeat(r: &mut Reader, label: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    r.i += 1;
    let body = r.steps(&["until"])?;
    r.expect("until", "REPEAT …")?;
    let cond = r.expr(&["end"], "UNTIL")?;
    r.end("repeat", opened)?;
    Ok(Some(Box::new(Loop { label, kind: Kind::Until(cond), body })))
}

fn loop_(r: &mut Reader, label: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    r.i += 1;
    let body = r.steps(&["end"])?;
    r.end("loop", opened)?;
    Ok(Some(Box::new(Loop { label, kind: Kind::Ever, body })))
}

fn for_(r: &mut Reader, label: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let opened = r.at();
    let (Some(var), Some("in" | "as")) = (r.word_at(r.i + 1), r.word_at(r.i + 2).as_deref()) else { return Ok(None) };
    r.i += 3;
    let mut query = r.expr(&["do"], "FOR … IN")?;
    if wrapped(&query) {
        query = query[1..query.len() - 1].trim().to_string(); // ((SELECT …): the query)
    }
    r.expect("do", "FOR … IN (query)")?;
    let body = r.steps(&["end"])?;
    r.end("for", opened)?;
    Ok(Some(Box::new(Loop { label, kind: Kind::For(var, query), body })))
}

/// Is `q` one pair of parentheses around the rest (`(SELECT …)`)?
fn wrapped(q: &str) -> bool {
    let t = tokens(q);
    let mut depth = 0;
    for (i, x) in t.iter().enumerate() {
        depth += (x.k == K::Open) as i32 - (x.k == K::Close) as i32;
        if depth == 0 {
            return i == t.len() - 1 && t[0].k == K::Open;
        }
    }
    false
}

impl Step for Loop {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, at: usize, path: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            if let Kind::For(var, query) = &self.kind {
                r.at = at;
                let q = crate::routines::prepare(&r.app.lake, &r.ready(query).await?, &HashMap::new(), r.views).await?;
                let batches = Box::pin(r.app.query(&q, None)).await.with_context(|| format!("FOR {var} IN ({})", short(query)))?;
                let mut n = 0;
                r.rows.push(var.clone());
                r.blocks.push(vec![]);
                let out = async {
                    for b in &batches {
                        for row in 0..b.num_rows() {
                            for (f, col) in b.schema().fields().iter().zip(b.columns()) {
                                let name = format!("{var}__{}", f.name().to_lowercase());
                                if n == 0 {
                                    r.keep(&name);
                                }
                                if !col.data_type().is_nested() {
                                    crate::vars::put(&name, Some(crate::vars::of_column(col.slice(row, 1).as_ref())?));
                                }
                            }
                            if let Some(f) = r.pass(&self.body, &path, n, &self.label).await? {
                                return Ok(f);
                            }
                            n += 1;
                        }
                    }
                    Ok(Flow::Next)
                }
                .await;
                r.rows.pop();
                r.unwind();
                return out;
            }
            for n in 0.. {
                r.at = at;
                if let Kind::While(c) = &self.kind {
                    if !r.truth(c).await? {
                        break;
                    }
                }
                if let Some(f) = r.pass(&self.body, &path, n, &self.label).await? {
                    return Ok(f);
                }
                r.at = at;
                if let Kind::Until(c) = &self.kind {
                    if r.truth(c).await? {
                        break;
                    }
                }
            }
            Ok(Flow::Next)
        })
    }
}

/// `LEAVE [label]`, `ITERATE [label]`, `RETURN [value]`.
enum Jump {
    Leave(Option<String>),
    Iterate(Option<String>),
    Return(Option<String>),
}

fn leave(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    Ok(Some(Box::new(Jump::Leave(Some(r.rest().to_lowercase()).filter(|l| !l.is_empty())))))
}

fn iterate(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    Ok(Some(Box::new(Jump::Iterate(Some(r.rest().to_lowercase()).filter(|l| !l.is_empty())))))
}

fn return_(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    Ok(Some(Box::new(Jump::Return(Some(r.rest()).filter(|v| !v.is_empty())))))
}

impl Step for Jump {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, _: usize, _: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            Ok(match self {
                Jump::Leave(l) => Flow::Leave(l.clone()),
                Jump::Iterate(l) => Flow::Iterate(l.clone()),
                Jump::Return(v) => {
                    r.last = match v {
                        Some(v) => Outcome::Rows(vec![r.row(&format!("({v}) AS result")).await?]),
                        None => Outcome::Done(serde_json::json!({})),
                    };
                    Flow::Return
                }
            })
        })
    }
}

/// `RAISE 'text %', $x` (an error, P0001), `RAISE NOTICE …` (a notice), `RAISE;` (the error being
/// handled, again), `PRINT value`, `ASSERT cond [, 'text']` (P0004 unless it holds).
enum Say {
    Raise { level: Option<String>, args: Vec<String> },
    Print(String),
    Assert(String, Option<String>),
}

/// `a, b, c` at the top level (not inside parentheses or strings).
fn commas(s: &str) -> Vec<String> {
    let (t, mut out, mut from, mut depth) = (tokens(s), vec![], 0, 0);
    for x in &t {
        match x.k {
            K::Open => depth += 1,
            K::Close => depth -= 1,
            K::Comma if depth == 0 => {
                out.push(s[from..x.at].trim().to_string());
                from = x.end;
            }
            _ => {}
        }
    }
    out.push(s[from..].trim().to_string());
    out.retain(|x| !x.is_empty());
    out
}

fn raise(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    let level = r.word_at(r.i).filter(|w| matches!(w.as_str(), "exception" | "notice" | "warning" | "info" | "log" | "debug"));
    r.i += level.is_some() as usize;
    Ok(Some(Box::new(Say::Raise { level: level.filter(|l| l != "exception"), args: commas(&r.rest()) })))
}

fn print(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    let e = r.expr(&[], "PRINT")?;
    r.eat_semi();
    Ok(Some(Box::new(Say::Print(e))))
}

fn assert(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    r.i += 1;
    let mut all = commas(&r.rest()).into_iter();
    let cond = all.next().context("ASSERT needs a condition")?;
    Ok(Some(Box::new(Say::Assert(cond, all.next()))))
}

/// `fmt`'s `%`s → the values in turn (`%%`: a `%`), as Postgres's RAISE does.
fn format(fmt: &str, values: &[Option<String>]) -> String {
    let (mut out, mut next, mut chars) = (String::new(), values.iter(), fmt.chars().peekable());
    while let Some(c) = chars.next() {
        match c {
            '%' if chars.peek() == Some(&'%') => {
                chars.next();
                out.push('%');
            }
            '%' => out.push_str(next.next().map_or("%", |v| v.as_deref().unwrap_or("NULL"))),
            c => out.push(c),
        }
    }
    out
}

impl Step for Say {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, _: usize, _: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            match self {
                Say::Raise { level: None, args } if args.is_empty() => {
                    let (code, said) = r.caught.last().cloned().context("RAISE; raises the error being handled, so it goes in an EXCEPTION handler")?;
                    return Err(crate::codes::coded(code, said));
                }
                Say::Raise { level, args } => {
                    let all = r.texts(args).await?;
                    let text = format(all.first().cloned().flatten().as_deref().unwrap_or(""), &all[1..]);
                    match level.as_deref() {
                        None => return Err(crate::codes::coded("P0001", text)),
                        Some("warning") => crate::routines::heard(&format!("WARNING: {text}")),
                        Some(_) => crate::routines::heard(&text),
                    }
                }
                Say::Print(e) => {
                    let text = r.texts(std::slice::from_ref(e)).await?.pop().flatten();
                    crate::routines::heard(text.as_deref().unwrap_or("NULL"));
                }
                Say::Assert(cond, text) => {
                    if !r.truth(cond).await? {
                        let said = match text {
                            Some(t) => r.texts(std::slice::from_ref(t)).await?.pop().flatten().unwrap_or_default(),
                            None => format!("assertion failed: {}", short(cond)),
                        };
                        return Err(crate::codes::coded("P0004", said));
                    }
                }
            }
            Ok(Flow::Next)
        })
    }
}

/// `EXECUTE IMMEDIATE 'sql' [INTO $a, …] [USING x, …]` (the values bound as `$1`, `$2`, …), and
/// `CALL p(…) INTO $x`: a statement's answer, its first row, into variables.
struct Execute {
    sql: Option<String>,
    call: Option<String>,
    into: Vec<String>,
    using: Vec<String>,
}

fn into(r: &mut Reader) -> Vec<String> {
    let mut out = vec![];
    if r.eat("into") {
        while let Some(x) = r.t.get(r.i).filter(|x| x.k == K::Var) {
            out.push(r.text[x.at + 1..x.end].to_string());
            r.i += 1;
            if !r.t.get(r.i).is_some_and(|x| x.k == K::Comma) {
                break;
            }
            r.i += 1;
        }
    }
    out
}

fn execute(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    if r.word_at(r.i + 1).as_deref() != Some("immediate") {
        return Ok(None); // (EXECUTE name: a prepared statement's)
    }
    r.i += 2;
    let sql = r.expr(&["into", "using"], "EXECUTE IMMEDIATE")?;
    let mut x = Execute { sql: Some(sql), call: None, into: into(r), using: vec![] };
    if r.eat("using") {
        x.using = commas(&r.until(&["into"]));
    }
    if x.into.is_empty() {
        x.into = into(r);
    }
    ensure!(r.i >= r.t.len() || r.t[r.i].k == K::Semi, "line {}: EXECUTE IMMEDIATE 'sql' [INTO $a, …] [USING value, …]", r.line(r.at()));
    r.eat_semi();
    Ok(Some(Box::new(x)))
}

fn call_into(r: &mut Reader, _: Option<String>) -> Result<Option<Box<dyn Step>>> {
    let call = r.until(&["into"]);
    let vars = into(r);
    if vars.is_empty() || r.i < r.t.len() && r.t[r.i].k != K::Semi {
        return Ok(None); // (a CALL: a statement)
    }
    r.eat_semi();
    Ok(Some(Box::new(Execute { sql: None, call: Some(call), into: vars, using: vec![] })))
}

impl Step for Execute {
    fn run<'a>(&'a self, r: &'a mut Runner<'_>, _: usize, path: String) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            let job = r.job.as_ref().map(|j| if path.is_empty() { j.clone() } else { format!("{j}:{path}") });
            let out = match (&self.sql, &self.call) {
                (Some(sql), _) => {
                    let text = r.texts(std::slice::from_ref(sql)).await?.pop().flatten().context("EXECUTE IMMEDIATE NULL")?;
                    let row = r.row(&self.using.iter().enumerate().map(|(i, u)| format!("({u}) AS \"{}\"", i + 1)).collect::<Vec<_>>().join(", ")).await;
                    let mut given = HashMap::new();
                    if !self.using.is_empty() {
                        let row = row?;
                        for (i, col) in row.columns().iter().enumerate() {
                            given.insert((i + 1).to_string(), serde_json::json!({"sql": crate::vars::of_column(col.as_ref())?.sql}));
                        }
                    }
                    Box::pin(crate::routines::script(r.app, &text, &given, r.views, r.who, job)).await.with_context(|| format!("EXECUTE IMMEDIATE {}", short(&text)))?
                }
                (None, Some(call)) => {
                    let call = crate::routines::prepare(&r.app.lake, &r.ready(call).await?, &HashMap::new(), r.views).await?;
                    Box::pin(crate::routines::one(r.app, &call, r.who, job)).await?
                }
                _ => unreachable!("an EXECUTE runs SQL or a CALL"),
            };
            if !self.into.is_empty() {
                let Outcome::Rows(batches) = &out else { bail!("INTO ${}: the statement gave no rows", self.into[0]) };
                let first = batches.iter().find(|b| b.num_rows() > 0);
                for (i, name) in self.into.iter().enumerate() {
                    match first {
                        Some(b) if i < b.num_columns() => r.set(name, b.column(i).slice(0, 1).as_ref()).await?,
                        Some(b) => bail!("INTO ${name}: the answer has {} columns", b.num_columns()),
                        None => Box::pin(crate::vars::apply(r.app, crate::vars::Change::Set { name: name.clone(), value: "NULL".into() })).await.map(|_| ())?,
                    }
                }
            }
            r.last = out;
            Ok(Flow::Next)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(s: &str) -> Vec<String> { crate::routines::split(s).into_iter().map(|x| x.trim().to_string()).collect() }

    #[test]
    fn blocks_are_one_statement() {
        assert_eq!(split("SELECT 1; IF $n = 0 THEN PRINT 'a'; ELSE PRINT 'b'; END IF; SELECT 2").len(), 3);
        assert_eq!(split("BEGIN; INSERT INTO t VALUES (1); COMMIT;").len(), 3); // (a transaction)
        assert_eq!(split("BEGIN TRANSACTION; SELECT 1; COMMIT").len(), 3);
        assert_eq!(split("BEGIN SELECT 1; IF a THEN IF b THEN SELECT 2; END IF; END IF; END; SELECT 3").len(), 2);
        assert_eq!(split("DROP TABLE IF EXISTS t; SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t").len(), 2);
        assert_eq!(split("SELECT CASE WHEN a THEN if(b, 1, 2) ELSE repeat('x', 2) END; IF a THEN IF (b) THEN SELECT 1; END IF; END IF").len(), 2);
        assert_eq!(split("outer: WHILE true DO LEAVE outer; END WHILE outer; SELECT x::int FROM t").len(), 2);
        assert_eq!(split("FOR r IN (SELECT 1 AS a) DO PRINT $r.a; END FOR; SELECT repeat('a', 2)").len(), 2);
        let (done, rest) = crate::routines::statements("IF a THEN SELECT 1;");
        assert!(done.is_empty() && rest.contains("IF a")); // (the shell waits for END IF)
    }

    #[test]
    fn read() {
        assert!(is("IF $n = 0 THEN PRINT 'none'; END IF"));
        assert!(is("print 'x'") && is("RAISE NOTICE 'x %', 1") && is("CALL p() INTO $x") && is("SELECT * FROM IDENTIFIER('t')"));
        assert!(!is("BEGIN") && !is("CALL p()") && !is("EXECUTE q(1)") && !is("SELECT 1") && !is("FOR x"));
        assert!(parse("IF a THEN SELECT 1;").err().unwrap().to_string().contains("isn't closed"));
        assert!(parse("WHILE a DO SELECT 1; END IF;").err().unwrap().to_string().contains("END IF where the WHILE on line 1 ends"));
        assert_eq!(binds("FOR r IN (SELECT 1) DO $n = $n + 1; END FOR; CALL p() INTO $a, $b"), ["error", "sqlstate", "r", "n", "a", "b"]);
        assert_eq!(format("a % b %% c %", &[Some("1".into()), None]), "a 1 b % c NULL");
        assert_eq!(commas("'x %', f(a, b), $c"), ["'x %'", "f(a, b)", "$c"]);
    }
}
