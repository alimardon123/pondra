//! A statement's words, as a statement's reader takes them one at a time: the tokens `text::tokens` makes (one
//! tokenization per statement), without spaces and comments, so quoting is SQL's and never a pattern's. The readers of
//! Pondra's own statements (users, shares, identity providers) read with these, and step 8's PR 8 moves every reader
//! of a statement family onto them (l6/step8-pr8-plan.md).
use anyhow::{bail, Result};

/// A statement's tokens, last first (`next` takes the one after those taken), and where each is in the text.
#[derive(Default)]
pub struct Words(pub(crate) Vec<W>, Spans);

/// Where each of a statement's words starts and ends in its text, in order: which are written together (`joined`).
#[derive(Default)]
pub(crate) struct Spans {
    starts: Vec<usize>,
    ends: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) enum W {
    Word(String, bool), // (its text as SQL resolves it, and whether it was quoted)
    Str(String),
    Num(String),
    Sym(char),
}

impl W {
    pub(crate) fn word(&self) -> String {
        match self {
            W::Word(w, false) => w.clone(),
            _ => String::new(),
        }
    }
}

impl Words {
    pub(crate) fn of(sql: &str) -> Option<Words> {
        use datafusion::sql::sqlparser::tokenizer::Token;
        let tokens = crate::text::tokens(sql)?;
        let (mut out, mut starts, mut ends) = (vec![], vec![], vec![]);
        for crate::text::Solid { token: t, start, end } in tokens.solid() {
            out.push(match t {
                Token::Word(w) if w.quote_style.is_some() => W::Word(w.value.clone(), true),
                Token::Word(w) => W::Word(w.value.to_lowercase(), false),
                Token::SingleQuotedString(s) => W::Str(s.clone()),
                Token::Number(n, _) => W::Num(n.clone()),
                Token::Comma => W::Sym(','),
                Token::LParen => W::Sym('('),
                Token::RParen => W::Sym(')'),
                Token::Period => W::Sym('.'),
                Token::Eq => W::Sym('='),
                Token::RArrow => W::Sym('⇒'), // (`=>`)
                Token::Minus => W::Sym('-'),
                Token::SemiColon => continue,
                _ => W::Sym('?'),
            });
            starts.push(start);
            ends.push(end);
        }
        out.reverse(); // (taken from the end: the next is the last)
        Some(Words(out, Spans { starts, ends }))
    }
    /// Whether the word `k` after the next is written right after the one before it, with no space or comment between.
    fn tight(&self, k: usize) -> bool {
        let Spans { starts, ends } = &self.1;
        let i = starts.len() - self.0.len() + k; // (its place among all the statement's words)
        i > 0 && i < starts.len() && starts[i] == ends[i - 1]
    }
    /// A name that may be written as one word with `-` in it, as a lake's name is its folder's (`pondra-ab12`): the next
    /// part (`part`), and the words, numbers and `-` written right after it with no space or comment between. A quoted
    /// part is the whole name, and a `-` ends none.
    pub(crate) fn joined(&mut self) -> Result<String> {
        let quoted = matches!(self.0.last(), Some(W::Word(_, true)));
        let mut name = self.part()?;
        let piece = |w: Option<&W>| match w {
            Some(W::Word(w, false)) => Some(w.clone()),
            Some(W::Num(n)) if n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => Some(n.clone()),
            _ => None,
        };
        while !quoted && self.tight(0) {
            let n = self.0.len();
            let dash = matches!(self.0.last(), Some(W::Sym('-'))) && self.tight(1);
            if let Some(p) = piece(self.0.last()) {
                name.push_str(&p); // (a word after a number, as the tokenizer splits `1a2b`)
                self.0.pop();
            } else if let (true, Some(p)) = (dash, n.checked_sub(2).and_then(|k| piece(self.0.get(k)))) {
                name.push('-');
                name.push_str(&p);
                self.0.truncate(n - 2);
            } else {
                break;
            }
        }
        Ok(name)
    }
    pub(crate) fn peek_word(&self) -> Option<String> { self.0.last().map(|w| w.word()) }
    pub(crate) fn next(&mut self) -> Option<W> { self.0.pop() }
    /// The next word, if it is `w` (taken).
    pub(crate) fn is(&mut self, w: &str) -> bool {
        let yes = self.peek_word().as_deref() == Some(w);
        if yes {
            self.0.pop();
        }
        yes
    }
    pub(crate) fn expect(&mut self, w: &str) -> Result<()> { if self.is(w) { Ok(()) } else { bail!("expected {} {}", w.to_uppercase(), self.near()) } }
    /// These words, in order (`["not", "exists"]`).
    pub(crate) fn expect_all(&mut self, words: &[&str]) -> Result<()> { words.iter().try_for_each(|w| self.expect(w)) }
    pub(crate) fn expect_sym(&mut self, c: char) -> Result<()> { if self.sym(c) { Ok(()) } else { bail!("expected {c} {}", self.near()) } }
    pub(crate) fn sym(&mut self, c: char) -> bool {
        let yes = matches!(self.0.last(), Some(W::Sym(s)) if *s == c);
        if yes {
            self.0.pop();
        }
        yes
    }
    pub(crate) fn near(&self) -> String {
        match self.0.last() {
            None => "at the end".into(),
            Some(W::Word(w, _)) | Some(W::Str(w)) | Some(W::Num(w)) => format!("near {w}"),
            Some(W::Sym(c)) => format!("near {c}"),
        }
    }
    /// One part of a name: `a`, `"A"`.
    pub(crate) fn part(&mut self) -> Result<String> {
        if let Some(W::Word(..)) = self.0.last() {
            if let Some(W::Word(w, _)) = self.0.pop() {
                return Ok(w);
            }
        }
        bail!("expected a name {}", self.near())
    }
    /// A name: `a`, `"A"`, or dotted (`s.t`, `lake.s.t`).
    pub(crate) fn name(&mut self) -> Result<String> {
        let mut parts = vec![];
        loop {
            match self.next() {
                Some(W::Word(w, _)) => parts.push(w),
                _ => bail!("expected a name {}", self.near()),
            }
            if !self.sym('.') {
                return Ok(parts.join("."));
            }
        }
    }
    pub(crate) fn names(&mut self) -> Result<Vec<String>> {
        let mut all = vec![self.name()?];
        while self.sym(',') {
            all.push(self.name()?);
        }
        Ok(all)
    }
    pub(crate) fn string(&mut self) -> Result<String> {
        match self.next() {
            Some(W::Str(s)) => Ok(s),
            _ => bail!("expected a quoted string {}", self.near()),
        }
    }
    /// Whether the words still to take begin with `pattern`'s, a statement entry's upper-cased words (`statements`):
    /// each is an unquoted word, in any case, and `*` a name (`a`, `"A"`, `s.t`). Takes none.
    pub(crate) fn begins(&self, pattern: &[&str]) -> bool {
        let mut at = self.0.iter().rev().peekable();
        for p in pattern {
            if *p == "*" {
                loop {
                    if !matches!(at.next(), Some(W::Word(..))) {
                        return false;
                    }
                    if !matches!(at.peek(), Some(W::Sym('.'))) {
                        break;
                    }
                    at.next();
                }
            } else if !matches!(at.next(), Some(W::Word(w, false)) if w.eq_ignore_ascii_case(p)) {
                return false;
            }
        }
        true
    }
    pub(crate) fn done(&self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { bail!("unexpected words {}", self.near()) } }
}
