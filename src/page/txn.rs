//! A page's transactions, as a node's (`txn.rs`, ADR-067): BEGIN … COMMIT or ROLLBACK, and savepoints within one. A page
//! runs one statement at a time, so a transaction takes no locks and no snapshot of its own: its writes change the page's
//! tables as a statement's do, and it holds how each table it wrote was before its first write (`found`), which a ROLLBACK
//! puts back (`Page::undo`) and which a COMMIT lets go once it has kept those tables (`Page::written`). A savepoint holds
//! the tables the transaction had written as they were when it was taken. DDL runs at once, as on a node (`at_once`). The
//! statements' modes and the words and codes of the refusals are `rules::txn`'s, shared with the node.
use super::*;
use crate::rules::txn::{self as rules, Level};

/// The tables a transaction wrote, each as it was before (its rows and its rules), for a ROLLBACK to put back.
type Found = BTreeMap<String, (Before, Option<Ruled>)>;

/// A page's open transaction.
pub(super) struct Txn {
    level: Level,
    read_only: bool,
    /// The words of the statement that failed it: the ones after it are refused (25P02) until it ends.
    failed: Option<String>,
    found: Found,
    /// Its savepoints, oldest first, each with the tables it had written as they were then.
    marks: Vec<(String, Found)>,
}

impl Txn {
    /// The newest savepoint called `name`; none fails the transaction (3B001), as in Postgres.
    fn mark(&mut self, name: &str) -> anyhow::Result<usize> {
        rules::mark_of(self.marks.iter().map(|(n, _)| n.as_str()), name).map_err(|why| {
            self.failed.get_or_insert_with(|| why.clone());
            rules::no_savepoint(&why)
        })
    }

    /// The refusal of a failed transaction (25P02), for each statement but its end.
    fn going(&self) -> anyhow::Result<()> {
        self.failed.as_deref().map_or(Ok(()), |why| Err(rules::aborted(why)))
    }
}

impl Page {
    fn txn(&self) -> std::sync::MutexGuard<'_, Option<Txn>> {
        self.txn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Says `note` with the answer of the statement that runs now (`x-pondra-notices`, as a node's HTTP says it).
    pub(super) fn note(&self, note: String) {
        self.notes.lock().unwrap_or_else(PoisonError::into_inner).push(note);
    }

    /// The notes said while the statement ran (`note`), taken.
    pub(super) fn notes(&self) -> Vec<String> {
        std::mem::take(&mut *self.notes.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// A transaction word (`text::control`): BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE or ROLLBACK TO, answered as a node's
    /// HTTP answers it (`{"transaction": "begin"}`), a failed transaction's COMMIT being its ROLLBACK.
    pub(super) async fn control(&self, word: &str, sql: &str) -> anyhow::Result<Answer> {
        let tag = match word {
            "BEGIN" => self.begin(sql)?,
            "COMMIT" => self.commit().await?,
            "ROLLBACK" => self.rollback().await?,
            "SAVEPOINT" => self.savepoint(sql).await?,
            "RELEASE" => self.release(sql)?,
            _ => self.rollback_to(sql).await?,
        };
        answered(json!({"transaction": tag}))
    }

    /// `BEGIN [modes]`: a transaction at the level and access mode its modes give (READ COMMITTED and READ WRITE when they
    /// give none). A BEGIN in a transaction changes nothing, and says so.
    fn begin(&self, sql: &str) -> anyhow::Result<&'static str> {
        let (level, read_only) = rules::plan(&rules::begin_modes(sql)?)?;
        let mut txn = self.txn();
        if txn.is_some() {
            drop(txn);
            self.note(format!("WARNING: {}", rules::ALREADY));
            return Ok("begin");
        }
        *txn = Some(Txn { level: level.unwrap_or(Level::Committed), read_only: read_only.unwrap_or(false), failed: None, found: Found::new(), marks: vec![] });
        Ok("begin")
    }

    /// `COMMIT`: each table the transaction wrote is kept, as a statement's write keeps its table. A store that refuses one
    /// (a full store) rolls the transaction back: its tables are put back, and kept again as they are.
    async fn commit(&self) -> anyhow::Result<&'static str> {
        let Some(txn) = self.txn().take() else {
            self.note(format!("WARNING: {}", rules::NONE_OPEN));
            return Ok("commit");
        };
        if txn.failed.is_some() {
            self.put_back(txn.found).await?;
            return Ok("rollback");
        }
        let names: Vec<String> = txn.found.keys().cloned().collect();
        for name in &names {
            if let Err(e) = self.written(name).await {
                self.put_back(txn.found).await?;
                for name in &names {
                    self.written(name).await.ok(); // (the files the commit kept go back to the tables as they are again, as far as the store takes them)
                }
                return Err(e.context(format!("{name}: the transaction was rolled back: the page couldn't keep it")));
            }
        }
        Ok("commit")
    }

    /// `ROLLBACK`: each table the transaction wrote is put back as it found it. (Its identity columns' numbers stay taken,
    /// as a sequence's do in Postgres.)
    async fn rollback(&self) -> anyhow::Result<&'static str> {
        match self.txn().take() {
            Some(txn) => self.put_back(txn.found).await?,
            None => self.note(format!("WARNING: {}", rules::NONE_OPEN)),
        }
        Ok("rollback")
    }

    /// The tables in `found` put back as they were, with their rules (`undo`).
    async fn put_back(&self, found: Found) -> anyhow::Result<()> {
        for (name, (before, had)) in found {
            self.undo(&name, before, had).await?;
        }
        Ok(())
    }

    /// `SAVEPOINT name`: the tables the transaction has written, as they are now, for `ROLLBACK TO` to put back.
    async fn savepoint(&self, sql: &str) -> anyhow::Result<&'static str> {
        let name = rules::savepoint_name(sql)?;
        let written: Vec<String> = {
            let txn = self.txn();
            let t = txn.as_ref().ok_or_else(|| rules::outside("SAVEPOINT"))?;
            t.going()?;
            t.found.keys().cloned().collect()
        };
        let mut now = Found::new();
        for table in written {
            now.insert(table.clone(), (self.before(&table).await?, self.ruled_of(&table)));
        }
        if let Some(t) = self.txn().as_mut() {
            t.marks.push((name, now));
        }
        Ok("savepoint")
    }

    /// `RELEASE [SAVEPOINT] name`: forgets the savepoint and those taken after it. Their writes stay.
    fn release(&self, sql: &str) -> anyhow::Result<&'static str> {
        let name = rules::savepoint_name(sql)?;
        let mut txn = self.txn();
        let t = txn.as_mut().ok_or_else(|| rules::outside("RELEASE SAVEPOINT"))?;
        t.going()?;
        let i = t.mark(&name)?;
        t.marks.truncate(i);
        Ok("release")
    }

    /// `ROLLBACK [WORK] TO [SAVEPOINT] name`: the tables go back to the savepoint, which stays, and a failed transaction goes
    /// on. A table first written after the savepoint goes back to how the transaction found it, and is no longer its.
    async fn rollback_to(&self, sql: &str) -> anyhow::Result<&'static str> {
        let name = rules::savepoint_name(sql)?;
        let (at, since) = {
            let mut txn = self.txn();
            let t = txn.as_mut().ok_or_else(|| rules::outside("ROLLBACK TO SAVEPOINT"))?;
            let i = t.mark(&name)?;
            t.marks.truncate(i + 1);
            t.failed = None;
            let at = t.marks[i].1.clone();
            let (since, kept): (Found, Found) = std::mem::take(&mut t.found).into_iter().partition(|(n, _)| !at.contains_key(n));
            t.found = kept;
            (at, since)
        };
        self.put_back(since).await?;
        self.put_back(at).await?;
        Ok("rollback")
    }

    /// The checks each statement meets first in an open transaction, as a node's (`txn::gate`): a failed transaction refuses
    /// it (25P02), and a READ ONLY one refuses a write (25006), named as Postgres names it.
    pub(super) fn gate(&self, sql: &str) -> anyhow::Result<()> {
        let txn = self.txn();
        let Some(t) = txn.as_ref() else { return Ok(()) };
        t.going()?;
        match t.read_only.then(|| rules::write_word(sql)).flatten() {
            Some(word) => Err(rules::read_only(&word)),
            None => Ok(()),
        }
    }

    /// A statement failed in an open transaction: the transaction is failed now, with its words (25P02 for those after it).
    pub(super) fn failed(&self, e: &anyhow::Error) {
        if let Some(t) = self.txn().as_mut() {
            t.failed.get_or_insert_with(|| crate::codes::told(e, true));
        }
    }

    /// A write to `name` in an open transaction, which takes the table as it was before (`before`, `had`) the first time:
    /// true when one is open, so the table is kept at its COMMIT and not now.
    pub(super) fn in_txn(&self, name: &str, before: &Before, had: &Option<Ruled>) -> bool {
        let mut txn = self.txn();
        let Some(t) = txn.as_mut() else { return false };
        t.found.entry(name.to_string()).or_insert_with(|| (before.clone(), had.clone()));
        true
    }

    /// DDL in an open transaction, as a node's (`txn::ddl_at_once`, ADR-067 §8): it runs at once and is kept as it would be
    /// outside one, so ROLLBACK won't undo it, and the notice it gives once it has run says so. Refused under REPEATABLE
    /// READ (its snapshot can't see what it makes) and on a table the transaction has written (`tables`). `word` names it as
    /// Postgres does. None outside a transaction.
    pub(super) fn at_once(&self, word: &str, tables: &[&str]) -> anyhow::Result<Option<String>> {
        let txn = self.txn();
        let Some(t) = txn.as_ref() else { return Ok(None) };
        if t.level == Level::Repeatable {
            return Err(rules::ddl_repeatable());
        }
        if let Some(name) = tables.iter().find(|n| t.found.contains_key(**n)) {
            return Err(rules::ddl_pending(name));
        }
        Ok(Some(rules::ran_at_once(word)))
    }

    /// `ddl`, DDL on `tables`, run as `at_once` says: refused, or noted once it has run, in an open transaction.
    pub(super) async fn at_once_run<T>(&self, word: &str, tables: &[&str], ddl: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
        let note = self.at_once(word, tables)?;
        let out = ddl.await?;
        if let Some(note) = note {
            self.note(note);
        }
        Ok(out)
    }
}
