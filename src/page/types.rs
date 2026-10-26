//! Enum types on a page, as a node keeps them (`types.rs`): `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE` and `RENAME TO`,
//! and `DROP TYPE`. The page holds each type's labels by its name (`Page::types`), which the front reads for a cast and
//! `enum_range` (`PageHost::enum_labels`) and a CREATE TABLE for a column of the type (`made_table`), and a kept page keeps each
//! as `types/<name>.json`. A column of one holds text, and its table's rules hold the labels, as a node's table does: ADD VALUE
//! gives the label to each table using the type, and RENAME TO renames the type in each one's CREATE text. What each statement
//! means, and the words of its refusals, are `rules::types`'s, shared with the node.
use super::*;
use crate::rules::types::{self as rules, Change};
use datafusion::sql::sqlparser::ast::{DataType as SqlType, Ident, ObjectName};

const TYPES: &str = "types";

impl Page {
    /// A type statement (`rules::types::change`), answered as a node answers it; None for any other statement.
    pub(super) async fn type_statement(&self, text: &str) -> anyhow::Result<Option<Answer>> {
        let Some(change) = rules::change(text) else { return Ok(None) };
        let change = change.map_err(|why| anyhow::anyhow!(why))?;
        let out = match change {
            Change::Create { name, labels, if_not_exists } => self.at_once_run("CREATE TYPE", &[], self.create_type(&name, labels, if_not_exists)).await?,
            Change::Add { name, label, if_not_exists, before, after } => {
                let name = self.type_held(&name)?;
                let using = self.using(&name);
                let tables: Vec<&str> = using.iter().map(|(t, _)| t.as_str()).collect();
                self.at_once_run("ALTER TYPE", &tables, self.add_label(&name, &label, before.as_deref(), after.as_deref(), if_not_exists)).await?
            }
            Change::Rename { name, to } => {
                let name = self.type_held(&name)?;
                let using = self.using(&name);
                let tables: Vec<&str> = using.iter().map(|(t, _)| t.as_str()).collect();
                self.at_once_run("ALTER TYPE", &tables, self.rename_type(&name, &to)).await?
            }
            Change::Drop { names, if_exists } => self.at_once_run("DROP TYPE", &[], self.drop_types(&names, if_exists)).await?,
        };
        if let Some(notice) = out["notice"].as_str() {
            self.note(notice.to_string());
        }
        answered(out).map(Some)
    }

    fn types(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Vec<String>>> {
        self.types.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The labels of the type `name`, in order (`PageHost::enum_labels`, and a column's type at CREATE TABLE).
    pub(super) fn type_labels(&self, name: &str) -> Option<Vec<String>> {
        self.types().get(here(name)?).cloned()
    }

    /// The page's name of the type `name`, which it holds: refused as a node refuses one it hasn't.
    fn type_held(&self, name: &str) -> anyhow::Result<String> {
        match here(name) {
            Some(local) if self.types().contains_key(local) => Ok(local.to_string()),
            _ => Err(anyhow::anyhow!(rules::missing(name))),
        }
    }

    /// The page's tables with a column of the type `name`, each with those columns.
    fn using(&self, name: &str) -> Vec<(String, Vec<String>)> {
        let all = self.ruled.lock().unwrap_or_else(PoisonError::into_inner);
        let mut using: Vec<(String, Vec<String>)> = all
            .iter()
            .map(|(table, r)| (table.clone(), r.table.columns.iter().filter(|c| c.labels.as_ref().is_some_and(|(of, _)| of.as_deref() == Some(name))).map(|c| c.name.clone()).collect::<Vec<_>>()))
            .filter(|(_, columns)| !columns.is_empty())
            .collect();
        using.sort();
        using
    }

    async fn create_type(&self, name: &str, labels: Vec<String>, if_not_exists: bool) -> anyhow::Result<serde_json::Value> {
        let local = here(name).ok_or_else(|| crate::codes::coded("3F000", format!("schema \"{}\" does not exist", crate::text::split(name).0)))?.to_string();
        if self.types().contains_key(&local) {
            anyhow::ensure!(if_not_exists, rules::exists(&local));
            return Ok(json!({"type": local, "exists": true, "notice": rules::skipping(&local)}));
        }
        crate::rules::create::labelled(&labels)?;
        self.keep_type(&local, Some(&labels)).await?;
        self.types().insert(local.clone(), labels);
        Ok(json!({"type": local}))
    }

    /// ADD VALUE: the type's labels with the new one, and each table using it with them, as a node's commit gives them.
    async fn add_label(&self, name: &str, label: &str, before: Option<&str>, after: Option<&str>, if_not_exists: bool) -> anyhow::Result<serde_json::Value> {
        let mut labels = self.type_labels(name).unwrap_or_default();
        if !rules::add_label(&mut labels, label, before, after, if_not_exists)? {
            return Ok(json!({"type": name, "unchanged": true, "notice": rules::label_skipping(label)}));
        }
        self.keep_type(name, Some(&labels)).await?;
        self.types().insert(name.to_string(), labels.clone());
        self.retyped(name, |r| {
            let mut table = (*r.table).clone();
            for c in table.columns.iter_mut() {
                if let Some((Some(of), l)) = c.labels.as_mut() {
                    if of == name {
                        *l = labels.clone();
                    }
                }
            }
            Ok(Ruled { table: Arc::new(table), ..r.clone() })
        })?;
        Ok(json!({"type": name, "added": label}))
    }

    /// RENAME TO: the type under its new name, and each table using it with the name in its rules and its CREATE text, kept
    /// again, so a kept page reads each one's columns as of the type it is now.
    async fn rename_type(&self, name: &str, to: &str) -> anyhow::Result<serde_json::Value> {
        anyhow::ensure!(!self.types().contains_key(to), rules::exists(to));
        let labels = self.type_labels(name).unwrap_or_default();
        self.keep_type(to, Some(&labels)).await?;
        self.keep_type(name, None).await?;
        {
            let mut types = self.types();
            types.remove(name);
            types.insert(to.to_string(), labels);
        }
        let renamed = self.retyped(name, |r| {
            let mut c = self.create_of(&r.create)?;
            for column in c.columns.iter_mut() {
                if matches!(&column.data_type, SqlType::Custom(n, args) if args.is_empty() && here(&crate::text::object(n)) == Some(name)) {
                    column.data_type = SqlType::Custom(ObjectName::from(vec![Ident::new(to)]), vec![]);
                }
            }
            let mut table = (*r.table).clone();
            for c in table.columns.iter_mut() {
                if let Some((of @ Some(_), _)) = c.labels.as_mut() {
                    if of.as_deref() == Some(name) {
                        *of = Some(to.to_string());
                    }
                }
            }
            Ok(Ruled { create: SqlStatement::CreateTable(c).to_string(), table: Arc::new(table), counters: r.counters.clone() })
        })?;
        for table in renamed {
            self.written(&table).await?;
        }
        Ok(json!({"type": name, "renamed": to}))
    }

    /// DROP TYPE: each type named, none while a column uses it, all or none.
    async fn drop_types(&self, names: &[String], if_exists: bool) -> anyhow::Result<serde_json::Value> {
        let mut gone = vec![];
        for n in names {
            let local = match self.type_held(n) {
                Ok(local) => local,
                Err(_) if if_exists => continue,
                Err(e) => return Err(e),
            };
            let using: Vec<String> = self.using(&local).into_iter().flat_map(|(t, columns)| columns.into_iter().map(move |c| format!("{t}.{c}"))).collect();
            rules::in_use(&local, &using)?;
            gone.push(local);
        }
        for n in &gone {
            self.keep_type(n, None).await?;
            self.types().remove(n);
        }
        Ok(json!({"type": gone.first(), "dropped": gone}))
    }

    /// Each table using the type `name` with its rules as `change` makes them: the tables changed.
    fn retyped(&self, name: &str, change: impl Fn(&Ruled) -> anyhow::Result<Ruled>) -> anyhow::Result<Vec<String>> {
        let tables: Vec<String> = self.using(name).into_iter().map(|(t, _)| t).collect();
        for table in &tables {
            if let Some(r) = self.ruled_of(table) {
                self.set_ruled(table, Some(change(&r)?));
            }
        }
        Ok(tables)
    }

    /// Keeps the type `name`'s labels in a kept page's store (`type_at`), or with None forgets it. A page in memory keeps
    /// nothing.
    async fn keep_type(&self, name: &str, labels: Option<&[String]>) -> anyhow::Result<()> {
        if !self.keeps {
            return Ok(());
        }
        let at = type_at(name);
        match labels {
            Some(labels) => {
                let file = serde_json::to_vec(&json!({"name": name, "labels": labels}))?;
                self.files.put_opts(&at, PutPayload::from(file), PutOptions { mode: PutMode::Overwrite, ..Default::default() }).await?;
            }
            None => match self.files.delete(&at).await {
                Ok(()) | Err(StoreError::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            },
        }
        Ok(())
    }

    /// The kept types read back into the page, before its tables: a table's rules read its columns' types. A file that isn't
    /// a kept type is listed by its path and the words of why (`unread`).
    pub(super) async fn types_back(&mut self) -> anyhow::Result<()> {
        let listed = self.files.list(Some(&Path::from(TYPES))).try_collect::<Vec<_>>().await?;
        for object in listed.iter().filter(|o| o.location.as_ref().ends_with(".json")) {
            if let Err(e) = self.type_back(&object.location).await {
                self.unread.push((object.location.to_string(), crate::codes::words(&e)));
            }
        }
        Ok(())
    }

    async fn type_back(&self, at: &Path) -> anyhow::Result<()> {
        let file = self.files.get(at).await?.bytes().await?;
        let kept: serde_json::Value = serde_json::from_slice(&file).context("it is not a kept type")?;
        let (Some(name), Some(labels)) = (kept["name"].as_str(), kept["labels"].as_array()) else { anyhow::bail!("it has no name and labels, so it is not a kept type") };
        let labels: Vec<String> = labels.iter().map(|l| l.as_str().map(str::to_string)).collect::<Option<_>>().context("its labels are not text")?;
        self.types().insert(name.to_string(), labels);
        Ok(())
    }
}

/// A type's name in the page's one schema (`public`, in the catalog `page`): None for another schema's.
fn here(name: &str) -> Option<&str> {
    let local = name.strip_prefix("page.").unwrap_or(name);
    match local.split_once('.') {
        None => Some(local),
        Some((crate::text::PUBLIC, rest)) if !rest.contains('.') => Some(rest),
        _ => None,
    }
}

/// Where a kept page keeps the type `name`: its name and labels, as JSON.
fn type_at(name: &str) -> Path {
    Path::from_iter([TYPES, format!("{name}.json").as_str()])
}
