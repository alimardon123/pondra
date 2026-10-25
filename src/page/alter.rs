//! ALTER TABLE, ALTER VIEW … RENAME TO and TRUNCATE on a page, as a node answers them (`ddl::alter_column`, `ddl::rename`,
//! `write::parse`). An ALTER TABLE's actions change the table in memory, in order, and the table is kept once they have all
//! run, so a refused action changes nothing. A TRUNCATE is its table's DELETE (`truncated`): its rows, rules, kept file and
//! answer are the page's DELETE's. The words and codes of the refusals are `rules::alter`'s, shared with the node.
use super::*;
use crate::rules::alter::{beside_type, fills_existing, has_column, last_column, names_column, no_column, not_alone, owes_rows, rename_column_in, stays, used_by, view_rename};
use datafusion::arrow::array::{new_null_array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan};
use datafusion::sql::sqlparser::ast::{AlterTable, AlterTableOperation, ColumnDef, ColumnOption, CreateTable, Ident, ObjectName, RenameTableNameKind, Statement as SqlStatement, TableConstraint};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

/// The name a new column's type is planned under: a CREATE TABLE planned alone to read the type a page gives the column, and
/// never run, so no table of the page has the name.
const COLUMN_TYPE: &str = "the new column's type";

/// A page table as an ALTER TABLE changes it, in memory: its schema, its rows partition by partition, its column defaults and
/// its rules. Each action changes it in turn; `commit_altered` keeps the result once every action has run.
struct Altering {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    defaults: HashMap<String, Expr>,
    ruled: Option<Ruled>,
}

impl Altering {
    /// The table `name` as it is now, with its rules `ruled`.
    async fn of(name: &str, table: &Arc<dyn TableProvider>, ruled: Option<Ruled>) -> anyhow::Result<Altering> {
        let partitions = partitions_of(table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        let schema = table.schema();
        let defaults = schema.fields().iter().filter_map(|f| table.get_column_default(f.name()).map(|e| (f.name().clone(), e.clone()))).collect();
        Ok(Altering { schema, partitions, defaults, ruled })
    }

    /// The position of the column `name`, if the table has one.
    fn index(&self, name: &str) -> Option<usize> {
        self.schema.fields().iter().position(|f| f.name() == name)
    }

    /// The rows over `schema`: each batch's columns as `columns` gives them, partition by partition.
    fn reshape(&mut self, schema: SchemaRef, columns: impl Fn(&RecordBatch) -> Vec<ArrayRef>) -> anyhow::Result<()> {
        let mut partitions = vec![];
        for part in &self.partitions {
            let mut batches = vec![];
            for batch in part {
                batches.push(RecordBatch::try_new(schema.clone(), columns(batch))?);
            }
            partitions.push(batches);
        }
        self.partitions = partitions;
        self.schema = schema;
        Ok(())
    }
}

/// `fields` as a schema with the notes `schema` has.
fn with_fields(schema: &Schema, fields: Vec<FieldRef>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// A table or view name as a statement's ObjectName, for a plain name (quoted, so it is the name as it is).
fn ident_name(name: &str) -> ObjectName {
    ObjectName::from(vec![Ident::with_quote('"', name)])
}

/// The name a text (`"Big V"`, `public.w`) reads as, as a statement's name does.
fn object_of(text: &str) -> anyhow::Result<ObjectName> {
    Ok(Parser::new(&GenericDialect {}).try_with_sql(text)?.parse_object_name(false)?)
}

/// The refusal of a table or view that isn't there (42P01).
fn no_table(name: &str) -> anyhow::Error {
    crate::codes::coded("42P01", format!("no table {name}"))
}

/// Takes a column `column` out of a CREATE TABLE's text: its definition, the table constraints that name it (a UNIQUE, a CHECK,
/// a key that isn't enforced, a foreign key), and the CHECKs of other columns that name it, as Postgres drops them.
fn drop_from(c: &mut CreateTable, column: &str) {
    c.columns.retain(|col| crate::text::ident(&col.name) != column);
    c.constraints.retain(|k: &TableConstraint| !names_column(k, column));
    for col in &mut c.columns {
        col.options.retain(|o| !(matches!(o.option, ColumnOption::Check(_)) && names_column(&o.option, column)));
    }
}

impl Page {
    /// The page's ALTER TABLE, and ALTER VIEW … RENAME TO, answered here before anything is planned, as a node answers them.
    /// None for any other statement (and for a text that doesn't parse, which the planner answers).
    pub(super) async fn alter_statement(&self, text: &str) -> anyhow::Result<Option<Answer>> {
        if crate::text::first_words(text, 1).first().map(String::as_str) != Some("ALTER") {
            return Ok(None);
        }
        if let Some((from, to)) = view_rename(crate::text::first_word(text)) {
            return self.rename_to(&object_of(&from)?, &object_of(&to)?).await.map(Some);
        }
        let Some(SqlStatement::AlterTable(a)) = self.statement_of(text) else { return Ok(None) };
        if let [AlterTableOperation::RenameTable { table_name }] = &a.operations[..] {
            let (RenameTableNameKind::To(to) | RenameTableNameKind::As(to)) = table_name;
            return self.rename_to(&a.name, to).await.map(Some);
        }
        self.alter_table(&a).await.map(Some)
    }

    /// The DELETE a TRUNCATE is: `TRUNCATE [TABLE] t` of one of the page's tables, as `sql` plans and runs a DELETE. None for any
    /// other statement. A name that isn't a page table is 42P01, and one past a table is refused as a node refuses it.
    pub(super) fn truncated(&self, text: &str) -> anyhow::Result<Option<String>> {
        if crate::text::first_words(text, 1).first().map(String::as_str) != Some("TRUNCATE") {
            return Ok(None);
        }
        let Some(SqlStatement::Truncate(t)) = self.statement_of(text) else { return Ok(None) };
        let [target] = &t.table_names[..] else { return Err(crate::codes::coded("0A000", "TRUNCATE one table at a time")) };
        let name = self.table_named(&target.name)?;
        if self.views_held().contains_key(&name) {
            anyhow::bail!("{name} is a view: write to the tables it reads");
        }
        if !self.default_schema()?.1.table_exist(&name) {
            return Err(no_table(&name));
        }
        Ok(Some(format!("DELETE FROM {}", target.name)))
    }

    /// A name a statement gives a page table, as the page's one schema reads it: its table, or 42P01 for a name in another schema
    /// (a table of another schema is no page table).
    fn table_named(&self, name: &ObjectName) -> anyhow::Result<String> {
        let reference = self.table_ref(name)?;
        let state = self.ctx.state();
        let options = state.config().options();
        let here = reference.schema().is_none_or(|s| s == options.catalog.default_schema) && reference.catalog().is_none_or(|c| c == options.catalog.default_catalog);
        if !here {
            return Err(no_table(&reference.to_string()));
        }
        Ok(reference.table().to_string())
    }

    /// A CREATE TABLE's text, parsed as the page's session parses SQL.
    fn create_of(&self, text: &str) -> anyhow::Result<CreateTable> {
        let DfStatement::Statement(s) = self.parsed(text)? else { anyhow::bail!("{text} is not a CREATE TABLE") };
        let SqlStatement::CreateTable(c) = *s else { anyhow::bail!("{text} is not a CREATE TABLE") };
        Ok(c)
    }

    /// The rules a CREATE TABLE `c` makes over `schema` (`made_table`), with its text kept: None when the table has nothing a
    /// write could break (`has_rules`), as `ruled_made` gives none. The counters are the table's own (`carried`): an ALTER
    /// changes a table's columns, not its sequences, so its identity columns go on counting where they were.
    fn ruled_with(&self, c: &CreateTable, schema: &SchemaRef, carried: Option<&Ruled>) -> anyhow::Result<Option<Ruled>> {
        let table = self.made_table(c, schema)?;
        let counters = carried.map(|r| r.counters.clone()).unwrap_or_default();
        Ok(has_rules(&table).then(|| Ruled { create: SqlStatement::CreateTable(c.clone()).to_string(), table: Arc::new(table), counters }))
    }

    /// An ALTER TABLE of one page table: its actions in order, on the table as it is (`Altering`), and kept once if any of them
    /// changed it. The answer is the last action's, as a node answers a list of actions.
    async fn alter_table(&self, a: &AlterTable) -> anyhow::Result<Answer> {
        let name = self.table_named(&a.name)?;
        let (_, page) = self.default_schema()?;
        let Some(table) = page.table(&name).await? else { return Err(no_table(&name)) };
        let mut altering = Altering::of(&name, &table, self.ruled_of(&name)).await?;
        let mut changed = false;
        let mut said = serde_json::json!({});
        for action in &a.operations {
            let (did, answer) = self.alter_action(&name, &mut altering, action).await?;
            changed |= did;
            said = answer;
        }
        if changed {
            self.commit_altered(&name, altering).await?;
        }
        answered(said)
    }

    /// One action of an ALTER TABLE on `name`: whether it changed the table, and its answer.
    async fn alter_action(&self, name: &str, altering: &mut Altering, action: &AlterTableOperation) -> anyhow::Result<(bool, serde_json::Value)> {
        match action {
            AlterTableOperation::AddColumn { if_not_exists, column_def, column_position, .. } => {
                if column_position.is_some() {
                    return Err(crate::codes::coded("0A000", format!("a page adds a column at the end of {name}: ADD COLUMN takes no FIRST or AFTER")));
                }
                let column = crate::text::ident(&column_def.name);
                // (an identity column's counter starts with its table, and a page makes one with the table, not in an ADD COLUMN)
                if matches!(crate::rules::create::identity_of(column_def), Ok(Some(_))) {
                    return Err(crate::codes::coded("0A000", "a page adds no identity column yet: make the table with it"));
                }
                if fills_existing(column_def) {
                    anyhow::bail!(owes_rows(name, &column));
                }
                if let Some(what) = beside_type(column_def) {
                    anyhow::bail!(not_alone(name, &column, &what));
                }
                if altering.index(&column).is_some() {
                    if *if_not_exists {
                        return Ok((false, serde_json::json!({"table": name, "unchanged": true})));
                    }
                    anyhow::bail!(has_column(name, &column));
                }
                self.add_column(name, altering, column_def).await?;
                Ok((true, serde_json::json!({"table": name})))
            }
            AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                let mut changed = false;
                let mut said = serde_json::json!({});
                for column in column_names {
                    let (did, answer) = self.drop_column(name, altering, &crate::text::ident(column), *if_exists).await?;
                    changed |= did;
                    said = answer;
                }
                Ok((changed, said))
            }
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                self.rename_column(name, altering, &crate::text::ident(old_column_name), &crate::text::ident(new_column_name)).await
            }
            AlterTableOperation::RenameTable { .. } => Err(crate::codes::coded("0A000", format!("a page takes RENAME TO only as an ALTER TABLE of its own: ALTER TABLE {name} RENAME TO …"))),
            other => Err(crate::codes::coded("0A000", format!("a page doesn't take {other} yet (ALTER TABLE {name}): it takes ADD COLUMN, DROP COLUMN and RENAME COLUMN, and RENAME TO alone; keep the table's other changes on a node"))),
        }
    }

    /// ADD COLUMN: the column's type is the one a page's CREATE TABLE gives it (planned alone, never run), and the rows there get
    /// NULL. Its rules are the table's, with the column's definition added (`ruled_with`); a table with no rules gets the column's
    /// alone, which makes rules only when the column holds a value (VARCHAR(n), uuid, json).
    async fn add_column(&self, name: &str, altering: &mut Altering, column: &ColumnDef) -> anyhow::Result<()> {
        let text = format!("CREATE TABLE {} ({column})", crate::ext_names::quoted(COLUMN_TYPE));
        let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(made)) = self.planned(&text).await?.plan else {
            anyhow::bail!("{column} is not planned as a column");
        };
        let field = made.input.schema().as_arrow().field(0).clone();
        let ty: DataType = field.data_type().clone();
        let mut fields: Vec<FieldRef> = altering.schema.fields().iter().cloned().collect();
        fields.push(Arc::new(field));
        let schema = with_fields(&altering.schema, fields);
        altering.reshape(schema.clone(), |batch| {
            let mut columns = batch.columns().to_vec();
            columns.push(new_null_array(&ty, batch.num_rows()));
            columns
        })?;
        let ruled = match &altering.ruled {
            Some(r) => {
                let mut c = self.create_of(&r.create)?;
                c.columns.push(column.clone());
                self.ruled_with(&c, &schema, Some(r))?
            }
            None => {
                let mut c = self.create_of(&text)?;
                c.name = ident_name(name);
                self.ruled_with(&c, &schema, None)?
            }
        };
        altering.ruled = ruled;
        Ok(())
    }

    /// DROP COLUMN of `column`: refused while a view names the table's columns (`used_by`), or the column is the key (`stays`), or
    /// the table's last column. Its rows and default go with it, and its rules as `drop_from` takes them.
    async fn drop_column(&self, name: &str, altering: &mut Altering, column: &str, if_exists: bool) -> anyhow::Result<(bool, serde_json::Value)> {
        let views = self.readers(name)?;
        if !views.is_empty() {
            return Err(used_by(name, &views.iter().map(|v| format!("view {v}")).collect::<Vec<_>>(), true));
        }
        let Some(at) = altering.index(column) else {
            anyhow::ensure!(if_exists, no_column(name, column));
            return Ok((false, serde_json::json!({"table": name, "unchanged": true})));
        };
        if altering.ruled.as_ref().is_some_and(|r| r.table.key.iter().any(|k| k == column)) {
            anyhow::bail!(stays(name, column, "key"));
        }
        anyhow::ensure!(altering.schema.fields().len() > 1, last_column(name, column));
        let keep: Vec<usize> = (0..altering.schema.fields().len()).filter(|&i| i != at).collect();
        let fields: Vec<FieldRef> = keep.iter().map(|&i| altering.schema.fields()[i].clone()).collect();
        let schema = with_fields(&altering.schema, fields);
        altering.reshape(schema.clone(), |batch| keep.iter().map(|&i| batch.column(i).clone()).collect())?;
        altering.defaults.remove(column);
        altering.ruled = match &altering.ruled {
            Some(r) => {
                let mut c = self.create_of(&r.create)?;
                drop_from(&mut c, column);
                self.ruled_with(&c, &schema, Some(r))?
            }
            None => None,
        };
        Ok((true, serde_json::json!({"table": name, "altered": {"dropped": column}})))
    }

    /// RENAME COLUMN `from` TO `to`: refused while a view names the table's columns (`used_by`), or `from` is missing, or `to` is
    /// taken. The rows keep their values under the new name, and the rules follow it: the column's definition, and every
    /// expression that names it (`rename_column_in`), so a CHECK, a key and a UNIQUE read the column as it is now.
    async fn rename_column(&self, name: &str, altering: &mut Altering, from: &str, to: &str) -> anyhow::Result<(bool, serde_json::Value)> {
        let views = self.readers(name)?;
        if !views.is_empty() {
            return Err(used_by(name, &views.iter().map(|v| format!("view {v}")).collect::<Vec<_>>(), false));
        }
        let Some(at) = altering.index(from) else { anyhow::bail!(no_column(name, from)) };
        anyhow::ensure!(altering.index(to).is_none(), has_column(name, to));
        let mut fields: Vec<FieldRef> = altering.schema.fields().iter().cloned().collect();
        fields[at] = Arc::new(fields[at].as_ref().clone().with_name(to));
        let schema = with_fields(&altering.schema, fields);
        altering.reshape(schema.clone(), |batch| batch.columns().to_vec())?;
        if let Some(default) = altering.defaults.remove(from) {
            altering.defaults.insert(to.to_string(), default);
        }
        altering.ruled = match &altering.ruled {
            Some(r) => {
                let mut c = self.create_of(&r.create)?;
                for col in &mut c.columns {
                    if crate::text::ident(&col.name) == from {
                        col.name = Ident::with_quote('"', to);
                    }
                }
                rename_column_in(&mut c, from, to);
                self.ruled_with(&c, &schema, Some(r))?
            }
            None => None,
        };
        Ok((true, serde_json::json!({"table": name, "altered": {"renamed": from, "to": to}})))
    }

    /// RENAME TO: a view of the name is renamed with its query (its kept file moves, `keep_view`), and a table with its rows and
    /// rules, its kept file moving with it. A name a table or a view has is refused (42P07). Views that read the table by its old
    /// name fail when read, as on a node.
    async fn rename_to(&self, from: &ObjectName, to: &ObjectName) -> anyhow::Result<Answer> {
        let name = self.table_named(from)?;
        let to = self.table_named(to)?;
        let (_, page) = self.default_schema()?;
        anyhow::ensure!(!page.table_exist(&to) && !self.views_held().contains_key(&to), "{to} exists already");
        if let Some(query) = self.views_held().get(&name).cloned() {
            self.unread_there(&name, &view_at(&name), "view").await?;
            self.unread_there(&to, &view_at(&to), "view").await?;
            self.keep_view(&to, Some(&query)).await?;
            if let Err(e) = self.keep_view(&name, None).await {
                let _ = self.keep_view(&to, None).await;
                return Err(e);
            }
            {
                let mut views = self.views.lock().unwrap_or_else(PoisonError::into_inner);
                views.remove(&name);
                views.insert(to.clone(), query);
            }
            return answered(serde_json::json!({"view": name, "renamed": to}));
        }
        let Some(table) = page.table(&name).await? else { return Err(crate::codes::coded("42P01", format!("no table or view {name}"))) };
        self.unwritable(&name).await?;
        self.unread_there(&to, &kept_at(&to), "table").await?;
        let before = self.before(&name).await?;
        let had = self.ruled_of(&name);
        let moved = match &had {
            Some(r) => {
                let mut c = self.create_of(&r.create)?;
                c.name = ident_name(&to);
                Some(Ruled { create: SqlStatement::CreateTable(c).to_string(), table: r.table.clone(), counters: r.counters.clone() })
            }
            None => None,
        };
        page.deregister_table(&name)?;
        register_page_table(page.as_ref(), to.clone(), inner(&table).clone())?;
        self.set_ruled(&name, None);
        self.set_ruled(&to, moved);
        let kept = match self.written(&to).await {
            Ok(()) if self.keeps => self.forget(&name).await,
            other => other,
        };
        if let Err(e) = kept {
            page.deregister_table(&to)?;
            self.set_ruled(&to, None);
            let _ = self.forget(&to).await;
            return Err(self.undone(&name, before, had, e, ": the page couldn't keep it").await);
        }
        answered(serde_json::json!({"table": name, "renamed": to}))
    }

    /// Keeps an ALTER TABLE's table once, as `written` keeps a write: the table is registered under its name with the altered
    /// schema, rows and defaults, and its rules set, and a write the store refuses is undone to what the table was (`undone`).
    async fn commit_altered(&self, name: &str, altering: Altering) -> anyhow::Result<()> {
        self.unwritable(name).await?;
        let before = self.before(name).await?;
        let had = self.ruled_of(name);
        let table = MemTable::try_new(altering.schema, altering.partitions)?.with_column_defaults(altering.defaults);
        let (_, page) = self.default_schema()?;
        page.deregister_table(name)?;
        register_page_table(page.as_ref(), name.to_string(), Arc::new(table))?;
        self.set_ruled(name, altering.ruled);
        if let Err(e) = self.written(name).await {
            return Err(self.undone(name, before, had, e, ": the page couldn't keep it").await);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A statement as a client sends it, its answer in CSV.
    async fn sql(page: &Page, text: &str) -> Answer {
        page.request("POST", "/sql?format=csv", &[], text.to_string().into()).await
    }

    /// `sql`, which must answer 200.
    async fn ok(page: &Page, text: &str) -> Answer {
        let answer = sql(page, text).await;
        assert_eq!(answer.status, 200, "{text}: {}", body(&answer));
        answer
    }

    /// The text of an answer's body.
    fn body(answer: &Answer) -> String {
        String::from_utf8_lossy(&answer.body).into_owned()
    }

    #[tokio::test]
    async fn each_action_answers_as_the_node_does() {
        let page = Page::open(&Options::default());
        ok(&page, "CREATE TABLE t (id INT PRIMARY KEY, n INT)").await;
        for (text, answer) in [
            ("ALTER TABLE t ADD COLUMN w INT", r#"{"table":"t"}"#),
            ("ALTER TABLE t ADD COLUMN IF NOT EXISTS w INT", r#"{"table":"t","unchanged":true}"#),
            ("ALTER TABLE t RENAME COLUMN w TO x", r#"{"table":"t","altered":{"renamed":"w","to":"x"}}"#),
            ("ALTER TABLE t DROP COLUMN x", r#"{"table":"t","altered":{"dropped":"x"}}"#),
            ("ALTER TABLE t DROP COLUMN IF EXISTS x", r#"{"table":"t","unchanged":true}"#),
        ] {
            let got = sql(&page, text).await;
            assert_eq!((got.status, body(&got).as_str()), (200, answer), "{text}");
        }
        ok(&page, "CREATE VIEW v AS SELECT id FROM t").await;
        let renamed = sql(&page, "ALTER VIEW v RENAME TO v2").await;
        assert_eq!((renamed.status, body(&renamed).as_str()), (200, r#"{"view":"v","renamed":"v2"}"#));
        let renamed = sql(&page, "ALTER TABLE t RENAME TO u").await;
        assert_eq!((renamed.status, body(&renamed).as_str()), (200, r#"{"table":"t","renamed":"u"}"#));
        let read = sql(&page, "SELECT count(*) AS n FROM v2").await;
        assert_eq!(read.header("x-pondra-sqlstate"), Some("42P01"), "a view reads its table by the old name, which is gone: {}", body(&read));
        assert_eq!(body(&ok(&page, "SELECT count(*) AS n FROM u").await), "n\n0\n");
    }

    #[tokio::test]
    async fn rows_follow_an_added_renamed_and_dropped_column() {
        let page = Page::open(&Options::default());
        ok(&page, "CREATE TABLE r (id INT, name VARCHAR(5))").await;
        ok(&page, "INSERT INTO r VALUES (1, 'a'), (2, 'b')").await;
        ok(&page, "ALTER TABLE r ADD COLUMN w INT").await;
        assert_eq!(body(&ok(&page, "SELECT count(*) AS n, count(w) AS held FROM r").await), "n,held\n2,0\n");
        ok(&page, "INSERT INTO r (id, name, w) VALUES (3, 'c', 30)").await;
        ok(&page, "ALTER TABLE r RENAME COLUMN w TO z").await;
        assert_eq!(body(&ok(&page, "SELECT sum(z) AS total, count(z) AS held FROM r").await), "total,held\n30,1\n");
        ok(&page, "ALTER TABLE r DROP COLUMN name").await;
        assert_eq!(body(&ok(&page, "SELECT id, z FROM r WHERE id = 3").await), "id,z\n3,30\n");
        ok(&page, "TRUNCATE r").await;
        assert_eq!(body(&ok(&page, "SELECT count(*) AS n FROM r").await), "n\n0\n");
        ok(&page, "TRUNCATE TABLE r").await;
    }

    #[tokio::test]
    async fn a_renamed_key_unique_and_check_still_refuse_rows() {
        let page = Page::open(&Options::default());
        ok(&page, "CREATE TABLE r (id INT PRIMARY KEY, n INT CHECK (n > 0), u INT UNIQUE)").await;
        ok(&page, "INSERT INTO r VALUES (1, 1, 10)").await;
        for text in ["ALTER TABLE r RENAME COLUMN id TO key", "ALTER TABLE r RENAME COLUMN n TO m", "ALTER TABLE r RENAME COLUMN u TO v"] {
            ok(&page, text).await;
        }
        for (text, sqlstate) in [
            ("INSERT INTO r (key, m, v) VALUES (1, 2, 20)", "23505"),
            ("INSERT INTO r (key, m, v) VALUES (2, 0, 20)", "23514"),
            ("INSERT INTO r (key, m, v) VALUES (2, 2, 10)", "23505"),
        ] {
            let got = sql(&page, text).await;
            assert_eq!((got.status != 200, got.header("x-pondra-sqlstate")), (true, Some(sqlstate)), "{text}: {}", body(&got));
        }
        ok(&page, "INSERT INTO r (key, m, v) VALUES (2, 2, 20)").await;
        assert_eq!(body(&ok(&page, "SELECT count(*) AS n FROM r").await), "n\n2\n");
    }

    #[tokio::test]
    async fn a_dropped_column_takes_its_checks_with_it() {
        let page = Page::open(&Options::default());
        ok(&page, "CREATE TABLE c (id INT PRIMARY KEY, n INT CHECK (n > 0), CHECK (n IS NOT NULL), CHECK (id > 0))").await;
        let got = sql(&page, "INSERT INTO c (id) VALUES (1)").await;
        assert_eq!(got.header("x-pondra-sqlstate"), Some("23514"), "{}", body(&got));
        ok(&page, "ALTER TABLE c DROP COLUMN n").await;
        let got = sql(&page, "INSERT INTO c (id) VALUES (0)").await;
        assert_eq!(got.header("x-pondra-sqlstate"), Some("23514"), "the check on id stays: {}", body(&got));
        ok(&page, "INSERT INTO c (id) VALUES (1)").await;
        assert_eq!(body(&ok(&page, "SELECT count(*) AS n FROM c").await), "n\n1\n");
    }

    #[tokio::test]
    async fn an_alter_refusal_carries_the_code_a_node_gives() {
        let page = Page::open(&Options::default());
        ok(&page, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").await;
        ok(&page, "CREATE TABLE other (x INT)").await;
        // (no view reads t yet: a node checks a column's views before its name, so a view's refusals come after these)
        for (text, sqlstate) in [
            ("ALTER TABLE t ADD COLUMN a INT", "42701"),
            ("ALTER TABLE t ADD COLUMN d INT DEFAULT 7", "0A000"),
            ("ALTER TABLE t ADD COLUMN d INT NOT NULL", "0A000"),
            ("ALTER TABLE t ADD COLUMN d INT CHECK (d > 0)", "0A000"),
            ("ALTER TABLE t ADD COLUMN d INT UNIQUE", "0A000"),
            ("ALTER TABLE t ADD COLUMN d INT FIRST", "0A000"),
            ("ALTER TABLE t RENAME COLUMN nope TO z", "42703"),
            ("ALTER TABLE t RENAME COLUMN b TO a", "42701"),
            ("ALTER TABLE t DROP COLUMN nope", "42703"),
            ("ALTER TABLE t DROP COLUMN id", "0A000"),
            ("ALTER TABLE nope ADD COLUMN x INT", "42P01"),
            ("ALTER TABLE t RENAME TO other", "42P07"),
            ("ALTER VIEW nope RENAME TO z", "42P01"),
            ("ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", "0A000"),
            ("ALTER TABLE t ADD COLUMN x INT, RENAME TO u", "0A000"),
            ("TRUNCATE nope", "42P01"),
            ("TRUNCATE t, other", "0A000"),
        ] {
            let got = sql(&page, text).await;
            assert_eq!((got.status != 200, got.header("x-pondra-sqlstate")), (true, Some(sqlstate)), "{text}: {}", body(&got));
        }
        ok(&page, "CREATE VIEW v AS SELECT a FROM t").await;
        for (text, sqlstate) in [
            ("ALTER TABLE t RENAME TO v", "42P07"),
            ("ALTER TABLE t DROP COLUMN nope", "2BP01"),
            ("ALTER TABLE t DROP COLUMN a", "2BP01"),
            ("ALTER TABLE t RENAME COLUMN a TO z", "0A000"),
            ("TRUNCATE v", "42809"),
        ] {
            let got = sql(&page, text).await;
            assert_eq!((got.status != 200, got.header("x-pondra-sqlstate")), (true, Some(sqlstate)), "{text}: {}", body(&got));
        }
    }

    #[tokio::test]
    async fn a_kept_page_reads_an_altered_and_renamed_table_with_its_rules() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        ok(&a, "CREATE TABLE k (id INT PRIMARY KEY, n INT CHECK (n > 0), u INT UNIQUE)").await;
        ok(&a, "INSERT INTO k VALUES (1, 1, 10)").await;
        ok(&a, "ALTER TABLE k ADD COLUMN s VARCHAR(3)").await;
        ok(&a, "ALTER TABLE k RENAME COLUMN n TO m").await;
        ok(&a, "ALTER TABLE k RENAME COLUMN id TO key").await;
        ok(&a, "ALTER TABLE k RENAME TO kk").await;
        drop(a);
        let b = Page::kept(&options).await.unwrap();
        assert!(b.unread().is_empty(), "{:?}", b.unread());
        assert_eq!(body(&ok(&b, "SELECT count(*) AS n FROM kk").await), "n\n1\n");
        for (text, sqlstate) in [
            ("INSERT INTO kk (key, m, u) VALUES (1, 2, 20)", "23505"),
            ("INSERT INTO kk (key, m, u) VALUES (2, 0, 20)", "23514"),
            ("INSERT INTO kk (key, m, u, s) VALUES (2, 2, 20, 'long!')", "22001"),
        ] {
            let got = sql(&b, text).await;
            assert_eq!(got.header("x-pondra-sqlstate"), Some(sqlstate), "{text}: {}", body(&got));
        }
        ok(&b, "INSERT INTO kk (key, m, u, s) VALUES (2, 2, 20, 'abc')").await;
        let listed: serde_json::Value = serde_json::from_slice(&b.request("GET", "/objects", &[], bytes::Bytes::new()).await.body).unwrap();
        let names: Vec<&str> = listed["objects"].as_array().unwrap().iter().filter_map(|o| o["name"].as_str()).collect();
        assert_eq!(names, ["kk"], "the table's old name is gone from the store too");
    }
}
