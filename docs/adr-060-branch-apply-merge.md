# ADR-060: Branch, apply, merge: the team's workflow in a terminal and in the console

**Date:** 2026-10-10 · **Status:** accepted (Alimardon, 2026-10-10, 17:09–17:53: "everything should
be comprehensible, just like the software engineer workflow"; files and capture recommended on a
card) · **Amends:** ADR-047 §4 and §7, ADR-058 step 4 · **Builds on:** ADR-033 (the workspace),
ADR-043 (`CLONE`), ADR-054 (one workspace beside the lakes), ADR-058 (an environment is a server) ·
**Thread:** Team workflow

## Context

Round 34 shipped:
- `pondra branch`, `plan`, `deploy`, `test` and `diff`;
- `pondra dev`, which deploys on every save into a database named after the Git branch;
- a Git hook that made that database on `git switch`;
- a test database made again from prod before every deploy.

Alimardon found it hard to follow:
- `git switch` and then `pondra dev` were two steps whose relation wasn't clear;
- "deploy" reads as CI/CD promotion;
- a test database made again on every deploy loses what QA put there.

He asked for:
- one workflow, as familiar as Git's;
- both a single server and a server per environment, with the same commands;
- the console able to do it all as well.

## Decision

### 1. A branch is a Git branch and a database of its name

A branch's database is cloned from prod and copies no file. The checked-out Git branch picks the
database, so Pondra keeps no branch state of its own and nothing can drift out of step with Git.

```
pondra branch add-discounts      # git switch -c add-discounts, and database add_discounts, cloned from prod
pondra switch fix-tax            # git switch fix-tax, its database made if it isn't there
pondra branch                    # the branches: owner, age, * on yours
pondra branch -d add-discounts   # its database dropped, and the Git branch if Git agrees
pondra plan                      # what apply would change in your branch
pondra apply                     # your branch's database made to match the files, then the tests
pondra apply --watch             # the same on every save
pondra apply test                # an environment, by name (CI's, usually)
```

A merge is a pull request. `pondra dev`, the Git hook and `--if-missing` are gone.

### 2. `apply`, not `deploy`

Making a database match declared files is what Terraform, kubectl and Atlas call `apply`, paired with
`plan`. Moving work to test and prod stays CI's job, through merges and approvals. The word changes
everywhere a person sees it:
- `pondra apply`;
- `CALL apply(…)`;
- `POST /apply`;
- `GRANT APPLY ON DATABASE prod`;
- `pondra.applies`.

What is stored keeps its old names (`dl`, `dm/`, `files/.deploys/`), so the lake's format doesn't
change.

### 3. A database is made when a command needs it

- **A branch** is cloned from `[env.dev] clone`, else from prod, by `CREATE DATABASE IF NOT EXISTS …
  CLONE`.
- **An environment with `clone = "prod"`** (test) is made once and kept, so QA's rows stay.
  `apply test --fresh` makes it again. Running REFRESH before every apply was rejected: it is refused
  once the database has views of its own following the refreshed tables, and it can't keep
  migrations' records.
- **A database of its own** (prod) is made empty on its first apply.
- **A brand-new database is made as the files are now** (Alimardon chose "Record, don't run",
  2026-10-11): its objects come from `objects/` as they are, so its migrations, which changed what older
  files made, are recorded as run and never run (a rename has nothing to rename there). The rows it
  starts with are in `seeds/`, loaded once into a new database, as dbt, Rails and Prisma do. New means
  nothing in the schemas the project's objects are in (another schema's tables don't count) and nothing
  applied, or only its first applies, which failed. A clone is never new: it carries its base's rows and
  its record of migrations done.
- **On `main`**, `pondra apply` names the environments to choose from, instead of guessing.

### 4. One server or one per environment, the same commands

```toml
[project]
server = "https://pondra.acme.com"        # layout A: everything here
[env.prod]
protected = true
# server = "https://prod.acme.com"        # layout B: each environment its own,
[env.test]
clone = "prod"
# server = "https://test.acme.com"
[env.dev]
# server = "https://dev.acme.com"         # where branches are made
```

- **Where an environment is:** its own `server`; for a branch, `[env.dev]`'s; otherwise
  `[project] server`.
- **Where a clone is made:** on the server its source is on, the source's own node does it. On
  another server, that server's default database does it, where prod is attached read-only
  (ADR-058).

### 5. Who may make and drop a branch

- `CLONE` is enough to make a branch. Its maker owns it and is a superuser in it, so a developer
  needs no admin.
- Only its owner or an admin drops it, so nobody drops another person's work. Test and the pull
  requests' databases are CI's, so a developer can't drop them.
- `GRANT CLONE ON DATABASE prod` on a server where prod is attached lets a user branch it there
  (layout B).
- `pondra.databases` names each branch's owner.

### 6. CI is set up by one command

`pondra ci init` does three things:
- It writes the workflow: each pull request gets `pr-N`, made fresh, applied, tested and diffed; a
  merge applies to test; an approval applies to prod.
- It protects prod in pondra.toml.
- It makes the users CI signs in as, `ci` (CLONE) and `ci_prod` (APPLY on prod), with tokens, and
  stores them with `gh` as secrets and a `prod` environment that the person running it reviews.

Running it again rotates the tokens. In layout B it prints the three statements that attach prod on
the other servers, since only the person setting it up holds a read-only key of prod's bucket.

### 7. The files are the truth; a database is the files applied, plus its rows

- **The console and an editor work the same way:** change a file, and saving applies it to your
  branch. In the console that is Save. In a terminal it is `pondra apply --watch`.
- **A change made straight in a database** (a CREATE in a SQL cell, psql, a BI tool) is not written
  into the files on its own:
  - a database holds state and files hold intent;
  - a catalog can't tell a rename from a drop and a create, or a scratch table from a product;
  - automatic sync would write pull requests nobody wrote;
  - it would need a writer that is always on.

  Instead, `pondra status` lists, like `git status`:
  - **unapplied:** in the files, not in the database;
  - **untracked:** in the database, within the project's schemas, not in the files;
  - **drifted:** in both, but different.

  `pondra capture [NAME…]` writes chosen objects into `objects/`, one object to a file, in the
  author's own text when one statement made it. In the console, an **Add to project** button sits by
  a CREATE. On prod, protection refuses such changes anyway.
- **Plans look only at the schemas the project's files use**, so two projects on one prod never see
  each other's objects as drift.

### 8. The console: Git folders

The console has no `git`. A **Git folder** is a workspace folder paired with a branch of a repository
(`.pondra-git.json` in it: repository, branch, commit, each file's blob id). It works wherever the
workspace lives (today a lake's `files/`; ADR-054 beside the lakes).

**Hosts.** GitHub and GitLab are spoken to over their REST APIs, each one registry entry behind a
small trait:
- read a branch's head and tree;
- read a blob;
- commit files on top of an expected commit;
- make a branch;
- open a pull request.

No `git` program is needed, so it runs in the distroless image and as a Windows service.

**A person's token is a secret.** Each person makes their own, so commits and pull requests are
theirs:
```sql
CREATE SECRET ann_git (TYPE git, TOKEN 'ghp_…');
```

**Procedures** are Pondra's own, like `run` and `apply`. Each runs as its caller.
```sql
CALL git_checkout('/Shared/sales', repository => 'https://github.com/acme/sales', branch => 'main');
CALL git_checkout('/Shared/sales', branch => 'add-discounts');    -- a new name: made from the folder's commit
SELECT * FROM pondra.git_status('/Shared/sales');
CALL git_commit('/Shared/sales', 'Discounts by channel');         -- every changed file; fast-forward only
CALL git_commit('/Shared/sales', 'Discounts', paths => ['objects/sales/discounts.sql']);   -- just these
CALL git_pull('/Shared/sales');                                   -- a file changed on both sides refused by name
CALL git_pull_request('/Shared/sales', title => 'Discounts by channel');
```

**How the console uses them** (in the console round):
- **The branch picker** lists the databases that have a base.
- **New branch** clones a database and checks out the Git branch of the same name.
- **Save** writes the file and applies it.
- **Commit** is Synapse's and Data Factory's *Commit all* (Alimardon, 2026-10-10 19:23): one message,
  and a list of the changed files, all ticked. Each object is one file, so unticking a file keeps that
  object out of the commit; the object level costs nothing more.
- **Open pull request** calls `git_pull_request`.

The merge happens on the host, where review and CI are. Their *Publish* from main is `apply prod`, by
CI on approval or by someone granted APPLY. No generated publish branch is needed: prod is applied
from main's own files, and every apply records the commit it was of.

### 9. Every tool takes part the same way (Alimardon, 2026-10-10 19:32)

Future tools (an ETL canvas, a BI tool, models, ontologies, an extension's objects) go through branch,
apply, merge, status, capture, diff, protection, ownership and the audit log exactly as tables do. They
plug in; nothing in this workflow is edited to admit them. Each thing a tool makes is one of three
shapes (the rules for new kinds, ADR-049):

- **An object** (a model, a dashboard, an ETL job, a connection): one `KINDS` entry. It is defined by a
  statement, `SHOW CREATE` writes it back, `on_clone` says what a branch does with it, and `project`
  says whether a project may declare it. Apply, export, status and capture read the entry.
- **A file the tool keeps** (a canvas, a report's layout, a notebook): the tool registers the project
  folder it owns (`dashboards/`, say). Apply writes those files into the database's workspace, and each
  save is a version (`files::keep`). Status and drift treat them as objects.
- **A pattern** (an ontology, a flow, a task graph): it stores nothing of its own, so it travels with
  the objects it is read from.

Apply's kinds are the registry's (round 34, after #62): `apply::Kind` is an entry of `objects::KINDS`
(its word, `project`, and `order`: what others name comes first), or what `pondra.toml` and GRANTs
say. Sequences, indexes and enum types are declared in `objects/` too. How a change is made stays in
apply, by the kind's word: a table is altered in place, a sequence by `ALTER SEQUENCE` (its next value
kept; an option taken away is refused), a type's labels only added at the end, an index made again,
the rest replaced. `project_check.py` checks that every kind `GET /kinds` says a project may declare
goes export → apply → export unchanged, so a new kind fails there until the project declares it.

One gap today: a tool's files aren't applied yet. They are built with the first tool that keeps files.

## Rejected

- **A database per developer** (`pondra dev`'s first shape): one database can't hold two features at
  once, and it goes stale.
- **A Git hook that makes the database:** it is invisible, needs installing, and fails on Windows
  shells. Making the database when a command needs it does the same job with nothing installed.
- **Copying Git's branch commands exactly** (`pondra branch NAME` without switching, `switch -c`):
  Git's least liked part. `branch NAME` starts a branch and goes to it; `switch` goes to one that
  exists.
- **Automatic two-way sync between files and databases** (§7).
- **For the console: the `git` program on the server, a Git library in the binary, or Git's wire
  protocol by hand:** a dependency the image lacks, a large dependency, or a lot of code. None of
  them opens pull requests.

## Tests

- `tools/project_check.py` covers:
  - branch, switch, list, delete;
  - apply to a branch, to test (kept, then `--fresh`), to prod (made empty on its first apply);
  - main refused;
  - `--watch`;
  - diff;
  - `ci init` with a stand-in `gh`.
- `tools/environments_check.py`:
  - `owners_check`: CLONE makes a branch and its owner; only the owner or an admin drops it;
  - `layout_b_check`: two servers on a simulated bucket, `ci init`, a pull request's database made on
    dev from prod, prod applied with CI's token alone.
- The console's Git folders get `tools/git_check.py` against a small GitHub- and GitLab-shaped API
  in Python, when they are built.
