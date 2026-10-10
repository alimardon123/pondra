# ADR-058: An environment is a server, with a bucket and keys of its own

**Date:** 2026-10-09 · **Status:** accepted (Alimardon chose "Separate servers", 2026-10-09 21:26) ·
**Amends:** ADR-047 §2, §3, §6 and its phases · **Builds on:** ADR-032 (`pondra serve --lakes`),
ADR-035 (users, grants, the audit log), ADR-046 (`vend`), ADR-054 (one workspace per folder of lakes),
and the comparison in `designs/environments-layout-and-security.md`

## Context

ADR-047 runs prod on its own cluster, but keeps it in the same bucket as every branch, served next to
them by one `pondra serve --lakes`. Dev's nodes therefore hold a key that can write prod's files, and
only Pondra's own checks stop them. Databricks gives each environment a workspace of its own (often a
cloud account), and each catalog a storage credential of its own. Regulated Snowflake users keep an
account per environment. A security engineer wants the wall between dev and prod in the cloud's IAM,
not in our code. Alimardon asked how environments map to workspaces, how safe branches are, and how
natural the developer's day is. He chose separate servers.

## Decision

1. **An environment is a server.** Each has its own nodes, users, tokens, workspace files (ADR-054)
   and secrets, and its own bucket or prefix, written only with its own cloud credentials. One server
   is the unit Databricks calls a workspace.
   - prod: `pondra serve --lake s3://acme-prod/prod`. Only prod's nodes hold a key that writes
     `acme-prod`.
   - dev: `pondra serve --lakes s3://acme-dev/`. It holds test, every pull request's database and
     every developer's branch. It writes only `acme-dev`.
   - A regulated company can also give test a server of its own. A small team can still run one
     server for everything (ADR-047 as built): nothing here is needed to start.
2. **A branch can have its base on another server, still zero copy.**
   - Dev attaches prod's lake by URL with a **read-only key** of prod's bucket. The key is a secret
     on dev's server, scoped to that prefix, and is used for that attachment alone.
   - The pin goes to prod's leader over HTTPS, as any write to another lake does (invariant 20),
     with a token of prod's that holds `CLONE` on the schemas prod's admin lets out (ADR-047's
     privilege), and nothing else. prod answers with the branch's own key: its renewal, REFRESH's
     new pin and the unpin carry that key, good for that pin alone (built on one server first: the
     follow-up to #40). With a read-only key the inbox fallback can't be used, so a pin needs prod's
     leader to be up. A branch can't be made while prod is down; it can still be read.
   - The read-only key reads prod's catalog, and prod's own keys are in it (`z/auth`: the nodes'
     key and the sessions' signing key, in the clear today). So before a branch reads across
     servers, prod's keys are sealed by its master key (`PONDRA_SECRET_KEY`), as its secrets are
     (invariant 177): otherwise dev could sign in to prod as its nodes.
   - Dev's nodes read prod's files with the read-only key, through their own SSD tier. Their writes
     go to `acme-dev`. `pondra.diff` and REFRESH work as they do on one server.
   - A clone copies only tables that the token's principal may read whole on prod (ADR-047 §3, and
     `users::across` from #44). Prod's admin decides what dev may see by granting that principal,
     not by trusting dev.
3. **Prod is protected from its first deploy.** ADR-047's phase-2 `protected` and `DEPLOY` move up.
   - In a protected database, the project's objects change only through a deploy, made with a token
     that holds `DEPLOY` (CI's).
   - People read, and keep making their own objects where they are granted to.
   - `ALTER DATABASE prod SET (protected = false)` is the break-glass. It needs an admin, is recorded
     in `pondra.audit`, and `pondra.databases` shows it until the database is protected again.
4. **`pondra.toml` names a server per environment.** It already does: `[env.prod] url = "https://…"`,
   with a token kept per server by `pondra login`. A new key, `[env.dev] base = "prod"`, tells
   `pondra branch` where branches come from, so CI's three lines stay the same.
5. **What dev may see of personal data comes later.** A column masked in prod stays masked in its
   branches, and a table marked sensitive branches with no rows or a sample. That waits for column
   masks.

## What it costs and what it doesn't

- **No new service.** Each environment is a `pondra serve`. Dev's branches still copy no data, and
  prod still does no work for them.
- **Prod's bucket answers dev's reads.** Dev's SSD tier and its own request budget (`budget.rs`,
  principle 7) keep them bounded. Dev's writes, merges and listings no longer touch prod's bucket.
- **Prod's storage holds what branches pin,** as before: idle branches expire after 14 days, and
  REFRESH moves the pin forward.

## Invariants (proposed)

1. A branch whose base is on another server reads that base only with the attachment's own key. It
   never writes or deletes anything under that base's prefix, whatever its own key allows.
2. A pin across servers is granted by the base's leader to a principal holding `CLONE`. A clone
   copies only what that principal may read whole.
3. In a protected database, nothing but a deploy made with `DEPLOY` changes a project's object.
   Lifting the protection is an admin's statement, recorded in the audit log.

## Build order (none of it holds #40 or 0.33.0)

#40 needs no change: per-environment servers in `pondra.toml` already exist, and REFRESH, plan and
deploy are the same on either layout.

1. **Before 0.33.0** (already owed): CLONE checks read rights on its source (`users::across`,
   after #44); REFRESH and the unpin go by the branch's key, and REFRESH brings only the schemas
   the clone took. *Built in #46.*
2. **Cross-server branches.** prod's own keys sealed by its master key (*built in round 34:*
   `users::Kept`, at the lake's format 3; sealed only by a master key every node can share,
   `PONDRA_SECRET_KEY` or a key service, never the machine's own, which a node on another machine
   couldn't open). An attachment with its
   own read-only key (stored as a secret). The pin over HTTPS with a `CLONE` token; renewal,
   REFRESH and unpin with the branch's key. `[env.*] base`. Checks: two buckets,
   with dev's key going through a proxy that refuses every write to prod's bucket, so the whole of
   `environments_check.py` passes there. *Built in round 34:* `ATTACH 'url' AS prod (READ_ONLY,
   ENDPOINT 'https://…')` reads with the secret covering the URL through a store that refuses every
   write and delete (`store::Reach`, `ReadOnly`), and `write::across` lets only a pin or an unpin
   through, to the leader at `ENDPOINT` (a `TYPE pondra` secret's token, or the branch's `pb_` key).
   `GRANT CLONE ON DATABASE | SCHEMA` lets a token pin and do nothing else; a clone limited to some
   schemas takes only those. The branch keeps the read-only key in its own catalog. `[env.dev] base
   = "prod"` makes `pondra branch` branch on dev's server. Checked by `environments_check.py
   --across` (two buckets behind gates: dev's key writes only `acme-dev`, prod's read-only key only
   reads `acme-prod`). Not yet: pins of a branch on another server are let go by `DROP DATABASE` or
   REFRESH only, never swept as idle (prod can't see dev's bucket).
3. **Protected databases and `DEPLOY`,** with the break-glass and its audit. Checks: a protected
   database's objects change only through a deploy; a person's own schema is still theirs. *Built in
   round 34:* `ALTER DATABASE d SET (protected = true | false)`, an admin's; a lift is written to
   `pondra.audit` and shown in `pondra.databases` until the database is protected again. `GRANT
   DEPLOY ON DATABASE d` deploys without an admin token, and on a protected database only a DEPLOY
   holder or the node's operator may deploy. `[env.prod] protected = true` makes the first deploy
   that applies protect the database. A deploy's statements, tests included, run as the deploy
   (`protect::DEPLOYING`); a procedure it would start without waiting is refused, since it would
   outlive the deploy. Checked by `environments_check.py`.
4. **The developer's day:** `pondra ci init` (the workflow: a branch, a deploy, tests and the diff
   on each pull request, test on merge, prod on approval), `pondra dev` (deploy and test on save),
   and a Git hook that moves the branch's database with `git switch`. *Built in round 34:* `pondra ci
   init` writes the workflow to `.github/workflows/pondra.yml` (pinned to this pondra, with a test job
   when pondra.toml has `[env.test]`) and says the four things left to set up; `pondra dev` makes the git
   branch's database, then deploys and tests on each save (never prod, `main` or a protected database;
   a failed deploy is printed and the watch goes on); `pondra branch --if-missing` leaves a database that
   is there, and `pondra branch --hook` installs the `post-checkout` hook that calls it on `git switch`.
   Checked by `project_check.py`, which runs the hook, the watch and the workflow's jobs.
5. **Later:** masked branches; a branch on the laptop over `vend` (round 35's in-process mode).

**Names** (checked by the SQL review thread, 2026-10-09):

- `ATTACH 's3://acme-prod/prod' AS prod (READ_ONLY)` takes DuckDB's spelling. The bucket's key is a
  `CREATE SECRET` scoped to that prefix. Prod's leader is reached with
  `CREATE SECRET prod_clone (TYPE pondra, TOKEN '…', SCOPE 'https://prod.acme.com')`.
- `CREATE DATABASE ali CLONE prod` and `ALTER DATABASE ali REFRESH` stay as they are.
- `GRANT CLONE ON DATABASE prod | SCHEMA sales TO dev_server` and `GRANT DEPLOY ON DATABASE prod TO
  ci` are granted to ordinary users and roles, so `REVOKE`, `SHOW GRANTS` and `pondra.objects` list
  them with no special case. Both go in the registry's privilege list, so `GRANT ALL` and
  `pondra.kinds` know them.
- `ALTER DATABASE prod SET (protected = true | false)`, which `SHOW CREATE DATABASE` gives back.
- `[env.dev] base = "prod"` in `pondra.toml`.

## Rejected

- **One bucket with IAM conditions per prefix:** it works on S3, but not the same way on R2, GCS and
  Azure, and one server's nodes would still hold both keys.
- **A copy of prod per environment:** slow, costly at terabytes, and stale the moment it's made.
- **Dev reading prod through prod's nodes (Delta Sharing):** prod would do dev's work. Sharing stays
  for partners (ADR-046).
