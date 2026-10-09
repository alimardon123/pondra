# ADR-046: Sharing: a door to the files a grant covers

**Date:** 2026-10-03 · **Status:** accepted (Alimardon, 2026-10-03: "Build it now"; phase 1 built)
· **Builds on:** ADR-001 (files in place, never copied), ADR-003 (serverless: your compute, the
leader's commit), ADR-029 (anyone's compute, one catalog), ADR-035 (users, grants, one check; §4
scoped URLs), ADR-043 (`CLONE`), ADR-048 (`pondra.audit`)

## Context

Alimardon, 2026-10-03: share one table with another company, securely and with no extra work for
us; can Pondra's catalog stand beside Unity Catalog's and Snowflake's on features, security and
sharing, in Pondra's own way; later, share data, notebooks, code and models with other companies,
by a marketplace or by invites. And every part built so it is easy to change or replace.

Until now another company could reach a table only through our node (their queries on our
compute, our node open to the internet) or as a copy we push (`COPY … TO` their bucket on a task).
The Iceberg REST door vends no credentials: a reader of it needs the bucket's keys, which open
every table. Unity Catalog shares through Delta Sharing (an open REST protocol: the server answers
short-lived signed URLs, the recipient reads Parquet from the bucket with its own compute);
Snowflake shares without copies between its own accounts.

## Decision

1. **One part turns a grant into bytes outside the nodes: `vend.rs`.** For a list of a lake's
   objects it answers links that read each one for `PONDRA_SHARE_URL_SECS` (900; at most a week)
   and nothing else. A lake on S3, R2, GCS or Azure signs them with its own credentials
   (object_store's `Signer`), so the bytes never pass through a node. A lake on a disk links to
   the node (`/delta-sharing/files/{link}`), whose link carries the object, its end and its
   recipient under an HMAC of the lake's key; the node serves it, a range at a time. A link names
   only a table's file (`data/…`, no `..`). The Delta Sharing door uses it now; Iceberg REST's
   remote signing and the browser can later.

2. **One door: the Delta Sharing protocol** (`sharing.rs`, `/delta-sharing/…` on every node): list
   shares, schemas and tables, a table's version (`startingTimestamp` too), metadata and files
   (`query`: `version`, `timestamp`, `limitHint`, partition `predicateHints`), in both the Parquet
   and the Delta response formats. What it serves is the table's published Delta log
   (`read_delta::replay`), so its versions are durable (invariant 16) and deletes, renamed columns
   (column mapping), purges and shadowed keys come as Pondra already publishes them: deletion
   vectors, the `columnMapping` feature. A client that asks for Parquet only gets a table that
   needs neither; one that needs them is refused by name (400), never handed deleted rows. The
   door signs in a recipient by its token alone (users' tokens mean nothing there, nor a
   recipient's anywhere else), answers 401 after a pause to a bad one, and every request is a row
   of `pondra.audit` (door `sharing`, class `share`): who, what, how it ended. `changes` (CDF) is
   phase 2: refused by name.

3. **In SQL: shares and recipients, Databricks' words** (`shares.rs`, catalog `sh/` and `sr/`,
   each a prefix of its own, invariant 104):

   ```sql
   CREATE SHARE acme COMMENT 'Orders for Acme';
   ALTER SHARE acme ADD TABLE acme_orders;                              -- a table or materialized view
   ALTER SHARE acme ADD TABLE sales.orders PARTITION (region = 'EU') AS sales.orders_eu;
   ALTER SHARE acme ADD TABLE sales.orders WITH HISTORY;                -- older versions too
   CREATE RECIPIENT acme_corp EXPIRES IN '90 days';                     -- answers its profile, once
   GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp;
   ALTER RECIPIENT acme_corp ROTATE TOKEN;
   REVOKE SELECT ON SHARE acme FROM RECIPIENT acme_corp;
   SHOW SHARES;  SHOW RECIPIENTS;  DESCRIBE SHARE acme;                 -- pondra.shares, pondra.recipients
   ```

   Snowflake's `GRANT SELECT ON TABLE t TO SHARE s` is the same as `ALTER SHARE s ADD TABLE t`. A
   recipient is an entry of its own, not a user: it signs in only at the sharing door, and holds
   its token's hash and its end. The profile is Delta Sharing's JSON (`endpoint`, `bearerToken`,
   `expirationTime`), shown once; the endpoint is the node's advertised address or
   `PONDRA_SHARING_URL`. Adding a table to a share publishes it as Delta from then on. A share
   holds what whole files carry exactly: a table, a materialized view, or partitions of an append
   table by a `partition_by` column (invariant 26). Some columns or some rows are a materialized
   view of it, computed once as rows land. Refused by name: a table of another lake, an external
   table, a clone (its files are in another table's folder), a merge or `order_by` table. Rows
   still in the log show after the next tiering round, as for Delta and Iceberg readers.

4. **Pondra reads shares: invites between lakes.** `ATTACH '<profile JSON or endpoint>' AS acme
   (TYPE share, SHARE 'acme', TOKEN '…')` keeps the token as a sealed secret `share_acme` scoped to
   the endpoint, and `acme.schema.table` reads through the door with our compute: each statement
   asks for the files and reads them through the signed links (`sharing::Links`, an object store
   over them) with the Delta reader (`read_delta::table`, deletion vectors included). It reads any
   Delta Sharing server, Databricks' open shares among them. A profile sent is an invite, and
   attaching it accepts it. `DETACH` drops the secret.

5. **The console's Share…** on a table or materialized view: pick or name a share and a recipient,
   the statements shown as they are built, the new recipient's profile shown once to download or
   copy (`share.js`, loaded when first used).

6. **The recipient's reads are GETs outside the nodes' request budget.** A recipient's links per
   minute are capped (`PONDRA_SHARE_FILES_PER_MINUTE`, 10,000, on each node), so one recipient can't hold the bucket to its
   limit (principle 7). On S3 their egress is ours to pay; on R2 it is free.

## Rejected or deferred

- **A marketplace: rejected for now.** A central, always-on listing service, payments, terms and
  moderation are what principle 2 rules out. A share's `COMMENT` and a provider's own page are the
  listing.
- **Clean rooms: after 1.0.**
- **Partners with bucket keys or IAM roles per partner:** every table, or a policy per partner in
  every cloud.
- **A protocol of our own:** Delta Sharing has the clients already, and Pondra already writes Delta.
- **Phase 2:** Iceberg REST with remote signing through `vend`; a share's history as changes (CDF);
  folders of the workspace (notebooks, code, models as files) shared between Pondra lakes.
- **Phase 3** (the main thread's security round): views run with their owner's rights, row
  policies and column masks (a table with one refused by the sharing door), SSO, tags, lineage.

## Consequences

- No new service and no compute of ours per read: a node answers a small JSON per query.
- `tools/sharing_check.py` (the `delta-sharing` client and Pondra's `ATTACH`, local disk and
  `--s3`) checks it: a share read equals the table, through deletes, updates, a renamed column,
  partitions and history; refusals; the audit.
