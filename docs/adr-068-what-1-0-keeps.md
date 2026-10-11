# ADR-068: What 1.0 keeps, and how anything changes after it

**Date:** 2026-10-11 · **Status:** accepted (the 1.0 plan, `designs/one-month-to-1-0.md`, gives lane L7
"what 1.0 keeps stable … and how anything is deprecated") · **Builds on:** ADR-032 §8 (compatibility
from 1.0), ADR-039 (a lake's format, every release's lake, rolling upgrades), ADR-041 (how Pondra is
installed) · **Thread:** Durability and upgrades

## Context

ADR-032 §8 promised that from 1.0 on, "the lake's format, SQL, the HTTP API, the clients' APIs and
the command line stay compatible: a change that breaks one needs a deprecation release first". It
didn't say what each of those covers, how a deprecation reaches the people using the old way, how
long the old way lasts, or which releases get fixes. People choosing a database for years ask exactly
that, and Postgres, DuckDB (from its 1.0) and Kafka each answer it in writing. A promise kept by
memory and review also breaks quietly: a flag renamed in a refactor is a broken script somewhere.

## Decision

### 1. Versions

From 1.0, releases are `major.minor.patch`.
- **A patch** fixes. It adds nothing, and never moves a lake's format.
- **A minor** adds. Everything 1.0 keeps (§2) keeps working, and a lake's format may move (ADR-039:
  only once every live node knows the new one).
- **A major** may remove what a minor release deprecated (§4). None is planned.

### 2. What 1.0 keeps

| Surface | Kept | May still change |
|---|---|---|
| The lake | Every 1.x opens every lake an earlier release wrote (0.22 on), and answers as it did (`upgrade_check.py lakes`). A lake moves to a newer format only when every live node knows it. | The files' layout inside a lake. Other engines read the published Delta and Iceberg tables, which follow those formats' own specifications. |
| SQL | Statements, clauses, functions and their arguments, types, casts, the names and columns of system tables (`pondra.*`), `information_schema`, Postgres's catalog as Postgres has it, every SQLSTATE a statement fails with. | A system table may gain columns at its end. Error messages' wording (their codes stay). `EXPLAIN`'s text and plans. The order of rows a query doesn't sort. |
| The HTTP API | Every route in the reference, its parameters, its JSON fields and status codes, the `x-pondra-*` headers. | New fields in answers (a client ignores what it doesn't know). `/cluster/*` is between nodes only: ADR-039's rolling upgrades are its promise. |
| The other doors | Postgres's wire protocol as drivers use it, Flight SQL, the Kafka APIs and versions served, MCP's tools and their arguments, the Iceberg REST catalog and Delta Sharing as their specifications say. | |
| The clients | Python's and JavaScript's public names (no leading `_`), their arguments, and frames' methods. | |
| The command line | Commands, flags, documented `PONDRA_*` variables, exit codes, and what `--json` and `--format` print. | The text written for people. |
| Running it | `pondra.toml` keys, `pondra service` options, `/metrics` names (labels may be added), `/healthz` and `/ready`, the container's paths and the Helm chart's values. | |

**Not kept:** what the reference marks as a preview, `PONDRA_DEBUG_*` and other undocumented
variables, log lines, speed, and the console's look. A preview says so in its reference page and in
`pondra.kinds`; it graduates in a minor release, or goes.

**Security comes first.** A fix that can't keep a behaviour that is itself the hole may change it in a
patch. The release notes say what changed and why.

### 3. The promise is checked, not remembered

`tools/surface.py` writes what 1.0 keeps from a build into `docs/surface.json`:
- the command line's commands and flags (`--help` of each);
- the HTTP routes and their methods (read from `server.rs`'s router);
- SQL's functions and their signatures, the system tables and their columns, `pondra.kinds`;
- the SQLSTATEs in `codes.rs`;
- Python's and JavaScript's public names;
- `/metrics`' names.

CI (`build.yml`'s suite) compares a pull request's build with its `docs/surface.json`, and that file
with main's:
- **A name gone or changed fails**, unless `pondra.deprecations` says it was deprecated and the
  release is a new major.
- **A name added fails until `docs/surface.json` is updated** in the same pull request, so every
  addition to the promise is a line a reviewer sees.

The check runs from now, and reports until the freeze (10-31): eight lanes adding names at once would
otherwise all edit one file. At the freeze the file is written with `--promise`, and from then on it
fails as above. Until 1.0, a name changed on purpose updates the file in its own pull request.

### 4. How something changes after 1.0

1. **The new way comes first**, in a minor release, beside the old one.
2. **The old way is deprecated** in the same or a later minor release:
   - it has one entry in one list in the code (`deprecated.rs`): the old name, the release, the new
     way, and the earliest major that may remove it;
   - it keeps working, and its first use in a session says what to write instead, through every door
     that carries words: a Postgres `NOTICE`, the `x-pondra-notices` header, MCP's notices, the
     shell, the console's messages, the command line's standard error;
   - `pondra.deprecations` lists the entries, and the release notes and the reference say the same.
3. **It is removed only in a major release**, at least twelve months after its deprecation.
   `surface.py` fails a removal that skips a step.

Nothing is deprecated before 1.0 (until then a better name simply replaces the old one, ADR-032 §8),
so `deprecated.rs` and `pondra.deprecations` are built with the first deprecation, not before.

### 5. Which releases get fixes

- The newest minor release gets every fix as a patch.
- A security fix also goes to the minor release before it, for six months after the newer one ships.
- Any 1.x upgrades to any later 1.y in one step, with no release between to visit. A cluster upgrades
  a node at a time while it serves (ADR-039 §5), the nodes mixing any two 1.x releases meanwhile.
- **Going back** works while the lake's format hasn't moved past the older release. The older release
  refuses a lake it can't read by name, before it reads anything.

### 6. The upgrade guide says it per install

`guides/upgrades.mdx` gains how to move each install to a new release, and how to stay on one:
- pip (`pip install -U pondra`, `pondra==1.2.*`);
- npm;
- the one-line installers (`PONDRA_VERSION`);
- the container's tag;
- the Helm chart (`image.tag`, rolling by default);
- `pondra service`;
- Homebrew, Scoop and winget, once published.

A new reference page, `reference/compatibility.mdx`, is §1–§5 for users. (The website is the docs
lane's: this ADR is its source.)

## Rejected

- **Calendar versions** (DuckDB's early releases, Ubuntu's). They say when, not what may break, and
  the format and deprecation rules need "minor" and "major".
- **Long-term-support releases.** One small team can't backport to several lines. The newest minor,
  plus security fixes for the one before, is what we can keep.
- **A promise without a check.** Every project that keeps one by review breaks it eventually.

## Tests

- `tools/surface.py --check docs/surface.json --base <main's>` in `build.yml`'s suite: once the
  file is promised, a name gone, changed or added without the file fails.
- `upgrade_check.py lakes` (every release's lake since 0.22) and `rolling` (two releases at once)
  stay what §2's "the lake" row and §5's one-step upgrades rest on.

## Invariants (for AGENTS.md, from L7's 370–384)

- **371. What 1.0 keeps is a file the build checks** (`docs/surface.json`, `tools/surface.py` in
  CI). Until the freeze it reports. Once the file is promised, a name gone or changed fails unless it
  was deprecated and the release is a new major, and a name added fails until the file lists it.

(The deprecation list gets its own invariant with the first deprecation, which builds it.)
