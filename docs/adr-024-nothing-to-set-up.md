# ADR-024: Nothing to set up (0.22.1)

**Date:** 2026-09-27 · **Status:** built · **Follows:** ADR-018 (install anywhere)

## Context

0.22.0 was the first release on PyPI. The owner installed it on Windows and ran into two gaps:

1. **`pondra` wasn't found.** Their Python is installed for every user, so `pip install` fell back
   to a user install. The binary went to `%APPDATA%\Python\Python312\Scripts`, which isn't on PATH.
   Python still found it (`pondra.local()` looks there), but the terminal didn't. pip warns about
   this for console scripts, not for a binary shipped as a wheel script (as ruff and uv ship
   theirs), so there was no warning either. macOS user installs have the same gap.
2. **`pip install pondra` alone couldn't answer a query.** Since round 22, `con.sql(…)` returns a
   frame, and a frame fetched its rows as Arrow, which needs pyarrow (an optional extra). The
   README's first example failed with `ModuleNotFoundError`. CI missed it because every check
   installed pyarrow alongside the wheel.

Then, in the shell, three more:

3. **A folder of lakes wasn't a server's databases.** The owner keeps `lake`, `mydb` and
   `mylake` side by side. `pondra mydb` saw only `mydb` until an `ATTACH` for each of the others,
   and the `ATTACH` was then saved in `mydb`'s catalog.
4. **`FROM t` answered with no columns.** DuckDB reads `FROM t` as `SELECT * FROM t`; DataFusion
   planned it with an empty projection, and the shell printed an empty table.
5. **`UPDATE mylake.dbo.t …` said "dbo.t is another lake's: change it on a node of that lake".**
   That refusal is round 19's design (a change is its own lake's leader's, invariant 56), but the
   message named neither the lake nor what to do, and didn't say that `INSERT` works.

A pip package can't change PATH: wheels run no code when they're installed, by design. DuckDB's
pip package has no command at all (only `import duckdb`); its command-line tool is a separate
download.

## Decision

**One-line installers, attached to every release.**
- `irm https://github.com/alimardon123/pondra/releases/latest/download/install.ps1 | iex` on
  Windows.
- `curl -fsSL …/install.sh | sh` on Linux and macOS.

Each downloads the binary alone (`pondra-<platform>.zip` or `.tar.gz`, from `tools/package.py`,
named without a version so `releases/latest/download/` stays put). It goes into the user's own
folder (`%LOCALAPPDATA%\Programs\pondra`, `~/.local/bin`), and that folder goes on the user's PATH:

- **Windows:** the user's `Path` in the registry, kept as stored (`%VARIABLES%` unexpanded), and a
  settings-changed broadcast so new terminals see it. `irm | iex` runs in the current session, so
  this terminal's PATH is updated too and `pondra` works at once.
- **Linux and macOS:** one `export PATH=…` line in the shell's startup file (`.zshrc`, `.bashrc`,
  or `.bash_profile` on macOS, else `.profile`), added once. A piped script can't change its
  parent shell, so it prints the `export` for this terminal.

No admin rights anywhere. `PONDRA_VERSION`, `PONDRA_INSTALL` and `PONDRA_ARCHIVE` choose a
release, a folder, or a build of your own (CI's).

**`python -m pondra`** (`python/pondra/__main__.py`) runs the binary pip installed, wherever pip
put it, as `python -m pip` does. On Linux and macOS it becomes pondra (`execv`); on Windows it
waits for it, leaving Ctrl+C to pondra. When `pondra` isn't on PATH and it's run in a terminal, it
says where the binary is and that **`python -m pondra --add-to-path`** fixes that once, the same
way the installers do. It's a command you run, not something done behind your back.

**Rows without pyarrow.** When pyarrow isn't installed:
- `rows()`, `item()`, a script's rows and a procedure's answer ask the node for JSON, with nulls
  put back as `None` (the node's JSON leaves them out).
- `show()` and a notebook's display ask for the node's text table (`show()` does so always now).
- `collect()`, `to_pandas()` and `to_polars()` say they need pyarrow.
- Python procedures still need pyarrow (their runner reads its arguments as Arrow); calling one
  without it fails with "No module named 'pyarrow'".

**The shell's databases are the lakes beside it.** The shell starts its node with
`--attach-found <its current folder>`: every subfolder there that holds a lake is attached under
its folder's name, for as long as the node runs (`ddl::attach_found`), and the startup line lists
the databases. Only the shell does this (a server attaches nothing it wasn't told about), and
nothing is written to the catalog, so the lake doesn't remember the folder it was once opened
from. Left out: the shell's own lake, a name the catalog already attaches (that `ATTACH` says
where), a schema's name, and a lake that won't open (said in the node's log, never fatal).
`DETACH` of one says it stays for the session.

**`FROM t` is `SELECT * FROM t`**, alone, in a subquery or CTE, or as a view's query:
`routines::expand` already sees every query at every door, and now also turns a FROM-first
select without a SELECT into one with `*` (a pattern on the text keeps the rest from being parsed
twice).

**A change to an attached lake says where it runs:** "mylake.dbo.t is in attached lake mylake:
UPDATE, DELETE and MERGE run on that lake's own node for now (pondra <its folder>); INSERT works
from here". Letting them run from here is round 23's (the change sent to that lake's leader, or
led for a moment when none runs, as an `INSERT` is).

## Rejected

- **pyarrow as a requirement.** It's about 40 MB, and on old Linux (glibc 2.17) pip would pick
  its newest version and try to build it from source.
- **A console-script launcher instead of the binary.** pip would then warn about PATH, but every
  `pondra` would start Python first, and the warning still leaves the work to the user.
- **Accepting `ATTACH mylake AS mylake`** (a name for a path): DuckDB refuses it too, and the
  owner was fine with quotes.
- **winget and Homebrew now.** Both want a submission per release, and winget reviews take days.
  They're worth doing once there are users asking for them.

## Tests (each fails without its fix: `logs/round22/0.22.1-negative-checks.txt`)

`tools/try_packages.sh` runs on Linux, Windows and macOS on every push (the build workflow) and
before every release:

- `package_check.py` in a fresh virtualenv **before** pyarrow is installed: rows as JSON (a null
  comes back as `None`), one value, a text table, SQL procedures, `collect()` asking for pyarrow.
  0.22.0's client fails it with `ModuleNotFoundError`.
- The same after pyarrow, with a Python procedure.
- Both runs check `python -m pondra --version` against the binary (0.22.0: "No module named
  pondra.__main__"), and `python -m pondra --add-to-path` twice ("now", then "already") with a
  home of its own. On Windows the second part runs only in CI, since it sets the real `Path`.
- **The installer, with this build's archive.** On Linux and macOS, in a clean shell with only
  `/usr/bin:/bin` and the startup file, `command -v pondra` must be the installed one; without
  the PATH step it isn't found. On Windows, in CI, `tools/try_install.ps1` runs `install.ps1` as
  `irm | iex` does. `Get-Command pondra` must then be the installed one and the user's `Path` must
  hold the folder.

- **`smoke.py`** (Linux, Windows and macOS, every push): a shell started in a folder with two
  other lakes lists them in `.databases` and reads one with `FROM lake.sales.orders`; the same
  lake opened from elsewhere lists neither. 0.22.0's binary fails the first two.
- **`harness.py schemas`**: `FROM t` alone, as a subquery and as a view's query on three nodes;
  `UPDATE` on an attached lake names it and says `INSERT works from here`. 0.22.0's binary fails
  with "No field named id".

The GitHub release now opens with an install table (`.github/release.md`) above GitHub's
generated notes.
