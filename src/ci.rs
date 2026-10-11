//! `pondra ci init` (ADR-060 §6): the workflow each pull request, merge and approval runs, prod protected in
//! pondra.toml, the users CI signs in as with fresh tokens, and those tokens stored in GitHub through `gh`.

use crate::project::{maker, node, read_toml, say, server_of, Node, Toml, Where};
use anyhow::{ensure, Context, Result};
use serde_json::json;
use std::path::Path;

/// The workflow `pondra ci init` writes, in three parts: the pull requests and the merge's test job
/// (in `workflow`), then prod, which waits for test when there is one. `@VERSION@` is this pondra.
const WORKFLOW_TOP: &str = r##"# Pondra (pondra ci init): each pull request gets a branch of prod with its code applied and
# tested, and the diff on the pull request; a merge to main applies to test; an approval in
# GitHub's "prod" environment applies the same commit to prod.
name: pondra
on:
  pull_request:
    types: [opened, synchronize, reopened, closed]
  push:
    branches: [main]
concurrency: pondra-${{ github.event.pull_request.number || github.ref }}
jobs:
  pull-request:
    if: github.event_name == 'pull_request' && github.event.action != 'closed'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply pr-${{ github.event.number }} --fresh
      - run: pondra diff pr-${{ github.event.number }} >> "$GITHUB_STEP_SUMMARY"
  closed:
    if: github.event_name == 'pull_request' && github.event.action == 'closed'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra branch -d pr-${{ github.event.number }}
"##;

const WORKFLOW_TEST: &str = r##"  test:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_TEST_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply test
"##;

const WORKFLOW_PROD: &str = r##"    runs-on: ubuntu-latest
    environment: prod
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_PROD_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply prod
"##;

/// The whole workflow for this pondra; the test job and prod's `needs` only when pondra.toml has [env.test].
fn workflow(version: &str, test: bool) -> String {
    let mut y = String::from(WORKFLOW_TOP);
    if test {
        y.push_str(WORKFLOW_TEST);
    }
    y.push_str("  prod:\n    if: github.event_name == 'push'\n");
    if test {
        y.push_str("    needs: test\n");
    }
    y.push_str(WORKFLOW_PROD);
    y.replace("@VERSION@", version)
}

/// `pondra ci init`: sets CI up for the project, one step at a time, each line printed as its step is done. A
/// step that fails stops here with the steps before it done, and running it again is safe: each one does what
/// it did, or rotates what it made.
pub async fn init(at: &Where, force: bool) -> Result<()> {
    let dir = Path::new(&at.project);
    ensure!(dir.join("pondra.toml").exists(), "no pondra.toml here: pondra init first");
    write_workflow(dir, force)?;
    protect_file(dir)?;
    let toml = read_toml(dir)?;
    let tokens = ci_users(at, &toml).await.context("the users CI signs in as weren't all made (the steps above are done)")?;
    github(dir, &tokens).context("GitHub wasn't set up (the steps above are done: fix gh, or add the secrets by hand)")?;
    say("next: git add .github pondra.toml && git commit -m \"Pondra CI\" && git push\n");
    Ok(())
}

/// Step 1: .github/workflows/pondra.yml. A file that is ours already is said to be; one that differs is kept
/// (and said so) unless `force` writes ours over it.
fn write_workflow(project: &Path, force: bool) -> Result<()> {
    let path = project.join(".github/workflows/pondra.yml");
    let ours = workflow(env!("CARGO_PKG_VERSION"), read_toml(project)?.env.contains_key("test"));
    let shown = ".github/workflows/pondra.yml";
    let there = match std::fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e).with_context(|| path.display().to_string()),
    };
    match there {
        Some(text) if text == ours => say(&format!("✓ {shown} (as it was)\n")),
        Some(_) if !force => say(&format!("• kept {shown}: yours differs (--force writes ours)\n")),
        _ => {
            std::fs::create_dir_all(path.parent().unwrap_or(project))?;
            std::fs::write(&path, ours)?;
            say(&format!("✓ {shown}\n"));
        }
    }
    Ok(())
}

/// Step 2: prod protected in pondra.toml, so the first apply protects it. Said only when the file changes.
fn protect_file(project: &Path) -> Result<()> {
    let path = project.join("pondra.toml");
    let text = std::fs::read_to_string(&path).with_context(|| path.display().to_string())?;
    if let Some(edited) = protect_prod(&text) {
        std::fs::write(&path, edited)?;
        say("✓ pondra.toml: prod is protected\n");
    }
    Ok(())
}

/// pondra.toml's text with `protected = true` under `[env.prod]`, edited in place (the rest of the file stays as
/// it is): a `protected = false` there is changed, a missing key is added under the header, and a missing section
/// is appended. None when prod is protected already.
fn protect_prod(text: &str) -> Option<String> {
    let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
    let Some(head) = lines.iter().position(|l| l.trim().starts_with("[env.prod]")) else {
        let gap = if text.is_empty() || text.ends_with('\n') { "" } else { "\n" };
        return Some(format!("{text}{gap}\n[env.prod]\nprotected = true\n"));
    };
    // (the section runs to the next header: a `protected` key past it belongs to another section)
    let end = lines[head + 1..].iter().position(|l| l.trim_start().starts_with('[')).map_or(lines.len(), |i| head + 1 + i);
    let key_at = lines[head + 1..end].iter().position(|l| key_of(l) == Some("protected")).map(|i| head + 1 + i);
    match key_at {
        Some(at) => {
            let is_false = lines[at].split_once('=').is_some_and(|(_, v)| v.trim_start().starts_with("false"));
            if !is_false {
                return None;
            }
            lines[at] = lines[at].replacen("false", "true", 1);
        }
        None => lines.insert(head + 1, "protected = true".to_string()),
    }
    Some(lines.join("\n"))
}

/// The key a line of TOML sets (`protected = true` gives `protected`); None for a comment, a header or a blank line.
fn key_of(line: &str) -> Option<&str> {
    let line = line.trim_start();
    if line.starts_with('#') || line.starts_with('[') {
        return None;
    }
    line.split_once('=').map(|(key, _)| key.trim_end())
}

/// The CI tokens one `ci init` made, each for the GitHub secret of its name.
struct Tokens {
    dev: Option<String>,  // PONDRA_DEV_TOKEN: ci, on the server branches are made on
    test: Option<String>, // PONDRA_TEST_TOKEN: ci, on test's server (dev's own when test is on the same server)
    prod: String,         // PONDRA_PROD_TOKEN: ci_prod, for the prod environment
}

/// Steps 3 and 4: the users CI signs in as, each with a fresh token. ci_prod applies to prod; ci may clone prod on
/// the server branches are made on, and on test's server when that is another. A server that doesn't attach prod
/// yet (Layout B) gets no ci: its notice (step 4) says what to run there, and ci_init again after that.
async fn ci_users(at: &Where, toml: &Toml) -> Result<Tokens> {
    let prod = node(at, toml, "prod").await?;
    let prod_token = ci_user(&prod, "ci_prod").await?;
    prod.sql("GRANT APPLY ON DATABASE prod TO ci_prod").await?;
    let mut waiting = vec![]; // (the servers that don't attach prod yet)
    let dev = ci_on(at, toml, &prod, "dev", &mut waiting).await?;
    let has_test = toml.env.contains_key("test");
    let test_apart = has_test && server_of(toml, "test") != server_of(toml, "dev");
    let test = match (has_test, test_apart) {
        (false, _) => None,
        (true, false) => dev.clone(), // (on the branches' server: the same token)
        (true, true) => ci_on(at, toml, &prod, "test", &mut waiting).await?,
    };
    let mut said = vec![format!("ci_prod on {}", server_name(toml, "prod", &prod.base))];
    if dev.is_some() {
        said.push(format!("ci on {}", server_name(toml, "dev", &prod.base)));
    }
    if test_apart && test.is_some() {
        said.push(format!("ci on {}", server_name(toml, "test", &prod.base)));
    }
    say(&format!("✓ users: {}\n", said.join(", ")));
    if !waiting.is_empty() {
        link_prod(&prod, &waiting).await?;
    }
    Ok(Tokens { dev, test, prod: prod_token })
}

/// Makes `ci` on the server `env`'s branches (or test) are made on, with CLONE on prod there, and gives back its
/// token. A server that doesn't attach prod yet gets no user: it is named in `waiting` for step 4.
async fn ci_on(at: &Where, toml: &Toml, prod: &Node, env: &str, waiting: &mut Vec<String>) -> Result<Option<String>> {
    let node = maker(at, toml, env, "prod").await?;
    if node.base != prod.base && !attaches_prod(&node).await? {
        waiting.push(server_name(toml, env, &prod.base));
        return Ok(None);
    }
    let token = ci_user(&node, "ci").await?;
    node.sql("GRANT CLONE ON DATABASE prod TO ci").await?;
    Ok(Some(token))
}

/// The server an environment's users are made on, as the notices and the users line name it: its server, else
/// prod's address (a database given by its own url).
fn server_name(toml: &Toml, env: &str, prod_base: &str) -> String {
    server_of(toml, env).unwrap_or_else(|| prod_base.to_string())
}

/// Whether prod is attached on the server a node is on: its `pondra.databases` names it there.
async fn attaches_prod(node: &Node) -> Result<bool> {
    let rows = node.sql("SELECT count(*) AS n FROM pondra.databases WHERE name = 'prod'").await?;
    Ok(rows[0]["n"].as_u64().unwrap_or(0) > 0)
}

/// Makes `user` on `node` if it isn't there, and gives it a fresh token named `github`: the old one is dropped
/// first, so running ci init again rotates it. Returns the new token.
async fn ci_user(node: &Node, user: &str) -> Result<String> {
    node.sql(&format!("CREATE USER IF NOT EXISTS {user}")).await?;
    // (the first run has no token to drop: that is not an error)
    if let Err(e) = node.sql(&format!("DROP TOKEN github FOR USER {user}")).await {
        ensure!(format!("{e:#}").contains("has no token"), "{e:#}");
    }
    let made = node.sql(&format!("CREATE TOKEN github FOR USER {user}")).await?;
    Ok(made["token"].as_str().context("CREATE TOKEN gave no token")?.to_string())
}

/// Step 4 (Layout B): for each server that doesn't attach prod yet, the statements that attach it, with a token of
/// ci_link (CLONE on prod) that lets its branches clone prod. The token is printed here, and only here.
async fn link_prod(prod: &Node, waiting: &[String]) -> Result<()> {
    let token = ci_user(prod, "ci_link").await?;
    prod.sql("GRANT CLONE ON DATABASE prod TO ci_link").await?;
    let rows = prod.sql("SELECT location FROM pondra.databases WHERE name = 'prod'").await?;
    let location = rows[0]["location"].as_str().context("prod's location")?;
    let base = &prod.base; // (prod's own address: its database's URL on its server)
    for server in waiting {
        say(&format!(
            "• {server} doesn't attach prod yet. Run this there with a key of prod's bucket that only reads\n  (TYPE gcs or azure for those clouds), then pondra ci init again:\n    CREATE SECRET prod_read (TYPE s3, KEY_ID '…', SECRET '…', SCOPE '{location}');\n    CREATE SECRET prod_link (TYPE pondra, TOKEN '{token}', SCOPE '{base}');\n    ATTACH '{location}' AS prod (READ_ONLY, ENDPOINT '{base}');\n  The token is shown this once.\n"
        ));
    }
    Ok(())
}

/// Step 5: GitHub's secrets and the prod environment, set through gh when gh is signed in and names this folder's
/// repository (each value on gh's standard input). Otherwise the values are printed once, and the environment is
/// described for the person to make.
fn github(dir: &Path, tokens: &Tokens) -> Result<()> {
    let mut repo_secrets: Vec<(&str, &str)> = vec![];
    if let Some(t) = &tokens.dev {
        repo_secrets.push(("PONDRA_DEV_TOKEN", t.as_str()));
    }
    if let Some(t) = &tokens.test {
        repo_secrets.push(("PONDRA_TEST_TOKEN", t.as_str()));
    }
    let Some(repo) = repo_of(dir) else {
        let mut shown: Vec<String> = repo_secrets.iter().map(|(name, value)| format!("{name}={value}")).collect();
        shown.push(format!("PONDRA_PROD_TOKEN={}", tokens.prod));
        say(&format!("• add these to GitHub (shown once): {}\n", shown.join(" ")));
        say("  and an environment named prod (Settings → Environments → prod, with required reviewers), with PONDRA_PROD_TOKEN as its secret\n");
        return Ok(());
    };
    for &(name, value) in &repo_secrets {
        gh(dir, &["secret", "set", name], Some(value))?;
    }
    // (the reviewer is the person running this: the environment's approval is theirs)
    let me = gh(dir, &["api", "user", "-q", ".id"], None)?;
    let reviewer: u64 = me.parse().with_context(|| format!("gh's user id: {me}"))?;
    let environment = format!("repos/{repo}/environments/prod");
    let body = json!({"reviewers": [{"type": "User", "id": reviewer}]}).to_string();
    gh(dir, &["api", "-X", "PUT", environment.as_str(), "--input", "-"], Some(body.as_str()))?;
    gh(dir, &["secret", "set", "PONDRA_PROD_TOKEN", "--env", "prod"], Some(tokens.prod.as_str()))?;
    let mut names: Vec<&str> = repo_secrets.iter().map(|(name, _)| *name).collect();
    names.push("PONDRA_PROD_TOKEN (in prod)");
    say(&format!("✓ GitHub {repo}: secrets {}; environment prod, reviewed by you\n", names.join(", ")));
    Ok(())
}

/// The GitHub repository gh names for this folder, when gh is signed in to GitHub.
fn repo_of(dir: &Path) -> Option<String> {
    gh(dir, &["auth", "status"], None).ok()?;
    gh(dir, &["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"], None).ok().filter(|r| !r.is_empty())
}

/// Runs gh in `dir` with `input` on its standard input: a token goes there, never on the command line, where `ps`
/// shows it. Bails with gh's own words when it refuses.
fn gh(dir: &Path, args: &[&str], input: Option<&str>) -> Result<String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("gh")
        .args(args)
        .current_dir(dir)
        .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("gh isn't installed (https://cli.github.com), or not on PATH")?;
    if let Some(text) = input {
        child.stdin.take().context("gh's standard input")?.write_all(text.as_bytes())?;
    }
    let out = child.wait_with_output()?;
    ensure!(out.status.success(), "gh {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim());
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protect_prod_edits_the_text_in_place() {
        // no section: appended after a blank line (with or without a final newline)
        assert_eq!(protect_prod("[project]\nname = \"x\"\n").as_deref(), Some("[project]\nname = \"x\"\n\n[env.prod]\nprotected = true\n"));
        assert_eq!(protect_prod("[project]\nname = \"x\"").as_deref(), Some("[project]\nname = \"x\"\n\n[env.prod]\nprotected = true\n"));
        // a false is changed, and what follows it on the line stays
        assert_eq!(protect_prod("[env.prod]\nprotected = false  # for now\nvalues = {}\n").as_deref(), Some("[env.prod]\nprotected = true  # for now\nvalues = {}\n"));
        // already true: nothing to change
        assert_eq!(protect_prod("[env.prod]\nprotected = true\n"), None);
        // no key: added under the header, which may carry a comment of its own
        assert_eq!(protect_prod("[env.prod]  # the live one\nvalues = {}\n").as_deref(), Some("[env.prod]  # the live one\nprotected = true\nvalues = {}\n"));
        // another section's key is not prod's, and a commented-out key is not a key
        assert_eq!(protect_prod("[env.test]\nprotected = false\n# protected = false\n[env.prod]\n").as_deref(), Some("[env.test]\nprotected = false\n# protected = false\n[env.prod]\nprotected = true\n"));
    }
}
