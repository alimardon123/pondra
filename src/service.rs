//! `pondra service`: a node this machine keeps running (ADR-041), started at boot and again if it
//! stops, stopped the way a node stops (SIGTERM; its standard input closing on Windows), so a
//! leader hands its term on at once. Each OS's own manager does it: systemd, launchd, Windows's
//! service manager. Nothing of ours runs besides the node but, on Windows, the small supervisor
//! the manager talks to.
//!
//! What the node runs with is one file, `services/<name>.json` (its `serve` options, folder and
//! the PONDRA_*, AWS_*, AZURE_* and GOOGLE_* variables set when it was installed, readable by its
//! user alone). The manager starts `pondra service run --config <file>`, which becomes `pondra
//! serve …` (exec on Unix; a child it supervises on Windows), so a newer binary at the same path
//! is what runs after a restart.
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command as Run;

#[derive(clap::Subcommand)]
pub enum Command {
    /// Install it and start it: `pondra serve`'s options follow (default: `--lake ./lake`), e.g.
    /// `pondra service install --lake s3://bucket/lake --addr 0.0.0.0:8080 --pg 0.0.0.0:5432`.
    /// With sudo (an Administrator on Windows) it runs at boot, as the user who called sudo;
    /// otherwise as you, from when you log in (and at boot where systemd lets it linger).
    #[command(disable_help_flag = true)]
    Install {
        /// Its name, before the options: several nodes on one machine need one each.
        #[arg(long, default_value = "pondra")]
        name: String,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "SERVE OPTIONS")]
        serve: Vec<String>,
    },
    /// Stop it and remove it (the lake and its logs stay).
    Uninstall {
        #[arg(long, default_value = "pondra")]
        name: String,
    },
    /// Whether it runs, what it serves, the node's role, and where its logs are.
    Status {
        #[arg(long, default_value = "pondra")]
        name: String,
    },
}

/// What the node runs with: written by `install`, read by `run` and `status`.
#[derive(Serialize, Deserialize)]
struct Config {
    args: Vec<String>,
    cwd: PathBuf,
    env: BTreeMap<String, String>,
}

pub async fn command(cmd: Command) -> Result<()> {
    match cmd {
        Command::Install { name, serve } => install(&name, serve),
        Command::Uninstall { name } => uninstall(&name),
        Command::Status { name } => status(&name).await,
    }
}

fn install(name: &str, mut args: Vec<String>) -> Result<()> {
    ensure!(!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'), "a service's name is letters, digits, - and _");
    if args.iter().any(|a| a == "--help" || a == "-h") {
        use clap::CommandFactory;
        return Ok(crate::Cli::command().find_subcommand_mut("serve").expect("serve").print_help()?);
    }
    // (the options as `serve` takes them: a mistake is said now, not in a log at boot)
    let cli = crate::Cli::try_parse_from(["pondra", "serve"].into_iter().map(String::from).chain(args.iter().cloned())).map_err(|e| anyhow::anyhow!("{}", e.render()))?;
    let Some(crate::Cmd::Serve { path, dir, databases, stop_with_stdin, .. }) = cli.cmd else { unreachable!("parsed as serve") };
    ensure!(!stop_with_stdin, "leave out --stop-with-stdin: the service manager stops the node");
    let cwd = std::env::current_dir()?;
    if path.is_none() && dir.is_none() && databases.is_none() {
        args.splice(0..0, ["--lake".to_string(), cwd.join("lake").to_string_lossy().into_owned()]);
    }
    let kept = ["PONDRA_", "AWS_", "AZURE_", "GOOGLE_"];
    let env: BTreeMap<_, _> = std::env::vars().filter(|(k, v)| kept.iter().any(|p| k.starts_with(p)) && !["PONDRA_OWNER_KEY", "PONDRA_SUPERVISED"].contains(&k.as_str()) && !v.contains('\n')).collect();
    let config = Config { args, cwd, env };
    let exe = std::env::current_exe()?.canonicalize()?;
    let file = config_file(name, system())?;
    write_config(&file, &config)?;
    let how = os::install(name, &exe, &file)?;
    println!("{name}: installed and started, {how}");
    println!("  pondra serve {}", config.args.join(" "));
    if !config.env.is_empty() {
        println!("  with {} (kept in {}, readable by the service's user alone)", config.env.keys().cloned().collect::<Vec<_>>().join(", "), file.display());
    }
    println!("  `pondra service status --name {name}` says how it is; `pondra service uninstall --name {name}` removes it");
    Ok(())
}

fn uninstall(name: &str) -> Result<()> {
    let file = installed(name)?;
    os::uninstall(name, &file)?;
    std::fs::remove_file(&file).with_context(|| format!("removing {}", file.display()))?;
    println!("{name}: stopped and removed (its lake and logs stay)");
    Ok(())
}

async fn status(name: &str) -> Result<()> {
    let file = installed(name)?;
    let config: Config = serde_json::from_slice(&std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?)?;
    let (state, logs) = os::status(name, &file)?;
    println!("{name}: {state}");
    println!("  pondra serve {}", config.args.join(" "));
    // (the node itself: from this machine its HTTP port answers plainly, TLS or not)
    let addr = config.args.iter().position(|a| a == "--addr").and_then(|i| config.args.get(i + 1)).map_or("127.0.0.1:8080", |a| a.as_str());
    let port = addr.rsplit(':').next().unwrap_or("8080");
    let asked = reqwest::Client::new().get(format!("http://127.0.0.1:{port}/stats")).timeout(std::time::Duration::from_secs(3)).send().await;
    match asked {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(s) => println!("  node: {} of term {} (leader {}), {} node(s) in the cluster", s["role"].as_str().unwrap_or("?"), s["term"], s["leader"].as_str().unwrap_or("?"), s["nodes"].as_array().map_or(0, |n| n.len())),
            Err(_) => println!("  node: answers on {port}"),
        },
        Err(_) => println!("  node: not answering on port {port} yet"),
    }
    println!("  logs: {logs}");
    Ok(())
}

/// `pondra service run --config FILE`, as the manager starts it: becomes the node. Called from
/// `main` before anything else (Windows's dispatcher wants the main thread).
pub fn run_from_manager() -> ! {
    let args: Vec<String> = std::env::args().collect();
    let file = args.iter().position(|a| a == "--config").and_then(|i| args.get(i + 1)).cloned().unwrap_or_default();
    let err = os::run(Path::new(&file)).err().unwrap_or_else(|| anyhow::anyhow!("stopped"));
    eprintln!("pondra service run: {err:#}");
    std::process::exit(1)
}

fn read_config(file: &Path) -> Result<Config> {
    Ok(serde_json::from_slice(&std::fs::read(file).with_context(|| format!("reading {}", file.display()))?)?)
}

/// The command a service's node runs: `pondra serve` with its options, folder and variables.
fn serve(exe: &Path, config: &Config) -> Run {
    let mut cmd = Run::new(exe);
    cmd.arg("serve").args(&config.args).envs(&config.env).current_dir(&config.cwd);
    cmd
}

/// A machine-wide service (root, an Administrator) or the user's own.
fn system() -> bool {
    #[cfg(unix)]
    return unsafe { libc::geteuid() } == 0;
    #[cfg(not(unix))]
    true
}

fn config_file(name: &str, system: bool) -> Result<PathBuf> {
    let dir = match (cfg!(windows), system) {
        (true, _) => PathBuf::from(std::env::var_os("ProgramData").context("no %ProgramData%")?).join("pondra"),
        (false, true) => PathBuf::from("/etc/pondra"),
        (false, false) => PathBuf::from(std::env::var_os("HOME").context("no $HOME")?).join(".config").join("pondra"),
    };
    Ok(dir.join("services").join(format!("{name}.json")))
}

/// The installed service's file: the machine's, else the user's.
fn installed(name: &str) -> Result<PathBuf> {
    for system in [true, false] {
        match config_file(name, system) {
            Ok(f) if f.exists() => return Ok(f),
            _ => {}
        }
    }
    bail!("no service named {name} here (`pondra service install --name {name} …` makes it)")
}

/// The user a machine-wide service runs as on Unix: whoever called sudo (`SUDO_USER`), never root
/// unless root asked directly.
#[cfg(unix)]
fn sudo_user() -> Option<(String, u32, u32)> {
    let user = std::env::var("SUDO_USER").ok().filter(|u| u != "root")?;
    let id = |k: &str| std::env::var(k).ok()?.parse().ok();
    Some((user, id("SUDO_UID")?, id("SUDO_GID")?))
}

fn write_config(file: &Path, config: &Config) -> Result<()> {
    let dir = file.parent().expect("a folder");
    std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
    let text = serde_json::to_vec_pretty(config)?;
    let new = file.with_extension("json.new");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    std::io::Write::write_all(&mut options.open(&new)?, &text)?;
    #[cfg(unix)]
    if let (true, Some((_, uid, gid))) = (system(), sudo_user()) {
        std::os::unix::fs::chown(&new, Some(uid), Some(gid))?; // (its user reads it: `run` does, as that user)
    }
    std::fs::rename(&new, file)?;
    #[cfg(windows)]
    {
        // (SYSTEM and Administrators only: it may hold tokens and keys)
        let ok = Run::new("icacls").arg(file).args(["/inheritance:r", "/grant:r", "*S-1-5-18:F", "*S-1-5-32-544:F"]).output()?.status.success();
        ensure!(ok, "icacls couldn't keep {} to SYSTEM and Administrators", file.display());
    }
    Ok(())
}

/// Run a manager's command, saying what it said if it failed.
#[cfg(unix)]
fn manage(cmd: &mut Run) -> Result<String> {
    let out = cmd.output().with_context(|| format!("running {:?}", cmd.get_program()))?;
    let said = String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    ensure!(out.status.success(), "{} {}: {}", cmd.get_program().to_string_lossy(), cmd.get_args().map(|a| a.to_string_lossy()).collect::<Vec<_>>().join(" "), said.trim());
    Ok(said)
}

#[cfg(target_os = "linux")]
mod os {
    use super::*;

    fn unit(name: &str, system: bool) -> Result<PathBuf> {
        Ok(match system {
            true => PathBuf::from("/etc/systemd/system"),
            false => PathBuf::from(std::env::var_os("HOME").context("no $HOME")?).join(".config/systemd/user"),
        }
        .join(format!("{name}.service")))
    }

    fn systemctl(system: bool) -> Run {
        let mut c = Run::new("systemctl");
        if !system {
            c.arg("--user");
        }
        c
    }

    /// systemd's quoting: each word in double quotes, `\` and `"` escaped, `%` and `$` doubled.
    fn quoted(s: &str) -> String { format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%").replace('$', "$$")) }

    pub fn install(name: &str, exe: &Path, file: &Path) -> Result<String> {
        let system = system();
        let path = unit(name, system)?;
        let start = [exe.to_string_lossy().as_ref(), "service", "run", "--config", file.to_string_lossy().as_ref()].map(quoted).join(" ");
        let user = sudo_user().filter(|_| system).map(|(u, ..)| format!("User={u}\n")).unwrap_or_default();
        let text = format!(
            "# Made by `pondra service install` (ADR-041); `pondra service uninstall --name {name}` removes it.\n\
             [Unit]\nDescription=Pondra node ({name})\nDocumentation=https://alimardon123.github.io/pondra/guides/deploy/\n\
             After=network-online.target\nWants=network-online.target\n\n\
             [Service]\nExecStart={start}\n{user}Restart=always\nRestartSec=2\n\
             # (SIGTERM: a leader hands its term on at once; acknowledged writes are already durable)\n\
             TimeoutStopSec=60\nLimitNOFILE=1048576\n{}\n\
             [Install]\nWantedBy={}\n",
            if system { "NoNewPrivileges=yes\n" } else { "" },
            if system { "multi-user.target" } else { "default.target" },
        );
        std::fs::create_dir_all(path.parent().expect("a folder"))?;
        std::fs::write(&path, text)?;
        manage(systemctl(system).arg("daemon-reload"))?;
        manage(systemctl(system).args(["enable", name]))?;
        manage(systemctl(system).args(["restart", name]))?;
        if system {
            return Ok(format!("a system service ({}), run as {}, at boot", path.display(), sudo_user().map_or("root".into(), |u| u.0)));
        }
        // (a user's services run while they're logged in, unless systemd lets them linger)
        let user = std::env::var("USER").unwrap_or_default();
        let lingering = || Run::new("loginctl").args(["show-user", &user, "-p", "Linger", "--value"]).output().is_ok_and(|o| o.stdout.starts_with(b"yes"));
        let lingers = lingering() || (Run::new("loginctl").args(["enable-linger", &user]).output().is_ok_and(|o| o.status.success()) && lingering());
        Ok(format!("a user service ({}), {}", path.display(), match lingers {
            true => "at boot too (lingering)",
            false => "while you're logged in (`sudo loginctl enable-linger $USER` for boot; or install it with sudo)",
        }))
    }

    pub fn uninstall(name: &str, file: &Path) -> Result<()> {
        let system = file.starts_with("/etc");
        let _ = systemctl(system).args(["disable", "--now", name]).output();
        let path = unit(name, system)?;
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        manage(systemctl(system).arg("daemon-reload")).map(drop)
    }

    pub fn status(name: &str, file: &Path) -> Result<(String, String)> {
        let system = file.starts_with("/etc");
        let shown = manage(systemctl(system).args(["show", name, "-p", "ActiveState,SubState,MainPID,User"]))?;
        let field = |k: &str| shown.lines().find_map(|l| l.strip_prefix(&format!("{k}="))).unwrap_or_default().to_string();
        let pid = field("MainPID");
        let state = format!("{} ({}){}, a {} service{}", field("ActiveState"), field("SubState"), if pid != "0" { format!(", pid {pid}") } else { String::new() },
                            if system { "system" } else { "user" }, Some(field("User")).filter(|u| !u.is_empty()).map_or(String::new(), |u| format!(", as {u}")));
        Ok((state, format!("journalctl {}-u {name}", if system { "" } else { "--user " })))
    }

    pub fn run(file: &Path) -> Result<()> {
        use std::os::unix::process::CommandExt;
        let config = read_config(file)?;
        Err(serve(&std::env::current_exe()?, &config).exec().into()) // (only returns if it couldn't start)
    }
}

#[cfg(target_os = "macos")]
mod os {
    use super::*;

    fn home() -> Result<PathBuf> { Ok(PathBuf::from(std::env::var_os("HOME").context("no $HOME")?)) }

    /// launchd's file, domain and log of a service: a daemon (root) or the user's agent.
    fn places(name: &str, system: bool) -> Result<(PathBuf, String, PathBuf)> {
        Ok(match system {
            true => (PathBuf::from(format!("/Library/LaunchDaemons/{name}.plist")), "system".into(), PathBuf::from(format!("/Library/Logs/pondra/{name}.log"))),
            false => {
                let uid = unsafe { libc::getuid() };
                // (the logged-in user's domain; over ssh with nobody logged in, the user's background one)
                let gui = Run::new("launchctl").args(["print", &format!("gui/{uid}")]).output().is_ok_and(|o| o.status.success());
                (home()?.join(format!("Library/LaunchAgents/{name}.plist")), format!("{}/{uid}", if gui { "gui" } else { "user" }), home()?.join(format!("Library/Logs/pondra/{name}.log")))
            }
        })
    }

    fn xml(s: &str) -> String { s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;") }

    pub fn install(name: &str, exe: &Path, file: &Path) -> Result<String> {
        let system = system();
        let (plist, domain, log) = places(name, system)?;
        let program = [exe.to_string_lossy().as_ref(), "service", "run", "--config", file.to_string_lossy().as_ref()].map(|a| format!("<string>{}</string>", xml(a))).join("");
        let user = sudo_user().filter(|_| system);
        std::fs::create_dir_all(log.parent().expect("a folder"))?;
        std::fs::OpenOptions::new().create(true).append(true).open(&log)?;
        if let Some((_, uid, gid)) = &user {
            std::os::unix::fs::chown(&log, Some(*uid), Some(*gid))?; // (the node writes it as its user)
        }
        let text = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
             <!-- Made by `pondra service install` (ADR-041); `pondra service uninstall --name {name}` removes it. -->\n\
             <plist version=\"1.0\"><dict>\n<key>Label</key><string>{name}</string>\n<key>ProgramArguments</key><array>{program}</array>\n{}\
             <key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n<key>ThrottleInterval</key><integer>2</integer>\n\
             <key>ExitTimeOut</key><integer>60</integer>\n<key>SoftResourceLimits</key><dict><key>NumberOfFiles</key><integer>65536</integer></dict>\n\
             <key>StandardOutPath</key><string>{}</string>\n<key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",
            user.as_ref().map_or(String::new(), |(u, ..)| format!("<key>UserName</key><string>{}</string>\n", xml(u))),
            xml(&log.to_string_lossy()), xml(&log.to_string_lossy()),
        );
        let _ = Run::new("launchctl").args(["bootout", &format!("{domain}/{name}")]).output(); // (installed again: the new settings)
        std::fs::create_dir_all(plist.parent().expect("a folder"))?;
        std::fs::write(&plist, text)?;
        manage(Run::new("launchctl").args(["enable", &format!("{domain}/{name}")]))?;
        manage(Run::new("launchctl").arg("bootstrap").arg(&domain).arg(&plist))?;
        Ok(match system {
            true => format!("a launchd daemon ({}), run as {}, at boot", plist.display(), user.map_or("root".into(), |u| u.0)),
            false => format!("a launchd agent ({}), while you're logged in (install it with sudo for boot)", plist.display()),
        })
    }

    pub fn uninstall(name: &str, file: &Path) -> Result<()> {
        let (plist, domain, _) = places(name, file.starts_with("/etc"))?;
        let _ = Run::new("launchctl").args(["bootout", &format!("{domain}/{name}")]).output();
        if plist.exists() {
            std::fs::remove_file(&plist)?;
        }
        Ok(())
    }

    pub fn status(name: &str, file: &Path) -> Result<(String, String)> {
        let system = file.starts_with("/etc");
        let (_, domain, log) = places(name, system)?;
        let shown = Run::new("launchctl").args(["print", &format!("{domain}/{name}")]).output()?;
        let text = String::from_utf8_lossy(&shown.stdout);
        let field = |k: &str| text.lines().find_map(|l| l.trim().strip_prefix(&format!("{k} = "))).map(str::to_string);
        let state = match shown.status.success() {
            true => format!("{}{}, a launchd {}", field("state").unwrap_or_else(|| "loaded".into()), field("pid").map_or(String::new(), |p| format!(", pid {p}")), if system { "daemon" } else { "agent" }),
            false => "installed, not loaded (`pondra service install` again loads it)".into(),
        };
        Ok((state, log.display().to_string()))
    }

    pub fn run(file: &Path) -> Result<()> {
        use std::os::unix::process::CommandExt;
        let config = read_config(file)?;
        Err(serve(&std::env::current_exe()?, &config).exec().into())
    }
}

#[cfg(windows)]
mod os {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{mpsc, OnceLock};
    use std::time::{Duration, Instant};
    use windows_service::service::{
        ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode, ServiceFailureActions,
        ServiceFailureResetPeriod, ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    const ACCESS: ServiceAccess = ServiceAccess::QUERY_STATUS.union(ServiceAccess::START).union(ServiceAccess::STOP).union(ServiceAccess::CHANGE_CONFIG).union(ServiceAccess::DELETE);

    fn manager(access: ServiceManagerAccess) -> Result<ServiceManager> {
        ServiceManager::local_computer(None::<&str>, access).context("the service manager refused: run it from a terminal opened as Administrator")
    }

    fn log_file(name: &str) -> Result<PathBuf> { Ok(config_file(name, true)?.parent().and_then(Path::parent).expect("pondra's folder").join("logs").join(format!("{name}.log"))) }

    fn stop(service: &windows_service::service::Service) {
        if service.stop().is_ok() {
            let deadline = Instant::now() + Duration::from_secs(70);
            while Instant::now() < deadline && service.query_status().is_ok_and(|s| s.current_state != ServiceState::Stopped) {
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }

    pub fn install(name: &str, exe: &Path, file: &Path) -> Result<String> {
        let manager = manager(ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE)?;
        let info = ServiceInfo {
            name: name.into(),
            display_name: format!("Pondra ({name})").into(),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: exe.to_path_buf(),
            launch_arguments: ["service", "run", "--config"].into_iter().map(OsString::from).chain([file.as_os_str().to_owned()]).collect(),
            dependencies: vec![],
            account_name: None, // (LocalSystem)
            account_password: None,
        };
        let service = match manager.open_service(name, ACCESS) {
            Ok(s) => {
                stop(&s); // (installed again: the new settings)
                s.change_config(&info)?;
                s
            }
            Err(_) => manager.create_service(&info, ACCESS)?,
        };
        service.set_description("A Pondra node (pondra service install). Its settings: services\\<name>.json in %ProgramData%\\pondra.")?;
        let restart = ServiceAction { action_type: ServiceActionType::Restart, delay: Duration::from_secs(5) };
        service.update_failure_actions(ServiceFailureActions { reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(86400)), reboot_msg: None, command: None, actions: Some(vec![restart; 3]) })?;
        service.set_preshutdown_timeout(Duration::from_secs(60))?; // (Windows shutting down waits for the node to hand on)
        service.start(&[] as &[&str])?;
        Ok(format!("a Windows service (Services: \"Pondra ({name})\"), run as LocalSystem, at boot"))
    }

    pub fn uninstall(name: &str, _file: &Path) -> Result<()> {
        let service = manager(ServiceManagerAccess::CONNECT)?.open_service(name, ACCESS)?;
        stop(&service);
        Ok(service.delete()?)
    }

    pub fn status(name: &str, _file: &Path) -> Result<(String, String)> {
        let service = manager(ServiceManagerAccess::CONNECT)?.open_service(name, ServiceAccess::QUERY_STATUS)?;
        let s = service.query_status()?;
        Ok((format!("{:?}{}, a Windows service", s.current_state, s.process_id.map_or(String::new(), |p| format!(", supervisor pid {p}"))), log_file(name)?.display().to_string()))
    }

    static CONFIG: OnceLock<PathBuf> = OnceLock::new();
    windows_service::define_windows_service!(service_main, from_dispatcher);

    pub fn run(file: &Path) -> Result<()> {
        let _ = CONFIG.set(file.to_path_buf());
        let name = file.file_stem().unwrap_or_default().to_owned();
        windows_service::service_dispatcher::start(&name, service_main).context("this is for Windows's service manager (`pondra service install`); run `pondra serve …` yourself")
    }

    fn from_dispatcher(_: Vec<OsString>) {
        let file = CONFIG.get().cloned().unwrap_or_default();
        if let Err(e) = supervise(&file) {
            let name = file.file_stem().unwrap_or_default().to_string_lossy().into_owned();
            if let Ok(log) = log_file(&name) {
                let _ = std::fs::OpenOptions::new().create(true).append(true).open(log).and_then(|mut f| std::io::Write::write_all(&mut f, format!("pondra service: {e:#}\n").as_bytes()));
            }
        }
    }

    /// The node as a child of this process: started, started again whenever it ends on its own
    /// (a node that must rejoin its cluster ends to be started again: `cluster::restart`), and
    /// stopped by closing its standard input, as a node started by another program is.
    fn supervise(file: &Path) -> Result<()> {
        let name = file.file_stem().unwrap_or_default().to_string_lossy().into_owned();
        let (stop_tx, stop_rx) = mpsc::channel();
        let handle = service_control_handler::register(&name, move |c| match c {
            ServiceControl::Stop | ServiceControl::Preshutdown | ServiceControl::Shutdown => {
                let _ = stop_tx.send(());
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        })?;
        let set = |state, accept, wait: u64| handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS, current_state: state, controls_accepted: accept, exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0, wait_hint: Duration::from_secs(wait), process_id: None,
        });
        set(ServiceState::Running, ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN, 0)?;
        let (config, exe, log) = (read_config(file)?, std::env::current_exe()?, log_file(&name)?);
        std::fs::create_dir_all(log.parent().expect("a folder"))?;
        let mut pause = Duration::from_secs(2);
        loop {
            if std::fs::metadata(&log).is_ok_and(|m| m.len() > 64 << 20) {
                let _ = std::fs::rename(&log, log.with_extension("log.1")); // (one old log kept)
            }
            let out = std::fs::OpenOptions::new().create(true).append(true).open(&log)?;
            let started = Instant::now();
            let mut child = serve(&exe, &config).arg("--stop-with-stdin").env("PONDRA_SUPERVISED", "1")
                .stdin(std::process::Stdio::piped()).stdout(out.try_clone()?).stderr(out).spawn()?;
            let stopping = loop {
                if stop_rx.recv_timeout(Duration::from_millis(500)).is_ok() {
                    break true;
                }
                if child.try_wait()?.is_some() {
                    break false;
                }
            };
            if stopping {
                set(ServiceState::StopPending, ServiceControlAccept::empty(), 60)?;
                drop(child.stdin.take()); // (the node stops: a leader hands its term on)
                let deadline = Instant::now() + Duration::from_secs(55);
                while child.try_wait()?.is_none() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(200));
                }
                let _ = child.kill();
                return Ok(set(ServiceState::Stopped, ServiceControlAccept::empty(), 0)?);
            }
            // (ended on its own: again, at once if it ran a while, slower each time if it can't start)
            pause = if started.elapsed() > Duration::from_secs(60) { Duration::from_secs(2) } else { (pause * 2).min(Duration::from_secs(60)) };
            if stop_rx.recv_timeout(pause).is_ok() {
                return Ok(set(ServiceState::Stopped, ServiceControlAccept::empty(), 0)?);
            }
        }
    }
}
