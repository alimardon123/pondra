//! What the node tests share: a free port, and a leader in a process of its own (the pondra program).
#![allow(dead_code)] // (each test file uses what it needs)
use std::path::Path;
use std::process::{Child, Command, Stdio};

/// A free `host:port` on this machine: the port is bound and let go, so a node can have it next.
pub fn free_addr() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    format!("127.0.0.1:{port}")
}

/// A leader in a process of its own, as `pondra serve --lake` starts one. Its standard input stays
/// open to it (nothing reads it: the node isn't given `--stop-with-stdin`). Killed when dropped, so a
/// check that fails leaves no node running.
pub struct Leader(pub Child);

impl Leader {
    pub fn start(lake: &str, addr: &str, log: &Path) -> Leader {
        let log = std::fs::File::create(log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_pondra"))
            .args(["serve", "--lake", lake, "--addr", addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("the pondra program starts");
        Leader(child)
    }

    /// SIGKILL, as a machine losing the process: nothing is said to the lake, and the process is reaped.
    pub fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        self.kill();
    }
}
