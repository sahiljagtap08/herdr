//! A second server must not share a running server's session data directory.
#![cfg(unix)]

pub mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct TestBase {
    base: PathBuf,
    config_home: PathBuf,
    runtime_dir: PathBuf,
    data_dir: PathBuf,
}

impl TestBase {
    fn new(tag: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = PathBuf::from(format!(
            "/tmp/herdr-datalock-{tag}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&base).unwrap();
        let base = fs::canonicalize(base).unwrap();
        let config_home = base.join("config");
        let runtime_dir = base.join("runtime");
        let data_dir = config_home.join(if cfg!(debug_assertions) {
            "herdr-dev"
        } else {
            "herdr"
        });
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        fs::write(
            data_dir.join("config.toml"),
            "onboarding = false\n[terminal]\ndefault_shell = \"/bin/sh\"\n",
        )
        .unwrap();
        support::register_runtime_dir(&runtime_dir);
        Self {
            base,
            config_home,
            runtime_dir,
            data_dir,
        }
    }

    fn spawn_server(&self, socket_path: &Path, session: Option<&str>) -> Child {
        let mut command = Command::new(env!("CARGO_BIN_EXE_herdr"));
        command
            .arg("server")
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("XDG_RUNTIME_DIR", &self.runtime_dir)
            .env("HERDR_SOCKET_PATH", socket_path)
            .env("SHELL", "/bin/sh")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .env_remove("HERDR_STARTUP_CWD")
            .env_remove("HERDR_SESSION")
            .env_remove("HERDR_ENV")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(session) = session {
            command.env("HERDR_SESSION", session);
        }
        let child = command.spawn().unwrap();
        support::register_spawned_herdr_pid(Some(child.id()));
        child
    }
}

impl Drop for TestBase {
    fn drop(&mut self) {
        support::cleanup_test_base(&self.base);
    }
}

struct RunningServer(Child);

impl Drop for RunningServer {
    fn drop(&mut self) {
        let pid = self.0.id();
        let _ = self.0.kill();
        let _ = self.0.wait();
        support::unregister_spawned_herdr_pid(Some(pid));
    }
}

fn request(socket_path: &Path, method: &str, params: serde_json::Value) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    writeln!(
        stream,
        "{}",
        serde_json::json!({"id": "lock", "method": method, "params": params})
    )
    .unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    let response: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert!(response.get("error").is_none(), "{method}: {response}");
    response
}

fn workspace_labels(socket_path: &Path) -> Vec<String> {
    request(socket_path, "workspace.list", serde_json::json!({}))["result"]["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|workspace| workspace["label"].as_str().unwrap().to_owned())
        .collect()
}

fn session_labels(data_dir: &Path) -> Vec<String> {
    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(data_dir.join("session.json")).unwrap()).unwrap();
    snapshot["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|workspace| workspace["custom_name"].as_str().unwrap().to_owned())
        .collect()
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "second server did not exit; it would be sharing the first server's session.json"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn second_server_on_the_same_data_dir_refuses_to_start() {
    let base = TestBase::new("refuse");
    let first_socket = base.runtime_dir.join("first.sock");
    let second_socket = base.runtime_dir.join("second.sock");
    let project = base.base.join("project");
    fs::create_dir_all(&project).unwrap();

    let _first = RunningServer(base.spawn_server(&first_socket, None));
    support::wait_for_socket(&first_socket, Duration::from_secs(5));
    request(
        &first_socket,
        "workspace.create",
        serde_json::json!({"cwd": project, "label": "first-only"}),
    );
    // Autosave writes session.json shortly after the workspace is created.
    support::wait_for_file(&base.data_dir.join("session.json"), Duration::from_secs(20));
    assert_eq!(session_labels(&base.data_dir), ["first-only"]);
    assert!(base.data_dir.join("server.lock").exists());

    let mut second = base.spawn_server(&second_socket, None);
    let status = wait_for_exit(&mut second, Duration::from_secs(10));
    let mut stderr = String::new();
    second
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    support::unregister_spawned_herdr_pid(Some(second.id()));

    assert_eq!(status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("already owns the session data"),
        "stderr: {stderr}"
    );
    assert!(
        !second_socket.exists(),
        "the refused server must not leave its api socket behind"
    );

    // The running server and its persisted session are untouched.
    assert_eq!(workspace_labels(&first_socket), ["first-only"]);
    assert_eq!(session_labels(&base.data_dir), ["first-only"]);
}

#[test]
fn a_named_session_still_starts_next_to_the_default_server() {
    let base = TestBase::new("named");
    let first_socket = base.runtime_dir.join("first.sock");
    let named_socket = base.runtime_dir.join("named.sock");

    let _first = RunningServer(base.spawn_server(&first_socket, None));
    support::wait_for_socket(&first_socket, Duration::from_secs(5));

    // Named sessions own their own data directory, so the lock must not
    // treat them as a second owner of the default session.
    let _named = RunningServer(base.spawn_server(&named_socket, Some("other")));
    support::wait_for_socket(&named_socket, Duration::from_secs(5));
    request(&named_socket, "workspace.list", serde_json::json!({}));
    assert!(base
        .data_dir
        .join("sessions")
        .join("other")
        .join("server.lock")
        .exists());
}
