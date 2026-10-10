use std::path::Path;
use std::process::{Child, Command, ExitStatus};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::oneshot;

use crate::LaunchConfig;
use crate::topology::TopologyLease;

const POLL: Duration = Duration::from_millis(10);
const KILL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub struct ExitReport {
    pub status: ExitStatus,
    pub forced: bool,
}

type Outcome = std::result::Result<ExitReport, String>;

/// Owns one foreground RustFS child, not arbitrary processes or descendants.
/// Dropping this handle requests shutdown; the supervisor retains storage ownership.
pub struct RunningRustfs {
    pid: u32,
    stop: Option<Sender<()>>,
    completion: oneshot::Receiver<Outcome>,
    outcome: Option<Outcome>,
    shutdown_timeout: Duration,
}

impl RunningRustfs {
    /// Returns once the OS has spawned the child, not when RustFS is ready.
    /// Observe `wait` for startup/runtime exits; readiness is a separate native probe.
    pub async fn start(config: LaunchConfig) -> Result<Self> {
        Self::start_with(config, LaunchConfig::command).await
    }

    async fn start_with(
        config: LaunchConfig,
        command: impl FnOnce(&LaunchConfig, &Path) -> Command + Send + 'static,
    ) -> Result<Self> {
        config.validate()?;
        let shutdown_timeout = config.shutdown_grace + KILL_TIMEOUT + Duration::from_secs(1);
        let (stop, requests) = mpsc::channel();
        let (started, startup) = oneshot::channel();
        let (complete, completion) = oneshot::channel();
        std::thread::Builder::new()
            .name("rustfs-supervisor".into())
            .spawn(move || {
                let mut started = Some(started);
                let result = run(config, command, requests, &mut started)
                    .map_err(|error| format!("{error:#}"));
                if let Some(started) = started {
                    let error = result
                        .as_ref()
                        .expect_err("startup did not complete")
                        .clone();
                    if started.send(Err(error)).is_err() {
                        tracing::debug!("RustFS start caller cancelled");
                    }
                }
                if let Err(error) = &result {
                    tracing::error!(%error, "RustFS supervisor failed");
                }
                if complete.send(result).is_err() {
                    tracing::debug!("RustFS owner no longer waiting for exit");
                }
            })
            .context("starting RustFS supervisor thread")?;
        let mut process = Self {
            pid: 0,
            stop: Some(stop),
            completion,
            outcome: None,
            shutdown_timeout,
        };
        process.pid = startup
            .await
            .context("RustFS supervisor stopped during startup")?
            .map_err(anyhow::Error::msg)?;
        Ok(process)
    }

    pub fn id(&self) -> u32 {
        self.pid
    }

    pub fn observed_exit(&mut self) -> Result<Option<ExitReport>> {
        if self.outcome.is_none() {
            match self.completion.try_recv() {
                Ok(outcome) => self.outcome = Some(outcome),
                Err(oneshot::error::TryRecvError::Empty) => return Ok(None),
                Err(error) => {
                    self.outcome = Some(Err(format!("RustFS supervisor lost: {error}")));
                }
            }
        }
        self.outcome
            .as_ref()
            .expect("exit outcome recorded")
            .clone()
            .map(Some)
            .map_err(anyhow::Error::msg)
    }

    /// A service exit without an explicit shutdown request is always an error.
    pub async fn wait(&mut self) -> Result<ExitReport> {
        if self.outcome.is_none() {
            self.outcome = Some(
                (&mut self.completion)
                    .await
                    .unwrap_or_else(|error| Err(format!("RustFS supervisor lost: {error}"))),
            );
        }
        self.outcome
            .as_ref()
            .expect("outcome recorded")
            .clone()
            .map_err(anyhow::Error::msg)
    }

    pub async fn shutdown(&mut self) -> Result<ExitReport> {
        self.request_stop();
        tokio::time::timeout(self.shutdown_timeout, self.wait())
            .await
            .context("RustFS shutdown deadline exceeded; process ownership remains unresolved")?
    }

    fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take()
            && stop.send(()).is_err()
        {
            tracing::debug!("RustFS supervisor already finished; retaining its exit result");
        }
    }
}

impl Drop for RunningRustfs {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn run(
    config: LaunchConfig,
    command: impl FnOnce(&LaunchConfig, &Path) -> Command,
    requests: Receiver<()>,
    started: &mut Option<oneshot::Sender<std::result::Result<u32, String>>>,
) -> Result<ExitReport> {
    let binary = config.binary.verify()?;
    let lease = TopologyLease::acquire(&config.state_directory, &config.topology)?;
    let mut command = command(&config, &binary);
    lease.inherit_lock(&mut command)?;
    if requests.try_recv() != Err(mpsc::TryRecvError::Empty) {
        bail!("RustFS startup cancelled before launch");
    }
    lease.begin_process()?;
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            lease.finish_process()?;
            return Err(error).context("spawning pinned RustFS executable");
        }
    };
    let mut child = OwnedChild(child);
    if started
        .take()
        .expect("startup sender present")
        .send(Ok(child.0.id()))
        .is_err()
    {
        tracing::debug!("RustFS start caller cancelled after spawn; stopping owned child");
    }
    let (report, requested) = supervise(&mut child.0, requests, config.shutdown_grace)?;
    lease.finish_process()?;
    if !requested {
        bail!("RustFS exited unexpectedly with {}", report.status);
    }
    Ok(report)
}

fn supervise(
    child: &mut Child,
    requests: Receiver<()>,
    grace: Duration,
) -> Result<(ExitReport, bool)> {
    let mut stopping = None;
    let mut forced_at = None;
    loop {
        if let Some(status) = child.try_wait().context("observing owned RustFS child")? {
            return Ok((
                ExitReport {
                    status,
                    forced: forced_at.is_some(),
                },
                stopping.is_some(),
            ));
        }
        if stopping.is_none() {
            match requests.recv_timeout(POLL) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                    stopping = Some(Instant::now());
                    if !terminate(child)? {
                        forced_at = Some(Instant::now());
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
        } else {
            if let Some(killed) = forced_at {
                if Instant::now().duration_since(killed) >= KILL_TIMEOUT {
                    bail!("owned RustFS child did not exit after forced shutdown");
                }
            } else if stopping.is_some_and(|time| time.elapsed() >= grace) {
                child
                    .kill()
                    .context("forcing owned RustFS child shutdown")?;
                forced_at = Some(Instant::now());
            }
            std::thread::sleep(POLL);
        }
    }
}

fn terminate(child: &mut Child) -> Result<bool> {
    #[cfg(unix)]
    {
        let raw = i32::try_from(child.id()).context("invalid owned child PID")?;
        let pid = rustix::process::Pid::from_raw(raw).context("invalid owned child PID")?;
        // The unreaped Child retains this PID; it cannot refer to a replacement process.
        match rustix::process::kill_process(pid, rustix::process::Signal::TERM) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(true),
            Err(error) => Err(error).context("terminating owned RustFS child"),
        }
    }
    #[cfg(not(unix))]
    {
        child.kill().context("terminating owned RustFS child")?;
        Ok(false)
    }
}

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        match self.0.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => tracing::error!(%error, "Could not confirm owned RustFS exit"),
        }
        // Error paths leave the durable active marker in place, preventing unsafe reuse.
        if let Err(error) = self.0.kill() {
            tracing::error!(%error, "Could not kill unresolved owned RustFS child");
        }
        let deadline = Instant::now() + KILL_TIMEOUT;
        while Instant::now() < deadline {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(POLL),
                Err(error) => {
                    tracing::error!(%error, "Could not reap unresolved owned RustFS child");
                    return;
                }
            }
        }
        tracing::error!("Owned RustFS child did not exit during error cleanup");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryPin, CredentialFiles, Topology};
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::process::Stdio;

    fn config(root: &Path) -> LaunchConfig {
        for directory in ["state", "data"] {
            fs::create_dir(root.join(directory)).unwrap();
        }
        fs::write(root.join("access"), "fixture-access").unwrap();
        fs::write(root.join("secret"), "fixture-secret-not-for-deployment").unwrap();
        let binary = std::env::current_exe().unwrap();
        let hash = crate::config::hex_digest(&Sha256::digest(fs::read(&binary).unwrap()));
        LaunchConfig {
            binary: BinaryPin::new(binary, &hash).unwrap(),
            credentials: CredentialFiles {
                access_key: root.join("access"),
                secret_key: root.join("secret"),
            },
            state_directory: root.join("state"),
            topology: Topology {
                pools: vec![root.join("data").to_str().unwrap().into()],
                local_node: None,
                erasure_set_drive_count: None,
            },
            address: "127.0.0.1:19000".parse().unwrap(),
            native_tls: None,
            shutdown_grace: Duration::from_millis(100),
        }
    }

    fn fixture_command(config: &LaunchConfig, binary: &Path, mode: &str) -> Command {
        let mut command = Command::new(binary);
        command
            .args(["--exact", "process::tests::child_fixture", "--nocapture"])
            .env("KUBERIC_RUSTFS_TEST_CHILD", mode)
            .env("KUBERIC_RUSTFS_TEST_ROOT", &config.state_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    async fn ready(root: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !root.join("state").join("fixture-ready").exists() {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn child_fixture() {
        let Ok(mode) = std::env::var("KUBERIC_RUSTFS_TEST_CHILD") else {
            return;
        };
        if mode == "exit" {
            std::process::exit(23);
        }
        let root = std::env::var_os("KUBERIC_RUSTFS_TEST_ROOT").unwrap();
        #[cfg(unix)]
        if mode == "ignore-term" {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let _signal =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .unwrap();
                    fs::write(Path::new(&root).join("fixture-ready"), "ready").unwrap();
                    std::future::pending::<()>().await;
                });
        }
        fs::write(Path::new(&root).join("fixture-ready"), "ready").unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn owns_only_its_child_and_restarts_without_modifying_data() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let config = config(first.path());
        let mut a = RunningRustfs::start_with(config.clone(), |c, b| fixture_command(c, b, "run"))
            .await
            .unwrap();
        let mut b = RunningRustfs::start_with(config_for(second.path()), |c, b| {
            fixture_command(c, b, "run")
        })
        .await
        .unwrap();
        ready(first.path()).await;
        ready(second.path()).await;
        fs::write(first.path().join("data").join("sentinel"), b"retained").unwrap();
        let concurrent =
            RunningRustfs::start_with(config.clone(), |c, b| fixture_command(c, b, "run")).await;
        assert!(
            concurrent
                .err()
                .unwrap()
                .to_string()
                .contains("already owned")
        );
        let report = a.shutdown().await.unwrap();
        #[cfg(windows)]
        assert!(report.forced);
        #[cfg(unix)]
        assert!(!report.forced);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), b.wait())
                .await
                .is_err()
        );
        assert!(!first.path().join("state").join("process-active").exists());
        let mut restarted = RunningRustfs::start_with(config, |c, b| fixture_command(c, b, "run"))
            .await
            .unwrap();
        assert_eq!(
            fs::read(first.path().join("data").join("sentinel")).unwrap(),
            b"retained"
        );
        restarted.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    fn config_for(root: &Path) -> LaunchConfig {
        config(root)
    }

    #[test]
    fn native_tls_is_validated_and_required_off_loopback() {
        let root = tempfile::tempdir().unwrap();
        let mut config = config(root.path());
        config.address = "0.0.0.0:19000".parse().unwrap();
        assert!(config.validate().is_err());
        let tls = root.path().join("tls");
        fs::create_dir(&tls).unwrap();
        config.native_tls = Some(crate::NativeTlsConfig {
            directory: tls.clone(),
        });
        assert!(config.validate().is_err());
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        fs::write(tls.join("rustfs_cert.pem"), certificate.cert.pem()).unwrap();
        fs::write(tls.join("ca.crt"), certificate.cert.pem()).unwrap();
        fs::write(
            tls.join("rustfs_key.pem"),
            certificate.signing_key.serialize_pem(),
        )
        .unwrap();
        config.validate().unwrap();
        let command = config.command(&std::env::current_exe().unwrap());
        assert!(
            command
                .get_envs()
                .any(|(name, value)| name == "RUSTFS_TLS_PATH" && value == Some(tls.as_os_str()))
        );
        fs::write(tls.join("ca.crt"), "").unwrap();
        assert!(config.validate().is_err());
        fs::write(tls.join("ca.crt"), certificate.cert.pem()).unwrap();
        let different = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        fs::write(
            tls.join("rustfs_key.pem"),
            different.signing_key.serialize_pem(),
        )
        .unwrap();
        assert!(config.validate().is_err());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn inherited_lock_blocks_recovery_until_the_orphaned_child_exits() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let lease = TopologyLease::acquire(&config.state_directory, &config.topology).unwrap();
        let mut command = fixture_command(&config, &std::env::current_exe().unwrap(), "run");
        lease.inherit_lock(&mut command).unwrap();
        lease.begin_process().unwrap();
        let mut child = OwnedChild(command.spawn().unwrap());
        drop(command);
        drop(lease);
        ready(root.path()).await;
        assert!(TopologyLease::acquire(&config.state_directory, &config.topology).is_err());
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let recovered = TopologyLease::acquire(&config.state_directory, &config.topology).unwrap();
        assert!(!config.state_directory.join("process-active").exists());
        drop(recovered);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires the pinned real RustFS executable"]
    async fn native_executable_retains_ownership_after_supervisor_lock_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let mut config = config(root.path());
        config.binary = BinaryPin::new(
            std::env::var_os("KUBERIC_RUSTFS_TEST_BINARY")
                .expect("missing native binary")
                .into(),
            &std::env::var("KUBERIC_RUSTFS_TEST_SHA256").expect("missing native checksum"),
        )
        .unwrap();
        config.address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        config.validate().unwrap();
        let binary = config.binary.verify().unwrap();
        let lease = TopologyLease::acquire(&config.state_directory, &config.topology).unwrap();
        let mut command = config.command(&binary);
        lease.inherit_lock(&mut command).unwrap();
        lease.begin_process().unwrap();
        let mut child = OwnedChild(command.spawn().unwrap());
        drop(command);
        drop(lease);
        let health = crate::HealthClient::new(
            &format!("http://{}", config.address),
            Duration::from_secs(1),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                assert!(child.0.try_wait().unwrap().is_none(), "native child exited");
                match health.probe(crate::HealthProbe::Readiness).await {
                    Ok(crate::HealthStatus::Healthy) => break,
                    result => eprintln!("native ownership test startup: {result:?}"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap();
        let error = TopologyLease::acquire(&config.state_directory, &config.topology)
            .err()
            .expect("ready native child released its inherited ownership lock");
        assert!(error.to_string().contains("already owned"), "{error:#}");
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let recovered = TopologyLease::acquire(&config.state_directory, &config.topology).unwrap();
        assert!(!config.state_directory.join("process-active").exists());
        drop(recovered);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forces_shutdown_when_child_ignores_sigterm() {
        let root = tempfile::tempdir().unwrap();
        let mut process = RunningRustfs::start_with(config(root.path()), |c, b| {
            fixture_command(c, b, "ignore-term")
        })
        .await
        .unwrap();
        ready(root.path()).await;
        let started = Instant::now();
        let report = process.shutdown().await.unwrap();
        assert!(report.forced);
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(started.elapsed() < Duration::from_secs(6));
        assert!(!root.path().join("state").join("process-active").exists());
    }

    #[tokio::test]
    async fn cancelled_start_does_not_leave_an_owned_process() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let (entered, entering) = oneshot::channel();
        let task = tokio::spawn(RunningRustfs::start_with(config.clone(), move |c, b| {
            entered.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(100));
            fixture_command(c, b, "run")
        }));
        entering.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match TopologyLease::acquire(&config.state_directory, &config.topology) {
                    Ok(_) => break,
                    Err(error) => {
                        assert!(error.to_string().contains("already owned"), "{error:#}");
                        tokio::time::sleep(POLL).await;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(!root.path().join("state").join("fixture-ready").exists());
        assert!(!root.path().join("state").join("process-active").exists());
    }

    #[tokio::test]
    async fn invalid_launch_inputs_fail_before_persisting_topology() {
        let root = tempfile::tempdir().unwrap();
        let valid = config(root.path());
        let mut bad = valid.clone();
        bad.address.set_port(0);
        assert!(
            RunningRustfs::start(bad)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("port")
        );
        let mut bad = valid.clone();
        bad.shutdown_grace = Duration::ZERO;
        assert!(
            RunningRustfs::start(bad)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("grace")
        );
        let mut bad = valid;
        bad.credentials.secret_key = root.path().join("missing");
        assert!(
            RunningRustfs::start(bad)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("secret-key")
        );
        assert!(!root.path().join("state").join("topology.json").exists());
    }

    #[tokio::test]
    async fn unexpected_exit_is_reported_and_ownership_is_cleared() {
        let root = tempfile::tempdir().unwrap();
        let mut process =
            RunningRustfs::start_with(config(root.path()), |c, b| fixture_command(c, b, "exit"))
                .await
                .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), process.wait())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("unexpectedly"));
        assert!(error.to_string().contains("23"));
        assert!(process.shutdown().await.is_err());
        assert!(!root.path().join("state").join("process-active").exists());
    }

    #[tokio::test]
    async fn drop_stops_child_before_releasing_ownership() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let process =
            RunningRustfs::start_with(config.clone(), |c, b| fixture_command(c, b, "run"))
                .await
                .unwrap();
        ready(root.path()).await;
        drop(process);
        tokio::time::timeout(Duration::from_secs(6), async {
            while root.path().join("state").join("process-active").exists() {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .unwrap();
        let mut restarted = RunningRustfs::start_with(config, |c, b| fixture_command(c, b, "run"))
            .await
            .unwrap();
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wrong_binary_hash_and_spawn_failures_are_explicit() {
        let root = tempfile::tempdir().unwrap();
        let valid = config(root.path());
        let mut invalid = valid.clone();
        invalid.binary = BinaryPin::new(std::env::current_exe().unwrap(), &"0".repeat(64)).unwrap();
        let error = RunningRustfs::start(invalid).await.err().unwrap();
        assert!(error.to_string().contains("SHA-256"));
        assert!(!root.path().join("state").join("topology.json").exists());
        let error = RunningRustfs::start_with(valid, |c, _| {
            Command::new(c.state_directory.join("missing-binary"))
        })
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("spawning"));
        assert!(!root.path().join("state").join("process-active").exists());
    }

    #[test]
    fn launch_uses_ordered_arguments_and_credential_files_not_secrets() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let command = config.command(Path::new("rustfs"));
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "server",
                "--address",
                "127.0.0.1:19000",
                "--",
                &config.topology.pools[0]
            ]
        );
        let env: std::collections::BTreeMap<_, _> = command.get_envs().collect();
        assert_eq!(
            env[std::ffi::OsStr::new("RUSTFS_ACCESS_KEY_FILE")],
            Some(config.credentials.access_key.as_os_str())
        );
        assert!(!env.contains_key(std::ffi::OsStr::new("RUSTFS_SECRET_KEY")));
        assert!(!format!("{config:?} {command:?}").contains("fixture-secret-not-for-deployment"));
    }
}
