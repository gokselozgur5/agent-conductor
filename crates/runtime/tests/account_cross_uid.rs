//! Privileged Linux fixture: distinct real UIDs, no vendor credentials or host users created.
#![cfg(unix)]
use conductor_runtime::{
    AccountChannelAddress, AccountNativeLaunch, AccountRunnerConnection, AdapterKind,
    ExecutionAccount, ResumePacket, RunnerEvent, RunnerProcessIdentity, VendorPin,
};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

struct Fixture {
    root: tempfile::TempDir,
    account: ExecutionAccount,
    receiver: PathBuf,
    socket: PathBuf,
    runner: PathBuf,
    pin: VendorPin,
}

fn fixture() -> Fixture {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(
        nix::unistd::geteuid().as_raw(),
        0,
        "requires isolated CI root fixture"
    );
    let account = ExecutionAccount::resolve("nobody").unwrap();
    assert_ne!(account.uid, 0);
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let receiver = root.path().join("receiver");
    fs::create_dir(&receiver).unwrap();
    nix::unistd::chown(
        &receiver,
        Some(nix::unistd::Uid::from_raw(account.uid)),
        Some(nix::unistd::Gid::from_raw(account.gid)),
    )
    .unwrap();
    fs::set_permissions(&receiver, fs::Permissions::from_mode(0o700)).unwrap();
    let runner = root.path().join("runner");
    fs::copy(env!("CARGO_BIN_EXE_conductor-runner"), &runner).unwrap();
    fs::set_permissions(&runner, fs::Permissions::from_mode(0o755)).unwrap();
    let executable = root.path().join("vendor");
    fs::write(&executable, "#!/usr/bin/python3\nimport os,pathlib,json,sys\nassert os.getuid()==os.geteuid()!=0\nassert os.getgid()==os.getegid()!=0\nassert os.getgroups()==[]\nassert '--dangerously-skip-permissions' in sys.argv\nassert pathlib.Path('baseline').read_text()=='selected bytes'\npathlib.Path('produced').write_text(json.dumps({'uid':os.getuid(),'gid':os.getgid(),'home':os.environ['HOME'],'user':os.environ['USER']}))\nprint('cross UID candidate')\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    let pin = VendorPin {
        kind: AdapterKind::Claude,
        version: "fixture".into(),
        executable_sha256: conductor_runtime::binary_sha256(&executable).unwrap(),
        executable,
        conformance_sha256: "b".repeat(64),
    };
    let socket = receiver.join("worker.sock");
    Fixture {
        root,
        account,
        receiver,
        socket,
        runner,
        pin,
    }
}

fn address(f: &Fixture, digest: String) -> AccountChannelAddress {
    AccountChannelAddress {
        path: f.socket.clone(),
        worker_uid: f.account.uid,
        coordinator_uid: 0,
        packet_sha256: digest,
    }
}

async fn start(f: &Fixture) -> tokio::process::Child {
    receiving_account_preflight(f).await;
    let mut child = tokio::process::Command::new("/usr/bin/setpriv")
        .args([
            "--reuid",
            &f.account.uid.to_string(),
            "--regid",
            &f.account.gid.to_string(),
            "--clear-groups",
        ])
        .arg(&f.runner)
        .arg("--account-worker")
        .arg(&f.socket)
        .arg("0")
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f.socket.exists() {
            assert!(child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fs::metadata(&f.socket).unwrap().uid(), f.account.uid);
    child
}

async fn receiving_account_preflight(f: &Fixture) {
    let output = tokio::process::Command::new("/usr/bin/setpriv")
        .args([
            "--reuid", &f.account.uid.to_string(),
            "--regid", &f.account.gid.to_string(), "--clear-groups",
            "/usr/bin/python3", "-c",
            "import os,socket,sys,json\np=sys.argv[1]\ns=os.stat(p)\nprint(json.dumps({'uid':os.getuid(),'euid':os.geteuid(),'gid':os.getgid(),'path':p,'canonical':os.path.realpath(p),'owner':s.st_uid,'mode':oct(s.st_mode&0o777),'cwd':os.getcwd()}),flush=True)\nassert os.getuid()==os.geteuid()==s.st_uid\nassert p==os.path.realpath(p)\nassert s.st_mode&0o022==0\nx=socket.socket(socket.AF_UNIX)\nx.bind(p+'/probe.sock')\nx.close()\nos.unlink(p+'/probe.sock')\n",
        ])
        .arg(&f.receiver).env_clear().kill_on_drop(true).output().await.unwrap();
    eprintln!(
        "receiver fixture preflight: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.status.success(),
        "receiver preflight failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn stage(f: &Fixture) -> PathBuf {
    let source = f.root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("baseline"), "selected bytes").unwrap();
    let local = f.root.path().join("local-snapshot");
    let manifest =
        conductor_runtime::snapshot_paths(&source, &local, &["baseline".into()]).unwrap();
    let remote = f.receiver.join("baseline");
    conductor_runtime::stage_account_snapshot(
        &address(f, manifest.sha256().unwrap()),
        &local.join("tree"),
        &manifest,
        &remote,
    )
    .await
    .unwrap();
    assert_eq!(
        fs::metadata(remote.join("tree/baseline")).unwrap().uid(),
        f.account.uid
    );
    remote.join("tree")
}

fn packet(f: &Fixture, workspace: &Path) -> ResumePacket {
    serde_json::from_value(serde_json::json!({
        "contract": {"version":1,"task_id":"cross-uid","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker",
            "adapter":{"kind":"claude","version":f.pin.version,"executable_sha256":f.pin.executable_sha256,"conformance_sha256":f.pin.conformance_sha256},
            "workspace":workspace,"starting_head":"c".repeat(40),
            "authority":{"read_roots":["/"],"write_roots":["/"],"network":"unrestricted","allow_workspace_commands":true},
            "full_capability":{"account":f.account,"path":"/usr/bin:/bin"},"instructions":[],"checks":[],"max_runtime_ms":5000},
        "objective":"Produce the fixture result","checkpoint":null,"untrusted_context":[]
    })).unwrap()
}

async fn assert_wrong_coordinator_refused(f: &Fixture) {
    // The receiving account can reach its own socket, but cannot impersonate root.
    let output = tokio::process::Command::new("/usr/bin/setpriv")
        .args([
            "--reuid", &f.account.uid.to_string(),
            "--regid", &f.account.gid.to_string(), "--clear-groups",
            "/usr/bin/python3", "-c",
            "import socket,sys\ns=socket.socket(socket.AF_UNIX)\ns.settimeout(2)\ns.connect(sys.argv[1])\nassert s.recv(1)==b''\n",
        ])
        .arg(&f.socket).env_clear().kill_on_drop(true).output().await.unwrap();
    assert!(
        output.status.success(),
        "wrong coordinator was not rejected before payload"
    );
}

#[tokio::test]
#[ignore = "requires Linux root; explicitly executed in an isolated hosted CI fixture"]
async fn distinct_uid_worker_stages_executes_and_returns_exact_candidate() {
    let f = fixture();
    let mut worker = start(&f).await;
    assert_wrong_coordinator_refused(&f).await;
    let workspace = stage(&f).await;
    let packet = packet(&f, &workspace);
    let digest = packet.sha256().unwrap();
    let selected = address(&f, digest.clone());
    let mut wrong = selected.clone();
    wrong.worker_uid = 0;
    assert!(
        conductor_runtime::connect_account_channel(&wrong, Duration::from_secs(2))
            .await
            .is_err()
    );
    let launch = AccountNativeLaunch {
        pin: f.pin.clone(),
        packet: packet.clone(),
    };
    let identity = RunnerProcessIdentity {
        authority_id: "a".repeat(64),
        packet_sha256: digest,
        wrapper_identity: uuid::Uuid::now_v7().to_string(),
    };
    let acknowledged = std::sync::atomic::AtomicBool::new(false);
    let mut connection = AccountRunnerConnection::connect(&selected, &launch, &identity, |_| {
        assert!(!workspace.join("produced").exists());
        acknowledged.store(true, std::sync::atomic::Ordering::SeqCst);
        async { Ok(conductor_runtime::continuous_millis().unwrap() + 5000) }
    })
    .await
    .unwrap();
    assert!(acknowledged.load(std::sync::atomic::Ordering::SeqCst));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(8), connection.event())
            .await
            .unwrap()
            .unwrap(),
        RunnerEvent::Finished {
            exit_code: Some(0),
            ..
        }
    ));
    drop(connection);
    assert_eq!(
        fs::metadata(workspace.join("produced")).unwrap().uid(),
        f.account.uid
    );
    let receipt = conductor_runtime::capture_account_candidate(
        &selected,
        &conductor_runtime::AccountCaptureRequest {
            packet_sha256: packet.sha256().unwrap(),
            source: workspace,
            destination: f.receiver.join("candidate"),
            baseline_paths: vec!["baseline".into()],
        },
    )
    .await
    .unwrap();
    let copied = f.root.path().join("imported");
    let manifest = conductor_runtime::fetch_account_snapshot(
        &address(&f, receipt.snapshot_sha256.clone()),
        &f.receiver.join("candidate"),
        &copied,
    )
    .await
    .unwrap();
    assert_eq!(manifest.sha256().unwrap(), receipt.snapshot_sha256);
    let produced = copied.join("tree/produced");
    assert_eq!(fs::metadata(&produced).unwrap().uid(), 0);
    let value: serde_json::Value = serde_json::from_slice(&fs::read(produced).unwrap()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"uid":f.account.uid,"gid":f.account.gid,"home":f.account.home,"user":f.account.name})
    );
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(i32::try_from(worker.id().unwrap()).unwrap()),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), worker.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!f.socket.exists());
}
