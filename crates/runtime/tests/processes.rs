#![cfg(unix)]
use conductor_runtime::{ProcessError, ProcessSpec, run_process};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::sync::watch;

fn spec(directory: &std::path::Path, script: &str) -> ProcessSpec {
    ProcessSpec {
        executable: "/bin/sh".into(),
        arguments: vec!["-c".into(), script.into()],
        directory: directory.to_path_buf(),
        environment: BTreeMap::new(),
        timeout: Duration::from_secs(2),
        output_limit: 1024,
    }
}

#[tokio::test]
async fn captures_bounded_output_and_preserves_nonzero_exit_as_data() {
    let directory = tempfile::tempdir().expect("directory");
    let (_keepalive, cancellation) = watch::channel(false);
    let output = run_process(
        &spec(
            directory.path(),
            "printf result; printf failure >&2; exit 7",
        ),
        cancellation,
    )
    .await
    .expect("process collected");
    assert_eq!(output.exit_code, Some(7));
    assert_eq!(output.stdout, b"result");
    assert_eq!(output.stderr, b"failure");
}

#[tokio::test]
async fn parent_environment_is_not_inherited() {
    let directory = tempfile::tempdir().expect("directory");
    let (_keepalive, cancellation) = watch::channel(false);
    let output = run_process(
        &spec(
            directory.path(),
            "printf '%s' \"${HOME-unset}:${OPENAI_API_KEY-unset}:${ANTHROPIC_API_KEY-unset}\"",
        ),
        cancellation,
    )
    .await
    .expect("process collected");
    assert_eq!(output.stdout, b"unset:unset:unset");
}

#[tokio::test]
async fn output_flood_and_closed_pipe_hang_are_bounded() {
    let directory = tempfile::tempdir().expect("directory");
    let (_keepalive, cancellation) = watch::channel(false);
    let result = run_process(
        &spec(directory.path(), "while :; do printf 1234567890; done"),
        cancellation,
    )
    .await;
    assert!(matches!(result, Err(ProcessError::OutputLimit)));
    let mut closed = spec(directory.path(), "exec 1>&- 2>&-; /bin/sleep 10");
    closed.timeout = Duration::from_millis(100);
    let start = Instant::now();
    let (_keepalive, cancellation) = watch::channel(false);
    assert!(matches!(
        run_process(&closed, cancellation).await,
        Err(ProcessError::Timeout)
    ));
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn lost_supervisor_and_explicit_cancel_fence_descendants() {
    for drop_sender in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (sender, cancellation) = watch::channel(false);
        let script = "( /bin/sleep 1; printf escaped > escaped ) & printf ready > ready; wait";
        let specification = spec(directory.path(), script);
        let process = tokio::spawn(async move { run_process(&specification, cancellation).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !directory.path().join("ready").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("child group ready");
        if drop_sender {
            drop(sender);
        } else {
            sender.send(true).expect("cancel");
        }
        assert!(matches!(
            process.await.expect("join"),
            Err(ProcessError::Cancelled)
        ));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(
            !directory.path().join("escaped").exists(),
            "descendant must be fenced before return"
        );
    }
}

#[tokio::test]
async fn cancellation_before_spawn_has_no_side_effect() {
    let directory = tempfile::tempdir().expect("directory");
    let (_sender, cancellation) = watch::channel(true);
    assert!(matches!(
        run_process(
            &spec(directory.path(), "touch should-not-exist"),
            cancellation
        )
        .await,
        Err(ProcessError::Cancelled)
    ));
    assert!(!directory.path().join("should-not-exist").exists());
}
