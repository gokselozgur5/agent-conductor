use std::{
    fs,
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::PathBuf,
    process::Command,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use conductor_protocol::{ErrorBody, ErrorCode, RequestEnvelope, ResponseEnvelope, ResponseResult};

fn socket_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    PathBuf::from(format!("/tmp/acx-{}-{nonce}.sock", std::process::id()))
}

#[test]
fn json_error_uses_the_structured_nonzero_exit_status() {
    let socket = socket_path();
    let listener = UnixListener::bind(&socket).expect("bind fake daemon");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept client");
        let mut input = Vec::new();
        stream.read_to_end(&mut input).expect("read request");
        let request: RequestEnvelope = serde_json::from_slice(&input).expect("decode request");
        let response = ResponseEnvelope::failure(
            Some(&request),
            ErrorBody::new(ErrorCode::Conflict, "test conflict"),
        );
        serde_json::to_writer(&mut stream, &response).expect("write response");
        stream.write_all(b"\n").expect("write frame ending");
    });

    let output = Command::new(env!("CARGO_BIN_EXE_conductorctl"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .arg("get-task")
        .arg("missing")
        .output()
        .expect("run conductorctl");
    server.join().expect("fake daemon joins");

    assert_eq!(output.status.code(), Some(3));
    let response: ResponseEnvelope =
        serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert!(matches!(
        response.result,
        ResponseResult::Error(ErrorBody {
            code: ErrorCode::Conflict,
            ..
        })
    ));

    fs::remove_file(&socket).expect("remove socket");
}
