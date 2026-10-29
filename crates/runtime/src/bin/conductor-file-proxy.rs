//! Fixed MCP stdio entry point. The host supplies capability only via environment.
#[cfg(unix)]
#[tokio::main]
async fn main() {
    use conductor_runtime::{BROKER_HANDOFF_ENV, connect_broker_endpoint, serve_owned_broker_mcp};
    use std::time::Duration;
    let result = async {
        if std::env::args_os().len() != 1 {
            return Err(());
        }
        let value = std::env::var(BROKER_HANDOFF_ENV).map_err(|_| ())?;
        let (stream, scope) = connect_broker_endpoint(&value, Duration::from_secs(10))
            .await
            .map_err(|_| ())?;
        drop(value);
        serve_owned_broker_mcp(
            tokio::io::stdin(),
            tokio::io::stdout(),
            stream,
            scope,
            Duration::from_secs(30),
        )
        .await
        .map_err(|_| ())
    }
    .await;
    if result.is_err() {
        eprintln!("file proxy connection or protocol failed");
    }
    // Tokio's blocking stdin thread cannot be cancelled. Channel ownership has
    // already been dropped; exit does not claim the host's broker is settled.
    std::process::exit(i32::from(result.is_err()));
}
#[cfg(not(unix))]
fn main() {
    eprintln!("file proxy requires a supported Unix host");
    std::process::exit(1);
}
