//! Authenticated loopback operator access to the existing control service.
mod controls;
mod workflows;
use axum::{
    Router,
    body::Body,
    extract::{Query, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use conductor_protocol::Request as ControlRequest;
use conductor_service::ControlService;
use sha2::{Digest, Sha256};
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;

struct BoardState<S> {
    service: Arc<S>,
    token_digest: [u8; 32],
    host: String,
}
impl<S> Clone for BoardState<S> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            token_digest: self.token_digest,
            host: self.host.clone(),
        }
    }
}
/// Optional local operator UI. Only the authentication digest is retained.
pub struct OperatorBoard {
    listener: TcpListener,
    router: Router,
    address: SocketAddr,
}
impl OperatorBoard {
    /// Binds only loopback with an explicit nonempty operator credential.
    /// The token is never logged, embedded in a URL, or sent in HTML.
    /// # Errors
    /// Refuses non-loopback addresses, invalid tokens and listener failures.
    pub async fn bind<S: ControlService>(
        address: SocketAddr,
        token: String,
        service: S,
    ) -> std::io::Result<Self> {
        if !address.ip().is_loopback()
            || !(32..=256).contains(&token.len())
            || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "loopback address and 32–256 printable-byte operator token required",
            ));
        }
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let state = BoardState {
            service: Arc::new(service),
            token_digest: Sha256::digest(token.as_bytes()).into(),
            host: address.to_string(),
        };
        let router = Router::new()
            .route(
                "/",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                        include_str!("board/index.html"),
                    )
                }),
            )
            .route(
                "/app.js",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                        include_str!("board/app.js"),
                    )
                }),
            )
            .route(
                "/style.css",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                        include_str!("board/style.css"),
                    )
                }),
            )
            .route("/api/tasks", get(tasks::<S>))
            .route("/api/history", get(history::<S>))
            .route("/api/checkpoint", get(checkpoint::<S>))
            .route("/api/health", get(health::<S>))
            .route("/api/control", post(controls::apply::<S>))
            .route("/api/workflows/continue", post(workflows::resume::<S>))
            .route("/api/workflows/reassign", post(workflows::reassign::<S>))
            .route(
                "/api/workflows",
                get(workflows::list::<S>).post(workflows::submit::<S>),
            )
            .layer(middleware::from_fn_with_state(state.clone(), boundary::<S>))
            .with_state(state);
        Ok(Self {
            listener,
            router,
            address,
        })
    }
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
    /// Serves until shutdown; ongoing service requests have a bounded deadline.
    /// # Errors
    /// Returns listener/server failures.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
    }
}
async fn boundary<S: ControlService>(
    State(state): State<BoardState<S>>,
    request: Request,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok());
    let origin = request.headers().get(header::ORIGIN);
    let expected_origin = format!("http://{}", state.host);
    if host != Some(state.host.as_str())
        || origin.is_some_and(|value| value.to_str().ok() != Some(expected_origin.as_str()))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if request.uri().path().starts_with("/api/") {
        let supplied = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if !supplied.is_some_and(|value| {
            value.len() <= 256
                && <[u8; 32]>::from(Sha256::digest(value.as_bytes())) == state.token_digest
        }) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    response
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskQuery {
    project_id: Option<String>,
    after: Option<String>,
}
async fn tasks<S: ControlService>(
    State(state): State<BoardState<S>>,
    Query(params): Query<TaskQuery>,
) -> Response {
    query(
        &state,
        ControlRequest::ListTaskPage {
            project_id: params.project_id,
            after: params.after,
        },
    )
    .await
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryQuery {
    task_id: String,
    before: Option<String>,
}
async fn history<S: ControlService>(
    State(state): State<BoardState<S>>,
    Query(params): Query<HistoryQuery>,
) -> Response {
    query(
        &state,
        ControlRequest::TaskHistory {
            task_id: params.task_id,
            before: params.before,
        },
    )
    .await
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointQuery {
    task_id: String,
}
async fn checkpoint<S: ControlService>(
    State(state): State<BoardState<S>>,
    Query(params): Query<CheckpointQuery>,
) -> Response {
    query(
        &state,
        ControlRequest::TaskCheckpoint {
            task_id: params.task_id,
        },
    )
    .await
}
async fn health<S: ControlService>(State(state): State<BoardState<S>>) -> Response {
    query(&state, ControlRequest::Health).await
}
async fn query<S: ControlService>(state: &BoardState<S>, request: ControlRequest) -> Response {
    let Ok(Ok(value)) =
        tokio::time::timeout(Duration::from_secs(3), state.service.handle(request)).await
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match serde_json::to_vec(&value) {
        Ok(bytes) if bytes.len() <= 1024 * 1024 => (
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(bytes),
        )
            .into_response(),
        _ => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct Stub(Arc<AtomicUsize>);
    impl ControlService for Stub {
        async fn handle(
            &self,
            _: ControlRequest,
        ) -> Result<conductor_protocol::Response, conductor_protocol::ErrorBody> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(conductor_protocol::Response::Tasks { tasks: vec![] })
        }
    }
    pub(super) async fn request(
        address: SocketAddr,
        host: &str,
        path: &str,
        extra: &str,
    ) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n{extra}\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }
    #[tokio::test]
    async fn authentication_host_and_origin_gate_authoritative_queries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let token = "synthetic-board-test-credential-000000";
        let board = OperatorBoard::bind(
            "127.0.0.1:0".parse().unwrap(),
            token.into(),
            Stub(Arc::clone(&calls)),
        )
        .await
        .unwrap();
        let address = board.address();
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(board.run_until(async move {
            let _ = stopped.changed().await;
        }));
        let auth = format!("Authorization: Bearer {token}\r\n");
        assert!(
            request(address, &address.to_string(), "/api/tasks", "")
                .await
                .starts_with("HTTP/1.1 401")
        );
        assert!(
            request(address, "evil.example", "/api/tasks", &auth)
                .await
                .starts_with("HTTP/1.1 403")
        );
        assert!(
            request(
                address,
                &address.to_string(),
                "/api/tasks",
                &format!("{auth}Origin: http://evil.example\r\n")
            )
            .await
            .starts_with("HTTP/1.1 403")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let shell = request(address, &address.to_string(), "/", "").await;
        assert!(shell.starts_with("HTTP/1.1 200"));
        assert!(!shell.contains(token));
        for _ in 0..2 {
            let response = request(address, &address.to_string(), "/api/tasks", &auth).await;
            assert!(response.starts_with("HTTP/1.1 200"));
            assert!(response.contains("\"type\":\"tasks\""));
            assert!(response.contains("cache-control: no-store"));
            assert!(response.contains("content-security-policy:"));
            assert!(!response.contains(token));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "every refresh reads service state"
        );
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn non_loopback_and_missing_strength_credentials_are_refused() {
        for (address, token) in [
            ("0.0.0.0:0", "synthetic-board-test-credential-000000"),
            ("127.0.0.1:0", "short"),
        ] {
            assert!(
                OperatorBoard::bind(
                    address.parse().unwrap(),
                    token.into(),
                    Stub(Arc::new(AtomicUsize::new(0)))
                )
                .await
                .is_err()
            );
        }
    }
}

#[cfg(test)]
mod controls_tests;
