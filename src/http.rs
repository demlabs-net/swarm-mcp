use std::{
    collections::VecDeque,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::{self, Next},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::DateTime;
use futures::StreamExt;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;

use crate::{AppState, config::Config, mcp::RoleMcp, store::ActivityRecord, telegram};

#[derive(Clone)]
struct RoleAuth {
    role: String,
    token: String,
    limiter: Arc<RequestLimiter>,
}

struct RequestLimiter {
    events: Mutex<VecDeque<Instant>>,
    limit: usize,
    window: Duration,
}

impl RequestLimiter {
    fn new(limit: usize, window: Duration) -> Self {
        Self {
            events: Mutex::new(VecDeque::new()),
            limit,
            window,
        }
    }

    async fn allow(&self) -> bool {
        let now = Instant::now();
        let mut events = self.events.lock().await;
        while events
            .front()
            .is_some_and(|created| now.duration_since(*created) >= self.window)
        {
            events.pop_front();
        }
        if events.len() >= self.limit {
            return false;
        }
        events.push_back(now);
        true
    }
}

#[derive(Clone)]
struct ActivityAuth {
    credentials: Arc<Vec<ActivityCredential>>,
}

#[derive(Clone)]
struct ActivityCredential {
    role: String,
    token: String,
    limiter: Arc<RequestLimiter>,
}

#[derive(Clone)]
struct AuthenticatedRole(String);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityInput {
    event: String,
    turn_id: String,
    #[serde(default)]
    detail: String,
    occurred_at: Option<String>,
}

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    serve_with_shutdown(state, shutdown_signal()).await
}

/// Serve until `shutdown` resolves (the CLI waits for SIGTERM/Ctrl-C). Split out
/// so tests can exercise the full server lifecycle with an immediate shutdown.
async fn serve_with_shutdown(
    state: Arc<AppState>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    let app = build_router(state.clone(), &cancellation);

    let outbox = state
        .dispatcher
        .spawn_outbox_worker(cancellation.child_token());
    let run_replies = state
        .dispatcher
        .spawn_run_reply_worker(cancellation.child_token());
    let telegram_inbound =
        telegram::spawn_inbound_worker(state.clone(), cancellation.child_token());
    let cleanup_state = state.clone();
    let cleanup_cancellation = cancellation.child_token();
    let cleanup = tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_state.config.cleanup_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cleanup_cancellation.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = cleanup_state.store
                        .cleanup(
                            cleanup_state.config.activity_retention_days,
                            cleanup_state.config.operation_retention_days,
                            cleanup_state.config.outbox_retention_days,
                            cleanup_state.config.rate_window,
                            cleanup_state.config.pending_stale_after,
                        )
                        .await
                    {
                        error!(error = %error, "state cleanup failed");
                    }
                }
            }
        }
    });

    let address = SocketAddr::new(state.config.bind_ip, state.config.port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(%address, "swarm MCP listening");
    let shutdown_token = cancellation.clone();
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            shutdown_token.cancel();
        })
        .await;
    cancellation.cancel();
    let _ = outbox.await;
    let _ = run_replies.await;
    if let Some(worker) = telegram_inbound {
        let _ = worker.await;
    }
    let _ = cleanup.await;
    result?;
    Ok(())
}

/// Плейсхолдер SSE для session-less GET: клиенты (go-sdk / Yandex AI
/// Studio) открывают event stream до initialize. Сессию создаёт сам rmcp при
/// первом POST initialize (без Mcp-Session-Id); после initialize клиент
/// открывает настоящий стрим сессии GET-ом уже с session id.
///
/// Yandex "HTTP with SSE" voice agents OPEN the stream and then WAIT for
/// `event: endpoint` before POSTing JSON-RPC — an empty stream times out.
/// The endpoint must be the PUBLIC path (nginx strips the /swarm prefix, so
/// the request path alone would 404 on the public side): prefix from
/// SWARM_PUBLIC_PREFIX (default "/swarm") + the (stripped) request path.
///
/// Replies to POSTs are ALSO fanned out over this stream (event: message) —
/// SSE-only clients ignore the POST response body and wait on the GET
/// channel. The fan-out comes from the shared broadcast: every POST response
/// (parsed from the body) is published tagged with its role.
async fn sse_bootstrap(
    State(events): State<std::sync::Arc<tokio::sync::broadcast::Sender<serde_json::Value>>>,
    request: Request,
    next: Next,
) -> Response {
    let is_get = request.method() == Method::GET;
    let accept_sse = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"));
    let has_session = request.headers().contains_key("mcp-session-id");
    if is_get && accept_sse && !has_session {
        let prefix = std::env::var("SWARM_PUBLIC_PREFIX").unwrap_or_else(|_| "/swarm".into());
        let endpoint = format!("{}{}", prefix, request.uri().path());
        let role = role_from_path(request.uri().path()).to_string();
        let stream = futures::stream::once(async move {
            Ok::<_, std::convert::Infallible>(
                SseEvent::default().event("endpoint").data(endpoint),
            )
        })
        .chain(futures::stream::unfold(
            events.subscribe(),
            move |mut rx| {
                let role = role.clone();
                async move {
                    loop {
                        match rx.recv().await {
                            Ok(evt) => {
                                if evt.get("role").and_then(|v| v.as_str()) == Some(&role) {
                                    if let Some(resp) = evt.get("response") {
                                        return Some((
                                            Ok::<_, std::convert::Infallible>(
                                                SseEvent::default()
                                                    .event("message")
                                                    .data(resp.to_string()),
                                            ),
                                            rx,
                                        ));
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                }
            },
        ));
        return Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response();
    }
    if request.method() == Method::POST {
        let role = role_from_path(request.uri().path()).to_string();
        let response = next.run(request).await;
        // Publish the JSON-RPC reply on the broadcast so SSE-only clients
        // (Yandex voice agents) receive it over their GET stream. The POST
        // body itself passes through unchanged — streamable clients read it
        // there.
        let (published, response) = {
            let (parts, body) = response.into_parts();
            let bytes = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
                Ok(b) => b,
                Err(_) => return Response::from_parts(parts, axum::body::Body::empty()),
            };
            let text = String::from_utf8_lossy(&bytes);
            // rmcp SSE replies lead with an EMPTY `data:` line (keep-alive
            // preamble) — skip empty payloads and take the first real one.
            let candidate = text
                .lines()
                .find_map(|l| l.strip_prefix("data:").map(str::trim).filter(|s| !s.is_empty()))
                .unwrap_or(text.trim());
            let value: Option<serde_json::Value> = serde_json::from_str(candidate).ok();
            let published = value.filter(|v| v.get("id").is_some());
            let response = Response::from_parts(parts, axum::body::Body::from(bytes));
            (published, response)
        };
        if let Some(resp) = published {
            let _ = events.send(serde_json::json!({
                "role": role,
                "response": resp,
            }));
        }
        return response;
    }
    next.run(request).await
}

/// The role segment of `/roles/{role}/mcp`.
fn role_from_path(path: &str) -> &str {
    path.trim_matches('/')
        .split('/')
        .nth(1)
        .unwrap_or_default()
}

pub(crate) fn build_router(state: Arc<AppState>, cancellation: &CancellationToken) -> Router {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready));
    // Server→client reply fan-out: SSE-only clients (Yandex voice agents)
    // wait for JSON-RPC replies on their GET stream, not in the POST body.
    // Every POST response is published here tagged with its role; the
    // sse_bootstrap GET stream filters by its own role.
    let (events_tx, _) = tokio::sync::broadcast::channel::<serde_json::Value>(256);
    let events_tx = std::sync::Arc::new(events_tx);

    for (role, path) in &state.config.role_paths {
        let handler_state = state.clone();
        let handler_role = role.clone();
        let server_config = StreamableHttpServerConfig::default()
            .with_allowed_hosts(state.config.allowed_hosts.clone())
            .with_allowed_origins(state.config.allowed_origins.clone())
            .with_max_request_body_bytes(state.config.max_request_body_bytes)
            .with_cancellation_token(cancellation.child_token());
        let service = StreamableHttpService::new(
            move || {
                Ok::<_, std::io::Error>(RoleMcp::new(handler_state.clone(), handler_role.clone()))
            },
            LocalSessionManager::default().into(),
            server_config,
        );
        let token = state
            .config
            .agents
            .get(role)
            .expect("configuration validates every role")
            .mcp_token
            .expose()
            .to_string();
        let protected = Router::new()
            .nest_service(path, service)
            .layer(middleware::from_fn_with_state(
                events_tx.clone(),
                sse_bootstrap,
            ))
            .layer(middleware::from_fn_with_state(
                RoleAuth {
                    role: role.clone(),
                    token,
                    limiter: Arc::new(RequestLimiter::new(
                        state.config.mcp_request_rate_limit,
                        state.config.mcp_request_rate_window,
                    )),
                },
                role_auth,
            ));
        app = app.merge(protected);
    }

    let activity_tokens = state
        .config
        .agent_roles
        .iter()
        .map(|role| {
            let token = state
                .config
                .agents
                .get(role)
                .expect("configuration validates every executor")
                .mcp_token
                .expose()
                .to_string();
            ActivityCredential {
                role: role.clone(),
                token,
                limiter: Arc::new(RequestLimiter::new(
                    state.config.mcp_request_rate_limit,
                    state.config.mcp_request_rate_window,
                )),
            }
        })
        .collect::<Vec<_>>();
    let activity_router = Router::new()
        .route(&state.config.activity_path, post(activity))
        .layer(middleware::from_fn_with_state(
            ActivityAuth {
                credentials: Arc::new(activity_tokens),
            },
            activity_auth,
        ));

    // Each role/activity sub-router carries its own middleware state (RoleAuth /
    // ActivityAuth) embedded in the layer. The outer router state (Arc<AppState>)
    // is baked into the route handlers by `with_state`, after which the router
    // itself is state-less (Router<()>), so further layers and `axum::serve` stay
    // trivial. Do not remove `with_state`: handlers would then fail to extract
    // `State<Arc<AppState>>`.
    let app: Router = app
        .merge(activity_router)
        .layer(DefaultBodyLimit::max(state.config.max_request_body_bytes))
        .layer(middleware::from_fn_with_state(
            state.config.clone(),
            validate_host_and_origin,
        ))
        .with_state::<()>(state.clone());
    app
}

async fn health() -> impl IntoResponse {
    Json(json!({"ok": true, "service": "swarm-mcp"}))
}

async fn ready(State(state): State<Arc<AppState>>) -> Response {
    match state.store.ready().await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true, "ready": true}))).into_response(),
        Err(error) => {
            error!(error = %error, "readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"ok": false, "ready": false})),
            )
                .into_response()
        }
    }
}

async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(role): Extension<AuthenticatedRole>,
    Json(input): Json<ActivityInput>,
) -> Response {
    if !matches!(input.event.as_str(), "started" | "completed" | "failed") {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "event must be started, completed, or failed",
        );
    }
    if let Err(error) = crate::dispatch::validate_identifier(&input.turn_id, "turn_id") {
        return api_error(StatusCode::UNPROCESSABLE_ENTITY, error.to_string());
    }
    if input.detail.chars().count() > state.config.max_message_chars {
        return api_error(StatusCode::PAYLOAD_TOO_LARGE, "detail is too long");
    }
    if input
        .occurred_at
        .as_deref()
        .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_err())
    {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "occurred_at must be an RFC 3339 timestamp",
        );
    }
    let targets = state
        .config
        .activity_routes
        .get(&role.0)
        .cloned()
        .unwrap_or_default();
    if !state.config.activity_enabled {
        return (
            StatusCode::ACCEPTED,
            Json(json!({
                "ok": true,
                "enabled": false,
                "recorded": false,
                "sender": role.0,
                "targets": targets,
            })),
        )
            .into_response();
    }
    match state
        .store
        .record_activity(ActivityRecord {
            sender: &role.0,
            targets: &targets,
            event: &input.event,
            turn_id: &input.turn_id,
            detail: input.detail.trim(),
            occurred_at: input.occurred_at.as_deref(),
            clock_skew: state.config.activity_clock_skew,
        })
        .await
    {
        Ok(results) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "ok": true,
                "enabled": true,
                "mode": "record-only",
                "sender": role.0,
                "targets": results.0,
            })),
        )
            .into_response(),
        Err(error) => {
            error!(sender = %role.0, error = %error, "activity persistence failed");
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "activity persistence failed",
            )
        }
    }
}

async fn role_auth(State(auth): State<RoleAuth>, request: Request, next: Next) -> Response {
    let Some(provided) = bearer_token(request.headers()) else {
        return unauthorized();
    };
    if !constant_time_equal(provided, &auth.token) {
        return unauthorized();
    }
    if !auth.limiter.allow().await {
        warn!(role = %auth.role, "MCP request rate limit exceeded");
        return api_error(StatusCode::TOO_MANY_REQUESTS, "request rate limit exceeded");
    }
    next.run(request).await
}

async fn activity_auth(
    State(auth): State<ActivityAuth>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(provided) = bearer_token(request.headers()) else {
        return unauthorized();
    };
    let credential = auth
        .credentials
        .iter()
        .find(|credential| constant_time_equal(provided, &credential.token));
    let Some(credential) = credential else {
        return unauthorized();
    };
    if !credential.limiter.allow().await {
        warn!(role = %credential.role, "activity request rate limit exceeded");
        return api_error(StatusCode::TOO_MANY_REQUESTS, "request rate limit exceeded");
    }
    request
        .extensions_mut()
        .insert(AuthenticatedRole(credential.role.clone()));
    next.run(request).await
}

async fn validate_host_and_origin(
    State(config): State<Arc<Config>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return api_error(StatusCode::BAD_REQUEST, "valid Host header required");
    };
    if !host_allowed(host, &config.allowed_hosts) {
        return api_error(StatusCode::FORBIDDEN, "Host is not allowed");
    }
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        let Ok(origin) = origin.to_str() else {
            return api_error(StatusCode::FORBIDDEN, "Origin is not allowed");
        };
        if !origin_allowed(origin, &config.allowed_origins) {
            return api_error(StatusCode::FORBIDDEN, "Origin is not allowed");
        }
    }
    next.run(request).await
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && token.trim() == token)
        .then_some(token)
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    left.len() == right.len() && left.as_bytes().ct_eq(right.as_bytes()).into()
}

fn host_allowed(host: &str, allowed: &[String]) -> bool {
    let Ok(authority) = http::uri::Authority::from_str(host) else {
        return false;
    };
    allowed.iter().any(|candidate| {
        let Ok(candidate) = http::uri::Authority::from_str(candidate) else {
            return false;
        };
        candidate.host().eq_ignore_ascii_case(authority.host())
            && candidate
                .port_u16()
                .is_none_or(|port| authority.port_u16() == Some(port))
    })
}

fn origin_allowed(origin: &str, allowed: &[String]) -> bool {
    if origin == "null" {
        return allowed.iter().any(|candidate| candidate == "null");
    }
    let Ok(origin) = Url::parse(origin) else {
        return false;
    };
    allowed.iter().any(|candidate| {
        let Ok(candidate) = Url::parse(candidate) else {
            return false;
        };
        origin.scheme() == candidate.scheme()
            && origin.host_str() == candidate.host_str()
            && origin.port_or_known_default() == candidate.port_or_known_default()
    })
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"ok": false, "error": "invalid bearer credentials"})),
    )
        .into_response()
}

fn api_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"ok": false, "error": message.into()}))).into_response()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            error!("failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => error!(error = %error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    shutdown_signal_with(ctrl_c, terminate).await;
}

/// Waits for either shutdown source; split out so tests can drive the select
/// with ready futures instead of installing process-global signal handlers.
async fn shutdown_signal_with(
    ctrl_c: impl std::future::Future<Output = ()>,
    terminate: impl std::future::Future<Output = ()>,
) {
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::io::Write;

    #[test]
    fn bearer_scheme_is_case_insensitive_but_value_is_strict() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("bEaReR token"),
        );
        assert_eq!(bearer_token(&headers), Some("token"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer  token"),
        );
        assert_eq!(bearer_token(&headers), None);
    }

    #[test]
    fn host_rules_distinguish_explicit_ports() {
        assert!(host_allowed("localhost:3004", &["localhost".to_string()]));
        assert!(host_allowed(
            "localhost:3004",
            &["localhost:3004".to_string()]
        ));
        assert!(!host_allowed(
            "localhost:3005",
            &["localhost:3004".to_string()]
        ));
    }

    #[test]
    fn origins_compare_normalized_tuple() {
        let allowed = vec!["https://example.com".to_string(), "null".to_string()];
        assert!(origin_allowed("https://example.com:443", &allowed));
        assert!(origin_allowed("null", &allowed));
        assert!(!origin_allowed("http://example.com", &allowed));
        assert!(!origin_allowed("not a URL", &allowed));
    }

    #[tokio::test]
    async fn request_limiter_bounds_valid_role_traffic() {
        let limiter = RequestLimiter::new(2, Duration::from_secs(60));
        assert!(limiter.allow().await);
        assert!(limiter.allow().await);
        assert!(!limiter.allow().await);
    }

    use crate::{AppState, testutil};
    use tower::ServiceExt;

    const MANAGER_TOKEN: &str = "fixture-mcp-token-manager-0123456789";
    const DEVELOPER_TOKEN: &str = "fixture-mcp-token-developer-0123456789";

    async fn test_state(
        config: crate::config::Config,
    ) -> (std::sync::Arc<AppState>, std::path::PathBuf) {
        let path = config.state_db_path.clone();
        let config = std::sync::Arc::new(config);
        let store = crate::store::Store::connect(&config)
            .await
            .expect("fixture store");
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())
            .expect("fixture dispatcher");
        (
            std::sync::Arc::new(AppState {
                config,
                store,
                dispatcher,
            }),
            path,
        )
    }

    fn get_request(uri: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .uri(uri)
            .header(header::HOST, "localhost");
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("fixture request")
    }

    #[tokio::test]
    async fn role_endpoint_rejects_wrong_and_missing_tokens() {
        let path = testutil::temp_db_path("http");
        let (state, db_path) = test_state(testutil::fixture_config(&path)).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        for token in [None, Some("wrong-token")] {
            let response = router
                .clone()
                .oneshot(get_request("/mcp/manager", token))
                .await
                .expect("request");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        // cross-role: developer's token against the manager endpoint
        let response = router
            .clone()
            .oneshot(get_request("/mcp/manager", Some(DEVELOPER_TOKEN)))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // valid token passes the guard (rmcp handles the transport from here)
        let response = router
            .clone()
            .oneshot(get_request("/mcp/manager", Some(MANAGER_TOKEN)))
            .await
            .expect("request");
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);

        drop(state);
        testutil::remove_db_files(&db_path).await;
    }

    #[tokio::test]
    async fn activity_endpoint_validates_and_records() {
        let path = testutil::temp_db_path("http");
        let (state, db_path) = test_state(testutil::fixture_config(&path)).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        assert_eq!(
            post_activity(
                &router,
                Some(DEVELOPER_TOKEN),
                json!({"event": "completed", "turn_id": "turn_1", "detail": "ok"})
            )
            .await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            post_activity(
                &router,
                Some(DEVELOPER_TOKEN),
                json!({"event": "launched", "turn_id": "turn_2"})
            )
            .await,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            post_activity(
                &router,
                Some(DEVELOPER_TOKEN),
                json!({"event": "completed"})
            )
            .await,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            post_activity(
                &router,
                None,
                json!({"event": "completed", "turn_id": "turn_3"})
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            post_activity(
                &router,
                Some(MANAGER_TOKEN),
                json!({"event": "completed", "turn_id": "turn_4"})
            )
            .await,
            StatusCode::UNAUTHORIZED,
            "manager token is not an executor activity credential"
        );

        drop(state);
        testutil::remove_db_files(&db_path).await;
    }

    async fn post_activity(
        router: &Router,
        token: Option<&str>,
        body: serde_json::Value,
    ) -> StatusCode {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/activity")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        router
            .clone()
            .oneshot(builder.body(Body::from(body.to_string())).expect("body"))
            .await
            .expect("request")
            .status()
    }

    #[tokio::test]
    async fn host_and_origin_guard_blocks_unlisted() {
        let path = testutil::temp_db_path("http");
        let (state, db_path) = test_state(testutil::fixture_config(&path)).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        let response = router
            .clone()
            .oneshot(get_request("/health", None))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .header(header::HOST, "evil.com")
                    .body(Body::empty())
                    .expect("body"),
            )
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("body"),
            )
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .header(header::HOST, "localhost")
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::empty())
                    .expect("body"),
            )
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .header(header::HOST, "localhost")
                    .header(header::ORIGIN, "http://localhost")
                    .body(Body::empty())
                    .expect("body"),
            )
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK);

        drop(state);
        testutil::remove_db_files(&db_path).await;
    }

    #[tokio::test]
    async fn role_endpoint_rate_limits_valid_traffic() {
        let path = testutil::temp_db_path("http");
        let mut config = testutil::fixture_config(&path);
        config.mcp_request_rate_limit = 2;
        let (state, db_path) = test_state(config).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        for _ in 0..2 {
            let response = router
                .clone()
                .oneshot(get_request("/mcp/manager", Some(MANAGER_TOKEN)))
                .await
                .expect("request");
            assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        }
        let response = router
            .clone()
            .oneshot(get_request("/mcp/manager", Some(MANAGER_TOKEN)))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        drop(state);
        testutil::remove_db_files(&db_path).await;
    }

    #[tokio::test]
    async fn serve_lifecycle_starts_and_shuts_down_cleanly() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("http-serve");
        let (state, db_path) = test_state(testutil::fixture_config(&path)).await;
        // An immediately-ready shutdown future exercises the whole server
        // lifecycle: bind, serve, graceful shutdown, worker cancellation.
        serve_with_shutdown(state.clone(), std::future::ready(())).await?;
        drop(state);
        testutil::remove_db_files(&db_path).await;
        Ok(())
    }

    #[tokio::test]
    async fn activity_endpoint_validation_and_mode_branches() {
        // detail too long -> 413; bad occurred_at -> 422; disabled mode -> 202 no record.
        let path = testutil::temp_db_path("http");
        let mut config = testutil::fixture_config(&path);
        config.max_message_chars = 10;
        let (state, db_path) = test_state(config).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        let response = post_activity(
            &router,
            Some(DEVELOPER_TOKEN),
            json!({"event": "completed", "turn_id": "turn_long", "detail": "x".repeat(20)}),
        )
        .await;
        assert_eq!(response, StatusCode::PAYLOAD_TOO_LARGE);
        let response = post_activity(
            &router,
            Some(DEVELOPER_TOKEN),
            json!({"event": "completed", "turn_id": "turn_bad", "occurred_at": "not-a-date"}),
        )
        .await;
        assert_eq!(response, StatusCode::UNPROCESSABLE_ENTITY);

        drop(state);
        testutil::remove_db_files(&db_path).await;

        let path = testutil::temp_db_path("http");
        let mut config = testutil::fixture_config(&path);
        config.activity_enabled = false;
        let (state, db_path) = test_state(config).await;
        let router = build_router(state.clone(), &CancellationToken::new());
        let response = post_activity(
            &router,
            Some(DEVELOPER_TOKEN),
            json!({"event": "completed", "turn_id": "turn_disabled"}),
        )
        .await;
        assert_eq!(response, StatusCode::ACCEPTED);
        drop(state);
        testutil::remove_db_files(&db_path).await;
    }

    #[cfg(unix)]
    async fn run_signal_child(child_test: &str, signal: &str) -> anyhow::Result<()> {
        use tokio::io::AsyncBufReadExt;

        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg(child_test)
            // libtest captures test stdout by default; the readiness marker must flow.
            .arg("--nocapture")
            .env("SWARM_MCP_CHILD_TEST", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        // Wait for the child to report that its signal handlers are installed,
        // so the signal can never race the handler registration.
        let stdout = child.stdout.take().expect("child stdout");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(line) = lines.next_line().await? {
                if line.contains("SWARM_CHILD_READY") {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            anyhow::bail!("child exited before reporting readiness");
        })
        .await??;

        std::process::Command::new("kill")
            .args([signal, &child.id().expect("child id").to_string()])
            .status()?;
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait()).await??;
        assert!(
            status.success(),
            "child must exit cleanly after {signal}: {status}"
        );
        Ok(())
    }

    /// Body of the SIGTERM child: waits for the real signal, then reports ready
    /// (only meaningful when spawned by `shutdown_signal_responds_to_sigterm`).
    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_child_sigterm() -> anyhow::Result<()> {
        if std::env::var("SWARM_MCP_CHILD_TEST").is_err() {
            return Ok(());
        }
        let signal = shutdown_signal();
        tokio::pin!(signal);
        // Poll once so the handlers are installed, then report readiness.
        tokio::select! {
            () = &mut signal => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        println!("SWARM_CHILD_READY");
        std::io::stdout().flush()?;
        signal.await;
        Ok(())
    }

    /// Body of the SIGINT child: exercises the Ctrl-C branch of the shutdown.
    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_child_sigint() -> anyhow::Result<()> {
        if std::env::var("SWARM_MCP_CHILD_TEST").is_err() {
            return Ok(());
        }
        let signal = shutdown_signal();
        tokio::pin!(signal);
        tokio::select! {
            () = &mut signal => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        println!("SWARM_CHILD_READY");
        std::io::stdout().flush()?;
        signal.await;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_responds_to_sigterm() -> anyhow::Result<()> {
        run_signal_child("http::tests::shutdown_signal_child_sigterm", "-TERM").await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_responds_to_sigint() -> anyhow::Result<()> {
        run_signal_child("http::tests::shutdown_signal_child_sigint", "-INT").await
    }

    #[tokio::test]
    async fn shutdown_selects_on_either_signal_source() {
        shutdown_signal_with(std::future::ready(()), std::future::pending::<()>()).await;
        shutdown_signal_with(std::future::pending::<()>(), std::future::ready(())).await;
    }

    #[tokio::test]
    async fn activity_endpoint_rate_limits_per_credential() {
        let path = testutil::temp_db_path("http");
        let mut config = testutil::fixture_config(&path);
        config.mcp_request_rate_limit = 1;
        let (state, db_path) = test_state(config).await;
        let router = build_router(state.clone(), &CancellationToken::new());

        let first = post_activity(
            &router,
            Some(DEVELOPER_TOKEN),
            json!({"event": "completed", "turn_id": "turn_1"}),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        let second = post_activity(
            &router,
            Some(DEVELOPER_TOKEN),
            json!({"event": "completed", "turn_id": "turn_2"}),
        )
        .await;
        assert_eq!(second, StatusCode::TOO_MANY_REQUESTS);

        drop(state);
        testutil::remove_db_files(&db_path).await;
    }
}
