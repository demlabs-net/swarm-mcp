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
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::DateTime;
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

use crate::{AppState, config::Config, mcp::RoleMcp, store::ActivityRecord};

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
    let cancellation = CancellationToken::new();
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready));

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
        let protected =
            Router::new()
                .nest_service(path, service)
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

    let app: Router = app
        .merge(activity_router)
        .layer(DefaultBodyLimit::max(state.config.max_request_body_bytes))
        .layer(middleware::from_fn_with_state(
            state.config.clone(),
            validate_host_and_origin,
        ))
        .with_state::<()>(state.clone());

    let outbox = state
        .dispatcher
        .spawn_outbox_worker(cancellation.child_token());
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
            shutdown_signal().await;
            shutdown_token.cancel();
        })
        .await;
    cancellation.cancel();
    let _ = outbox.await;
    let _ = cleanup.await;
    result?;
    Ok(())
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
    if let Err(message) = validate_identifier(&input.turn_id, "turn_id") {
        return api_error(StatusCode::UNPROCESSABLE_ENTITY, message);
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

fn validate_identifier(value: &str, field: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 160 {
        return Err("turn_id must contain between 1 and 160 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        return Err("turn_id contains unsupported characters");
    }
    let _ = field;
    Ok(())
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

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

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
}
