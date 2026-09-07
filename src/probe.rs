use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, bail, ensure};
use rmcp::{
    ServiceExt,
    model::{
        ClientCapabilities, ClientInfo, Implementation, ReadResourceRequestParams, ResourceContents,
    },
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::config::Config;

pub async fn catalog_probe(config: Arc<Config>, base_url: &str) -> anyhow::Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(config.api_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut roles = serde_json::Map::new();
    let mut catalogs_ok = true;

    for role in &config.all_roles {
        let (tools, resources) = catalog(&config, base_url, role).await?;
        let expected_tools = expected_tools(&config, role);
        let expected_resources = expected_resources(&config, role);
        let role_ok = tools == expected_tools && resources == expected_resources;
        catalogs_ok &= role_ok;
        roles.insert(
            role.clone(),
            json!({
                "tools": tools,
                "expected_tools": expected_tools,
                "resources": resources,
                "expected_resources": expected_resources,
                "ok": role_ok,
            }),
        );
    }

    let mut cross_auth_rejected = true;
    for role in &config.all_roles {
        let token = config
            .agents
            .get(role)
            .expect("configuration validates every role")
            .mcp_token
            .expose();
        for other in config.all_roles.iter().filter(|other| *other != role) {
            let response = client
                .get(endpoint(&config, base_url, other))
                .bearer_auth(token)
                .send()
                .await
                .with_context(|| format!("test {role} token against {other} endpoint"))?;
            cross_auth_rejected &= response.status() == reqwest::StatusCode::UNAUTHORIZED;
        }
    }

    let result = json!({
        "ok": catalogs_ok && cross_auth_rejected,
        "roles": roles,
        "cross_auth_rejected": cross_auth_rejected,
    });
    ensure!(
        catalogs_ok && cross_auth_rejected,
        "role catalog or endpoint isolation probe failed: {result}"
    );
    Ok(result)
}

pub async fn activity_probe(config: Arc<Config>, base_url: &str) -> anyhow::Result<Value> {
    if !config
        .activity_routes
        .values()
        .any(|targets| !targets.is_empty())
    {
        return Ok(json!({"ok": true, "skipped": "no activity routes"}));
    }
    let http = reqwest::Client::builder()
        .timeout(config.api_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut checked = Vec::new();
    for (sender, targets) in &config.activity_routes {
        let token = config
            .agents
            .get(sender)
            .expect("configuration validates activity senders")
            .mcp_token
            .expose();
        for target in targets {
            let turn_id = format!("activity_probe_{}", Uuid::new_v4().simple());
            let response = http
                .post(format!(
                    "{}{}",
                    base_url.trim_end_matches('/'),
                    config.activity_path
                ))
                .bearer_auth(token)
                .json(&json!({
                    "event": "completed",
                    "turn_id": turn_id,
                    "detail": "Passive activity deployment probe.",
                    "occurred_at": chrono::Utc::now().to_rfc3339(),
                }))
                .send()
                .await
                .context("send activity probe")?;
            ensure!(
                response.status().is_success(),
                "activity endpoint returned {}",
                response.status()
            );
            let delivery: Value = response.json().await.context("decode activity response")?;
            if delivery.get("enabled") == Some(&Value::Bool(false)) {
                return Ok(json!({"ok": true, "disabled": true}));
            }
            ensure!(
                delivery["mode"] == "record-only",
                "activity endpoint is not passive"
            );

            let snapshot =
                read_json_resource(&config, base_url, target, "swarm://activity").await?;
            let visible = snapshot
                .get("recent")
                .and_then(Value::as_array)
                .is_some_and(|events| {
                    events.iter().any(|event| {
                        event["sender"] == sender.as_str() && event["turn_id"] == turn_id
                    })
                });
            ensure!(
                snapshot["mode"] == "record-only" && visible,
                "activity signal was not visible to {target}"
            );

            for non_target in config.all_roles.iter().filter(|candidate| {
                !targets.contains(candidate)
                    && config
                        .dispatch_acl
                        .get(*candidate)
                        .is_some_and(|allowed| !allowed.is_empty())
            }) {
                let snapshot =
                    read_json_resource(&config, base_url, non_target, "swarm://activity").await?;
                let leaked = snapshot
                    .get("recent")
                    .and_then(Value::as_array)
                    .is_some_and(|events| {
                        events.iter().any(|event| {
                            event["sender"] == sender.as_str() && event["turn_id"] == turn_id
                        })
                    });
                ensure!(
                    !leaked,
                    "activity signal from {sender} leaked to non-target {non_target}"
                );
            }
            checked.push(json!({"sender": sender, "target": target, "turn_id": turn_id}));
        }
    }
    Ok(json!({"ok": true, "mode": "record-only", "checked": checked}))
}

pub async fn healthcheck(port: u16) -> anyhow::Result<()> {
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?
        .get(format!("http://127.0.0.1:{port}/ready"))
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "readiness returned {}",
        response.status()
    );
    Ok(())
}

async fn catalog(
    config: &Config,
    base_url: &str,
    role: &str,
) -> anyhow::Result<(BTreeSet<String>, BTreeSet<String>)> {
    let agent = config
        .agents
        .get(role)
        .with_context(|| format!("missing agent configuration for {role}"))?;
    let transport = StreamableHttpClientTransport::with_client(
        reqwest::Client::builder()
            .timeout(config.api_timeout)
            .build()?,
        StreamableHttpClientTransportConfig::with_uri(endpoint(config, base_url, role))
            .auth_header(agent.mcp_token.expose()),
    );
    let client = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("swarm-mcp-probe", env!("CARGO_PKG_VERSION")),
    )
    .serve(transport)
    .await
    .with_context(|| format!("initialize {role} MCP endpoint"))?;
    let tools = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect();
    let resources = client
        .list_all_resources()
        .await?
        .into_iter()
        .map(|resource| resource.uri)
        .collect();
    client.cancel().await?;
    Ok((tools, resources))
}

async fn read_json_resource(
    config: &Config,
    base_url: &str,
    role: &str,
    uri: &str,
) -> anyhow::Result<Value> {
    let agent = config
        .agents
        .get(role)
        .with_context(|| format!("missing agent configuration for {role}"))?;
    let transport = StreamableHttpClientTransport::with_client(
        reqwest::Client::builder()
            .timeout(config.api_timeout)
            .build()?,
        StreamableHttpClientTransportConfig::with_uri(endpoint(config, base_url, role))
            .auth_header(agent.mcp_token.expose()),
    );
    let client = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("swarm-mcp-probe", env!("CARGO_PKG_VERSION")),
    )
    .serve(transport)
    .await?;
    let resource = client
        .read_resource(ReadResourceRequestParams::new(uri))
        .await?;
    client.cancel().await?;
    let Some(ResourceContents::TextResourceContents { text, .. }) = resource.contents.first()
    else {
        bail!("{uri} returned no text content");
    };
    serde_json::from_str(text).with_context(|| format!("decode {uri}"))
}

fn expected_tools(config: &Config, role: &str) -> BTreeSet<String> {
    let mut tools = BTreeSet::from(["cancel_delivery".to_string()]);
    if config
        .dispatch_acl
        .get(role)
        .is_some_and(|targets| !targets.is_empty())
    {
        tools.insert("dispatch_to".to_string());
    }
    if config.global_authorities.contains(role) {
        tools.insert("dispatch_all".to_string());
    }
    if role == config.manager_role {
        tools.extend(
            [
                "messaging_clear_queue",
                "messaging_disable",
                "messaging_enable",
            ]
            .map(str::to_string),
        );
    }
    tools.extend(["msg_all", "msg_to"].map(str::to_string));
    tools.insert("telegram_reply".to_string());
    tools
}

fn expected_resources(config: &Config, role: &str) -> BTreeSet<String> {
    let mut resources = BTreeSet::from([
        "swarm://hierarchy".to_string(),
        "swarm://operations".to_string(),
        "swarm://outbox".to_string(),
    ]);
    if role == config.manager_role {
        resources.insert("swarm://executors".to_string());
        resources.insert("swarm://messaging".to_string());
    }
    if config
        .dispatch_acl
        .get(role)
        .is_some_and(|targets| !targets.is_empty())
    {
        resources.insert("swarm://activity".to_string());
    }
    resources
}

fn endpoint(config: &Config, base_url: &str, role: &str) -> String {
    format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        config
            .role_paths
            .get(role)
            .expect("configuration validates role paths")
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn config() -> (Config, std::path::PathBuf) {
        let path = crate::testutil::temp_db_path("probe");
        let config = crate::testutil::fixture_config(&path);
        (config, path)
    }

    #[tokio::test]
    async fn expected_catalogs_match_the_role_hierarchy() {
        let (config, path) = config();
        // manager: global authority
        assert_eq!(
            expected_tools(&config, "manager"),
            BTreeSet::from([
                "cancel_delivery".to_string(),
                "messaging_clear_queue".to_string(),
                "messaging_disable".to_string(),
                "messaging_enable".to_string(),
                "dispatch_all".to_string(),
                "dispatch_to".to_string(),
                "telegram_reply".to_string(),
                "msg_all".to_string(),
                "msg_to".to_string()
            ])
        );
        let mut manager_resources = BTreeSet::from([
            "swarm://hierarchy".to_string(),
            "swarm://operations".to_string(),
            "swarm://outbox".to_string(),
            "swarm://executors".to_string(),
            "swarm://activity".to_string(),
            "swarm://messaging".to_string(),
        ]);
        assert_eq!(expected_resources(&config, "manager"), manager_resources);
        // lead-developer: authority with single-target transport dispatch
        assert_eq!(
            expected_tools(&config, "lead-developer"),
            BTreeSet::from([
                "cancel_delivery".to_string(),
                "msg_all".to_string(),
                "msg_to".to_string(),
                "dispatch_to".to_string(),
                "telegram_reply".to_string()
            ])
        );
        manager_resources.remove("swarm://executors");
        manager_resources.remove("swarm://messaging");
        assert_eq!(
            expected_resources(&config, "lead-developer"),
            manager_resources
        );
        // plain executor: coordination only, no activity resource
        assert_eq!(
            expected_tools(&config, "developer"),
            BTreeSet::from([
                "cancel_delivery".to_string(),
                "msg_all".to_string(),
                "msg_to".to_string(),
                "telegram_reply".to_string()
            ])
        );
        assert_eq!(
            expected_resources(&config, "developer"),
            BTreeSet::from([
                "swarm://hierarchy".to_string(),
                "swarm://operations".to_string(),
                "swarm://outbox".to_string(),
            ])
        );
        crate::testutil::remove_db_files(&path).await;
    }

    #[tokio::test]
    async fn activity_probe_skips_when_no_routes_configured() {
        let (mut config, path) = config();
        config.activity_routes = BTreeMap::default();
        let result = activity_probe(Arc::new(config), "http://127.0.0.1:1")
            .await
            .unwrap();
        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["skipped"], json!("no activity routes"));
        crate::testutil::remove_db_files(&path).await;
    }

    /// Boot the real HTTP router (role MCP services + auth + activity endpoint)
    /// on an ephemeral port and run both probes against it — the same check the
    /// deployment runs, exercised in-process.
    async fn spawn_live_router() -> anyhow::Result<(Arc<Config>, std::path::PathBuf, u16)> {
        let path = crate::testutil::temp_db_path("probe-e2e");
        let mut config = crate::testutil::fixture_config(&path);
        // The probes connect to 127.0.0.1, so the Host guard must allow it.
        config.allowed_hosts = vec!["127.0.0.1".to_string()];
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(crate::AppState {
            config: config.clone(),
            store,
            dispatcher,
        });
        let router = crate::http::build_router(state, &tokio_util::sync::CancellationToken::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("live router serves");
        });
        Ok((config, path, port))
    }

    #[tokio::test]
    async fn catalog_probe_passes_against_a_live_router() -> anyhow::Result<()> {
        let (config, path, port) = spawn_live_router().await?;
        let result = catalog_probe(config.clone(), &format!("http://127.0.0.1:{port}")).await?;
        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["cross_auth_rejected"], json!(true));
        assert_eq!(result["roles"].as_object().unwrap().len(), 3);
        crate::testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn activity_probe_reports_disabled_activity() -> anyhow::Result<()> {
        let path = crate::testutil::temp_db_path("probe-e2e");
        let mut config = crate::testutil::fixture_config(&path);
        config.allowed_hosts = vec!["127.0.0.1".to_string()];
        config.activity_enabled = false;
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(crate::AppState {
            config: config.clone(),
            store,
            dispatcher,
        });
        let router = crate::http::build_router(state, &tokio_util::sync::CancellationToken::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("live router serves");
        });

        let result = activity_probe(config, &format!("http://127.0.0.1:{port}")).await?;
        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["disabled"], json!(true));
        crate::testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn activity_probe_passes_against_a_live_router() -> anyhow::Result<()> {
        let (config, path, port) = spawn_live_router().await?;
        let result = activity_probe(config.clone(), &format!("http://127.0.0.1:{port}")).await?;
        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["mode"], json!("record-only"));
        assert_eq!(result["checked"].as_array().unwrap().len(), 1);
        crate::testutil::remove_db_files(&path).await;
        Ok(())
    }
}
