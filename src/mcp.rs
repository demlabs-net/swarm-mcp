use std::{collections::BTreeMap, sync::Arc};

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorCode,
        Implementation, JsonObject, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
        ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
        ResourceContents, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
    },
    service::RequestContext,
};
use serde_json::{Value, json};

use crate::{
    AppState,
    config::TelegramBotMode,
    dispatch::{
        BroadcastArgs, MessageAllArgs, MessageArgs, OrderArgs, ReportArgs, ToolOutcome,
        parse_arguments, render_template,
    },
};

#[derive(Clone)]
pub struct RoleMcp {
    state: Arc<AppState>,
    role: String,
}

impl RoleMcp {
    pub fn new(state: Arc<AppState>, role: String) -> Self {
        Self { state, role }
    }

    fn tools(&self) -> Vec<Tool> {
        let mut tools = Vec::new();
        let config = &self.state.config;
        let targets = config
            .order_acl
            .get(&self.role)
            .cloned()
            .unwrap_or_default();
        if !targets.is_empty() {
            tools.push(tool(
                "order",
                "Assign a task to one executor authorized by the current hierarchy.",
                object_schema(
                    &json!({
                        "agent": {
                            "type": "string",
                            "enum": targets,
                            "description": "Authorized target executor."
                        },
                        "command": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars,
                            "description": "Concrete task command."
                        },
                        "idempotency_key": idempotency_schema()
                    }),
                    &["agent", "command"],
                ),
            ));
        }
        if config.global_authorities.contains(&self.role) {
            tools.push(tool(
                "order_all",
                "Assign the same task to every executor under this global authority.",
                object_schema(
                    &json!({
                        "command": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "idempotency_key": idempotency_schema()
                    }),
                    &["command"],
                ),
            ));
        }
        if config.agent_roles.contains(&self.role) {
            let supervisors = config.supervisors(&self.role);
            let peers = config
                .agent_roles
                .iter()
                .filter(|role| *role != &self.role)
                .cloned()
                .collect::<Vec<_>>();
            tools.push(tool(
                "report",
                "Report progress, completion, failure, or a blocker to an authorized supervisor.",
                object_schema(
                    &json!({
                        "summary": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "task_id": {
                            "type": "string",
                            "maxLength": crate::dispatch::MAX_IDENTIFIER_BYTES,
                            "default": ""
                        },
                        "status": {
                            "type": "string",
                            "enum": config.report_statuses,
                            "default": "completed"
                        },
                        "recipient": {
                            "type": "string",
                            "enum": supervisors,
                            "default": config.manager_role
                        },
                        "idempotency_key": idempotency_schema()
                    }),
                    &["summary"],
                ),
            ));
            tools.push(tool(
                "msg_to",
                "Send a direct coordination message to another executor.",
                object_schema(
                    &json!({
                        "agent": {"type": "string", "enum": peers},
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "idempotency_key": idempotency_schema()
                    }),
                    &["agent", "message"],
                ),
            ));
            tools.push(tool(
                "msg_all",
                "Send the same coordination message to every other executor.",
                object_schema(
                    &json!({
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "idempotency_key": idempotency_schema()
                    }),
                    &["message"],
                ),
            ));
        }
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    fn resources(&self) -> Vec<Resource> {
        let mut resources = vec![
            Resource::new("swarm://hierarchy", "hierarchy")
                .with_title("Development swarm hierarchy")
                .with_description("Role hierarchy, order ACL, supervisors, and capabilities.")
                .with_mime_type("application/json"),
            Resource::new("swarm://operations", "operations")
                .with_title("Role operation ledger")
                .with_description("Recent durable operations visible to this role.")
                .with_mime_type("application/json"),
            Resource::new("swarm://outbox", "outbox")
                .with_title("Telegram audit delivery")
                .with_description(
                    "Recent Telegram audit states. Executors see their own items; the manager sees all items.",
                )
                .with_mime_type("application/json"),
        ];
        if self.role == self.state.config.manager_role {
            resources.push(
                Resource::new("swarm://executors", "executors")
                    .with_title("Development swarm executors")
                    .with_description("Compatibility alias for swarm://hierarchy.")
                    .with_mime_type("application/json"),
            );
        }
        if self
            .state
            .config
            .order_acl
            .get(&self.role)
            .is_some_and(|targets| !targets.is_empty())
        {
            resources.push(
                Resource::new("swarm://activity", "activity")
                    .with_title("Subordinate activity state")
                    .with_description(
                        "Passive lifecycle state; reading it never starts an agent or sends Telegram.",
                    )
                    .with_mime_type("application/json"),
            );
        }
        resources.sort_by(|left, right| left.uri.cmp(&right.uri));
        resources
    }

    fn hierarchy(&self) -> Value {
        let config = &self.state.config;
        let targets = config
            .order_acl
            .get(&self.role)
            .cloned()
            .unwrap_or_default();
        let tools = self
            .tools()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>();
        json!({
            "caller": self.role,
            "manager": config.manager_role,
            "roles": config.all_roles.iter().map(|role| json!({
                "role": role,
                "description": if role == &config.manager_role {
                    "Swarm-wide manager"
                } else {
                    config.descriptions.get(role).map_or("", String::as_str)
                },
                "may_order": config.order_acl.get(role).cloned().unwrap_or_default(),
            })).collect::<Vec<_>>(),
            "caller_may_order": targets,
            "caller_supervisors": if config.agent_roles.contains(&self.role) {
                config.supervisors(&self.role)
            } else {
                Vec::<String>::new()
            },
            "tools": tools,
            "safeguards": {
                "durable_operations": true,
                "idempotency_keys": true,
                "rate_limit": config.rate_limit,
                "rate_window_seconds": config.rate_window.as_secs(),
                "max_inflight_dispatches": config.max_inflight_dispatches,
                "telegram_outbox": config.telegram_enabled,
                "telegram_bot_mode": match config.telegram_bot_mode {
                    TelegramBotMode::PerRole => "per-role",
                    TelegramBotMode::Shared => "shared",
                },
                "telegram_sender_configured": config.telegram_token_for(&self.role).is_some(),
                "telegram_inbound": config.telegram_inbound_enabled,
                "activity_mode": "record-only",
            }
        })
    }

    fn instructions(&self) -> String {
        let config = &self.state.config;
        let mut pieces = vec![if self.role == config.manager_role {
            config.manager_instructions.clone()
        } else {
            config.executor_instructions.clone()
        }];
        if let Some(targets) = config
            .order_acl
            .get(&self.role)
            .filter(|targets| !targets.is_empty())
        {
            let role = self.role.as_str();
            let target_list = targets.join(", ");
            let values = BTreeMap::from([("role", role), ("targets", target_list.as_str())]);
            match render_template(&config.authority_instructions, &values) {
                Ok(rendered) => pieces.push(rendered),
                Err(error) => {
                    tracing::error!(%role, error = %error, "authority instruction template failed");
                }
            }
        }
        pieces.push(
            format!(
                "Use a unique, stable idempotency_key when retrying any order, report, or message; never recycle it for another logical operation. A dispatch that definitively failed releases its key, so retrying with the same key re-executes; accepted, partial, and indeterminate results replay. Idempotency records are retained for {} days. Read swarm://operations to inspect accepted, partial, and indeterminate dispatches before retrying.",
                config.operation_retention_days
            )
        );
        pieces.join("\n\n")
    }
}

impl ServerHandler for RoleMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new("swarm-mcp", env!("CARGO_PKG_VERSION")))
        .with_instructions(self.instructions())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(self.tools()))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools().into_iter().find(|tool| tool.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        if self.get_tool(request.name.as_ref()).is_none() {
            return Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                format!(
                    "tool '{}' is not available to role '{}'",
                    request.name, self.role
                ),
                None,
            ));
        }
        let outcome = match request.name.as_ref() {
            "order" => match parse_arguments::<OrderArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.order(&self.role, args).await,
                Err(outcome) => outcome,
            },
            "order_all" => match parse_arguments::<BroadcastArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.order_all(&self.role, args).await,
                Err(outcome) => outcome,
            },
            "report" => match parse_arguments::<ReportArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.report(&self.role, args).await,
                Err(outcome) => outcome,
            },
            "msg_to" => match parse_arguments::<MessageArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.message(&self.role, args).await,
                Err(outcome) => outcome,
            },
            "msg_all" => match parse_arguments::<MessageAllArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.message_all(&self.role, args).await,
                Err(outcome) => outcome,
            },
            _ => unreachable!("tool availability was checked above"),
        };
        Ok(tool_result(outcome).into())
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(self.resources()))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        if !self
            .resources()
            .iter()
            .any(|resource| resource.uri == request.uri)
        {
            return Err(McpError::resource_not_found(
                format!(
                    "resource '{}' is not available to role '{}'",
                    request.uri, self.role
                ),
                None,
            ));
        }
        let value = match request.uri.as_str() {
            "swarm://hierarchy" | "swarm://executors" => self.hierarchy(),
            "swarm://activity" => {
                let allowed = self
                    .state
                    .config
                    .order_acl
                    .get(&self.role)
                    .cloned()
                    .unwrap_or_default();
                self.state
                    .store
                    .activity_snapshot(
                        &self.role,
                        &allowed,
                        self.state.config.activity_enabled,
                        self.state.config.activity_history_limit,
                        self.state.config.activity_stale_after,
                    )
                    .await
                    .map_err(internal_error)?
            }
            "swarm://operations" => self
                .state
                .store
                .recent_operations(&self.role, self.state.config.recent_operations_limit)
                .await
                .map_err(internal_error)?,
            "swarm://outbox" => self
                .state
                .store
                .recent_outbox(
                    &self.role,
                    &self.state.config.manager_role,
                    self.state.config.recent_operations_limit,
                )
                .await
                .map_err(internal_error)?,
            _ => unreachable!("resource availability was checked above"),
        };
        let text = serde_json::to_string_pretty(&value).map_err(internal_error)?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, request.uri).with_mime_type("application/json"),
        ])
        .into())
    }
}

fn tool(name: &'static str, description: &'static str, schema: Arc<JsonObject>) -> Tool {
    let mut value = Tool::new(name, description, schema);
    value.annotations = Some(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true),
    );
    value
}

fn object_schema(properties: &Value, required: &[&str]) -> Arc<JsonObject> {
    Arc::new(
        json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        })
        .as_object()
        .cloned()
        .expect("object schema must be an object"),
    )
}

fn idempotency_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": crate::dispatch::MAX_IDENTIFIER_BYTES,
        "pattern": "^[A-Za-z0-9_.:-]+$",
        "description": "Stable key for safe retries. Reusing it with different arguments is rejected."
    })
}

fn tool_result(outcome: ToolOutcome) -> CallToolResult {
    let mut result = if outcome.is_error {
        CallToolResult::structured_error(outcome.value)
    } else {
        CallToolResult::structured(outcome.value)
    };
    if result.content.is_empty() {
        result.content = vec![ContentBlock::text("{}")];
    }
    result
}

fn internal_error(error: impl std::fmt::Display) -> McpError {
    tracing::error!(error = %error, "MCP resource read failed");
    McpError::internal_error("persistent resource read failed", None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppState, testutil};
    use rmcp::ServiceExt;
    use rmcp::model::{CallToolRequestParams, ReadResourceRequestParams};

    async fn role_mcp(role: &str) -> anyhow::Result<(RoleMcp, std::path::PathBuf)> {
        let path = testutil::temp_db_path("mcp");
        let config = std::sync::Arc::new(testutil::fixture_config(&path));
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = std::sync::Arc::new(AppState {
            config,
            store,
            dispatcher,
        });
        Ok((RoleMcp::new(state, role.to_string()), path))
    }

    fn tool_names(mcp: &RoleMcp) -> Vec<String> {
        mcp.tools()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect()
    }

    fn resource_uris(mcp: &RoleMcp) -> Vec<String> {
        mcp.resources()
            .into_iter()
            .map(|resource| resource.uri)
            .collect()
    }

    #[tokio::test]
    async fn manager_catalog_is_global_authority() -> anyhow::Result<()> {
        let (mcp, path) = role_mcp("manager").await?;
        assert_eq!(tool_names(&mcp), vec!["order", "order_all"]);
        let resources = resource_uris(&mcp);
        assert!(resources.contains(&"swarm://executors".to_string()));
        assert!(resources.contains(&"swarm://activity".to_string()));
        // the order tool schema enumerates exactly the ACL targets
        let order = mcp
            .tools()
            .into_iter()
            .find(|tool| tool.name == "order")
            .unwrap();
        let schema = serde_json::to_value(&order.input_schema).unwrap();
        assert_eq!(
            schema["properties"]["agent"]["enum"],
            json!(["developer", "lead-developer"])
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn executor_catalog_is_coordination_only() -> anyhow::Result<()> {
        let (mcp, path) = role_mcp("developer").await?;
        assert_eq!(tool_names(&mcp), vec!["msg_all", "msg_to", "report"]);
        let resources = resource_uris(&mcp);
        assert!(!resources.contains(&"swarm://executors".to_string()));
        assert!(!resources.contains(&"swarm://activity".to_string()));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn authority_catalog_exposes_activity() -> anyhow::Result<()> {
        let (mcp, path) = role_mcp("lead-developer").await?;
        assert_eq!(
            tool_names(&mcp),
            vec!["msg_all", "msg_to", "order", "report"]
        );
        let resources = resource_uris(&mcp);
        assert!(resources.contains(&"swarm://activity".to_string()));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn hierarchy_json_reports_effective_acl() -> anyhow::Result<()> {
        let (mcp, path) = role_mcp("lead-developer").await?;
        let hierarchy = mcp.hierarchy();
        assert_eq!(hierarchy["caller"], json!("lead-developer"));
        assert_eq!(hierarchy["manager"], json!("manager"));
        assert_eq!(hierarchy["caller_may_order"], json!(["developer"]));
        assert_eq!(hierarchy["caller_supervisors"], json!(["manager"]));
        assert_eq!(
            hierarchy["safeguards"]["telegram_sender_configured"],
            json!(false)
        );
        assert_eq!(
            hierarchy["tools"],
            json!(["msg_all", "msg_to", "order", "report"])
        );
        assert!(mcp.instructions().contains("releases its key"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[test]
    fn parse_arguments_validates_the_json_contract() {
        let valid: Result<OrderArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"agent":"developer","command":"x"}"#).unwrap(),
        ));
        assert!(valid.is_ok());
        let unknown: Result<OrderArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"agent":"developer","command":"x","extra":1}"#).unwrap(),
        ));
        assert!(unknown.is_err(), "deny_unknown_fields must reject extras");
        let missing: Result<OrderArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"agent":"developer"}"#).unwrap(),
        ));
        assert!(missing.is_err(), "required fields are enforced");
        let empty: Result<OrderArgs, ToolOutcome> = parse_arguments(None);
        assert!(empty.is_err(), "no arguments is an error");
    }

    /// Boot the real router (auth + MCP services) with a mock Hermes and return
    /// the base URL plus the config needed to connect as any role.
    async fn spawn_live_router()
    -> anyhow::Result<(String, Arc<crate::config::Config>, std::path::PathBuf)> {
        let path = testutil::temp_db_path("mcp-e2e");
        let hermes =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "run-e2e"})))).await;
        let mut config = testutil::fixture_config(&path);
        config.allowed_hosts = vec!["127.0.0.1".to_string()];
        for agent in config.agents.values_mut() {
            agent.api_url = hermes.parse()?;
        }
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(AppState {
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
        Ok((format!("http://127.0.0.1:{port}"), config, path))
    }

    fn resource_text(response: &rmcp::model::ReadResourceResult) -> &str {
        let rmcp::model::ResourceContents::TextResourceContents { text, .. } =
            response.contents.first().expect("text content")
        else {
            panic!("resource must be text");
        };
        text.as_str()
    }

    #[tokio::test]
    async fn call_tool_and_read_resource_via_live_router() -> anyhow::Result<()> {
        let (base, config, path) = spawn_live_router().await?;
        let client = rmcp::model::ClientInfo::new(
            rmcp::model::ClientCapabilities::default(),
            rmcp::model::Implementation::new("mcp-e2e", env!("CARGO_PKG_VERSION")),
        )
        .serve(rmcp::transport::StreamableHttpClientTransport::with_client(
            reqwest::Client::builder().build()?,
            rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
                format!("{base}/mcp/manager"),
            )
            .auth_header(config.agents["manager"].mcp_token.expose()),
        ))
        .await?;

        // order tool end-to-end: MCP call -> ACL -> mock Hermes -> durable result.
        let call =
            client
                .call_tool(CallToolRequestParams::new("order").with_arguments(
                    serde_json::from_str(r#"{"agent":"developer","command":"do it"}"#)?,
                ))
                .await?;
        let structured = call.structured_content.expect("structured result");
        assert_eq!(structured["ok"], json!(true));
        assert_eq!(structured["run_id"], json!("run-e2e"));

        // order_all goes through the same wire path as a global authority.
        let call_all = client
            .call_tool(
                CallToolRequestParams::new("order_all")
                    .with_arguments(serde_json::from_str(r#"{"command":"broadcast"}"#)?),
            )
            .await?;
        let structured_all = call_all.structured_content.expect("structured result");
        assert_eq!(structured_all["ok"], json!(true));
        assert_eq!(
            structured_all["results"]["developer"]["run_id"],
            json!("run-e2e")
        );

        // Unknown tools are rejected with METHOD_NOT_FOUND, not silently ignored.
        assert!(
            client
                .call_tool(CallToolRequestParams::new("frobnicate"))
                .await
                .is_err()
        );

        // Resources are served from the store through the HTTP transport.
        let hierarchy = client
            .read_resource(ReadResourceRequestParams::new("swarm://hierarchy"))
            .await?;
        let value: Value = serde_json::from_str(resource_text(&hierarchy))?;
        assert_eq!(value["caller"], json!("manager"));
        assert_eq!(value["tools"], json!(["order", "order_all"]));

        let operations = client
            .read_resource(ReadResourceRequestParams::new("swarm://operations"))
            .await?;
        assert_eq!(operations.contents.len(), 1);

        // Unknown resources are rejected, never silently empty.
        assert!(
            client
                .read_resource(ReadResourceRequestParams::new("swarm://nope"))
                .await
                .is_err()
        );

        client.cancel().await?;
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn executor_client_gets_its_own_resources() -> anyhow::Result<()> {
        let (base, config, path) = spawn_live_router().await?;
        let client = rmcp::model::ClientInfo::new(
            rmcp::model::ClientCapabilities::default(),
            rmcp::model::Implementation::new("mcp-e2e", env!("CARGO_PKG_VERSION")),
        )
        .serve(rmcp::transport::StreamableHttpClientTransport::with_client(
            reqwest::Client::builder().build()?,
            rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
                format!("{base}/mcp/lead-developer"),
            )
            .auth_header(config.agents["lead-developer"].mcp_token.expose()),
        ))
        .await?;
        let activity = client
            .read_resource(ReadResourceRequestParams::new("swarm://activity"))
            .await?;
        let value: Value = serde_json::from_str(resource_text(&activity))?;
        assert_eq!(value["caller"], json!("lead-developer"));
        assert_eq!(value["enabled"], json!(true));
        assert_eq!(value["mode"], json!("record-only"));

        // report and msg_to arms of call_tool.
        let report =
            client
                .call_tool(CallToolRequestParams::new("report").with_arguments(
                    serde_json::from_str(r#"{"summary":"done","task_id":"t1"}"#)?,
                ))
                .await?;
        let report_value = report.structured_content.expect("structured result");
        assert_eq!(report_value["ok"], json!(true));
        assert_eq!(report_value["recipient"], json!("manager"));

        let msg =
            client
                .call_tool(CallToolRequestParams::new("msg_to").with_arguments(
                    serde_json::from_str(r#"{"agent":"developer","message":"ping"}"#)?,
                ))
                .await?;
        let msg_value = msg.structured_content.expect("structured result");
        assert_eq!(msg_value["ok"], json!(true));
        assert_eq!(msg_value["agent"], json!("developer"));

        // The executor must NOT see the manager-only alias.
        assert!(
            client
                .read_resource(ReadResourceRequestParams::new("swarm://executors"))
                .await
                .is_err()
        );

        client.cancel().await?;
        testutil::remove_db_files(&path).await;
        Ok(())
    }
}
