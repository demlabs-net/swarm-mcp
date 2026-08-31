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
        BroadcastArgs, ClearMessageQueueArgs, DisableMessagingArgs, DispatchArgs,
        EnableMessagingArgs, MessageAllArgs, MessageArgs, TelegramReplyArgs, ToolOutcome,
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
            .dispatch_acl
            .get(&self.role)
            .cloned()
            .unwrap_or_default();
        if !targets.is_empty() {
            tools.push(tool(
                "dispatch_to",
                "Deliver a transport message to one authorized role. This does not create, own, mutate, or validate a task; create the task/event in SLC MCP first and pass its returned content-free delivery envelope when a wake is needed.",
                object_schema(
                    &json!({
                        "recipient": {
                            "type": "string",
                            "enum": targets,
                            "description": "Authorized delivery target."
                        },
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars,
                            "description": "Payload to deliver. Task state must already exist in SLC MCP."
                        },
                        "correlation_id": correlation_schema(),
                        "idempotency_key": idempotency_schema()
                    }),
                    &["recipient", "message"],
                ),
            ));
        }
        if config.global_authorities.contains(&self.role) {
            tools.push(tool(
                "dispatch_all",
                "Deliver the same transport message to every role in this authority's delivery ACL. No task or status is created.",
                object_schema(
                    &json!({
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "correlation_id": correlation_schema(),
                        "idempotency_key": idempotency_schema()
                    }),
                    &["message"],
                ),
            ));
        }
        if self.role == config.manager_role {
            let agents = config.agent_roles.clone();
            tools.push(control_tool(
                "messaging_disable",
                "Immediately block an executor from sending or receiving Swarm transport and optionally cancel undelivered Telegram audits involving it.",
                object_schema(
                    &json!({
                        "agent": {"type": "string", "enum": agents},
                        "reason": {"type": "string", "maxLength": 500, "default": ""},
                        "clear_queue": {
                            "type": "boolean",
                            "default": true,
                            "description": "Cancel pending and dead Telegram audit items sent by or addressed to this role."
                        }
                    }),
                    &["agent"],
                ),
                true,
            ));
            tools.push(control_tool(
                "messaging_enable",
                "Re-enable one executor only after an explicit human resume request, root-cause verification, and the configured cooldown. Read swarm://messaging immediately first; cancelled queue items are never replayed.",
                object_schema(
                    &json!({
                        "agent": {"type": "string", "enum": config.agent_roles},
                        "reason": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 500,
                            "description": "Concrete root-cause remediation verified after an explicit human resume request."
                        },
                        "expected_disabled_at": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Exact changed_at value read from swarm://messaging immediately before this call."
                        }
                    }),
                    &["agent", "reason", "expected_disabled_at"],
                ),
                false,
            ));
            tools.push(control_tool(
                "messaging_clear_queue",
                "Cancel undelivered Telegram audit items sent by or addressed to one executor without deleting delivered audit history.",
                object_schema(
                    &json!({
                        "agent": {"type": "string", "enum": config.agent_roles},
                        "include_dead": {
                            "type": "boolean",
                            "default": true,
                            "description": "Also mark exhausted dead-letter items as cancelled."
                        }
                    }),
                    &["agent"],
                ),
                true,
            ));
        }

        let peers = config
            .all_roles
            .iter()
            .filter(|role| *role != &self.role)
            .cloned()
            .collect::<Vec<_>>();
        if !peers.is_empty() {
            tools.push(tool(
                "msg_to",
                "Deliver a direct wake to another role. Swarm stores delivery metadata only; for task communication pass the content-free delivery envelope returned by SLC MCP.",
                object_schema(
                    &json!({
                        "recipient": {"type": "string", "enum": peers},
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "correlation_id": correlation_schema(),
                        "idempotency_key": idempotency_schema()
                    }),
                    &["recipient", "message"],
                ),
            ));
            tools.push(tool(
                "msg_all",
                "Deliver the same coordination message to every other role. This is transport, not a task event.",
                object_schema(
                    &json!({
                        "message": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": config.max_message_chars
                        },
                        "correlation_id": correlation_schema(),
                        "idempotency_key": idempotency_schema()
                    }),
                    &["message"],
                ),
            ));
        }
        tools.push(tool(
            "telegram_reply",
            "Send a message (and optionally files) to the operator who started the most recent Telegram dispatch for this role.",
            object_schema(
                &json!({
                    "message": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": config.max_message_chars
                    },
                    "files": {
                        "type": "array",
                        "maxItems": 5,
                        "items": {
                            "type": "object",
                            "properties": {
                                "filename": {"type": "string", "minLength": 1, "maxLength": 255},
                                "mime_type": {"type": "string", "maxLength": 100},
                                "content_b64": {"type": "string", "minLength": 1, "maxLength": 8_000_000}
                            },
                            "required": ["filename", "content_b64"],
                            "additionalProperties": false
                        }
                    }
                }),
                &[],
            ),
        ));
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    fn resources(&self) -> Vec<Resource> {
        let mut resources = vec![
            Resource::new("swarm://hierarchy", "hierarchy")
                .with_title("Swarm transport topology")
                .with_description("Role delivery ACL and transport capabilities. Task hierarchy lives in SLC MCP.")
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
            resources.push(
                Resource::new("swarm://messaging", "messaging")
                    .with_title("Executor messaging circuit breakers")
                    .with_description(
                        "Persistent per-executor messaging state and counts of undelivered Telegram audits involving each role.",
                    )
                    .with_mime_type("application/json"),
            );
        }
        if self
            .state
            .config
            .dispatch_acl
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
            .dispatch_acl
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
                "may_dispatch": config.dispatch_acl.get(role).cloned().unwrap_or_default(),
            })).collect::<Vec<_>>(),
            "caller_may_dispatch": targets,
            "caller_delivery_sources": config.dispatch_sources(&self.role),
            "tools": tools,
            "domain_boundary": {
                "transport_only": true,
                "task_system": "slc-mcp",
                "correlation_id": "opaque"
            },
            "safeguards": {
                "durable_operations": true,
                "idempotency_keys": true,
                "rate_limit": config.rate_limit,
                "rate_window_seconds": config.rate_window.as_secs(),
                "duplicate_window_seconds": config.duplicate_window.as_secs(),
                "messaging_reenable_cooldown_seconds": config.messaging_reenable_cooldown.as_secs(),
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
            .dispatch_acl
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
                "Swarm MCP is transport only. Create and mutate tasks, statuses, task reports, lineage, and task messages in SLC MCP. Then use dispatch_to/msg_to only when an immediate wake or delivery is needed and pass the SLC id as opaque correlation_id. Use a unique, stable idempotency_key for delivery retries; never recycle it for another logical delivery. A definitively failed delivery releases its key, while accepted, partial, and indeterminate results replay. Content-identical deliveries are suppressed for {} seconds. Delivery records are retained for {} days; read swarm://operations before retrying.",
                config.duplicate_window.as_secs(), config.operation_retention_days
            )
        );
        if self.role == config.manager_role {
            pieces.push(format!(
                "Use messaging_disable as the emergency circuit breaker when an executor loops or floods communication. It blocks both directions through Swarm MCP and, by default, cancels undelivered Telegram audits sent by or addressed to that executor. A delivery rejected by this breaker is a terminal stop condition for the current run: do not call messaging_enable merely to make delivery succeed. Re-enable only in a later turn after an explicit human resume request, verified remediation, a fresh swarm://messaging read, and the {}-second cooldown. messaging_enable never replays cancelled items.",
                config.messaging_reenable_cooldown.as_secs()
            ));
        }
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
            "dispatch_to" => match parse_arguments::<DispatchArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.dispatch_to(&self.role, args).await,
                Err(outcome) => outcome,
            },
            "dispatch_all" => match parse_arguments::<BroadcastArgs>(request.arguments) {
                Ok(args) => self.state.dispatcher.dispatch_all(&self.role, args).await,
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
            "telegram_reply" => match parse_arguments::<TelegramReplyArgs>(request.arguments) {
                Ok(args) => {
                    self.state
                        .dispatcher
                        .telegram_reply(&self.role, args.message, args.files)
                        .await
                }
                Err(outcome) => outcome,
            },
            "messaging_disable" => match parse_arguments::<DisableMessagingArgs>(request.arguments)
            {
                Ok(args) => {
                    self.state
                        .dispatcher
                        .disable_messaging(&self.role, args)
                        .await
                }
                Err(outcome) => outcome,
            },
            "messaging_enable" => match parse_arguments::<EnableMessagingArgs>(request.arguments) {
                Ok(args) => {
                    self.state
                        .dispatcher
                        .enable_messaging(&self.role, args)
                        .await
                }
                Err(outcome) => outcome,
            },
            "messaging_clear_queue" => {
                match parse_arguments::<ClearMessageQueueArgs>(request.arguments) {
                    Ok(args) => {
                        self.state
                            .dispatcher
                            .clear_message_queue(&self.role, args)
                            .await
                    }
                    Err(outcome) => outcome,
                }
            }
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
                    .dispatch_acl
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
            "swarm://messaging" => self
                .state
                .store
                .messaging_snapshot(&self.state.config.agent_roles)
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

fn control_tool(
    name: &'static str,
    description: &'static str,
    schema: Arc<JsonObject>,
    destructive: bool,
) -> Tool {
    let mut value = Tool::new(name, description, schema);
    value.annotations = Some(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(destructive)
            .idempotent(true)
            .open_world(false),
    );
    value
}

fn object_schema(properties: &Value, required: &[&str]) -> Arc<JsonObject> {
    Arc::new(
        json!({
            "type": "object",
            "properties": properties,
            "required": required,
            // Lenient: LLM clients occasionally add stray fields (e.g. wait_s);
            // rejecting them breaks the whole call. Unknown fields are ignored.
            "additionalProperties": true,
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

fn correlation_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": crate::dispatch::MAX_IDENTIFIER_BYTES,
        "pattern": "^[A-Za-z0-9_.:-]+$",
        "description": "Optional opaque external reference, normally an SLC task/event id. Swarm MCP never resolves or mutates it."
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
        assert_eq!(
            tool_names(&mcp),
            vec![
                "dispatch_all",
                "dispatch_to",
                "messaging_clear_queue",
                "messaging_disable",
                "messaging_enable",
                "msg_all",
                "msg_to",
                "telegram_reply"
            ]
        );
        let resources = resource_uris(&mcp);
        assert!(resources.contains(&"swarm://executors".to_string()));
        assert!(resources.contains(&"swarm://activity".to_string()));
        assert!(resources.contains(&"swarm://messaging".to_string()));
        // the dispatch tool schema enumerates exactly the delivery ACL targets
        let dispatch = mcp
            .tools()
            .into_iter()
            .find(|tool| tool.name == "dispatch_to")
            .unwrap();
        let schema = serde_json::to_value(&dispatch.input_schema).unwrap();
        assert_eq!(
            schema["properties"]["recipient"]["enum"],
            json!(["developer", "lead-developer"])
        );
        let enable = mcp
            .tools()
            .into_iter()
            .find(|tool| tool.name == "messaging_enable")
            .unwrap();
        let enable_schema = serde_json::to_value(&enable.input_schema).unwrap();
        assert_eq!(
            enable_schema["required"],
            json!(["agent", "reason", "expected_disabled_at"])
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn executor_catalog_is_coordination_only() -> anyhow::Result<()> {
        let (mcp, path) = role_mcp("developer").await?;
        assert_eq!(
            tool_names(&mcp),
            vec!["msg_all", "msg_to", "telegram_reply"]
        );
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
            vec!["dispatch_to", "msg_all", "msg_to", "telegram_reply"]
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
        assert_eq!(hierarchy["caller_may_dispatch"], json!(["developer"]));
        assert_eq!(hierarchy["caller_delivery_sources"], json!(["manager"]));
        assert_eq!(
            hierarchy["domain_boundary"]["task_system"],
            json!("slc-mcp")
        );
        assert_eq!(
            hierarchy["safeguards"]["telegram_sender_configured"],
            json!(false)
        );
        assert_eq!(
            hierarchy["tools"],
            json!(["dispatch_to", "msg_all", "msg_to", "telegram_reply"])
        );
        assert!(mcp.instructions().contains("releases its key"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[test]
    fn parse_arguments_requires_known_fields_and_ignores_stray_fields() {
        let valid: Result<DispatchArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"recipient":"developer","message":"x"}"#).unwrap(),
        ));
        assert!(valid.is_ok());
        let unknown: Result<DispatchArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"recipient":"developer","message":"x","event_id":"task_event_1","wake_recommended":true}"#).unwrap(),
        ));
        let unknown = unknown.expect("stray LLM fields must be ignored");
        assert_eq!(unknown.agent, "developer");
        assert_eq!(unknown.message, "x");
        let missing: Result<DispatchArgs, ToolOutcome> = parse_arguments(Some(
            serde_json::from_str(r#"{"recipient":"developer"}"#).unwrap(),
        ));
        assert!(missing.is_err(), "required fields are enforced");
        let empty: Result<DispatchArgs, ToolOutcome> = parse_arguments(None);
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
            agent.api_url = Some(hermes.parse()?);
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

        // transport dispatch end-to-end: MCP call -> ACL -> mock Hermes -> durable result.
        let call = client
            .call_tool(CallToolRequestParams::new("dispatch_to").with_arguments(
                serde_json::from_str(
                    r#"{"recipient":"developer","message":"do it","correlation_id":"slc-task-1"}"#,
                )?,
            ))
            .await?;
        let structured = call.structured_content.expect("structured result");
        assert_eq!(structured["ok"], json!(true));
        assert_eq!(structured["run_id"], json!("run-e2e"));

        // dispatch_all goes through the same wire path as a global authority.
        let call_all = client
            .call_tool(
                CallToolRequestParams::new("dispatch_all")
                    .with_arguments(serde_json::from_str(r#"{"message":"broadcast"}"#)?),
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
        assert_eq!(
            value["tools"],
            json!([
                "dispatch_all",
                "dispatch_to",
                "messaging_clear_queue",
                "messaging_disable",
                "messaging_enable",
                "msg_all",
                "msg_to",
                "telegram_reply"
            ])
        );

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
        let manager = rmcp::model::ClientInfo::new(
            rmcp::model::ClientCapabilities::default(),
            rmcp::model::Implementation::new("mcp-e2e-manager", env!("CARGO_PKG_VERSION")),
        )
        .serve(rmcp::transport::StreamableHttpClientTransport::with_client(
            reqwest::Client::builder().build()?,
            rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
                format!("{base}/mcp/manager"),
            )
            .auth_header(config.agents["manager"].mcp_token.expose()),
        ))
        .await?;
        let delivery = manager
            .call_tool(CallToolRequestParams::new("dispatch_to").with_arguments(
                serde_json::from_value(json!({
                    "recipient": "lead-developer",
                    "message": "Review SLC task slc-task-42",
                    "correlation_id": "slc-task-42"
                }))?,
            ))
            .await?;
        assert_eq!(
            delivery.structured_content.as_ref().unwrap()["correlation_id"],
            json!("slc-task-42")
        );
        manager.cancel().await?;

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

        let msg =
            client
                .call_tool(CallToolRequestParams::new("msg_to").with_arguments(
                    serde_json::from_value(json!({"recipient": "developer", "message": "ping", "correlation_id": "slc-task-42"}))?,
                ))
                .await?;
        let msg_value = msg.structured_content.expect("structured result");
        assert_eq!(msg_value["ok"], json!(true));
        assert_eq!(msg_value["recipient"], json!("developer"));
        assert_eq!(msg_value["correlation_id"], json!("slc-task-42"));

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
