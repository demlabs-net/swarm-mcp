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
                            "maxLength": 160,
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
        "maxLength": 160,
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
}
