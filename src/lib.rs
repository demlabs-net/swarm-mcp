pub mod config;
pub mod dispatch;
pub mod http;
pub mod mcp;
pub mod probe;
pub mod store;
pub mod telegram;

use std::sync::Arc;

use anyhow::Context;
use config::Config;
use dispatch::Dispatcher;
use sqlx::SqlitePool;
use store::Store;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub dispatcher: Dispatcher,
}

impl AppState {
    pub async fn initialize(config: Config) -> anyhow::Result<Self> {
        let config = Arc::new(config);
        let store = Store::connect(&config)
            .await
            .context("initialize SQLite state store")?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())
            .context("initialize downstream clients")?;
        Ok(Self {
            config,
            store,
            dispatcher,
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        self.store.pool()
    }
}
