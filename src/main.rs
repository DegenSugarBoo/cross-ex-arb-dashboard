use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use cross_ex_arb::config::AppConfig;
use cross_ex_arb::discovery;
use cross_ex_arb::engine;
use cross_ex_arb::feeds;
use cross_ex_arb::model::{ArbRow, Exchange, ExchangeFeedHealth, MarketEvent};
use cross_ex_arb::ui::ArbApp;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn main() -> anyhow::Result<()> {
    let config = AppConfig::from_cli();
    init_tracing();

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    let discovery_result = runtime
        .block_on(discovery::discover_common(&config))
        .context("market discovery failed")?;

    tracing::info!(
        lighter = discovery_result.lighter_count,
        aster = discovery_result.aster_count,
        extended = discovery_result.extended_count,
        edge_x = discovery_result.edge_x_count,
        hyperliquid = discovery_result.hyperliquid_count,
        common = discovery_result.common_count,
        "startup discovery complete"
    );

    let snapshot: Arc<RwLock<Vec<ArbRow>>> = Arc::new(RwLock::new(Vec::new()));
    let exchange_health: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let (event_tx, event_rx) = mpsc::channel::<MarketEvent>(config.quote_channel_capacity);

    {
        let engine_markets = discovery_result.symbols.clone();
        let engine_snapshot = Arc::clone(&snapshot);
        let engine_health = Arc::clone(&exchange_health);
        let engine_config = config.clone();
        runtime.spawn(async move {
            engine::run_engine(
                event_rx,
                engine_markets,
                engine_config,
                engine_snapshot,
                engine_health,
            )
            .await;
        });
    }

    feeds::spawn_all_feeds(
        &runtime,
        &config,
        &discovery_result.symbols,
        event_tx.clone(),
    );

    if config.discovery_refresh_secs > 0 {
        let refresh_config = config.clone();
        runtime.spawn(async move {
            discovery::periodic_discovery_log(refresh_config).await;
        });
    }

    let ui_snapshot = Arc::clone(&snapshot);
    let ui_health = Arc::clone(&exchange_health);
    let ui_config = config.clone();
    let native_options = eframe::NativeOptions::default();
    let ui_result = eframe::run_native(
        "Arb Scanner",
        native_options,
        Box::new(move |_cc| Ok(Box::new(ArbApp::new(ui_snapshot, ui_health, ui_config)))),
    );

    drop(event_tx);
    runtime.shutdown_timeout(Duration::from_secs(1));

    ui_result.map_err(|err| anyhow::anyhow!(err.to_string()))
}
