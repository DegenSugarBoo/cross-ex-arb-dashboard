use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use cross_ex_arb::collector::{CollectorService, CollectorSettings};
use cross_ex_arb::config::AppConfig;
use cross_ex_arb::discovery::{self, DiscoveryResult};
use cross_ex_arb::engine;
use cross_ex_arb::feeds;
use cross_ex_arb::model::{
    ArbRow, Exchange, ExchangeFeedHealth, MarketEvent, NO_ROUTE_SELECTED, RouteHistorySnapshot,
};
use cross_ex_arb::ui::ArbApp;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn install_rustls_crypto_provider() -> anyhow::Result<()> {
    use tokio_rustls::rustls::crypto::{CryptoProvider, ring};

    if CryptoProvider::get_default().is_some() {
        return Ok(());
    }

    ring::default_provider().install_default().map_err(|_| {
        anyhow!("failed to install rustls crypto provider (ring) as process default")
    })?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let config = AppConfig::from_cli();
    init_tracing();
    install_rustls_crypto_provider()?;

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    let discovery_result = runtime
        .block_on(discovery::discover_common(&config))
        .context("market discovery failed")?;

    log_discovery_summary(&discovery_result);

    let launches_ui = config.launches_ui();
    let run_result = if launches_ui {
        run_ui_mode(&runtime, config, discovery_result)
    } else {
        run_collect_mode(&runtime, config, discovery_result)
    };

    let shutdown_secs = if launches_ui { 1 } else { 2 };
    runtime.shutdown_timeout(Duration::from_secs(shutdown_secs));
    run_result
}

fn log_discovery_summary(discovery_result: &DiscoveryResult) {
    tracing::info!(
        lighter = discovery_result.lighter_count,
        aster = discovery_result.aster_count,
        extended = discovery_result.extended_count,
        edge_x = discovery_result.edge_x_count,
        hyperliquid = discovery_result.hyperliquid_count,
        grvt = discovery_result.grvt_count,
        apex = discovery_result.apex_count,
        common = discovery_result.common_count,
        "startup discovery complete"
    );
}

fn run_ui_mode(
    runtime: &tokio::runtime::Runtime,
    config: AppConfig,
    discovery_result: DiscoveryResult,
) -> anyhow::Result<()> {
    let shared_markets = Arc::new(discovery_result.symbols);
    let snapshot: Arc<RwLock<Vec<ArbRow>>> = Arc::new(RwLock::new(Vec::new()));
    let exchange_health: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let selected_route_id = Arc::new(AtomicU32::new(NO_ROUTE_SELECTED));
    let detail_snapshot: Arc<RwLock<Option<RouteHistorySnapshot>>> = Arc::new(RwLock::new(None));
    let (event_tx, event_rx) = mpsc::channel::<MarketEvent>(config.quote_channel_capacity);

    {
        let engine_markets = Arc::clone(&shared_markets);
        let engine_snapshot = Arc::clone(&snapshot);
        let engine_health = Arc::clone(&exchange_health);
        let engine_selected_route_id = Arc::clone(&selected_route_id);
        let engine_detail_snapshot = Arc::clone(&detail_snapshot);
        let engine_config = config.clone();
        runtime.spawn(async move {
            engine::run_engine(
                event_rx,
                engine_markets,
                engine_config,
                engine_snapshot,
                engine_health,
                engine_selected_route_id,
                engine_detail_snapshot,
            )
            .await;
        });
    }

    feeds::spawn_all_feeds(
        runtime,
        &config,
        Arc::clone(&shared_markets),
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
    let ui_selected_route_id = Arc::clone(&selected_route_id);
    let ui_detail_snapshot = Arc::clone(&detail_snapshot);
    let ui_config = config.clone();
    let native_options = eframe::NativeOptions::default();
    let ui_result = eframe::run_native(
        "Arb Scanner",
        native_options,
        Box::new(move |_cc| {
            Ok(Box::new(ArbApp::new(
                ui_snapshot,
                ui_health,
                ui_selected_route_id,
                ui_detail_snapshot,
                ui_config,
            )))
        }),
    );

    drop(event_tx);

    ui_result.map_err(|err| anyhow::anyhow!(err.to_string()))
}

fn run_collect_mode(
    runtime: &tokio::runtime::Runtime,
    config: AppConfig,
    discovery_result: DiscoveryResult,
) -> anyhow::Result<()> {
    let shared_markets = Arc::new(discovery_result.symbols);
    let (event_tx, event_rx) = mpsc::channel::<MarketEvent>(config.quote_channel_capacity);

    feeds::spawn_all_feeds(
        runtime,
        &config,
        Arc::clone(&shared_markets),
        event_tx.clone(),
    );

    if config.discovery_refresh_secs > 0 {
        let refresh_config = config.clone();
        runtime.spawn(async move {
            discovery::periodic_discovery_log(refresh_config).await;
        });
    }

    let collector_settings = CollectorSettings {
        data_root: config.collector_data_root.clone(),
        compression: config.collector_compression,
        bootstrap_timeout_ms: config.collector_bootstrap_timeout_ms,
        write_buffer: config.collector_write_buffer,
        flush_interval_ms: config.collector_flush_interval_ms,
        max_open_files: config.collector_max_open_files,
    };
    tracing::info!(
        configured_max_open_files = collector_settings.max_open_files,
        data_root = %collector_settings.data_root.display(),
        compression = ?collector_settings.compression,
        "collector settings"
    );
    let mut collector = CollectorService::new(collector_settings, shared_markets.as_ref());

    let stats = runtime.block_on(async {
        collector
            .run_until_shutdown(event_rx, wait_for_shutdown_signal())
            .await
    })?;

    tracing::info!(
        data_root = %config.collector_data_root.display(),
        written_records = stats.written_records,
        dropped_records = stats.dropped_records,
        write_errors = stats.write_errors,
        "collector mode exited"
    );

    drop(event_tx);
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to install SIGTERM handler, falling back to ctrl-c only"
                );
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
