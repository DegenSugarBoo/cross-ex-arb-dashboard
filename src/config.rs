use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum CollectorCompression {
    Lz4hc,
    Zstd,
    None,
}

impl Default for CollectorCompression {
    fn default() -> Self {
        Self::Zstd
    }
}

impl CollectorCompression {
    pub fn file_extension(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Zstd => Some("zst"),
            Self::Lz4hc => Some("lz4"),
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "cross-ex-arb", version, about = "Arb Scanner")]
pub struct CliArgs {
    #[arg(
        long,
        default_value = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
    )]
    pub lighter_rest_url: String,
    #[arg(long, default_value = "https://fapi.asterdex.com/fapi/v1/exchangeInfo")]
    pub aster_rest_url: String,
    #[arg(long, default_value = "https://fapi.binance.com/fapi/v1/exchangeInfo")]
    pub binance_rest_url: String,
    #[arg(
        long,
        default_value = "https://api.bybit.com/v5/market/instruments-info"
    )]
    pub bybit_rest_url: String,
    #[arg(
        long,
        default_value = "https://api.starknet.extended.exchange/api/v1/info/markets"
    )]
    pub extended_rest_url: String,
    #[arg(
        long,
        default_value = "https://pro.edgex.exchange/api/v1/public/meta/getMetaData"
    )]
    pub edge_x_rest_url: String,
    #[arg(long, default_value = "https://api.hyperliquid.xyz/info")]
    pub hyperliquid_rest_url: String,
    #[arg(
        long,
        default_value = "https://market-data.grvt.io/full/v1/all_instruments"
    )]
    pub grvt_rest_url: String,
    #[arg(long, default_value = "https://omni.apex.exchange/api/v3/symbols")]
    pub apex_rest_url: String,
    #[arg(
        long,
        default_value = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
    )]
    pub lighter_ws_url: String,
    #[arg(long, default_value = "wss://fstream.asterdex.com/ws")]
    pub aster_ws_url: String,
    #[arg(long, default_value = "wss://fstream.binance.com/stream")]
    pub binance_ws_url: String,
    #[arg(long, default_value = "wss://stream.bybit.com/v5/public/linear")]
    pub bybit_ws_url: String,
    #[arg(
        long,
        default_value = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks?depth=1"
    )]
    pub extended_ws_url: String,
    #[arg(long, default_value = "wss://quote.edgex.exchange/api/v1/public/ws")]
    pub edge_x_ws_url: String,
    #[arg(long, default_value = "wss://api.hyperliquid.xyz/ws")]
    pub hyperliquid_ws_url: String,
    #[arg(long, default_value = "wss://market-data.grvt.io/ws/full")]
    pub grvt_ws_url: String,
    #[arg(long, default_value = "wss://quote.omni.apex.exchange/realtime_public")]
    pub apex_ws_url: String,
    #[arg(long, default_value = "https://omni.apex.exchange/api/v3/depth")]
    pub apex_depth_rest_url: String,
    #[arg(
        long,
        default_value = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
    )]
    pub lighter_funding_rest_url: String,
    #[arg(long, default_value = "https://fapi.asterdex.com/fapi/v1/premiumIndex")]
    pub aster_funding_rest_url: String,
    #[arg(
        long,
        default_value = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/funding"
    )]
    pub extended_funding_ws_url: String,
    #[arg(
        long,
        default_value = "https://pro.edgex.exchange/api/v1/public/funding/getLatestFundingRate"
    )]
    pub edge_x_funding_rest_url: String,
    #[arg(long, default_value = "https://api.hyperliquid.xyz/info")]
    pub hyperliquid_funding_rest_url: String,
    #[arg(long, default_value_t = 300)]
    pub funding_poll_secs: u64,
    #[arg(long, default_value_t = 2500)]
    pub stale_ms: i64,
    #[arg(long, default_value_t = 60)]
    pub ui_fps: u32,
    #[arg(long, default_value_t = 8192)]
    pub quote_channel_capacity: usize,
    #[arg(long, default_value_t = 10)]
    pub http_timeout_secs: u64,
    #[arg(long, default_value_t = 600)]
    pub discovery_refresh_secs: u64,
    #[arg(long, default_value_t = false)]
    pub collect_mode: bool,
    #[arg(long, default_value = "data")]
    pub collector_data_root: PathBuf,
    #[arg(long, value_enum, default_value_t = CollectorCompression::Zstd)]
    pub collector_compression: CollectorCompression,
    #[arg(long, default_value_t = 45_000)]
    pub collector_bootstrap_timeout_ms: u64,
    #[arg(long, default_value_t = 131_072)]
    pub collector_bootstrap_buffer_events: usize,
    #[arg(long, default_value_t = 16_384)]
    pub collector_write_buffer: usize,
    #[arg(long, default_value_t = 1_000)]
    pub collector_flush_interval_ms: u64,
    #[arg(long, default_value_t = 128)]
    pub collector_max_open_files: usize,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub lighter_rest_url: String,
    pub aster_rest_url: String,
    pub binance_rest_url: String,
    pub bybit_rest_url: String,
    pub extended_rest_url: String,
    pub edge_x_rest_url: String,
    pub hyperliquid_rest_url: String,
    pub grvt_rest_url: String,
    pub apex_rest_url: String,
    pub lighter_ws_url: String,
    pub aster_ws_url: String,
    pub binance_ws_url: String,
    pub bybit_ws_url: String,
    pub extended_ws_url: String,
    pub edge_x_ws_url: String,
    pub hyperliquid_ws_url: String,
    pub grvt_ws_url: String,
    pub apex_ws_url: String,
    pub apex_depth_rest_url: String,
    pub lighter_funding_rest_url: String,
    pub aster_funding_rest_url: String,
    pub extended_funding_ws_url: String,
    pub edge_x_funding_rest_url: String,
    pub hyperliquid_funding_rest_url: String,
    pub funding_poll_secs: u64,
    pub stale_ms: i64,
    pub ui_fps: u32,
    pub quote_channel_capacity: usize,
    pub http_timeout_secs: u64,
    pub discovery_refresh_secs: u64,
    pub collect_mode: bool,
    pub collector_data_root: PathBuf,
    pub collector_compression: CollectorCompression,
    pub collector_bootstrap_timeout_ms: u64,
    pub collector_bootstrap_buffer_events: usize,
    pub collector_write_buffer: usize,
    pub collector_flush_interval_ms: u64,
    pub collector_max_open_files: usize,
}

impl AppConfig {
    pub fn from_cli() -> Self {
        Self::from(CliArgs::parse())
    }

    pub fn launches_ui(&self) -> bool {
        !self.collect_mode
    }
}

impl From<CliArgs> for AppConfig {
    fn from(args: CliArgs) -> Self {
        Self {
            lighter_rest_url: args.lighter_rest_url,
            aster_rest_url: args.aster_rest_url,
            binance_rest_url: args.binance_rest_url,
            bybit_rest_url: args.bybit_rest_url,
            extended_rest_url: args.extended_rest_url,
            edge_x_rest_url: args.edge_x_rest_url,
            hyperliquid_rest_url: args.hyperliquid_rest_url,
            grvt_rest_url: args.grvt_rest_url,
            apex_rest_url: args.apex_rest_url,
            lighter_ws_url: args.lighter_ws_url,
            aster_ws_url: args.aster_ws_url,
            binance_ws_url: args.binance_ws_url,
            bybit_ws_url: args.bybit_ws_url,
            extended_ws_url: args.extended_ws_url,
            edge_x_ws_url: args.edge_x_ws_url,
            hyperliquid_ws_url: args.hyperliquid_ws_url,
            grvt_ws_url: args.grvt_ws_url,
            apex_ws_url: args.apex_ws_url,
            apex_depth_rest_url: args.apex_depth_rest_url,
            lighter_funding_rest_url: args.lighter_funding_rest_url,
            aster_funding_rest_url: args.aster_funding_rest_url,
            extended_funding_ws_url: args.extended_funding_ws_url,
            edge_x_funding_rest_url: args.edge_x_funding_rest_url,
            hyperliquid_funding_rest_url: args.hyperliquid_funding_rest_url,
            funding_poll_secs: args.funding_poll_secs,
            stale_ms: args.stale_ms,
            ui_fps: args.ui_fps,
            quote_channel_capacity: args.quote_channel_capacity,
            http_timeout_secs: args.http_timeout_secs,
            discovery_refresh_secs: args.discovery_refresh_secs,
            collect_mode: args.collect_mode,
            collector_data_root: args.collector_data_root,
            collector_compression: args.collector_compression,
            collector_bootstrap_timeout_ms: args.collector_bootstrap_timeout_ms,
            collector_bootstrap_buffer_events: args.collector_bootstrap_buffer_events,
            collector_write_buffer: args.collector_write_buffer,
            collector_flush_interval_ms: args.collector_flush_interval_ms,
            collector_max_open_files: args.collector_max_open_files,
        }
    }
}
