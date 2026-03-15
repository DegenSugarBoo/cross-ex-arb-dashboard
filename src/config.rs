use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "cross-ex-arb",
    version,
    about = "Arb Scanner"
)]
pub struct CliArgs {
    #[arg(
        long,
        default_value = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
    )]
    pub lighter_rest_url: String,
    #[arg(long, default_value = "https://fapi.asterdex.com/fapi/v1/exchangeInfo")]
    pub aster_rest_url: String,
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
        default_value = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
    )]
    pub lighter_ws_url: String,
    #[arg(long, default_value = "wss://fstream.asterdex.com/ws")]
    pub aster_ws_url: String,
    #[arg(
        long,
        default_value = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks?depth=1"
    )]
    pub extended_ws_url: String,
    #[arg(long, default_value = "wss://quote.edgex.exchange/api/v1/public/ws")]
    pub edge_x_ws_url: String,
    #[arg(long, default_value = "wss://api.hyperliquid.xyz/ws")]
    pub hyperliquid_ws_url: String,
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
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub lighter_rest_url: String,
    pub aster_rest_url: String,
    pub extended_rest_url: String,
    pub edge_x_rest_url: String,
    pub hyperliquid_rest_url: String,
    pub lighter_ws_url: String,
    pub aster_ws_url: String,
    pub extended_ws_url: String,
    pub edge_x_ws_url: String,
    pub hyperliquid_ws_url: String,
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
}

impl AppConfig {
    pub fn from_cli() -> Self {
        Self::from(CliArgs::parse())
    }
}

impl From<CliArgs> for AppConfig {
    fn from(args: CliArgs) -> Self {
        Self {
            lighter_rest_url: args.lighter_rest_url,
            aster_rest_url: args.aster_rest_url,
            extended_rest_url: args.extended_rest_url,
            edge_x_rest_url: args.edge_x_rest_url,
            hyperliquid_rest_url: args.hyperliquid_rest_url,
            lighter_ws_url: args.lighter_ws_url,
            aster_ws_url: args.aster_ws_url,
            extended_ws_url: args.extended_ws_url,
            edge_x_ws_url: args.edge_x_ws_url,
            hyperliquid_ws_url: args.hyperliquid_ws_url,
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
        }
    }
}
