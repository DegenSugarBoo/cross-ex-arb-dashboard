use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde::Serialize;

use crate::config::AppConfig;
use crate::model::{Exchange, MarketMeta};

const LIGHTER_MARKET_TYPE: &str = "perp";
const LIGHTER_STATUS: &str = "active";
const ASTER_CONTRACT_TYPE: &str = "PERPETUAL";
const ASTER_STATUS: &str = "TRADING";
const ASTER_QUOTE_ASSET: &str = "USDT";
const EXTENDED_STATUS_ACTIVE: &str = "ACTIVE";
const ASTER_BASE_TAKER_FEE_PCT: f64 = 0.04;
const EXTENDED_BASE_TAKER_FEE_PCT: f64 = 0.025;
const EDGE_X_BASE_TAKER_FEE_PCT: f64 = 0.038;
const HYPERLIQUID_BASE_TAKER_FEE_PCT: f64 = 0.045;

pub type SymbolMarkets = HashMap<String, Vec<MarketMeta>>;

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub lighter_count: usize,
    pub aster_count: usize,
    pub extended_count: usize,
    pub edge_x_count: usize,
    pub hyperliquid_count: usize,
    pub common_count: usize,
    pub symbols: SymbolMarkets,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterDiscoveryResponse {
    #[serde(default)]
    pub order_books: Vec<LighterOrderBook>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterOrderBook {
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_u32")]
    pub market_id: u32,
    pub market_type: String,
    pub status: String,
    #[serde(deserialize_with = "deserialize_f64")]
    pub taker_fee: f64,
    #[serde(deserialize_with = "deserialize_f64")]
    pub maker_fee: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterExchangeInfoResponse {
    #[serde(default)]
    pub symbols: Vec<AsterSymbol>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterSymbol {
    pub symbol: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "contractType")]
    pub contract_type: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtendedMarketsResponse {
    #[serde(default)]
    pub data: Vec<ExtendedMarket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtendedMarket {
    pub name: String,
    #[serde(rename = "assetName")]
    pub asset_name: Option<String>,
    pub active: Option<bool>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeXMetaDataResponse {
    pub data: Option<EdgeXMetaData>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeXMetaData {
    #[serde(default, rename = "contractList")]
    pub contract_list: Vec<EdgeXContract>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeXContract {
    #[serde(rename = "contractId")]
    pub contract_id: String,
    #[serde(rename = "contractName")]
    pub contract_name: String,
    #[serde(rename = "enableTrading")]
    pub enable_trading: Option<bool>,
    pub status: Option<String>,
    #[serde(rename = "takerFeeRate")]
    pub taker_fee_rate: Option<NumOrString>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidMetaResponse {
    #[serde(default)]
    pub universe: Vec<HyperliquidUniverseEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidUniverseEntry {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum NumOrString {
    Number(f64),
    String(String),
}

impl NumOrString {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::String(value) => value.parse::<f64>().ok(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct HyperliquidInfoRequest<'a> {
    #[serde(rename = "type")]
    req_type: &'a str,
}

fn deserialize_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match NumOrString::deserialize(deserializer)? {
        NumOrString::Number(value) => Ok(value),
        NumOrString::String(value) => value.parse().map_err(serde::de::Error::custom),
    }
}

fn deserialize_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum IntOrString {
        Integer(u32),
        String(String),
    }

    match IntOrString::deserialize(deserializer)? {
        IntOrString::Integer(value) => Ok(value),
        IntOrString::String(value) => value.parse().map_err(serde::de::Error::custom),
    }
}

pub fn normalize_base(input: &str) -> String {
    input.trim().to_ascii_uppercase()
}

pub fn is_lighter_perp_active(market: &LighterOrderBook) -> bool {
    market.market_type.eq_ignore_ascii_case(LIGHTER_MARKET_TYPE)
        && market.status.eq_ignore_ascii_case(LIGHTER_STATUS)
}

pub fn is_aster_perp_trading(symbol: &AsterSymbol) -> bool {
    symbol
        .contract_type
        .eq_ignore_ascii_case(ASTER_CONTRACT_TYPE)
        && symbol.status.eq_ignore_ascii_case(ASTER_STATUS)
        && symbol.quote_asset.eq_ignore_ascii_case(ASTER_QUOTE_ASSET)
}

pub fn is_extended_perp_trading(market: &ExtendedMarket) -> bool {
    market.active.unwrap_or(true)
        && market
            .status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case(EXTENDED_STATUS_ACTIVE))
            .unwrap_or(true)
}

fn is_edge_x_contract_trading(contract: &EdgeXContract) -> bool {
    let trading_enabled = contract.enable_trading.unwrap_or(true);
    let status_ok = contract
        .status
        .as_deref()
        .map(|status| {
            status.eq_ignore_ascii_case("ACTIVE")
                || status.eq_ignore_ascii_case("TRADING")
                || status.eq_ignore_ascii_case("ONLINE")
                || status.eq_ignore_ascii_case("ENABLED")
        })
        .unwrap_or(true);

    trading_enabled && status_ok
}

fn is_hyperliquid_perp_coin(coin: &str) -> bool {
    !coin.is_empty()
        && !coin.contains('/')
        && !coin.contains('@')
        && !coin.contains(':')
        && !coin.contains("-SPOT")
}

fn derive_base_from_extended_market_name(name: &str) -> Option<String> {
    let upper = normalize_base(name);
    if let Some((base, _)) = upper.split_once('-') {
        if !base.is_empty() {
            return Some(base.to_owned());
        }
    }

    if let Some((base, _)) = upper.split_once('/') {
        if !base.is_empty() {
            return Some(base.to_owned());
        }
    }

    None
}

fn extract_base_from_edge_x_contract(name: &str) -> Option<String> {
    let upper = normalize_base(name);

    if let Some(stripped) = upper.strip_suffix("-USDT") {
        if !stripped.is_empty() {
            return Some(stripped.to_owned());
        }
    }

    if let Some(stripped) = upper.strip_suffix("USDT") {
        if !stripped.is_empty() {
            return Some(stripped.to_owned());
        }
    }

    if let Some(stripped) = upper.strip_suffix("-USD") {
        if !stripped.is_empty() {
            return Some(stripped.to_owned());
        }
    }

    if let Some(stripped) = upper.strip_suffix("USD") {
        if !stripped.is_empty() {
            return Some(stripped.to_owned());
        }
    }

    if let Some((base, _)) = upper.split_once('-') {
        if !base.is_empty() {
            return Some(base.to_owned());
        }
    }

    if let Some((base, _)) = upper.split_once('/') {
        if !base.is_empty() {
            return Some(base.to_owned());
        }
    }

    None
}

fn index_by_common_symbols(exchange_markets: &[Vec<MarketMeta>]) -> SymbolMarkets {
    let mut all_symbols: HashMap<String, Vec<MarketMeta>> = HashMap::new();

    for markets in exchange_markets {
        for meta in markets {
            all_symbols
                .entry(meta.symbol_base.clone())
                .or_default()
                .push(meta.clone());
        }
    }

    all_symbols.retain(|_, metas| {
        metas.sort_by(|left, right| left.exchange.as_str().cmp(right.exchange.as_str()));
        metas.dedup_by_key(|meta| meta.exchange);
        metas.len() >= 2
    });

    all_symbols
}

fn parse_fee_or_default(value: Option<&NumOrString>, fallback: f64) -> f64 {
    value.and_then(NumOrString::as_f64).unwrap_or(fallback)
}

pub async fn discover_common(config: &AppConfig) -> anyhow::Result<DiscoveryResult> {
    let mut default_headers = HeaderMap::new();
    default_headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    default_headers.insert(
        USER_AGENT,
        HeaderValue::from_static("Mozilla/5.0 (compatible; cross-ex-arb/0.1)"),
    );

    let client = reqwest::Client::builder()
        .default_headers(default_headers)
        .timeout(Duration::from_secs(config.http_timeout_secs))
        .build()
        .context("failed to build HTTP client")?;

    let lighter_future = async {
        client
            .get(&config.lighter_rest_url)
            .send()
            .await
            .context("lighter discovery request failed")?
            .error_for_status()
            .context("lighter discovery non-success HTTP status")?
            .json::<LighterDiscoveryResponse>()
            .await
            .context("failed to parse lighter discovery payload")
    };

    let aster_future = async {
        client
            .get(&config.aster_rest_url)
            .send()
            .await
            .context("aster discovery request failed")?
            .error_for_status()
            .context("aster discovery non-success HTTP status")?
            .json::<AsterExchangeInfoResponse>()
            .await
            .context("failed to parse aster discovery payload")
    };

    let extended_future = async {
        client
            .get(&config.extended_rest_url)
            .send()
            .await
            .context("extended discovery request failed")?
            .error_for_status()
            .context("extended discovery non-success HTTP status")?
            .json::<ExtendedMarketsResponse>()
            .await
            .context("failed to parse extended discovery payload")
    };

    let edge_x_future = async {
        client
            .get(&config.edge_x_rest_url)
            .send()
            .await
            .context("edgeX discovery request failed")?
            .error_for_status()
            .context("edgeX discovery non-success HTTP status")?
            .json::<EdgeXMetaDataResponse>()
            .await
            .context("failed to parse edgeX discovery payload")
    };

    let hyperliquid_future = async {
        client
            .post(&config.hyperliquid_rest_url)
            .json(&HyperliquidInfoRequest { req_type: "meta" })
            .send()
            .await
            .context("hyperliquid discovery request failed")?
            .error_for_status()
            .context("hyperliquid discovery non-success HTTP status")?
            .json::<HyperliquidMetaResponse>()
            .await
            .context("failed to parse hyperliquid discovery payload")
    };

    let (lighter_result, aster_result, extended_result, edge_x_result, hyperliquid_result) = tokio::join!(
        lighter_future,
        aster_future,
        extended_future,
        edge_x_future,
        hyperliquid_future
    );

    let lighter_data = match lighter_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.lighter_rest_url, "lighter discovery unavailable; continuing without exchange");
            None
        }
    };
    let aster_data = match aster_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.aster_rest_url, "aster discovery unavailable; continuing without exchange");
            None
        }
    };
    let extended_data = match extended_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.extended_rest_url, "extended discovery unavailable; continuing without exchange");
            None
        }
    };
    let edge_x_data = match edge_x_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.edge_x_rest_url, "edgeX discovery unavailable; continuing without exchange");
            None
        }
    };
    let hyperliquid_data = match hyperliquid_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.hyperliquid_rest_url, "hyperliquid discovery unavailable; continuing without exchange");
            None
        }
    };

    let lighter_markets: Vec<MarketMeta> = lighter_data
        .as_ref()
        .map(|data| {
            data.order_books
                .iter()
                .filter(|market| is_lighter_perp_active(market))
                .map(|market| {
                    let symbol_base = normalize_base(&market.symbol);
                    MarketMeta {
                        exchange: Exchange::Lighter,
                        symbol_base,
                        exchange_symbol: market.symbol.clone(),
                        market_id: Some(market.market_id),
                        taker_fee_pct: market.taker_fee,
                        maker_fee_pct: market.maker_fee,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let aster_markets: Vec<MarketMeta> = aster_data
        .as_ref()
        .map(|data| {
            data.symbols
                .iter()
                .filter(|symbol| is_aster_perp_trading(symbol))
                .map(|symbol| MarketMeta {
                    exchange: Exchange::Aster,
                    symbol_base: normalize_base(&symbol.base_asset),
                    exchange_symbol: symbol.symbol.clone(),
                    market_id: None,
                    taker_fee_pct: ASTER_BASE_TAKER_FEE_PCT,
                    maker_fee_pct: 0.0,
                })
                .collect()
        })
        .unwrap_or_default();

    let extended_markets: Vec<MarketMeta> = extended_data
        .as_ref()
        .map(|data| {
            data.data
                .iter()
                .filter(|market| is_extended_perp_trading(market))
                .filter_map(|market| {
                    let symbol_base = market
                        .asset_name
                        .as_deref()
                        .map(normalize_base)
                        .or_else(|| derive_base_from_extended_market_name(&market.name))?;

                    Some(MarketMeta {
                        exchange: Exchange::Extended,
                        symbol_base,
                        exchange_symbol: market.name.clone(),
                        market_id: None,
                        taker_fee_pct: EXTENDED_BASE_TAKER_FEE_PCT,
                        maker_fee_pct: 0.0,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let edge_x_markets: Vec<MarketMeta> = edge_x_data
        .as_ref()
        .and_then(|data| data.data.as_ref())
        .map(|meta| {
            meta.contract_list
                .iter()
                .filter(|contract| is_edge_x_contract_trading(contract))
                .filter_map(|contract| {
                    let symbol_base = extract_base_from_edge_x_contract(&contract.contract_name)?;
                    let market_id = contract.contract_id.parse::<u32>().ok();

                    Some(MarketMeta {
                        exchange: Exchange::EdgeX,
                        symbol_base,
                        exchange_symbol: contract.contract_name.clone(),
                        market_id,
                        taker_fee_pct: parse_fee_or_default(
                            contract.taker_fee_rate.as_ref(),
                            EDGE_X_BASE_TAKER_FEE_PCT,
                        ),
                        maker_fee_pct: 0.0,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let hyperliquid_markets: Vec<MarketMeta> = hyperliquid_data
        .as_ref()
        .map(|data| {
            data.universe
                .iter()
                .filter(|entry| is_hyperliquid_perp_coin(&entry.name))
                .map(|entry| MarketMeta {
                    exchange: Exchange::Hyperliquid,
                    symbol_base: normalize_base(&entry.name),
                    exchange_symbol: entry.name.clone(),
                    market_id: None,
                    taker_fee_pct: HYPERLIQUID_BASE_TAKER_FEE_PCT,
                    maker_fee_pct: 0.0,
                })
                .collect()
        })
        .unwrap_or_default();

    let live_exchange_count = [
        !lighter_markets.is_empty(),
        !aster_markets.is_empty(),
        !extended_markets.is_empty(),
        !edge_x_markets.is_empty(),
        !hyperliquid_markets.is_empty(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();

    if live_exchange_count < 2 {
        tracing::warn!(
            live_exchange_count,
            "fewer than two exchanges discovered with active markets; opportunities may be empty"
        );
    }

    let symbols = index_by_common_symbols(&[
        lighter_markets.clone(),
        aster_markets.clone(),
        extended_markets.clone(),
        edge_x_markets.clone(),
        hyperliquid_markets.clone(),
    ]);

    Ok(DiscoveryResult {
        lighter_count: lighter_markets.len(),
        aster_count: aster_markets.len(),
        extended_count: extended_markets.len(),
        edge_x_count: edge_x_markets.len(),
        hyperliquid_count: hyperliquid_markets.len(),
        common_count: symbols.len(),
        symbols,
    })
}

pub async fn periodic_discovery_log(config: AppConfig) {
    if config.discovery_refresh_secs == 0 {
        return;
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(config.discovery_refresh_secs));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        match discover_common(&config).await {
            Ok(result) => tracing::info!(
                lighter = result.lighter_count,
                aster = result.aster_count,
                extended = result.extended_count,
                edge_x = result.edge_x_count,
                hyperliquid = result.hyperliquid_count,
                common = result.common_count,
                "periodic discovery refresh"
            ),
            Err(err) => tracing::warn!(error = %err, "periodic discovery refresh failed"),
        }
    }
}
