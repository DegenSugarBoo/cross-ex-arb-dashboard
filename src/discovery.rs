use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, anyhow};
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
const BINANCE_CONTRACT_TYPE: &str = "PERPETUAL";
const BINANCE_STATUS: &str = "TRADING";
const BINANCE_QUOTE_ASSET: &str = "USDT";
const BYBIT_CONTRACT_TYPE: &str = "LinearPerpetual";
const BYBIT_STATUS: &str = "Trading";
const BYBIT_QUOTE_COIN: &str = "USDT";
const EXTENDED_STATUS_ACTIVE: &str = "ACTIVE";
const ASTER_BASE_TAKER_FEE_PCT: f64 = 0.04;
const BINANCE_BASE_TAKER_FEE_PCT: f64 = 0.04;
const BYBIT_BASE_TAKER_FEE_PCT: f64 = 0.04;
const EXTENDED_BASE_TAKER_FEE_PCT: f64 = 0.025;
const EDGE_X_BASE_TAKER_FEE_PCT: f64 = 0.038;
const HYPERLIQUID_BASE_TAKER_FEE_PCT: f64 = 0.045;
const GRVT_BASE_TAKER_FEE_PCT: f64 = 0.045;
const APEX_BASE_TAKER_FEE_PCT: f64 = 0.05;
const IGNORED_COMMON_SYMBOLS: &[&str] = &["DIA"];

pub type SymbolMarkets = HashMap<String, Vec<MarketMeta>>;

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub lighter_count: usize,
    pub aster_count: usize,
    pub binance_count: usize,
    pub bybit_count: usize,
    pub extended_count: usize,
    pub edge_x_count: usize,
    pub hyperliquid_count: usize,
    pub grvt_count: usize,
    pub apex_count: usize,
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
pub struct BinanceExchangeInfoResponse {
    #[serde(default)]
    pub symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSymbol {
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
pub struct BybitInstrumentsResponse {
    #[serde(default, rename = "retCode")]
    pub ret_code: i64,
    #[serde(default, rename = "retMsg")]
    pub ret_msg: String,
    #[serde(default)]
    pub result: Option<BybitInstrumentsResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BybitInstrumentsResult {
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default, rename = "list")]
    pub list: Vec<BybitInstrument>,
    #[serde(default, rename = "nextPageCursor")]
    pub next_page_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BybitInstrument {
    pub symbol: String,
    #[serde(rename = "baseCoin")]
    pub base_coin: String,
    #[serde(rename = "quoteCoin")]
    pub quote_coin: String,
    #[serde(default, rename = "contractType")]
    pub contract_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, rename = "isPreListing")]
    pub is_pre_listing: Option<bool>,
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
pub enum GrvtInstrumentsResponse {
    List(Vec<GrvtInstrument>),
    Wrapped(GrvtInstrumentsEnvelope),
}

impl GrvtInstrumentsResponse {
    pub fn instruments(&self) -> Vec<&GrvtInstrument> {
        match self {
            Self::List(items) => items.iter().collect(),
            Self::Wrapped(items) => {
                if !items.data.is_empty() {
                    items.data.iter().collect()
                } else if !items.instruments.is_empty() {
                    items.instruments.iter().collect()
                } else {
                    items.result.iter().collect()
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrvtInstrumentsEnvelope {
    #[serde(default)]
    pub data: Vec<GrvtInstrument>,
    #[serde(default, rename = "instruments")]
    pub instruments: Vec<GrvtInstrument>,
    #[serde(default)]
    pub result: Vec<GrvtInstrument>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrvtInstrument {
    #[serde(
        default,
        alias = "instrumentName",
        alias = "instrument_name",
        alias = "name",
        alias = "instrument",
        alias = "symbolName"
    )]
    pub symbol: Option<String>,
    #[serde(
        default,
        alias = "instrumentType",
        alias = "instrument_type",
        alias = "contractType",
        alias = "contract_type",
        alias = "type",
        alias = "marketType",
        alias = "kind"
    )]
    pub instrument_type: Option<String>,
    #[serde(default, alias = "state")]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_opt_bool")]
    pub active: Option<bool>,
    #[serde(
        default,
        rename = "isActive",
        alias = "is_active",
        deserialize_with = "deserialize_opt_bool"
    )]
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApexSymbolsResponse {
    #[serde(default, rename = "perpetualContract")]
    pub perpetual_contract: Vec<ApexPerpetualContract>,
    #[serde(default, rename = "perpetualContracts")]
    pub perpetual_contracts: Vec<ApexPerpetualContract>,
    #[serde(default)]
    pub data: Option<ApexSymbolsData>,
}

impl ApexSymbolsResponse {
    pub fn contracts(&self) -> Vec<&ApexPerpetualContract> {
        if !self.perpetual_contract.is_empty() {
            return self.perpetual_contract.iter().collect();
        }
        if !self.perpetual_contracts.is_empty() {
            return self.perpetual_contracts.iter().collect();
        }
        if let Some(data) = &self.data {
            if !data.perpetual_contract.is_empty() {
                return data.perpetual_contract.iter().collect();
            }
            if !data.perpetual_contracts.is_empty() {
                return data.perpetual_contracts.iter().collect();
            }
            if let Some(contract_config) = &data.contract_config {
                if !contract_config.perpetual_contract.is_empty() {
                    return contract_config.perpetual_contract.iter().collect();
                }
                if !contract_config.perpetual_contracts.is_empty() {
                    return contract_config.perpetual_contracts.iter().collect();
                }
            }
        }
        Vec::new()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApexSymbolsData {
    #[serde(default, rename = "perpetualContract")]
    pub perpetual_contract: Vec<ApexPerpetualContract>,
    #[serde(default, rename = "perpetualContracts")]
    pub perpetual_contracts: Vec<ApexPerpetualContract>,
    #[serde(default, rename = "contractConfig")]
    pub contract_config: Option<ApexContractConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApexContractConfig {
    #[serde(default, rename = "perpetualContract")]
    pub perpetual_contract: Vec<ApexPerpetualContract>,
    #[serde(default, rename = "perpetualContracts")]
    pub perpetual_contracts: Vec<ApexPerpetualContract>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApexPerpetualContract {
    #[serde(default, rename = "crossSymbolName", alias = "crossSymbol")]
    pub cross_symbol_name: Option<String>,
    #[serde(default, rename = "symbol")]
    pub symbol: Option<String>,
    #[serde(default, rename = "baseTokenId", alias = "baseAsset", alias = "base")]
    pub base_token_id: Option<String>,
    #[serde(
        default,
        rename = "enableTrade",
        deserialize_with = "deserialize_opt_bool"
    )]
    pub enable_trade: Option<bool>,
    #[serde(
        default,
        rename = "enableDisplay",
        deserialize_with = "deserialize_opt_bool"
    )]
    pub enable_display: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(
        default,
        rename = "contractType",
        alias = "contract_type",
        alias = "symbolType"
    )]
    pub contract_type: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
struct GrvtAllInstrumentsRequest {
    is_active: bool,
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

fn deserialize_opt_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum BoolOrStringOrNum {
        Bool(bool),
        String(String),
        Integer(i64),
        Number(f64),
    }

    let value = Option::<BoolOrStringOrNum>::deserialize(deserializer)?;
    Ok(value.and_then(|entry| match entry {
        BoolOrStringOrNum::Bool(flag) => Some(flag),
        BoolOrStringOrNum::String(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => Some(true),
            "false" | "f" | "no" | "n" | "0" => Some(false),
            _ => None,
        },
        BoolOrStringOrNum::Integer(num) => Some(num != 0),
        BoolOrStringOrNum::Number(num) => Some(num != 0.0),
    }))
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

pub fn is_binance_usdt_perp_trading(symbol: &BinanceSymbol) -> bool {
    symbol
        .contract_type
        .eq_ignore_ascii_case(BINANCE_CONTRACT_TYPE)
        && symbol.status.eq_ignore_ascii_case(BINANCE_STATUS)
        && symbol.quote_asset.eq_ignore_ascii_case(BINANCE_QUOTE_ASSET)
}

pub fn is_extended_perp_trading(market: &ExtendedMarket) -> bool {
    market.active.unwrap_or(true)
        && market
            .status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case(EXTENDED_STATUS_ACTIVE))
            .unwrap_or(true)
}

fn status_allows_trading(status: Option<&str>) -> bool {
    status
        .map(|raw| {
            raw.eq_ignore_ascii_case("ACTIVE")
                || raw.eq_ignore_ascii_case("TRADING")
                || raw.eq_ignore_ascii_case("ONLINE")
                || raw.eq_ignore_ascii_case("ENABLED")
        })
        .unwrap_or(true)
}

pub fn is_grvt_perp_active(instrument: &GrvtInstrument) -> bool {
    let is_perp = instrument
        .instrument_type
        .as_deref()
        .map(|kind| kind.eq_ignore_ascii_case("PERPETUAL") || kind.eq_ignore_ascii_case("PERP"))
        .unwrap_or(false);
    if !is_perp {
        return false;
    }

    instrument
        .active
        .or(instrument.is_active)
        .unwrap_or_else(|| status_allows_trading(instrument.status.as_deref()))
}

pub fn derive_base_from_grvt_instrument(name: &str) -> Option<String> {
    let upper = normalize_base(name);

    for suffix in [
        "_USDT_PERP",
        "USDT_PERP",
        "-USDT-PERP",
        "_USD_PERP",
        "USD_PERP",
        "-USD-PERP",
    ] {
        if let Some(stripped) = upper.strip_suffix(suffix) {
            let trimmed = stripped.trim_end_matches(['_', '-', '/']);
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    if let Some((base, quote)) = upper.split_once('_') {
        if !base.is_empty() && (quote.starts_with("USDT") || quote.starts_with("USD")) {
            return Some(base.to_owned());
        }
    }

    if let Some((base, quote)) = upper.split_once('-') {
        if !base.is_empty() && (quote.starts_with("USDT") || quote.starts_with("USD")) {
            return Some(base.to_owned());
        }
    }

    None
}

pub fn is_apex_perp_trading(contract: &ApexPerpetualContract) -> bool {
    let trading_enabled = contract.enable_trade.unwrap_or(true);
    let display_enabled = contract.enable_display.unwrap_or(true);
    let is_supported_contract = contract
        .contract_type
        .as_deref()
        .map(|kind| {
            let upper = kind.trim().to_ascii_uppercase();
            !upper.contains("PREDICTION") && !upper.contains("STOCK")
        })
        .unwrap_or(true);

    trading_enabled
        && display_enabled
        && is_supported_contract
        && status_allows_trading(contract.status.as_deref())
}

fn parse_base_from_quote_symbol(symbol: &str) -> Option<String> {
    let upper = normalize_base(symbol);

    for suffix in [
        "-USDT", "_USDT", "/USDT", "USDT", "-USD", "_USD", "/USD", "USD",
    ] {
        if let Some(stripped) = upper.strip_suffix(suffix) {
            let trimmed = stripped.trim_end_matches(['_', '-', '/']);
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    if let Some((base, _)) = upper.split_once('-') {
        if !base.is_empty() {
            return Some(base.to_owned());
        }
    }
    if let Some((base, _)) = upper.split_once('_') {
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

pub fn normalize_apex_symbol_base(contract: &ApexPerpetualContract) -> Option<String> {
    if let Some(base) = contract
        .base_token_id
        .as_deref()
        .map(str::trim)
        .filter(|base| !base.is_empty())
    {
        return Some(normalize_base(base));
    }

    contract
        .symbol
        .as_deref()
        .or(contract.cross_symbol_name.as_deref())
        .and_then(parse_base_from_quote_symbol)
}

fn is_edge_x_contract_trading(contract: &EdgeXContract) -> bool {
    let trading_enabled = contract.enable_trading.unwrap_or(true);
    let status_ok = status_allows_trading(contract.status.as_deref());

    trading_enabled && status_ok
}

pub fn is_bybit_usdt_linear_perp_trading(instrument: &BybitInstrument) -> bool {
    instrument
        .contract_type
        .as_deref()
        .map(|kind| kind.eq_ignore_ascii_case(BYBIT_CONTRACT_TYPE))
        .unwrap_or(false)
        && instrument
            .status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case(BYBIT_STATUS))
            .unwrap_or(false)
        && instrument.quote_coin.eq_ignore_ascii_case(BYBIT_QUOTE_COIN)
        && !instrument.is_pre_listing.unwrap_or(false)
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

    all_symbols.retain(|symbol, _| {
        !IGNORED_COMMON_SYMBOLS
            .iter()
            .any(|ignored| symbol.eq_ignore_ascii_case(ignored))
    });

    all_symbols
}

fn parse_fee_or_default(value: Option<&NumOrString>, fallback: f64) -> f64 {
    value.and_then(NumOrString::as_f64).unwrap_or(fallback)
}

async fn fetch_bybit_linear_instruments(
    client: &reqwest::Client,
    rest_url: &str,
) -> anyhow::Result<Vec<BybitInstrument>> {
    let mut cursor: Option<String> = None;
    let mut instruments = Vec::new();

    loop {
        let mut request = client.get(rest_url).query(&[("category", "linear")]);
        if let Some(current_cursor) = cursor.as_deref().filter(|value| !value.is_empty()) {
            request = request.query(&[("cursor", current_cursor)]);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("bybit discovery request failed for {rest_url}"))?
            .error_for_status()
            .with_context(|| format!("bybit discovery non-success HTTP status for {rest_url}"))?
            .json::<BybitInstrumentsResponse>()
            .await
            .context("failed to parse bybit discovery payload")?;

        if response.ret_code != 0 {
            return Err(anyhow!(
                "bybit discovery retCode {}: {}",
                response.ret_code,
                response.ret_msg
            ));
        }

        let page = response.result.unwrap_or(BybitInstrumentsResult {
            category: None,
            list: Vec::new(),
            next_page_cursor: None,
        });
        instruments.extend(page.list);

        let next_cursor = page.next_page_cursor.unwrap_or_default().trim().to_owned();
        if next_cursor.is_empty() || cursor.as_deref() == Some(next_cursor.as_str()) {
            break;
        }
        cursor = Some(next_cursor);
    }

    Ok(instruments)
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

    let binance_future = async {
        client
            .get(&config.binance_rest_url)
            .send()
            .await
            .context("binance discovery request failed")?
            .error_for_status()
            .context("binance discovery non-success HTTP status")?
            .json::<BinanceExchangeInfoResponse>()
            .await
            .context("failed to parse binance discovery payload")
    };

    let bybit_future = fetch_bybit_linear_instruments(&client, &config.bybit_rest_url);

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

    let grvt_future = async {
        client
            .post(&config.grvt_rest_url)
            .json(&GrvtAllInstrumentsRequest { is_active: true })
            .send()
            .await
            .context("grvt discovery request failed")?
            .error_for_status()
            .context("grvt discovery non-success HTTP status")?
            .json::<GrvtInstrumentsResponse>()
            .await
            .context("failed to parse grvt discovery payload")
    };

    let apex_future = async {
        client
            .get(&config.apex_rest_url)
            .send()
            .await
            .context("apex discovery request failed")?
            .error_for_status()
            .context("apex discovery non-success HTTP status")?
            .json::<ApexSymbolsResponse>()
            .await
            .context("failed to parse apex discovery payload")
    };

    let (
        lighter_result,
        aster_result,
        binance_result,
        bybit_result,
        extended_result,
        edge_x_result,
        hyperliquid_result,
        grvt_result,
        apex_result,
    ) = tokio::join!(
        lighter_future,
        aster_future,
        binance_future,
        bybit_future,
        extended_future,
        edge_x_future,
        hyperliquid_future,
        grvt_future,
        apex_future
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
    let binance_data = match binance_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.binance_rest_url, "binance discovery unavailable; continuing without exchange");
            None
        }
    };
    let bybit_data = match bybit_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.bybit_rest_url, "bybit discovery unavailable; continuing without exchange");
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
    let grvt_data = match grvt_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.grvt_rest_url, "grvt discovery unavailable; continuing without exchange");
            None
        }
    };
    let apex_data = match apex_result {
        Ok(data) => Some(data),
        Err(err) => {
            tracing::warn!(error = %err, url = %config.apex_rest_url, "apex discovery unavailable; continuing without exchange");
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

    let binance_markets: Vec<MarketMeta> = binance_data
        .as_ref()
        .map(|data| {
            data.symbols
                .iter()
                .filter(|symbol| is_binance_usdt_perp_trading(symbol))
                .map(|symbol| MarketMeta {
                    exchange: Exchange::Binance,
                    symbol_base: normalize_base(&symbol.base_asset),
                    exchange_symbol: symbol.symbol.clone(),
                    market_id: None,
                    taker_fee_pct: BINANCE_BASE_TAKER_FEE_PCT,
                    maker_fee_pct: 0.0,
                })
                .collect()
        })
        .unwrap_or_default();

    let bybit_markets: Vec<MarketMeta> = bybit_data
        .as_ref()
        .map(|data| {
            data.iter()
                .filter(|instrument| is_bybit_usdt_linear_perp_trading(instrument))
                .map(|instrument| MarketMeta {
                    exchange: Exchange::Bybit,
                    symbol_base: normalize_base(&instrument.base_coin),
                    exchange_symbol: instrument.symbol.clone(),
                    market_id: None,
                    taker_fee_pct: BYBIT_BASE_TAKER_FEE_PCT,
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

    let grvt_markets: Vec<MarketMeta> = grvt_data
        .as_ref()
        .map(|data| {
            data.instruments()
                .into_iter()
                .filter(|instrument| is_grvt_perp_active(instrument))
                .filter_map(|instrument| {
                    let exchange_symbol = instrument.symbol.as_deref()?;
                    let symbol_base = derive_base_from_grvt_instrument(exchange_symbol)?;

                    Some(MarketMeta {
                        exchange: Exchange::Grvt,
                        symbol_base,
                        exchange_symbol: exchange_symbol.to_owned(),
                        market_id: None,
                        taker_fee_pct: GRVT_BASE_TAKER_FEE_PCT,
                        maker_fee_pct: 0.0,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let apex_markets: Vec<MarketMeta> = apex_data
        .as_ref()
        .map(|data| {
            data.contracts()
                .into_iter()
                .filter(|contract| is_apex_perp_trading(contract))
                .filter_map(|contract| {
                    let exchange_symbol = contract
                        .symbol
                        .as_deref()
                        .or(contract.cross_symbol_name.as_deref())?;
                    let symbol_base = normalize_apex_symbol_base(contract)?;

                    Some(MarketMeta {
                        exchange: Exchange::ApeX,
                        symbol_base,
                        exchange_symbol: exchange_symbol.to_owned(),
                        market_id: None,
                        taker_fee_pct: APEX_BASE_TAKER_FEE_PCT,
                        maker_fee_pct: 0.0,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let live_exchange_count = [
        !lighter_markets.is_empty(),
        !aster_markets.is_empty(),
        !binance_markets.is_empty(),
        !bybit_markets.is_empty(),
        !extended_markets.is_empty(),
        !edge_x_markets.is_empty(),
        !hyperliquid_markets.is_empty(),
        !grvt_markets.is_empty(),
        !apex_markets.is_empty(),
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
        binance_markets.clone(),
        bybit_markets.clone(),
        extended_markets.clone(),
        edge_x_markets.clone(),
        hyperliquid_markets.clone(),
        grvt_markets.clone(),
        apex_markets.clone(),
    ]);

    Ok(DiscoveryResult {
        lighter_count: lighter_markets.len(),
        aster_count: aster_markets.len(),
        binance_count: binance_markets.len(),
        bybit_count: bybit_markets.len(),
        extended_count: extended_markets.len(),
        edge_x_count: edge_x_markets.len(),
        hyperliquid_count: hyperliquid_markets.len(),
        grvt_count: grvt_markets.len(),
        apex_count: apex_markets.len(),
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
                binance = result.binance_count,
                bybit = result.bybit_count,
                extended = result.extended_count,
                edge_x = result.edge_x_count,
                hyperliquid = result.hyperliquid_count,
                grvt = result.grvt_count,
                apex = result.apex_count,
                common = result.common_count,
                "periodic discovery refresh"
            ),
            Err(err) => tracing::warn!(error = %err, "periodic discovery refresh failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::index_by_common_symbols;
    use crate::model::{Exchange, MarketMeta};

    fn meta(exchange: Exchange, symbol_base: &str, exchange_symbol: &str) -> MarketMeta {
        MarketMeta {
            exchange,
            symbol_base: symbol_base.to_owned(),
            exchange_symbol: exchange_symbol.to_owned(),
            market_id: None,
            taker_fee_pct: 0.04,
            maker_fee_pct: 0.0,
        }
    }

    #[test]
    fn common_symbol_index_ignores_dia() {
        let symbols = index_by_common_symbols(&[
            vec![
                meta(Exchange::Binance, "BTC", "BTCUSDT"),
                meta(Exchange::Binance, "DIA", "DIAUSDT"),
            ],
            vec![
                meta(Exchange::Bybit, "BTC", "BTCUSDT"),
                meta(Exchange::Bybit, "DIA", "DIAUSDT"),
            ],
        ]);

        assert!(symbols.contains_key("BTC"));
        assert!(!symbols.contains_key("DIA"));
    }
}
