use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde::Deserialize;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{
    ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms, funding_baseline_poll_ms,
    funding_cache_ttl_ms, jittered_poll_ms,
};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

pub struct LighterFeed;

impl ExchangeFeed for LighterFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Lighter
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.lighter_ws_url.clone();
        let feed_markets = Arc::clone(&markets);
        let quote_event_tx = event_tx.clone();
        runtime.spawn(async move {
            if let Err(err) = run_lighter_feed(&ws_url, &feed_markets, quote_event_tx).await {
                tracing::error!(error = %err, "lighter feed task terminated");
            }
        });

        if config.funding_poll_secs > 0 {
            let funding_url = config.lighter_funding_rest_url.clone();
            let poll_secs = config.funding_poll_secs;
            let timeout_secs = config.http_timeout_secs;
            let funding_markets = Arc::clone(&markets);
            runtime.spawn(async move {
                if let Err(err) = run_lighter_funding_poller(
                    &funding_url,
                    &funding_markets,
                    poll_secs,
                    timeout_secs,
                    event_tx,
                )
                .await
                {
                    tracing::error!(error = %err, "lighter funding poller terminated");
                }
            });
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BorrowedNumOrString<'a> {
    Number(f64),
    String(&'a str),
}

fn deserialize_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match BorrowedNumOrString::deserialize(deserializer)? {
        BorrowedNumOrString::Number(value) => Ok(value),
        BorrowedNumOrString::String(value) => value.parse().map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Deserialize)]
struct LighterTickerMessage<'a> {
    #[serde(rename = "type")]
    #[serde(default, borrow)]
    msg_type: Option<&'a str>,
    #[serde(default, borrow)]
    channel: Option<&'a str>,
    #[serde(default)]
    ticker: Option<LighterTickerPayload<'a>>,
    timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LighterTickerPayload<'a> {
    #[serde(default, borrow)]
    s: Option<&'a str>,
    #[serde(default)]
    a: Option<LighterSide>,
    #[serde(default)]
    b: Option<LighterSide>,
}

#[derive(Debug, Deserialize)]
struct LighterSide {
    #[serde(deserialize_with = "deserialize_f64")]
    price: f64,
    #[serde(deserialize_with = "deserialize_f64")]
    size: f64,
}

#[derive(Debug, Deserialize)]
struct LighterMarketStatsMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    channel: Option<String>,
    market_stats: Option<LighterMarketStatsPayload>,
    timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LighterMarketStatsPayload {
    symbol: Option<String>,
    market_id: Option<u32>,
    current_funding_rate: Option<String>,
    funding_timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LighterFundingRatesResponse {
    #[serde(default)]
    funding_rates: Vec<LighterFundingRate>,
}

#[derive(Debug, Deserialize)]
struct LighterFundingRate {
    exchange: String,
    symbol: String,
    #[serde(deserialize_with = "deserialize_f64")]
    rate: f64,
}

pub fn lighter_ticker_subscribe_payload(market_id: u32) -> String {
    format!("{{\"type\":\"subscribe\",\"channel\":\"ticker/{market_id}\"}}")
}

pub fn lighter_market_stats_subscribe_payload(market_id: u32) -> String {
    format!("{{\"type\":\"subscribe\",\"channel\":\"market_stats/{market_id}\"}}")
}

fn parse_market_id(channel: &str, expected_prefix: &str) -> Option<u32> {
    channel.strip_prefix(expected_prefix)?.parse::<u32>().ok()
}

pub fn parse_ticker_message(
    raw: &str,
    market_map: &HashMap<u32, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let message: LighterTickerMessage<'_> = sonic_rs::from_str(raw).ok()?;

    let msg_type = message.msg_type.as_deref()?;
    if msg_type != "update/ticker" && msg_type != "subscribed/ticker" {
        return None;
    }

    let ticker = message.ticker?;
    let ask = ticker.a?;
    let bid = ticker.b?;

    if ask.price <= 0.0 || bid.price <= 0.0 || ask.size <= 0.0 || bid.size <= 0.0 {
        return None;
    }

    let from_channel = message
        .channel
        .and_then(|channel| parse_market_id(channel, "ticker:"))
        .and_then(|market_id| market_map.get(&market_id).cloned());

    let symbol_base = from_channel.or_else(|| ticker.s.map(normalize_base))?;
    let exch_ts_ms = message.timestamp.unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Lighter,
        symbol_base: symbol_base.into(),
        bid_px: bid.price,
        bid_qty: bid.size,
        ask_px: ask.price,
        ask_qty: ask.size,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_market_stats_message(
    raw: &str,
    market_map: &HashMap<u32, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let message: LighterMarketStatsMessage = serde_json::from_str(raw).ok()?;

    let msg_type = message.msg_type.as_deref()?;
    if msg_type != "update/market_stats" && msg_type != "subscribed/market_stats" {
        return None;
    }

    let stats = message.market_stats?;
    let rate = stats.current_funding_rate?.parse::<f64>().ok()?;

    let from_channel = message
        .channel
        .as_deref()
        .and_then(|channel| parse_market_id(channel, "market_stats:"))
        .and_then(|market_id| market_map.get(&market_id).cloned());

    let from_market_id = stats
        .market_id
        .and_then(|market_id| market_map.get(&market_id).cloned());

    let symbol_base = from_channel
        .or(from_market_id)
        .or_else(|| stats.symbol.map(|symbol| normalize_base(&symbol)))?;

    let next_funding_ts_ms = stats.funding_timestamp;
    let exch_ts_ms = message.timestamp.unwrap_or(recv_ts_ms);

    Some(FundingUpdate {
        exchange: Exchange::Lighter,
        symbol_base: symbol_base.into(),
        funding_rate: rate,
        next_funding_ts_ms,
        recv_ts_ms: exch_ts_ms,
        stale_after_ms: None,
    })
}

pub async fn run_lighter_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let market_map: HashMap<u32, String> = markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Lighter)
                .and_then(|meta| meta.market_id.map(|id| (id, symbol_base.clone())))
        })
        .collect();
    let market_ids: Vec<u32> = market_map.keys().copied().collect();

    if market_ids.is_empty() {
        tracing::warn!("lighter feed skipped: no matched markets to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            subscriptions = market_ids.len(),
            "connecting lighter WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!("connected lighter WS");
                attempt = 0;

                let mut subscribe_failed = false;
                for market_id in &market_ids {
                    let ticker_payload = lighter_ticker_subscribe_payload(*market_id);
                    if let Err(err) = ws
                        .write_frame(Frame::text(ticker_payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(error = %err, market_id, "lighter ticker subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }
                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, "lighter subscribe flush failed");
                        subscribe_failed = true;
                    }
                }

                if subscribe_failed {
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                loop {
                    match ws.read_frame().await.context("lighter WS read error") {
                        Ok(frame) if frame.opcode == OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    tracing::debug!(error = %err, "lighter WS text frame was not valid UTF-8");
                                    continue;
                                }
                            };
                            let recv_ts_ms = now_ms();

                            if let Some(update) =
                                parse_ticker_message(text, &market_map, recv_ts_ms)
                            {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "lighter feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                                continue;
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            tracing::warn!("lighter WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "lighter WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "lighter WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

pub async fn run_lighter_funding_poller(
    rest_url: &str,
    markets: &SymbolMarkets,
    poll_secs: u64,
    timeout_secs: u64,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if poll_secs == 0 {
        return Ok(());
    }

    let tracked_symbols: HashMap<String, ()> = markets
        .keys()
        .map(|symbol| (normalize_base(symbol), ()))
        .collect();

    if tracked_symbols.is_empty() {
        tracing::warn!("lighter funding poller skipped: no matched symbols");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build lighter funding HTTP client")?;

    let baseline_ms = funding_baseline_poll_ms(poll_secs);
    let mut cache: HashMap<String, f64> = HashMap::new();

    loop {
        let poll_started_ms = now_ms();
        let planned_sleep_ms = jittered_poll_ms(baseline_ms, poll_started_ms);
        let ttl_ms = funding_cache_ttl_ms(planned_sleep_ms);
        match client
            .get(rest_url)
            .send()
            .await
            .and_then(|resp| resp.error_for_status())
        {
            Ok(resp) => match resp.json::<LighterFundingRatesResponse>().await {
                Ok(response) => {
                    let recv_ts_ms = now_ms();

                    for item in response.funding_rates {
                        if !item.exchange.eq_ignore_ascii_case("lighter") {
                            continue;
                        }

                        let symbol_base = normalize_base(&item.symbol);
                        if !tracked_symbols.contains_key(&symbol_base) {
                            continue;
                        }

                        let should_emit = cache
                            .get(&symbol_base)
                            .map(|prev| (item.rate - prev).abs() > FUNDING_CHANGE_EPSILON)
                            .unwrap_or(true);

                        if !should_emit {
                            continue;
                        }

                        cache.insert(symbol_base.clone(), item.rate);
                        let update = FundingUpdate {
                            exchange: Exchange::Lighter,
                            symbol_base: symbol_base.into(),
                            funding_rate: item.rate,
                            next_funding_ts_ms: None,
                            recv_ts_ms,
                            stale_after_ms: Some(ttl_ms),
                        };

                        if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
                            tracing::info!(
                                "lighter funding poller stopped: market event channel closed"
                            );
                            return Ok(());
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "lighter funding poll parse failed");
                }
            },
            Err(err) => {
                tracing::warn!(error = %err, "lighter funding poll request failed");
            }
        }

        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
    }
}
