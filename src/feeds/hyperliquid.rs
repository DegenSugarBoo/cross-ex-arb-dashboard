use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
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

pub struct HyperliquidFeed;

impl ExchangeFeed for HyperliquidFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Hyperliquid
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.hyperliquid_ws_url.clone();
        let feed_markets = Arc::clone(&markets);
        let quote_event_tx = event_tx.clone();
        runtime.spawn(async move {
            if let Err(err) = run_hyperliquid_feed(&ws_url, &feed_markets, quote_event_tx).await {
                tracing::error!(error = %err, "hyperliquid feed task terminated");
            }
        });

        if config.funding_poll_secs > 0 {
            let funding_url = config.hyperliquid_funding_rest_url.clone();
            let poll_secs = config.funding_poll_secs;
            let timeout_secs = config.http_timeout_secs;
            let funding_markets = Arc::clone(&markets);
            runtime.spawn(async move {
                if let Err(err) = run_hyperliquid_funding_poller(
                    &funding_url,
                    &funding_markets,
                    poll_secs,
                    timeout_secs,
                    event_tx,
                )
                .await
                {
                    tracing::error!(error = %err, "hyperliquid funding poller terminated");
                }
            });
        }
    }
}

#[derive(Debug, Serialize)]
struct HyperliquidSubscribeRequest<'a> {
    method: &'a str,
    subscription: HyperliquidSubscription<'a>,
}

#[derive(Debug, Serialize)]
struct HyperliquidSubscription<'a> {
    #[serde(rename = "type")]
    subscription_type: &'a str,
    coin: &'a str,
}

#[derive(Debug, Serialize)]
struct HyperliquidInfoRequest<'a> {
    #[serde(rename = "type")]
    req_type: &'a str,
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Typed fast-path structs for Hyperliquid BBO
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BorrowedNumOrStr<'a> {
    Number(f64),
    String(&'a str),
}

fn de_opt_f64_ns<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<BorrowedNumOrStr<'de>>::deserialize(deserializer)?;
    Ok(v.and_then(|p| match p {
        BorrowedNumOrStr::Number(n) => Some(n),
        BorrowedNumOrStr::String(s) => s.parse::<f64>().ok(),
    }))
}

fn de_opt_i64_ns<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<BorrowedNumOrStr<'de>>::deserialize(deserializer)?;
    Ok(v.and_then(|p| match p {
        BorrowedNumOrStr::Number(n) => Some(n as i64),
        BorrowedNumOrStr::String(s) => s.parse::<i64>().ok(),
    }))
}

#[derive(Debug, Deserialize)]
struct HlBboLevel {
    #[serde(default, deserialize_with = "de_opt_f64_ns")]
    px: Option<f64>,
    #[serde(
        default,
        alias = "size",
        alias = "q",
        alias = "qty",
        deserialize_with = "de_opt_f64_ns"
    )]
    sz: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct HlBboData<'a> {
    #[serde(default, borrow)]
    coin: Option<&'a str>,
    #[serde(default, alias = "symbol", borrow)]
    #[allow(dead_code)]
    _symbol: Option<&'a str>,
    #[serde(default, deserialize_with = "de_opt_i64_ns")]
    time: Option<i64>,
    #[serde(default)]
    bbo: Option<Vec<HlBboLevel>>,
}

#[derive(Debug, Deserialize)]
struct HlBboRoot<'a> {
    #[serde(default, borrow)]
    channel: Option<&'a str>,
    #[serde(default, borrow)]
    data: Option<HlBboData<'a>>,
    #[serde(default, deserialize_with = "de_opt_i64_ns")]
    time: Option<i64>,
}

pub fn parse_hyperliquid_bbo_message_fast(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: HlBboRoot<'_> = sonic_rs::from_str(raw).ok()?;
    if root.channel? != "bbo" {
        return None;
    }
    let data = root.data?;
    let coin = data.coin.or(data._symbol)?;
    let symbol_base = resolve_symbol_base(symbol_map, coin)?;

    let bbo = data.bbo?;
    if bbo.len() < 2 {
        return None;
    }
    let bid = &bbo[0];
    let ask = &bbo[1];
    let bid_px = bid.px.filter(|v| *v > 0.0)?;
    let bid_qty = bid.sz.filter(|v| *v > 0.0)?;
    let ask_px = ask.px.filter(|v| *v > 0.0)?;
    let ask_qty = ask.sz.filter(|v| *v > 0.0)?;

    let exch_ts_ms = data.time.or(root.time).unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Hyperliquid,
        symbol_base: symbol_base.into(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

fn hyperliquid_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Hyperliquid)
                .map(|meta| (normalize_base(&meta.exchange_symbol), symbol_base.clone()))
        })
        .collect()
}

fn resolve_symbol_base(symbol_map: &HashMap<String, String>, coin: &str) -> Option<String> {
    if let Some(symbol_base) = symbol_map.get(coin) {
        return Some(symbol_base.clone());
    }

    let normalized = normalize_base(coin);
    symbol_map.get(&normalized).cloned()
}

fn parse_book_level(level: &Value) -> Option<(f64, f64)> {
    match level {
        Value::Array(items) if items.len() >= 2 => {
            let px = as_f64(items.first()?)?;
            let qty = as_f64(items.get(1)?)?;
            if px > 0.0 && qty > 0.0 {
                Some((px, qty))
            } else {
                None
            }
        }
        Value::Object(map) => {
            let px = map
                .get("px")
                .or_else(|| map.get("price"))
                .or_else(|| map.get("p"))
                .and_then(as_f64)?;
            let qty = map
                .get("sz")
                .or_else(|| map.get("size"))
                .or_else(|| map.get("q"))
                .or_else(|| map.get("qty"))
                .and_then(as_f64)?;
            if px > 0.0 && qty > 0.0 {
                Some((px, qty))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn top_from_levels_side(levels: &Value, is_bid: bool) -> Option<(f64, f64)> {
    let levels = levels.as_array()?;

    // Hyperliquid levels are best-first in normal operation. Fast-path that
    // case and keep a full scan fallback for payload anomalies.
    if let Some(best) = levels.first().and_then(parse_book_level) {
        return Some(best);
    }

    let mut best_px: Option<f64> = None;
    let mut best_qty: Option<f64> = None;

    for level in levels {
        let (px, qty) = match parse_book_level(level) {
            Some(parsed) => parsed,
            None => continue,
        };

        match best_px {
            None => {
                best_px = Some(px);
                best_qty = Some(qty);
            }
            Some(current) => {
                let better = if is_bid { px > current } else { px < current };
                if better {
                    best_px = Some(px);
                    best_qty = Some(qty);
                }
            }
        }
    }

    Some((best_px?, best_qty?))
}

pub fn parse_hyperliquid_bbo_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    parse_hyperliquid_bbo_message_fast(raw, symbol_map, recv_ts_ms)
        .or_else(|| parse_hyperliquid_bbo_message_fallback(raw, symbol_map, recv_ts_ms))
}

pub fn parse_hyperliquid_bbo_message_fallback(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = sonic_rs::from_str(raw).ok()?;

    let channel = root.get("channel").and_then(Value::as_str)?;
    if channel != "bbo" {
        return None;
    }

    let data = root.get("data")?;
    let coin = data
        .get("coin")
        .or_else(|| data.get("symbol"))
        .and_then(Value::as_str)?;
    let symbol_base = resolve_symbol_base(symbol_map, coin)?;

    let bbo = data.get("bbo")?.as_array()?;
    if bbo.len() < 2 {
        return None;
    }

    let best_bid = parse_book_level(bbo.first()?)?;
    let best_ask = parse_book_level(bbo.get(1)?)?;

    let exch_ts_ms = data
        .get("time")
        .and_then(as_i64)
        .or_else(|| root.get("time").and_then(as_i64))
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Hyperliquid,
        symbol_base: symbol_base.into(),
        bid_px: best_bid.0,
        bid_qty: best_bid.1,
        ask_px: best_ask.0,
        ask_qty: best_ask.1,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_hyperliquid_l2book_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = sonic_rs::from_str(raw).ok()?;

    let channel = root.get("channel").and_then(Value::as_str)?;
    if channel != "l2Book" {
        return None;
    }

    let data = root.get("data")?;
    let coin = data
        .get("coin")
        .or_else(|| data.get("symbol"))
        .and_then(Value::as_str)?;
    let symbol_base = resolve_symbol_base(symbol_map, coin)?;

    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }

    let best_bid = top_from_levels_side(levels.first()?, true)?;
    let best_ask = top_from_levels_side(levels.get(1)?, false)?;

    let exch_ts_ms = data
        .get("time")
        .and_then(as_i64)
        .or_else(|| root.get("time").and_then(as_i64))
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Hyperliquid,
        symbol_base: symbol_base.into(),
        bid_px: best_bid.0,
        bid_qty: best_bid.1,
        ask_px: best_ask.0,
        ask_qty: best_ask.1,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub async fn run_hyperliquid_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = hyperliquid_symbol_map(markets);
    let coins: Vec<String> = symbol_map.keys().cloned().collect();

    if coins.is_empty() {
        tracing::warn!("hyperliquid feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            subscriptions = coins.len(),
            "connecting hyperliquid WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!("connected hyperliquid WS");
                attempt = 0;

                let mut subscribe_failed = false;
                for coin in &coins {
                    let payload = serde_json::to_string(&HyperliquidSubscribeRequest {
                        method: "subscribe",
                        subscription: HyperliquidSubscription {
                            subscription_type: "bbo",
                            coin,
                        },
                    })
                    .context("failed to serialize hyperliquid subscription")?;

                    if let Err(err) = ws
                        .write_frame(Frame::text(payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(error = %err, coin, "hyperliquid subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }
                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, "hyperliquid subscribe flush failed");
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
                    match ws.read_frame().await.context("hyperliquid WS read error") {
                        Ok(frame) if frame.opcode == OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    tracing::debug!(error = %err, "hyperliquid WS text frame was not valid UTF-8");
                                    continue;
                                }
                            };
                            let recv_ts_ms = now_ms();
                            if let Some(update) =
                                parse_hyperliquid_bbo_message(text, &symbol_map, recv_ts_ms)
                            {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "hyperliquid feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            tracing::warn!("hyperliquid WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "hyperliquid WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "hyperliquid WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

pub async fn run_hyperliquid_funding_poller(
    rest_url: &str,
    markets: &SymbolMarkets,
    poll_secs: u64,
    timeout_secs: u64,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if poll_secs == 0 {
        return Ok(());
    }

    let symbol_map = hyperliquid_symbol_map(markets);
    if symbol_map.is_empty() {
        tracing::warn!("hyperliquid funding poller skipped: no matched symbols");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build hyperliquid funding HTTP client")?;

    let baseline_ms = funding_baseline_poll_ms(poll_secs);
    let mut cache: HashMap<String, f64> = HashMap::new();

    loop {
        let poll_started_ms = now_ms();
        let planned_sleep_ms = jittered_poll_ms(baseline_ms, poll_started_ms);
        let ttl_ms = funding_cache_ttl_ms(planned_sleep_ms);

        match client
            .post(rest_url)
            .json(&HyperliquidInfoRequest {
                req_type: "metaAndAssetCtxs",
            })
            .send()
            .await
            .and_then(|resp| resp.error_for_status())
        {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(response) => {
                    let Some(response_items) = response.as_array() else {
                        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                        continue;
                    };

                    if response_items.len() < 2 {
                        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                        continue;
                    }

                    let Some(universe) =
                        response_items[0].get("universe").and_then(Value::as_array)
                    else {
                        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                        continue;
                    };

                    let Some(asset_ctxs) = response_items[1].as_array() else {
                        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                        continue;
                    };

                    let recv_ts_ms = now_ms();
                    let len = universe.len().min(asset_ctxs.len());

                    for idx in 0..len {
                        let Some(coin) = universe[idx]
                            .get("name")
                            .and_then(Value::as_str)
                            .map(normalize_base)
                        else {
                            continue;
                        };

                        let Some(symbol_base) = symbol_map.get(&coin).cloned() else {
                            continue;
                        };

                        let ctx = &asset_ctxs[idx];
                        let funding_rate = ctx
                            .get("funding")
                            .or_else(|| ctx.get("fundingRate"))
                            .or_else(|| ctx.get("currentFunding"))
                            .and_then(as_f64);

                        let Some(funding_rate) = funding_rate else {
                            continue;
                        };

                        let should_emit = cache
                            .get(&symbol_base)
                            .map(|prev| (funding_rate - prev).abs() > FUNDING_CHANGE_EPSILON)
                            .unwrap_or(true);

                        if !should_emit {
                            continue;
                        }

                        cache.insert(symbol_base.clone(), funding_rate);
                        let update = FundingUpdate {
                            exchange: Exchange::Hyperliquid,
                            symbol_base: symbol_base.into(),
                            funding_rate,
                            next_funding_ts_ms: ctx
                                .get("nextFundingTime")
                                .or_else(|| ctx.get("nextFundingTimestamp"))
                                .and_then(as_i64),
                            recv_ts_ms,
                            stale_after_ms: Some(ttl_ms),
                        };

                        if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
                            tracing::info!(
                                "hyperliquid funding poller stopped: market event channel closed"
                            );
                            return Ok(());
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "hyperliquid funding poll parse failed");
                }
            },
            Err(err) => {
                tracing::warn!(error = %err, "hyperliquid funding poll request failed");
            }
        }

        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
    }
}
