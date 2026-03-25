use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{
    ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms, funding_baseline_poll_ms,
    funding_cache_ttl_ms, jittered_poll_ms,
};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

const HYPERLIQUID_HEARTBEAT_SECS: u64 = 45;
const HYPERLIQUID_SUBSCRIPTIONS_PER_CONNECTION: usize = 50;

pub struct HyperliquidFeed;

fn hyperliquid_ping_payload() -> &'static str {
    "{\"method\":\"ping\"}"
}

fn pong_frame_for(frame: &Frame<'_>) -> Frame<'static> {
    Frame::pong(frame.payload.to_vec().into())
}

impl ExchangeFeed for HyperliquidFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Hyperliquid
    }

    fn spawn(
        &self,
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let ws_url = config.hyperliquid_ws_url.clone();
        let quote_symbol_map = hyperliquid_symbol_map(&markets);
        let shard_coins = hyperliquid_subscription_coins(&markets);
        let mut quote_handles = Vec::new();

        if shard_coins.is_empty() {
            tracing::warn!("hyperliquid feed skipped: no matched symbols to subscribe");
        } else {
            for (shard_id, chunk) in shard_coins
                .chunks(HYPERLIQUID_SUBSCRIPTIONS_PER_CONNECTION)
                .enumerate()
            {
                let shard_symbol_map = quote_symbol_map.clone();
                let shard_ws_url = ws_url.clone();
                let shard_coins = chunk.to_vec();
                tracing::info!(
                    shard_id,
                    subscriptions = shard_coins.len(),
                    coins = ?shard_coins,
                    "prepared hyperliquid shard"
                );
                let quote_event_tx = event_tx.clone();
                let quote_cancel = cancel_token.clone();
                let quote_task = runtime.spawn(async move {
                    tokio::select! {
                        _ = quote_cancel.cancelled() => {
                            tracing::info!(shard_id, "hyperliquid feed task cancelled");
                        }
                        result = run_hyperliquid_feed(
                            &shard_ws_url,
                            &shard_symbol_map,
                            shard_coins,
                            shard_id,
                            quote_event_tx,
                        ) => {
                            if let Err(err) = result {
                                tracing::error!(error = %err, shard_id, "hyperliquid feed task terminated");
                            }
                        }
                    }
                });
                quote_handles.push(quote_task);
            }
        }

        let mut handles = quote_handles;

        if config.funding_poll_secs > 0 {
            let funding_url = config.hyperliquid_funding_rest_url.clone();
            let poll_secs = config.funding_poll_secs;
            let timeout_secs = config.http_timeout_secs;
            let funding_markets = Arc::clone(&markets);
            let funding_cancel = cancel_token;
            let funding_task = runtime.spawn(async move {
                tokio::select! {
                    _ = funding_cancel.cancelled() => {
                        tracing::info!("hyperliquid funding poller cancelled");
                    }
                    result = run_hyperliquid_funding_poller(
                        &funding_url,
                        &funding_markets,
                        poll_secs,
                        timeout_secs,
                        event_tx,
                    ) => {
                        if let Err(err) = result {
                            tracing::error!(error = %err, "hyperliquid funding poller terminated");
                        }
                    }
                }
            });
            handles.push(funding_task);
        }

        handles
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
    let mut map = HashMap::new();

    for (symbol_base, per_exchange) in markets {
        let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Hyperliquid)
        else {
            continue;
        };

        map.insert(meta.exchange_symbol.clone(), symbol_base.clone());

        let normalized = normalize_base(&meta.exchange_symbol);
        map.entry(normalized).or_insert_with(|| symbol_base.clone());
    }

    map
}

fn hyperliquid_subscription_coins(markets: &SymbolMarkets) -> Vec<String> {
    let mut coins: Vec<String> = markets
        .values()
        .filter_map(|per_exchange| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Hyperliquid)
                .map(|meta| meta.exchange_symbol.clone())
        })
        .collect();
    coins.sort_unstable();
    coins.dedup();
    coins
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
    symbol_map: &HashMap<String, String>,
    coins: Vec<String>,
    shard_id: usize,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if coins.is_empty() {
        tracing::warn!("hyperliquid feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            shard_id,
            subscriptions = coins.len(),
            "connecting hyperliquid WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!(shard_id, "connected hyperliquid WS");
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
                        tracing::warn!(error = %err, coin, shard_id, "hyperliquid subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }
                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, shard_id, "hyperliquid subscribe flush failed");
                        subscribe_failed = true;
                    }
                }

                if subscribe_failed {
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                let mut heartbeat =
                    tokio::time::interval(Duration::from_secs(HYPERLIQUID_HEARTBEAT_SECS));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                heartbeat.tick().await;

                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(err) = ws
                                .write_frame(Frame::text(hyperliquid_ping_payload().as_bytes().to_vec().into()))
                                .await
                            {
                                tracing::warn!(error = %err, shard_id, "hyperliquid ping send failed");
                                break;
                            }
                            if let Err(err) = ws.flush().await {
                                tracing::warn!(error = %err, shard_id, "hyperliquid ping flush failed");
                                break;
                            }
                        }
                        frame_result = ws.read_frame() => match frame_result.context("hyperliquid WS read error") {
                        Ok(frame) if frame.opcode == OpCode::Ping => {
                            if let Err(err) = ws.write_frame(pong_frame_for(&frame)).await {
                                tracing::warn!(error = %err, shard_id, "hyperliquid WS pong send failed");
                                break;
                            }
                            if let Err(err) = ws.flush().await {
                                tracing::warn!(error = %err, shard_id, "hyperliquid WS pong flush failed");
                                break;
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Pong => {
                            tracing::debug!(payload_len = frame.payload.len(), shard_id, "hyperliquid WS pong received");
                        }
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
                                        shard_id,
                                        "hyperliquid feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            tracing::warn!(shard_id, "hyperliquid WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = ?err, shard_id, "hyperliquid WS message handling failed");
                            break;
                        }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, shard_id, "hyperliquid WS connect failed");
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{hyperliquid_subscription_coins, hyperliquid_symbol_map};
    use crate::discovery::SymbolMarkets;
    use crate::model::{Exchange, MarketMeta};

    fn hyperliquid_meta(symbol_base: &str, exchange_symbol: &str) -> MarketMeta {
        MarketMeta {
            exchange: Exchange::Hyperliquid,
            symbol_base: symbol_base.to_owned(),
            exchange_symbol: exchange_symbol.to_owned(),
            market_id: None,
            taker_fee_pct: 0.0,
            maker_fee_pct: 0.0,
        }
    }

    #[test]
    fn hyperliquid_subscriptions_preserve_exchange_symbol_casing() {
        let markets: SymbolMarkets = HashMap::from([
            ("KBONK".to_owned(), vec![hyperliquid_meta("KBONK", "kBONK")]),
            ("BTC".to_owned(), vec![hyperliquid_meta("BTC", "BTC")]),
        ]);

        assert_eq!(
            hyperliquid_subscription_coins(&markets),
            vec!["BTC".to_owned(), "kBONK".to_owned()]
        );
    }

    #[test]
    fn hyperliquid_symbol_map_accepts_exact_and_normalized_coin_names() {
        let markets: SymbolMarkets =
            HashMap::from([("KBONK".to_owned(), vec![hyperliquid_meta("KBONK", "kBONK")])]);

        let symbol_map = hyperliquid_symbol_map(&markets);

        assert_eq!(symbol_map.get("kBONK").map(String::as_str), Some("KBONK"));
        assert_eq!(symbol_map.get("KBONK").map(String::as_str), Some("KBONK"));
    }
}
