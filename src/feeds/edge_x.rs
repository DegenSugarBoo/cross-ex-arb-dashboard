use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::AppConfig;
use crate::discovery::SymbolMarkets;
use crate::feeds::{
    ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms, funding_baseline_poll_ms,
    funding_cache_ttl_ms, jittered_poll_ms,
};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};

pub struct EdgeXFeed;

impl ExchangeFeed for EdgeXFeed {
    fn exchange(&self) -> Exchange {
        Exchange::EdgeX
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: &SymbolMarkets,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.edge_x_ws_url.clone();
        let feed_markets = markets.clone();
        let quote_event_tx = event_tx.clone();
        runtime.spawn(async move {
            if let Err(err) = run_edge_x_feed(&ws_url, &feed_markets, quote_event_tx).await {
                tracing::error!(error = %err, "edgeX feed task terminated");
            }
        });

        tracing::info!(
            "edgeX funding sourced from WS ticker channel; REST funding poller disabled"
        );
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum NumOrString {
    Number(f64),
    String(String),
}

impl NumOrString {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::String(value) => value.parse::<f64>().ok(),
        }
    }

    fn as_u32(&self) -> Option<u32> {
        match self {
            Self::Number(value) => {
                if *value >= 0.0 {
                    Some(*value as u32)
                } else {
                    None
                }
            }
            Self::String(value) => value.parse::<u32>().ok(),
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(value) => Some(*value as i64),
            Self::String(value) => value.parse::<i64>().ok(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct EdgeXFundingResponse {
    data: Option<EdgeXFundingData>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EdgeXFundingData {
    One(EdgeXFundingItem),
    List(Vec<EdgeXFundingItem>),
    Page(EdgeXFundingPage),
}

impl EdgeXFundingData {
    fn into_vec(self) -> Vec<EdgeXFundingItem> {
        match self {
            Self::One(item) => vec![item],
            Self::List(items) => items,
            Self::Page(page) => page.data_list,
        }
    }
}

#[derive(Debug, Deserialize)]
struct EdgeXFundingPage {
    #[serde(default, rename = "dataList")]
    data_list: Vec<EdgeXFundingItem>,
}

#[derive(Debug, Deserialize)]
struct EdgeXFundingItem {
    #[serde(rename = "contractId")]
    contract_id: Option<NumOrString>,
    #[serde(rename = "fundingRate")]
    funding_rate: Option<NumOrString>,
    #[serde(rename = "fundingTime")]
    funding_time: Option<NumOrString>,
    #[serde(rename = "fundingTimestamp")]
    funding_timestamp: Option<NumOrString>,
    #[serde(rename = "nextFundingTimestamp")]
    next_funding_timestamp: Option<NumOrString>,
}

pub fn edge_x_depth_subscribe_payload(contract_id: u32) -> String {
    format!("{{\"type\":\"subscribe\",\"channel\":\"depth.{contract_id}.15\"}}")
}

pub fn edge_x_ticker_subscribe_payload(contract_id: u32) -> String {
    format!("{{\"type\":\"subscribe\",\"channel\":\"ticker.{contract_id}\"}}")
}

fn parse_channel_contract_id(channel: &str) -> Option<u32> {
    let mut parts = channel.split('.');
    let prefix = parts.next()?;
    let contract = parts.next()?;
    if prefix != "depth" {
        return None;
    }
    contract.parse::<u32>().ok()
}

fn parse_ticker_contract_id(channel: &str) -> Option<u32> {
    channel.strip_prefix("ticker.")?.parse::<u32>().ok()
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

fn top_from_levels(levels: &Value, is_bid: bool) -> Option<(f64, f64)> {
    let levels = levels.as_array()?;
    let mut best_px: Option<f64> = None;
    let mut best_qty: Option<f64> = None;

    for level in levels {
        let (px, qty) = match level {
            Value::Array(items) if items.len() >= 2 => {
                let px = as_f64(items.first()?)?;
                let qty = as_f64(items.get(1)?)?;
                (px, qty)
            }
            Value::Object(map) => {
                let px = map
                    .get("price")
                    .or_else(|| map.get("p"))
                    .or_else(|| map.get("px"))
                    .and_then(as_f64)?;
                let qty = map
                    .get("size")
                    .or_else(|| map.get("q"))
                    .or_else(|| map.get("qty"))
                    .or_else(|| map.get("s"))
                    .and_then(as_f64)?;
                (px, qty)
            }
            _ => continue,
        };

        if px <= 0.0 || qty <= 0.0 {
            continue;
        }

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

pub fn parse_edge_x_depth_message(
    raw: &str,
    market_map: &HashMap<u32, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = serde_json::from_str(raw).ok()?;

    let channel = root.get("channel").and_then(Value::as_str);
    let from_channel = channel.and_then(parse_channel_contract_id);

    let payload = root
        .get("content")
        .or_else(|| root.get("data"))
        .or_else(|| root.get("payload"))?;

    let first_data = payload
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .or(Some(payload))?;

    let contract_id = first_data
        .get("contractId")
        .and_then(|value| match value {
            Value::String(text) => text.parse::<u32>().ok(),
            Value::Number(number) => number.as_u64().map(|value| value as u32),
            _ => None,
        })
        .or(from_channel)?;

    let symbol_base = market_map.get(&contract_id)?.clone();

    let bids = first_data
        .get("bids")
        .or_else(|| first_data.get("b"))
        .or_else(|| payload.get("bids"))
        .or_else(|| payload.get("b"))?;

    let asks = first_data
        .get("asks")
        .or_else(|| first_data.get("a"))
        .or_else(|| payload.get("asks"))
        .or_else(|| payload.get("a"))?;

    let (bid_px, bid_qty) = top_from_levels(bids, true)?;
    let (ask_px, ask_qty) = top_from_levels(asks, false)?;

    let exch_ts_ms = first_data
        .get("timestamp")
        .and_then(as_i64)
        .or_else(|| first_data.get("t").and_then(as_i64))
        .or_else(|| root.get("timestamp").and_then(as_i64))
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::EdgeX,
        symbol_base,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_edge_x_ticker_funding_message(
    raw: &str,
    market_map: &HashMap<u32, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let root: Value = serde_json::from_str(raw).ok()?;
    let channel = root.get("channel").and_then(Value::as_str)?;
    let from_channel = parse_ticker_contract_id(channel)?;

    let payload = root
        .get("content")
        .or_else(|| root.get("data"))
        .or_else(|| root.get("payload"))?;

    let first_data = payload
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .or(Some(payload))?;

    let contract_id = first_data
        .get("contractId")
        .and_then(|value| match value {
            Value::String(text) => text.parse::<u32>().ok(),
            Value::Number(number) => number.as_u64().map(|value| value as u32),
            _ => None,
        })
        .unwrap_or(from_channel);

    let symbol_base = market_map.get(&contract_id)?.clone();
    let funding_rate = first_data
        .get("fundingRate")
        .or_else(|| first_data.get("funding_rate"))
        .or_else(|| first_data.get("r"))
        .and_then(as_f64)?;

    let next_funding_ts_ms = first_data
        .get("nextFundingTime")
        .and_then(as_i64)
        .or_else(|| first_data.get("nextFundingTimestamp").and_then(as_i64))
        .or_else(|| first_data.get("fundingTimestamp").and_then(as_i64));

    let exch_ts_ms = first_data
        .get("fundingTime")
        .and_then(as_i64)
        .or_else(|| first_data.get("timestamp").and_then(as_i64))
        .or_else(|| first_data.get("endTime").and_then(as_i64))
        .unwrap_or(recv_ts_ms);

    Some(FundingUpdate {
        exchange: Exchange::EdgeX,
        symbol_base,
        funding_rate,
        next_funding_ts_ms,
        recv_ts_ms: exch_ts_ms,
        stale_after_ms: None,
    })
}

fn edge_x_market_map(markets: &SymbolMarkets) -> HashMap<u32, String> {
    markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::EdgeX)
                .and_then(|meta| meta.market_id.map(|id| (id, symbol_base.clone())))
        })
        .collect()
}

async fn emit_edge_x_funding_updates(
    items: Vec<EdgeXFundingItem>,
    tracked_contracts: &HashMap<u32, String>,
    cache: &mut HashMap<String, f64>,
    ttl_ms: i64,
    recv_ts_ms: i64,
    event_tx: &mpsc::Sender<MarketEvent>,
) -> anyhow::Result<usize> {
    let mut emitted = 0usize;

    for item in items {
        let Some(contract_id) = item.contract_id.as_ref().and_then(NumOrString::as_u32) else {
            continue;
        };

        let Some(symbol_base) = tracked_contracts.get(&contract_id).cloned() else {
            continue;
        };

        let Some(funding_rate) = item.funding_rate.as_ref().and_then(NumOrString::as_f64) else {
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
        let next_funding_ts_ms = item
            .next_funding_timestamp
            .as_ref()
            .and_then(NumOrString::as_i64)
            .or_else(|| {
                item.funding_timestamp
                    .as_ref()
                    .and_then(NumOrString::as_i64)
            })
            .or_else(|| item.funding_time.as_ref().and_then(NumOrString::as_i64));

        let update = FundingUpdate {
            exchange: Exchange::EdgeX,
            symbol_base,
            funding_rate,
            next_funding_ts_ms,
            recv_ts_ms,
            stale_after_ms: Some(ttl_ms),
        };

        if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
            tracing::info!("edgeX funding poller stopped: market event channel closed");
            return Ok(emitted);
        }
        emitted = emitted.saturating_add(1);
    }

    Ok(emitted)
}

fn extract_ping_time(raw: &str) -> Option<String> {
    let value: Value = serde_json::from_str(raw).ok()?;
    if value.get("type")?.as_str()? != "ping" {
        return None;
    }

    value
        .get("time")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("timestamp")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

pub async fn run_edge_x_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let market_map = edge_x_market_map(markets);
    let market_ids: Vec<u32> = market_map.keys().copied().collect();

    if market_ids.is_empty() {
        tracing::warn!("edgeX feed skipped: no matched markets to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            subscriptions = market_ids.len(),
            "connecting edgeX WS"
        );

        match connect_async(ws_url).await {
            Ok((stream, _)) => {
                tracing::info!("connected edgeX WS");
                attempt = 0;

                let (mut write_half, mut read_half) = stream.split();

                let mut subscribe_failed = false;
                for contract_id in &market_ids {
                    let depth_payload = edge_x_depth_subscribe_payload(*contract_id);
                    if let Err(err) = write_half.send(Message::Text(depth_payload)).await {
                        tracing::warn!(
                            error = %err,
                            contract_id,
                            "edgeX depth subscribe failed"
                        );
                        subscribe_failed = true;
                        break;
                    }

                    let ticker_payload = edge_x_ticker_subscribe_payload(*contract_id);
                    if let Err(err) = write_half.send(Message::Text(ticker_payload)).await {
                        tracing::warn!(
                            error = %err,
                            contract_id,
                            "edgeX ticker subscribe failed"
                        );
                        subscribe_failed = true;
                        break;
                    }
                }

                if subscribe_failed {
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                while let Some(message_result) = read_half.next().await {
                    match message_result.context("edgeX WS read error") {
                        Ok(Message::Text(text)) => {
                            if let Some(ping_time) = extract_ping_time(text.as_ref()) {
                                let pong =
                                    format!("{{\"type\":\"pong\",\"time\":\"{ping_time}\"}}");
                                if let Err(err) = write_half.send(Message::Text(pong)).await {
                                    tracing::warn!(error = %err, "edgeX WS pong send failed");
                                    break;
                                }
                                continue;
                            }

                            let recv_ts_ms = now_ms();
                            if let Some(update) =
                                parse_edge_x_depth_message(text.as_ref(), &market_map, recv_ts_ms)
                            {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "edgeX feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }

                            if let Some(update) = parse_edge_x_ticker_funding_message(
                                text.as_ref(),
                                &market_map,
                                recv_ts_ms,
                            ) {
                                if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
                                    tracing::info!(
                                        "edgeX feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write_half.send(Message::Pong(payload)).await {
                                tracing::warn!(error = %err, "edgeX WS pong failed");
                                break;
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(?frame, "edgeX WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "edgeX WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "edgeX WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

pub async fn run_edge_x_funding_poller(
    rest_url: &str,
    markets: &SymbolMarkets,
    poll_secs: u64,
    timeout_secs: u64,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if poll_secs == 0 {
        return Ok(());
    }

    let tracked_contracts = edge_x_market_map(markets);
    if tracked_contracts.is_empty() {
        tracing::warn!("edgeX funding poller skipped: no matched symbols");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build edgeX funding HTTP client")?;

    let baseline_ms = funding_baseline_poll_ms(poll_secs);
    let mut contract_ids: Vec<u32> = tracked_contracts.keys().copied().collect();
    contract_ids.sort_unstable();
    let bulk_poll_base_ms = (baseline_ms / 10).max(15_000);
    let per_contract_base_ms = (baseline_ms / (contract_ids.len() as u64)).max(15_000);
    let page_url = rest_url.replace("getLatestFundingRate", "getFundingRatePage");
    let page_endpoint_enabled = page_url != rest_url;
    let mut cache: HashMap<String, f64> = HashMap::new();
    let mut cursor: usize = 0;
    let mut rate_limited_streak: u32 = 0;

    loop {
        let poll_started_ms = now_ms();
        let mut planned_sleep_ms = jittered_poll_ms(bulk_poll_base_ms, poll_started_ms);
        let ttl_ms = funding_cache_ttl_ms(baseline_ms);

        let mut hit_rate_limit = false;
        let mut emitted_any = false;

        match client.get(rest_url).send().await {
            Ok(resp) => {
                if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    hit_rate_limit = true;
                    tracing::warn!("edgeX funding bulk poll rate-limited");
                } else if let Err(err) = resp.error_for_status_ref() {
                    tracing::warn!(error = %err, "edgeX funding bulk poll request failed");
                } else {
                    match resp.json::<EdgeXFundingResponse>().await {
                        Ok(response) => {
                            let recv_ts_ms = now_ms();
                            let items = response
                                .data
                                .map(EdgeXFundingData::into_vec)
                                .unwrap_or_default();

                            if !items.is_empty() {
                                let emitted = emit_edge_x_funding_updates(
                                    items,
                                    &tracked_contracts,
                                    &mut cache,
                                    ttl_ms,
                                    recv_ts_ms,
                                    &event_tx,
                                )
                                .await?;
                                emitted_any = emitted > 0;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "edgeX funding bulk poll parse failed");
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "edgeX funding bulk poll request failed");
            }
        }

        if !hit_rate_limit && !emitted_any && page_endpoint_enabled {
            match client
                .get(&page_url)
                .query(&[("size", "300"), ("offsetData", "")])
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                        hit_rate_limit = true;
                        tracing::warn!("edgeX funding page poll rate-limited");
                    } else if let Err(err) = resp.error_for_status_ref() {
                        tracing::warn!(error = %err, "edgeX funding page poll request failed");
                    } else {
                        match resp.json::<EdgeXFundingResponse>().await {
                            Ok(response) => {
                                let recv_ts_ms = now_ms();
                                let items = response
                                    .data
                                    .map(EdgeXFundingData::into_vec)
                                    .unwrap_or_default();
                                if !items.is_empty() {
                                    let emitted = emit_edge_x_funding_updates(
                                        items,
                                        &tracked_contracts,
                                        &mut cache,
                                        ttl_ms,
                                        recv_ts_ms,
                                        &event_tx,
                                    )
                                    .await?;
                                    emitted_any = emitted > 0;
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "edgeX funding page poll parse failed"
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "edgeX funding page poll request failed");
                }
            }
        }

        if !hit_rate_limit && !emitted_any {
            let tracked_contract_id = contract_ids[cursor];
            cursor = (cursor + 1) % contract_ids.len();

            match client
                .get(rest_url)
                .query(&[("contractId", tracked_contract_id.to_string())])
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                        hit_rate_limit = true;
                        tracing::warn!(
                            contract_id = tracked_contract_id,
                            "edgeX per-contract funding poll rate-limited"
                        );
                    } else if let Err(err) = resp.error_for_status_ref() {
                        tracing::warn!(
                            error = %err,
                            contract_id = tracked_contract_id,
                            "edgeX funding poll request failed"
                        );
                    } else {
                        match resp.json::<EdgeXFundingResponse>().await {
                            Ok(response) => {
                                let recv_ts_ms = now_ms();
                                let items = response
                                    .data
                                    .map(EdgeXFundingData::into_vec)
                                    .unwrap_or_default();
                                if !items.is_empty() {
                                    let _ = emit_edge_x_funding_updates(
                                        items,
                                        &tracked_contracts,
                                        &mut cache,
                                        ttl_ms,
                                        recv_ts_ms,
                                        &event_tx,
                                    )
                                    .await?;
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    contract_id = tracked_contract_id,
                                    "edgeX funding poll parse failed"
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        contract_id = tracked_contract_id,
                        "edgeX funding poll request failed"
                    );
                }
            }

            planned_sleep_ms = planned_sleep_ms.max(per_contract_base_ms);
        }

        if hit_rate_limit {
            rate_limited_streak = rate_limited_streak.saturating_add(1);
            let exp = rate_limited_streak.min(4);
            let backoff_ms = 30_000u64.saturating_mul(1u64 << exp);
            planned_sleep_ms = planned_sleep_ms.max(backoff_ms);
            tracing::warn!(
                rate_limited_streak,
                sleep_ms = planned_sleep_ms,
                "edgeX funding poll backoff active"
            );
        } else {
            rate_limited_streak = 0;
        }

        tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
    }
}
