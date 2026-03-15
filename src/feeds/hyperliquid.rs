use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{
    ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms, funding_baseline_poll_ms,
    funding_cache_ttl_ms, jittered_poll_ms,
};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};

pub struct HyperliquidFeed;

impl ExchangeFeed for HyperliquidFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Hyperliquid
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: &SymbolMarkets,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.hyperliquid_ws_url.clone();
        let feed_markets = markets.clone();
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
            let funding_markets = markets.clone();
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

pub fn parse_hyperliquid_l2book_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = serde_json::from_str(raw).ok()?;

    let channel = root.get("channel").and_then(Value::as_str)?;
    if channel != "l2Book" {
        return None;
    }

    let data = root.get("data")?;
    let coin = data
        .get("coin")
        .or_else(|| data.get("symbol"))
        .and_then(Value::as_str)?;
    let symbol_base = symbol_map.get(&normalize_base(coin))?.clone();

    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }

    let best_bid = levels
        .first()?
        .as_array()?
        .iter()
        .find_map(parse_book_level)?;
    let best_ask = levels
        .get(1)?
        .as_array()?
        .iter()
        .find_map(parse_book_level)?;

    let exch_ts_ms = data
        .get("time")
        .and_then(as_i64)
        .or_else(|| root.get("time").and_then(as_i64))
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Hyperliquid,
        symbol_base,
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

        match connect_async(ws_url).await {
            Ok((stream, _)) => {
                tracing::info!("connected hyperliquid WS");
                attempt = 0;

                let (mut write_half, mut read_half) = stream.split();

                let mut subscribe_failed = false;
                for coin in &coins {
                    let payload = serde_json::to_string(&HyperliquidSubscribeRequest {
                        method: "subscribe",
                        subscription: HyperliquidSubscription {
                            subscription_type: "l2Book",
                            coin,
                        },
                    })
                    .context("failed to serialize hyperliquid subscription")?;

                    if let Err(err) = write_half.send(Message::Text(payload)).await {
                        tracing::warn!(error = %err, coin, "hyperliquid subscribe failed");
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
                    match message_result.context("hyperliquid WS read error") {
                        Ok(Message::Text(text)) => {
                            let recv_ts_ms = now_ms();
                            if let Some(update) = parse_hyperliquid_l2book_message(
                                text.as_ref(),
                                &symbol_map,
                                recv_ts_ms,
                            ) {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "hyperliquid feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write_half.send(Message::Pong(payload)).await {
                                tracing::warn!(error = %err, "hyperliquid WS pong failed");
                                break;
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(?frame, "hyperliquid WS closed by remote");
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
                            symbol_base,
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
