use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{
    ExchangeFeed, FUNDING_CHANGE_EPSILON, aster_adaptive_poll_ms, backoff_delay_ms,
    funding_baseline_poll_ms, funding_cache_ttl_ms, jittered_poll_ms,
};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};

pub struct AsterFeed;

impl ExchangeFeed for AsterFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Aster
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: &SymbolMarkets,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.aster_ws_url.clone();
        let feed_markets = markets.clone();
        let quote_event_tx = event_tx.clone();
        runtime.spawn(async move {
            if let Err(err) = run_aster_feed(&ws_url, &feed_markets, quote_event_tx).await {
                tracing::error!(error = %err, "aster feed task terminated");
            }
        });

        if config.funding_poll_secs > 0 {
            let funding_url = config.aster_funding_rest_url.clone();
            let poll_secs = config.funding_poll_secs;
            let timeout_secs = config.http_timeout_secs;
            let funding_markets = markets.clone();
            runtime.spawn(async move {
                if let Err(err) = run_aster_funding_poller(
                    &funding_url,
                    &funding_markets,
                    poll_secs,
                    timeout_secs,
                    event_tx,
                )
                .await
                {
                    tracing::error!(error = %err, "aster funding poller terminated");
                }
            });
        }
    }
}

#[derive(Debug, Deserialize)]
struct AsterBookTickerMessage {
    #[serde(rename = "e")]
    event_type: Option<String>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "b")]
    bid_px: Option<String>,
    #[serde(rename = "B")]
    bid_qty: Option<String>,
    #[serde(rename = "a")]
    ask_px: Option<String>,
    #[serde(rename = "A")]
    ask_qty: Option<String>,
    #[serde(rename = "T")]
    trade_ts_ms: Option<i64>,
    #[serde(rename = "E")]
    event_ts_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AsterMarkPriceMessage {
    #[serde(rename = "e")]
    event_type: Option<String>,
    #[serde(rename = "E")]
    event_ts_ms: Option<i64>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "r")]
    funding_rate: Option<String>,
    #[serde(rename = "T")]
    next_funding_ts_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AsterPremiumIndexItem {
    symbol: String,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: i64,
}

pub fn aster_subscribe_payload(exchange_symbols: &[String], request_id: u64) -> String {
    let mut params: Vec<String> = Vec::with_capacity(exchange_symbols.len());
    for symbol in exchange_symbols {
        let lower = symbol.to_ascii_lowercase();
        params.push(format!("{lower}@bookTicker"));
    }

    json!({
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id,
    })
    .to_string()
}

pub fn parse_book_ticker_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let message: AsterBookTickerMessage = serde_json::from_str(raw).ok()?;
    if message.event_type.as_deref() != Some("bookTicker") {
        return None;
    }

    let exchange_symbol = message.symbol?;
    let normalized_symbol = normalize_base(&exchange_symbol);
    let symbol_base = symbol_map.get(&normalized_symbol)?.clone();

    let bid_px = message.bid_px?.parse::<f64>().ok()?;
    let bid_qty = message.bid_qty?.parse::<f64>().ok()?;
    let ask_px = message.ask_px?.parse::<f64>().ok()?;
    let ask_qty = message.ask_qty?.parse::<f64>().ok()?;

    if bid_px <= 0.0 || ask_px <= 0.0 || bid_qty <= 0.0 || ask_qty <= 0.0 {
        return None;
    }

    let exch_ts_ms = message
        .event_ts_ms
        .or(message.trade_ts_ms)
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Aster,
        symbol_base,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_mark_price_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let message: AsterMarkPriceMessage = serde_json::from_str(raw).ok()?;
    if message.event_type.as_deref() != Some("markPriceUpdate") {
        return None;
    }

    let exchange_symbol = message.symbol?;
    let normalized_symbol = normalize_base(&exchange_symbol);
    let symbol_base = symbol_map.get(&normalized_symbol)?.clone();

    let funding_rate = message.funding_rate?.parse::<f64>().ok()?;

    Some(FundingUpdate {
        exchange: Exchange::Aster,
        symbol_base,
        funding_rate,
        next_funding_ts_ms: message.next_funding_ts_ms,
        recv_ts_ms: message.event_ts_ms.unwrap_or(recv_ts_ms),
        stale_after_ms: None,
    })
}

pub async fn run_aster_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map: HashMap<String, String> = markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Aster)
                .map(|meta| (normalize_base(&meta.exchange_symbol), symbol_base.clone()))
        })
        .collect();
    let mut subscribe_symbols: Vec<String> = symbol_map.keys().cloned().collect();
    subscribe_symbols.sort_unstable();

    if subscribe_symbols.is_empty() {
        tracing::warn!("aster feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            subscriptions = subscribe_symbols.len(),
            "connecting aster WS"
        );

        match connect_async(ws_url).await {
            Ok((stream, _)) => {
                tracing::info!("connected aster WS");
                attempt = 0;

                let (mut write_half, mut read_half) = stream.split();
                let payload = aster_subscribe_payload(&subscribe_symbols, 1);
                if let Err(err) = write_half.send(Message::Text(payload)).await {
                    tracing::warn!(error = %err, "aster subscribe failed");
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                while let Some(message_result) = read_half.next().await {
                    match message_result.context("aster WS read error") {
                        Ok(Message::Text(text)) => {
                            let recv_ts_ms = now_ms();

                            if let Some(update) =
                                parse_book_ticker_message(text.as_ref(), &symbol_map, recv_ts_ms)
                            {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "aster feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                                continue;
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(?frame, "aster WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "aster WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "aster WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

pub async fn run_aster_funding_poller(
    rest_url: &str,
    markets: &SymbolMarkets,
    poll_secs: u64,
    timeout_secs: u64,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    if poll_secs == 0 {
        return Ok(());
    }

    let tracked_symbol_map: HashMap<String, String> = markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Aster)
                .map(|meta| (normalize_base(&meta.exchange_symbol), symbol_base.clone()))
        })
        .collect();

    if tracked_symbol_map.is_empty() {
        tracing::warn!("aster funding poller skipped: no matched symbols");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build aster funding HTTP client")?;

    let baseline_ms = funding_baseline_poll_ms(poll_secs);
    let mut cache: HashMap<String, f64> = HashMap::new();

    loop {
        let poll_started_ms = now_ms();

        match client
            .get(rest_url)
            .send()
            .await
            .and_then(|resp| resp.error_for_status())
        {
            Ok(resp) => match resp.json::<Vec<AsterPremiumIndexItem>>().await {
                Ok(items) => {
                    let mut min_to_next_funding_ms: Option<i64> = None;
                    let mut parsed: Vec<(String, f64, i64)> = Vec::new();
                    let recv_ts_ms = now_ms();

                    for item in items {
                        let normalized = normalize_base(&item.symbol);
                        let Some(symbol_base) = tracked_symbol_map.get(&normalized).cloned() else {
                            continue;
                        };

                        let funding_rate = match item.last_funding_rate.parse::<f64>() {
                            Ok(rate) => rate,
                            Err(_) => continue,
                        };

                        let to_next = (item.next_funding_time - recv_ts_ms).max(0);
                        min_to_next_funding_ms = Some(match min_to_next_funding_ms {
                            Some(current_min) => current_min.min(to_next),
                            None => to_next,
                        });

                        parsed.push((symbol_base, funding_rate, item.next_funding_time));
                    }

                    let adaptive_base_ms =
                        aster_adaptive_poll_ms(baseline_ms, min_to_next_funding_ms);
                    let planned_sleep_ms = jittered_poll_ms(adaptive_base_ms, poll_started_ms);
                    let ttl_ms = funding_cache_ttl_ms(planned_sleep_ms);

                    for (symbol_base, funding_rate, next_funding_time) in parsed {
                        let should_emit = cache
                            .get(&symbol_base)
                            .map(|prev| (funding_rate - prev).abs() > FUNDING_CHANGE_EPSILON)
                            .unwrap_or(true);

                        if !should_emit {
                            continue;
                        }

                        cache.insert(symbol_base.clone(), funding_rate);
                        let update = FundingUpdate {
                            exchange: Exchange::Aster,
                            symbol_base,
                            funding_rate,
                            next_funding_ts_ms: Some(next_funding_time),
                            recv_ts_ms,
                            stale_after_ms: Some(ttl_ms),
                        };

                        if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
                            tracing::info!(
                                "aster funding poller stopped: market event channel closed"
                            );
                            return Ok(());
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "aster funding poll parse failed");
                    let planned_sleep_ms = jittered_poll_ms(baseline_ms, poll_started_ms);
                    tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
                }
            },
            Err(err) => {
                tracing::warn!(error = %err, "aster funding poll request failed");
                let planned_sleep_ms = jittered_poll_ms(baseline_ms, poll_started_ms);
                tokio::time::sleep(Duration::from_millis(planned_sleep_ms)).await;
            }
        }
    }
}
