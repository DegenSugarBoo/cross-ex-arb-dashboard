use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

pub struct GrvtFeed;

impl ExchangeFeed for GrvtFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Grvt
    }

    fn spawn(
        &self,
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let ws_url = config.grvt_ws_url.clone();
        let feed_markets = Arc::clone(&markets);
        let task = runtime.spawn(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("grvt feed task cancelled");
                }
                result = run_grvt_feed(&ws_url, &feed_markets, event_tx) => {
                    if let Err(err) = result {
                        tracing::error!(error = %err, "grvt feed task terminated");
                    }
                }
            }
        });
        vec![task]
    }
}

fn normalize_exchange_ts_ms(exch_ts: i64) -> i64 {
    let abs = exch_ts.abs();
    if abs >= 100_000_000_000_000_000 {
        exch_ts / 1_000_000
    } else if abs >= 100_000_000_000_000 {
        exch_ts / 1_000
    } else if abs >= 100_000_000_000 {
        exch_ts
    } else if abs >= 100_000_000 {
        exch_ts * 1_000
    } else {
        exch_ts
    }
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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BorrowedNumOrStr<'a> {
    Number(f64),
    String(&'a str),
}

fn de_opt_f64_from_num_or_str<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<BorrowedNumOrStr<'de>>::deserialize(deserializer)?;
    Ok(value.and_then(|entry| match entry {
        BorrowedNumOrStr::Number(num) => Some(num),
        BorrowedNumOrStr::String(text) => text.parse::<f64>().ok(),
    }))
}

fn de_opt_i64_from_num_or_str<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<BorrowedNumOrStr<'de>>::deserialize(deserializer)?;
    Ok(value.and_then(|entry| match entry {
        BorrowedNumOrStr::Number(num) => Some(num as i64),
        BorrowedNumOrStr::String(text) => text.parse::<i64>().ok(),
    }))
}

#[derive(Debug, Default, Deserialize)]
struct GrvtTickerFastFields {
    #[serde(
        default,
        rename = "bb",
        alias = "bestBidPrice",
        alias = "best_bid_price",
        deserialize_with = "de_opt_f64_from_num_or_str"
    )]
    bid_px: Option<f64>,
    #[serde(
        default,
        rename = "bb1",
        alias = "bestBidSize",
        alias = "best_bid_size",
        deserialize_with = "de_opt_f64_from_num_or_str"
    )]
    bid_qty: Option<f64>,
    #[serde(
        default,
        rename = "ba",
        alias = "bestAskPrice",
        alias = "best_ask_price",
        deserialize_with = "de_opt_f64_from_num_or_str"
    )]
    ask_px: Option<f64>,
    #[serde(
        default,
        rename = "ba1",
        alias = "bestAskSize",
        alias = "best_ask_size",
        deserialize_with = "de_opt_f64_from_num_or_str"
    )]
    ask_qty: Option<f64>,
    #[serde(
        default,
        rename = "fr2",
        alias = "fundingRate",
        alias = "funding_rate",
        alias = "fundingRate8hCurr",
        alias = "funding_rate_8h_curr",
        deserialize_with = "de_opt_f64_from_num_or_str"
    )]
    funding_rate: Option<f64>,
    #[serde(
        default,
        rename = "nf",
        alias = "nextFundingTime",
        alias = "next_funding_time",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    next_funding_ts: Option<i64>,
    #[serde(
        default,
        rename = "et",
        alias = "eventTime",
        alias = "event_time",
        alias = "timestamp",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    event_ts: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
struct GrvtTickerFastData<'a> {
    #[serde(
        default,
        borrow,
        rename = "s",
        alias = "symbol",
        alias = "instrument",
        alias = "market"
    )]
    symbol: Option<&'a str>,
    #[serde(
        default,
        borrow,
        rename = "selector",
        alias = "topic",
        alias = "stream",
        alias = "channel"
    )]
    selector: Option<&'a str>,
    #[serde(default)]
    f: Option<GrvtTickerFastFields>,
    #[serde(default)]
    feed: Option<GrvtTickerFastFields>,
    #[serde(default)]
    data: Option<GrvtTickerFastFields>,
    #[serde(
        default,
        rename = "et",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    event_ts: Option<i64>,
}

impl<'a> GrvtTickerFastData<'a> {
    fn fields(&self) -> Option<&GrvtTickerFastFields> {
        self.f
            .as_ref()
            .or(self.feed.as_ref())
            .or(self.data.as_ref())
    }
}

#[derive(Debug, Deserialize)]
struct GrvtTickerFastRoot<'a> {
    #[serde(
        default,
        borrow,
        rename = "s",
        alias = "stream",
        alias = "topic",
        alias = "channel"
    )]
    stream: Option<&'a str>,
    #[serde(default, borrow, rename = "selector")]
    selector: Option<&'a str>,
    #[serde(default)]
    d: Option<GrvtTickerFastData<'a>>,
    #[serde(default)]
    data: Option<GrvtTickerFastData<'a>>,
    #[serde(default)]
    payload: Option<GrvtTickerFastData<'a>>,
    #[serde(default)]
    f: Option<GrvtTickerFastFields>,
    #[serde(default)]
    feed: Option<GrvtTickerFastFields>,
    #[serde(
        default,
        rename = "et",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    event_ts: Option<i64>,
}

fn is_grvt_ticker_stream(value: &str) -> bool {
    value == "v1.ticker.d" || value.starts_with("v1.ticker.d.") || value.starts_with("v1.ticker.d:")
}

fn symbol_from_selector(selector: &str) -> Option<&str> {
    let trimmed = selector
        .strip_prefix("v1.ticker.d.")
        .or_else(|| selector.strip_prefix("v1.ticker.d:"))
        .unwrap_or(selector);
    let instrument = trimmed
        .split_once('@')
        .map(|(instrument, _)| instrument)
        .unwrap_or(trimmed);
    (!instrument.is_empty()).then_some(instrument)
}

pub fn grvt_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (symbol_base, per_exchange) in markets {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Grvt)
        {
            out.insert(meta.exchange_symbol.clone(), symbol_base.clone());
            out.insert(normalize_base(&meta.exchange_symbol), symbol_base.clone());
        }
    }
    out
}

fn resolve_symbol_base(
    symbol_map: &HashMap<String, String>,
    exchange_symbol: &str,
) -> Option<String> {
    if let Some(symbol_base) = symbol_map.get(exchange_symbol) {
        return Some(symbol_base.clone());
    }

    let normalized = normalize_base(exchange_symbol);
    if let Some(symbol_base) = symbol_map.get(&normalized) {
        return Some(symbol_base.clone());
    }

    if let Some(from_selector) = symbol_from_selector(exchange_symbol) {
        if let Some(symbol_base) = symbol_map.get(from_selector) {
            return Some(symbol_base.clone());
        }
        let normalized_selector = normalize_base(from_selector);
        if let Some(symbol_base) = symbol_map.get(&normalized_selector) {
            return Some(symbol_base.clone());
        }
    }

    if let Some((_, tail)) = exchange_symbol.rsplit_once(':') {
        return resolve_symbol_base(symbol_map, tail);
    }

    if let Some((_, tail)) = exchange_symbol.rsplit_once('.') {
        return resolve_symbol_base(symbol_map, tail);
    }

    None
}

fn build_grvt_events(
    symbol_map: &HashMap<String, String>,
    symbol_hint: &str,
    bid_px: Option<f64>,
    bid_qty: Option<f64>,
    ask_px: Option<f64>,
    ask_qty: Option<f64>,
    funding_rate: Option<f64>,
    next_funding_ts: Option<i64>,
    event_ts: i64,
    recv_ts_ms: i64,
) -> Option<(Option<QuoteUpdate>, Option<FundingUpdate>)> {
    let symbol_base = resolve_symbol_base(symbol_map, symbol_hint)?;

    let quote = match (bid_px, bid_qty, ask_px, ask_qty) {
        (Some(bid_px), Some(bid_qty), Some(ask_px), Some(ask_qty))
            if bid_px > 0.0 && ask_px > 0.0 && bid_qty > 0.0 && ask_qty > 0.0 =>
        {
            Some(QuoteUpdate {
                exchange: Exchange::Grvt,
                symbol_base: symbol_base.clone().into(),
                bid_px,
                bid_qty,
                ask_px,
                ask_qty,
                exch_ts_ms: event_ts,
                recv_ts_ms,
            })
        }
        _ => None,
    };

    let funding = funding_rate.map(|funding_rate| FundingUpdate {
        exchange: Exchange::Grvt,
        symbol_base: symbol_base.into(),
        funding_rate,
        next_funding_ts_ms: next_funding_ts,
        recv_ts_ms: event_ts,
        stale_after_ms: None,
    });

    if quote.is_none() && funding.is_none() {
        return None;
    }

    Some((quote, funding))
}

pub fn parse_grvt_ticker_message_fast(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<(Option<QuoteUpdate>, Option<FundingUpdate>)> {
    let root: GrvtTickerFastRoot<'_> = sonic_rs::from_str(raw).ok()?;
    let payload = root
        .d
        .as_ref()
        .or(root.data.as_ref())
        .or(root.payload.as_ref());
    let stream_hint = root
        .stream
        .or(root.selector)
        .or_else(|| payload.and_then(|payload| payload.selector));

    if let Some(stream) = stream_hint {
        if !is_grvt_ticker_stream(stream) {
            return None;
        }
    } else {
        return None;
    }

    let symbol_hint = payload
        .and_then(|payload| payload.symbol)
        .or_else(|| payload.and_then(|payload| payload.selector.and_then(symbol_from_selector)))
        .or_else(|| root.selector.and_then(symbol_from_selector))?;

    let fields = payload
        .and_then(GrvtTickerFastData::fields)
        .or(root.f.as_ref())
        .or(root.feed.as_ref())?;

    let event_ts = fields
        .event_ts
        .or_else(|| payload.and_then(|payload| payload.event_ts))
        .or(root.event_ts)
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);

    let next_funding_ts = fields.next_funding_ts.map(normalize_exchange_ts_ms);
    build_grvt_events(
        symbol_map,
        symbol_hint,
        fields.bid_px,
        fields.bid_qty,
        fields.ask_px,
        fields.ask_qty,
        fields.funding_rate,
        next_funding_ts,
        event_ts,
        recv_ts_ms,
    )
}

pub fn parse_grvt_ticker_message_fallback(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<(Option<QuoteUpdate>, Option<FundingUpdate>)> {
    let root: Value = sonic_rs::from_str(raw).ok()?;

    let stream_hint = root
        .get("stream")
        .or_else(|| root.get("topic"))
        .or_else(|| root.get("channel"))
        .or_else(|| root.get("s"))
        .and_then(Value::as_str)
        .or_else(|| root.get("selector").and_then(Value::as_str));
    let payload = root.get("d").or_else(|| root.get("payload"));
    let direct_fields = root.get("f").or_else(|| root.get("feed"));
    let selector_hint = payload
        .and_then(|payload| payload.get("selector"))
        .or_else(|| payload.and_then(|payload| payload.get("topic")))
        .or_else(|| payload.and_then(|payload| payload.get("stream")))
        .or_else(|| payload.and_then(|payload| payload.get("channel")))
        .and_then(Value::as_str)
        .or(stream_hint);

    if let Some(stream) = selector_hint {
        if !is_grvt_ticker_stream(stream) {
            return None;
        }
    } else {
        return None;
    }

    let symbol_hint = payload
        .and_then(|payload| payload.get("s"))
        .or_else(|| payload.and_then(|payload| payload.get("symbol")))
        .or_else(|| payload.and_then(|payload| payload.get("instrument")))
        .or_else(|| payload.and_then(|payload| payload.get("market")))
        .and_then(Value::as_str)
        .or_else(|| selector_hint.and_then(symbol_from_selector))?;

    let fields = payload
        .and_then(|payload| payload.get("f"))
        .or_else(|| payload.and_then(|payload| payload.get("feed")))
        .or_else(|| payload.and_then(|payload| payload.get("data")))
        .or(direct_fields)
        .or(payload)?;

    let bid_px = fields
        .get("bb")
        .or_else(|| fields.get("bestBidPrice"))
        .or_else(|| fields.get("best_bid_price"))
        .and_then(as_f64);
    let bid_qty = fields
        .get("bb1")
        .or_else(|| fields.get("bestBidSize"))
        .or_else(|| fields.get("best_bid_size"))
        .and_then(as_f64);
    let ask_px = fields
        .get("ba")
        .or_else(|| fields.get("bestAskPrice"))
        .or_else(|| fields.get("best_ask_price"))
        .and_then(as_f64);
    let ask_qty = fields
        .get("ba1")
        .or_else(|| fields.get("bestAskSize"))
        .or_else(|| fields.get("best_ask_size"))
        .and_then(as_f64);
    let funding_rate = fields
        .get("fr2")
        .or_else(|| fields.get("fundingRate"))
        .or_else(|| fields.get("funding_rate"))
        .or_else(|| fields.get("fundingRate8hCurr"))
        .or_else(|| fields.get("funding_rate_8h_curr"))
        .and_then(as_f64);

    let event_ts = fields
        .get("et")
        .or_else(|| fields.get("eventTime"))
        .or_else(|| fields.get("event_time"))
        .or_else(|| payload.and_then(|payload| payload.get("et")))
        .or_else(|| root.get("et"))
        .or_else(|| root.get("timestamp"))
        .and_then(as_i64)
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);

    let next_funding_ts = fields
        .get("nf")
        .or_else(|| fields.get("nextFundingTime"))
        .or_else(|| fields.get("next_funding_time"))
        .and_then(as_i64)
        .map(normalize_exchange_ts_ms);

    build_grvt_events(
        symbol_map,
        symbol_hint,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        funding_rate,
        next_funding_ts,
        event_ts,
        recv_ts_ms,
    )
}

pub fn parse_grvt_ticker_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<(Option<QuoteUpdate>, Option<FundingUpdate>)> {
    parse_grvt_ticker_message_fast(raw, symbol_map, recv_ts_ms)
        .or_else(|| parse_grvt_ticker_message_fallback(raw, symbol_map, recv_ts_ms))
}

fn grvt_selectors(markets: &SymbolMarkets) -> Vec<String> {
    let mut selectors = Vec::new();
    for per_exchange in markets.values() {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Grvt)
        {
            selectors.push(format!("{}@500", meta.exchange_symbol));
        }
    }
    selectors.sort_unstable();
    selectors.dedup();
    selectors
}

fn grvt_subscribe_payload(selector: &str, request_id: u64) -> String {
    format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{{\"stream\":\"v1.ticker.d\",\"selectors\":[\"{selector}\"]}},\"id\":{request_id}}}"
    )
}

pub async fn run_grvt_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = grvt_symbol_map(markets);
    let selectors = grvt_selectors(markets);
    if selectors.is_empty() {
        tracing::warn!("grvt feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;
    let mut funding_cache: HashMap<String, f64> = HashMap::new();

    loop {
        tracing::info!(
            attempt,
            subscriptions = selectors.len(),
            "connecting grvt WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!("connected grvt WS");
                attempt = 0;

                let mut subscribe_failed = false;
                for (idx, selector) in selectors.iter().enumerate() {
                    let payload = grvt_subscribe_payload(selector, idx as u64 + 1);
                    if let Err(err) = ws
                        .write_frame(Frame::text(payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(error = %err, selector, "grvt subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }
                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, "grvt subscribe flush failed");
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
                    match ws.read_frame().await.context("grvt WS read error") {
                        Ok(frame) if frame.opcode == OpCode::Text => {
                            let raw = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    tracing::debug!(error = %err, "grvt WS text frame was not valid UTF-8");
                                    continue;
                                }
                            };

                            if !raw.contains("v1.ticker.d") {
                                continue;
                            }

                            let recv_ts_ms = now_ms();
                            if let Some((quote, funding)) =
                                parse_grvt_ticker_message(raw, &symbol_map, recv_ts_ms)
                            {
                                if let Some(quote) = quote {
                                    if event_tx.send(MarketEvent::Quote(quote)).await.is_err() {
                                        tracing::info!(
                                            "grvt feed stopped: market event channel closed"
                                        );
                                        return Ok(());
                                    }
                                }

                                if let Some(funding) = funding {
                                    if funding.funding_rate.abs() > 0.5 {
                                        tracing::warn!(
                                            rate = funding.funding_rate,
                                            symbol = %funding.symbol_base,
                                            "grvt funding rate out of expected bounds; skipping"
                                        );
                                        continue;
                                    }

                                    let symbol = funding.symbol_base.to_string();
                                    let should_emit = funding_cache
                                        .get(&symbol)
                                        .map(|prev| {
                                            (funding.funding_rate - prev).abs()
                                                > FUNDING_CHANGE_EPSILON
                                        })
                                        .unwrap_or(true);

                                    if should_emit {
                                        funding_cache.insert(symbol, funding.funding_rate);
                                        if event_tx
                                            .send(MarketEvent::Funding(funding))
                                            .await
                                            .is_err()
                                        {
                                            tracing::info!(
                                                "grvt feed stopped: market event channel closed"
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            tracing::warn!("grvt WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "grvt WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "grvt WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{grvt_subscribe_payload, symbol_from_selector};

    #[test]
    fn grvt_selector_parser_accepts_current_live_format() {
        assert_eq!(
            symbol_from_selector("BTC_USDT_Perp@500"),
            Some("BTC_USDT_Perp")
        );
        assert_eq!(
            symbol_from_selector("v1.ticker.d.BTC_USDT_Perp"),
            Some("BTC_USDT_Perp")
        );
    }

    #[test]
    fn grvt_subscribe_payload_uses_json_rpc_2() {
        assert_eq!(
            grvt_subscribe_payload("BTC_USDT_Perp@500", 7),
            "{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{\"stream\":\"v1.ticker.d\",\"selectors\":[\"BTC_USDT_Perp@500\"]},\"id\":7}"
        );
    }
}
