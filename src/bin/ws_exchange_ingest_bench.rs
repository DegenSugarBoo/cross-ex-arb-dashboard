use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use cross_ex_arb::feeds::aster::{parse_book_ticker_message, parse_mark_price_message};
use cross_ex_arb::feeds::edge_x::{
    parse_edge_x_depth_message, parse_edge_x_ticker_funding_message,
};
use cross_ex_arb::feeds::extended::{
    parse_extended_funding_message, parse_extended_orderbook_message,
};
use cross_ex_arb::feeds::hyperliquid::parse_hyperliquid_bbo_message;
use cross_ex_arb::feeds::lighter::{parse_market_stats_message, parse_ticker_message};
use cross_ex_arb::ws_fast::connect_fast_websocket;
use fastwebsockets::OpCode;
use futures_util::SinkExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const WARMUP_FRAMES: usize = 20_000;
const MEASURE_FRAMES: usize = 150_000;
const ROUNDS: usize = 3;

const ASTER_BOOK: &str = r#"{"e":"bookTicker","s":"BTCUSDT","b":"101.0","B":"1.50","a":"101.2","A":"0.80","T":1700000001111,"E":1700000002222}"#;
const ASTER_MARK: &str = r#"{"e":"markPriceUpdate","E":1700000010000,"s":"BTCUSDT","r":"-0.00001411","T":1700006400000}"#;
const LIGHTER_TICKER: &str = r#"{"channel":"ticker:1","ticker":{"s":"BTC","a":{"price":"100.5","size":"1.25"},"b":{"price":"100.0","size":"2.50"}},"timestamp":1700000000000,"type":"update/ticker"}"#;
const LIGHTER_STATS: &str = r#"{"channel":"market_stats:1","market_stats":{"symbol":"BTC","market_id":1,"current_funding_rate":"0.0002","funding_timestamp":1700000400000},"timestamp":1700000000000,"type":"update/market_stats"}"#;
const EDGE_X_DEPTH: &str = r#"{"type":"update/depth","channel":"depth.101.15","content":{"data":[{"contractId":"101","bids":[["100.5","2.0"]],"asks":[["100.7","1.5"]],"timestamp":1700000020000}]}}"#;
const EDGE_X_TICKER: &str = r#"{"type":"quote-event","channel":"ticker.101","content":{"channel":"ticker.101","data":[{"contractId":"101","fundingRate":"-0.00002431","fundingTime":"1773532800000","nextFundingTime":"1773547200000"}]}}"#;
const EXTENDED_BOOK: &str = r#"{"timestamp":1700000000123,"data":{"m":"BTC-USD","b":[["100.1","4.0"]],"a":[["100.3","2.5"]]}}"#;
const EXTENDED_FUNDING: &str = r#"{"timestamp":1700000010000,"data":{"m":"BTC-USD","fundingRate":"-0.00012","nextFundingTime":1700003600000}}"#;
const HYPERLIQUID_BBO: &str = r#"{"channel":"bbo","data":{"coin":"BTC","time":1700000030000,"bbo":[{"px":"100.9","sz":"3.0","n":3},{"px":"101.0","sz":"2.0","n":2}]}}"#;

struct BenchContext {
    aster_symbol_map: HashMap<String, String>,
    lighter_market_map: HashMap<u32, String>,
    edge_market_map: HashMap<u32, String>,
    extended_symbol_map: HashMap<String, String>,
    hyper_symbol_map: HashMap<String, String>,
}

#[derive(Clone, Copy)]
enum Scenario {
    AsterBookTicker,
    AsterMarkPrice,
    LighterTicker,
    LighterFunding,
    EdgeXDepth,
    EdgeXTickerFunding,
    ExtendedOrderbook,
    ExtendedFunding,
    HyperliquidBbo,
}

impl Scenario {
    fn all() -> [Self; 9] {
        [
            Self::AsterBookTicker,
            Self::AsterMarkPrice,
            Self::LighterTicker,
            Self::LighterFunding,
            Self::EdgeXDepth,
            Self::EdgeXTickerFunding,
            Self::ExtendedOrderbook,
            Self::ExtendedFunding,
            Self::HyperliquidBbo,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::AsterBookTicker => "aster ws ingest bookTicker",
            Self::AsterMarkPrice => "aster ws ingest markPrice",
            Self::LighterTicker => "lighter ws ingest ticker",
            Self::LighterFunding => "lighter ws ingest marketStats",
            Self::EdgeXDepth => "edgeX ws ingest depth",
            Self::EdgeXTickerFunding => "edgeX ws ingest tickerFunding",
            Self::ExtendedOrderbook => "extended ws ingest orderbook",
            Self::ExtendedFunding => "extended ws ingest funding",
            Self::HyperliquidBbo => "hyperliquid ws ingest bbo",
        }
    }

    fn payload(self) -> &'static str {
        match self {
            Self::AsterBookTicker => ASTER_BOOK,
            Self::AsterMarkPrice => ASTER_MARK,
            Self::LighterTicker => LIGHTER_TICKER,
            Self::LighterFunding => LIGHTER_STATS,
            Self::EdgeXDepth => EDGE_X_DEPTH,
            Self::EdgeXTickerFunding => EDGE_X_TICKER,
            Self::ExtendedOrderbook => EXTENDED_BOOK,
            Self::ExtendedFunding => EXTENDED_FUNDING,
            Self::HyperliquidBbo => HYPERLIQUID_BBO,
        }
    }

    fn parse(self, raw: &str, ctx: &BenchContext, recv_ts_ms: i64) -> bool {
        match self {
            Self::AsterBookTicker => {
                parse_book_ticker_message(raw, &ctx.aster_symbol_map, recv_ts_ms).is_some()
            }
            Self::AsterMarkPrice => {
                parse_mark_price_message(raw, &ctx.aster_symbol_map, recv_ts_ms).is_some()
            }
            Self::LighterTicker => {
                parse_ticker_message(raw, &ctx.lighter_market_map, recv_ts_ms).is_some()
            }
            Self::LighterFunding => {
                parse_market_stats_message(raw, &ctx.lighter_market_map, recv_ts_ms).is_some()
            }
            Self::EdgeXDepth => {
                parse_edge_x_depth_message(raw, &ctx.edge_market_map, recv_ts_ms).is_some()
            }
            Self::EdgeXTickerFunding => {
                parse_edge_x_ticker_funding_message(raw, &ctx.edge_market_map, recv_ts_ms).is_some()
            }
            Self::ExtendedOrderbook => {
                parse_extended_orderbook_message(raw, &ctx.extended_symbol_map, recv_ts_ms)
                    .is_some()
            }
            Self::ExtendedFunding => {
                parse_extended_funding_message(raw, &ctx.extended_symbol_map, recv_ts_ms).is_some()
            }
            Self::HyperliquidBbo => {
                parse_hyperliquid_bbo_message(raw, &ctx.hyper_symbol_map, recv_ts_ms).is_some()
            }
        }
    }
}

async fn spawn_text_server(
    frames: usize,
    payload: &'static str,
) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind local benchmark listener")?;
    let addr = listener
        .local_addr()
        .context("failed to read local benchmark listener address")?;
    let payload_message = Message::Text(payload.to_owned().into());
    let url = format!("ws://{addr}/stream");

    let handle = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .context("benchmark server failed to accept client")?;
        let mut ws = accept_async(stream)
            .await
            .context("benchmark server websocket handshake failed")?;

        for _ in 0..frames {
            ws.send(payload_message.clone())
                .await
                .context("benchmark server failed to send websocket frame")?;
        }

        let _ = ws.close(None).await;
        Ok(())
    });

    Ok((url, handle))
}

async fn run_round(
    scenario: Scenario,
    frames: usize,
    ctx: &BenchContext,
) -> anyhow::Result<Duration> {
    let (url, server_handle) = spawn_text_server(frames, scenario.payload()).await?;
    let mut ws = connect_fast_websocket(&url)
        .await
        .with_context(|| format!("failed to connect benchmark client for {}", scenario.name()))?;

    let started = Instant::now();
    let mut received = 0usize;
    while received < frames {
        let frame = ws
            .read_frame()
            .await
            .with_context(|| format!("read frame failed for {}", scenario.name()))?;
        if frame.opcode != OpCode::Text {
            if frame.opcode == OpCode::Close {
                bail!("stream closed early for {}", scenario.name());
            }
            continue;
        }
        let raw = std::str::from_utf8(&frame.payload)
            .with_context(|| format!("invalid UTF-8 text frame for {}", scenario.name()))?;
        let recv_ts_ms = 1_700_000_000_000i64 + received as i64;
        if !scenario.parse(raw, ctx, recv_ts_ms) {
            bail!("parse failed for {}", scenario.name());
        }
        received = received.saturating_add(1);
    }
    let elapsed = started.elapsed();

    server_handle
        .await
        .with_context(|| format!("server task join failed for {}", scenario.name()))??;

    Ok(elapsed)
}

fn median_duration(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn print_row(name: &str, frames: usize, elapsed: Duration) {
    let ns_per_msg = elapsed.as_nanos() as f64 / frames as f64;
    let msg_per_s = frames as f64 / elapsed.as_secs_f64();
    println!(
        "{name:<34} {:>10.0} msg/s {:>9.1} ns/msg",
        msg_per_s, ns_per_msg
    );
}

fn bench_context() -> BenchContext {
    BenchContext {
        aster_symbol_map: HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]),
        lighter_market_map: HashMap::from([(1_u32, "BTC".to_owned())]),
        edge_market_map: HashMap::from([(101_u32, "BTC".to_owned())]),
        extended_symbol_map: HashMap::from([("BTC-USD".to_owned(), "BTC".to_owned())]),
        hyper_symbol_map: HashMap::from([("BTC".to_owned(), "BTC".to_owned())]),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!(
        "ws exchange ingest benchmark: warmup={} measure={} rounds={} (transport+parse)",
        WARMUP_FRAMES, MEASURE_FRAMES, ROUNDS
    );
    let ctx = bench_context();

    for scenario in Scenario::all() {
        let _ = run_round(scenario, WARMUP_FRAMES, &ctx).await?;
        let mut samples = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            samples.push(run_round(scenario, MEASURE_FRAMES, &ctx).await?);
        }
        print_row(scenario.name(), MEASURE_FRAMES, median_duration(samples));
    }

    Ok(())
}
