use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use cross_ex_arb::config::{AppConfig, CollectorCompression};
use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::engine::EngineState;
use cross_ex_arb::feeds::apex::{
    parse_apex_depth_event, parse_apex_depth_event_fallback, parse_apex_depth_event_fast,
    parse_apex_instrument_info_message,
};
use cross_ex_arb::feeds::aster::{parse_book_ticker_message, parse_mark_price_message};
use cross_ex_arb::feeds::edge_x::{
    parse_edge_x_depth_message, parse_edge_x_depth_message_fallback,
    parse_edge_x_depth_message_fast, parse_edge_x_ticker_funding_message,
    parse_edge_x_ticker_funding_message_fallback_value, parse_edge_x_ticker_funding_message_fast,
};
use cross_ex_arb::feeds::extended::{
    parse_extended_funding_message, parse_extended_orderbook_message,
};
use cross_ex_arb::feeds::grvt::parse_grvt_ticker_message;
use cross_ex_arb::feeds::hyperliquid::{
    parse_hyperliquid_bbo_message, parse_hyperliquid_bbo_message_fallback,
    parse_hyperliquid_bbo_message_fast, parse_hyperliquid_l2book_message,
};
use cross_ex_arb::feeds::lighter::{parse_market_stats_message, parse_ticker_message};
use cross_ex_arb::model::{
    Exchange, MarketEvent, MarketMeta, NO_ROUTE_SELECTED, QuoteUpdate, RouteHistorySnapshot,
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const LIGHTER_TICKER: &str = r#"{"channel":"ticker:1","ticker":{"s":"BTC","a":{"price":"100.5","size":"1.25"},"b":{"price":"100.0","size":"2.50"}},"timestamp":1700000000000,"type":"update/ticker"}"#;
const LIGHTER_STATS: &str = r#"{
    "channel":"market_stats:1",
    "market_stats":{
        "symbol":"BTC",
        "market_id":1,
        "current_funding_rate":"0.0002",
        "funding_timestamp":1700000400000
    },
    "timestamp":1700000000000,
    "type":"update/market_stats"
}"#;
const ASTER_BOOK: &str = r#"{"e":"bookTicker","s":"BTCUSDT","b":"101.0","B":"1.50","a":"101.2","A":"0.80","T":1700000001111,"E":1700000002222}"#;
const ASTER_MARK: &str = r#"{"e":"markPriceUpdate","E":1700000010000,"s":"BTCUSDT","r":"-0.00001411","T":1700006400000}"#;
const EXTENDED_BOOK: &str = r#"{
    "timestamp":1700000000123,
    "data":{
        "m":"BTC-USD",
        "b":[["100.1","4.0"]],
        "a":[["100.3","2.5"]]
    }
}"#;
const EXTENDED_FUNDING: &str = r#"{
    "timestamp":1700000010000,
    "data":{
        "m":"BTC-USD",
        "fundingRate":"-0.00012",
        "nextFundingTime":1700003600000
    }
}"#;
const EDGE_X_DEPTH: &str = r#"{
    "type":"update/depth",
    "channel":"depth.101.15",
    "content":{
        "data":[{
            "contractId":"101",
            "bids":[["100.5","2.0"]],
            "asks":[["100.7","1.5"]],
            "timestamp":1700000020000
        }]
    }
}"#;
const EDGE_X_TICKER: &str = r#"{
    "type":"quote-event",
    "channel":"ticker.101",
    "content":{
        "channel":"ticker.101",
        "data":[
            {
                "contractId":"101",
                "fundingRate":"-0.00002431",
                "fundingTime":"1773532800000",
                "nextFundingTime":"1773547200000"
            }
        ]
    }
}"#;
const HYPERLIQUID_BBO: &str = r#"{
    "channel":"bbo",
    "data":{
        "coin":"BTC",
        "time":1700000030000,
        "bbo":[
            {"px":"100.9","sz":"3.0","n":3},
            {"px":"101.0","sz":"2.0","n":2}
        ]
    }
}"#;
const HYPERLIQUID_L2: &str = r#"{
    "channel":"l2Book",
    "data":{
        "coin":"BTC",
        "time":1700000030000,
        "levels":[
            [{"px":"100.9","sz":"3.0","n":3}],
            [{"px":"101.0","sz":"2.0","n":2}]
        ]
    }
}"#;
const GRVT_TICKER: &str = r#"{
    "stream":"v1.ticker.d",
    "d":{
        "s":"BTC_USDT_Perp",
        "f":{
            "bb":"100.2",
            "bb1":"5.1",
            "ba":"100.4",
            "ba1":"4.2",
            "fr2":"0.00012",
            "nf":"1700003600000000000",
            "et":"1700000000123456789"
        }
    }
}"#;
const APEX_DEPTH: &str = r#"{
    "topic":"orderBook200.H.BTCUSDT",
    "type":"snapshot",
    "data":{
        "s":"BTCUSDT",
        "b":[["100.5","2.0"],["100.4","3.0"]],
        "a":[["100.7","1.5"],["100.8","1.1"]],
        "u":"1001",
        "ts":"1700000040000"
    }
}"#;
const APEX_FUNDING: &str = r#"{
    "topic":"instrumentInfo.H.BTCUSDT",
    "data":{
        "s":"BTCUSDT",
        "fundingRate":"-0.000031",
        "nextFundingTime":"1700006400000",
        "ts":"1700000041000"
    }
}"#;

fn print_parse_result(name: &str, iterations: usize, elapsed: std::time::Duration, parsed: usize) {
    let ns_per = elapsed.as_nanos() as f64 / iterations as f64;
    let per_sec = iterations as f64 / elapsed.as_secs_f64();
    println!(
        "{name:<34} {:>10.0} msg/s  {:>9.1} ns/msg  parsed={parsed}",
        per_sec, ns_per
    );
}

fn bench_parse<F>(name: &str, iterations: usize, mut f: F)
where
    F: FnMut(i64) -> bool,
{
    let start = Instant::now();
    let mut parsed = 0usize;
    for idx in 0..iterations {
        let recv_ts_ms = 1_700_000_000_000_i64 + idx as i64;
        if black_box(f(recv_ts_ms)) {
            parsed = parsed.saturating_add(1);
        }
    }
    print_parse_result(name, iterations, start.elapsed(), parsed);
}

fn make_markets(symbol_count: usize) -> SymbolMarkets {
    let mut markets = SymbolMarkets::new();
    for idx in 0..symbol_count {
        let symbol = format!("COIN{idx}");
        markets.insert(
            symbol.clone(),
            vec![
                MarketMeta {
                    exchange: Exchange::Lighter,
                    symbol_base: symbol.clone(),
                    exchange_symbol: symbol.clone(),
                    market_id: Some((idx as u32) * 10 + 1),
                    taker_fee_pct: 0.03,
                    maker_fee_pct: 0.0,
                },
                MarketMeta {
                    exchange: Exchange::Aster,
                    symbol_base: symbol.clone(),
                    exchange_symbol: format!("{symbol}USDT"),
                    market_id: None,
                    taker_fee_pct: 0.04,
                    maker_fee_pct: 0.005,
                },
                MarketMeta {
                    exchange: Exchange::Extended,
                    symbol_base: symbol.clone(),
                    exchange_symbol: format!("{symbol}-USD"),
                    market_id: None,
                    taker_fee_pct: 0.025,
                    maker_fee_pct: 0.002,
                },
                MarketMeta {
                    exchange: Exchange::EdgeX,
                    symbol_base: symbol.clone(),
                    exchange_symbol: format!("{symbol}USDT"),
                    market_id: Some((idx as u32) * 10 + 2),
                    taker_fee_pct: 0.038,
                    maker_fee_pct: 0.005,
                },
                MarketMeta {
                    exchange: Exchange::Hyperliquid,
                    symbol_base: symbol.clone(),
                    exchange_symbol: symbol.clone(),
                    market_id: None,
                    taker_fee_pct: 0.045,
                    maker_fee_pct: 0.0,
                },
                MarketMeta {
                    exchange: Exchange::Grvt,
                    symbol_base: symbol.clone(),
                    exchange_symbol: format!("{symbol}_USDT_Perp"),
                    market_id: None,
                    taker_fee_pct: 0.045,
                    maker_fee_pct: 0.0,
                },
                MarketMeta {
                    exchange: Exchange::ApeX,
                    symbol_base: symbol.clone(),
                    exchange_symbol: format!("{symbol}USDT"),
                    market_id: None,
                    taker_fee_pct: 0.05,
                    maker_fee_pct: 0.0,
                },
            ],
        );
    }
    markets
}

fn make_quote(exchange: Exchange, symbol: &str, offset: f64, recv_ts_ms: i64) -> QuoteUpdate {
    let (bid_px, ask_px) = match exchange {
        Exchange::Lighter => (99.90 + offset, 100.00 + offset),
        Exchange::Aster => (100.70 + offset, 100.82 + offset),
        Exchange::Extended => (100.55 + offset, 100.67 + offset),
        Exchange::EdgeX => (100.45 + offset, 100.57 + offset),
        Exchange::Hyperliquid => (100.35 + offset, 100.47 + offset),
        Exchange::Grvt => (100.28 + offset, 100.38 + offset),
        Exchange::ApeX => (100.22 + offset, 100.33 + offset),
    };

    QuoteUpdate {
        exchange,
        symbol_base: symbol.into(),
        bid_px,
        bid_qty: 3.0 + offset,
        ask_px,
        ask_qty: 2.2 + offset,
        exch_ts_ms: recv_ts_ms - 2,
        recv_ts_ms,
    }
}

fn make_event_templates(symbol_count: usize) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(symbol_count * Exchange::all().len());
    for idx in 0..symbol_count {
        let symbol = format!("COIN{idx}");
        for (ex_idx, exchange) in Exchange::all().iter().enumerate() {
            let quote = make_quote(*exchange, &symbol, ex_idx as f64 * 0.01, 1_700_000_000_000);
            events.push(MarketEvent::Quote(quote));
        }
    }
    events
}

fn run_engine_bench(symbol_count: usize, rounds: usize, snapshot_every: usize) {
    let markets = make_markets(symbol_count);
    let templates = make_event_templates(symbol_count);
    let mut state = EngineState::new(Arc::new(markets));
    let total_events = templates.len().saturating_mul(rounds);

    let start = Instant::now();
    let mut now_ms = 1_700_000_000_000_i64;
    let mut last_rows = 0usize;

    for round in 0..rounds {
        let bump = ((round % 7) as f64) * 0.0001;
        for (idx, template) in templates.iter().enumerate() {
            let mut event = template.clone();
            if let MarketEvent::Quote(quote) = &mut event {
                quote.bid_px += bump;
                quote.ask_px += bump;
                quote.recv_ts_ms = now_ms;
                quote.exch_ts_ms = now_ms - 2;
            }

            state.ingest_event(event, now_ms);

            if snapshot_every > 0 && idx % snapshot_every == 0 {
                let rows = state.ranked_snapshot(now_ms, 2_500);
                last_rows = rows.len();
                black_box(rows);
            }
            now_ms = now_ms.saturating_add(1);
        }
    }

    let elapsed = start.elapsed();
    let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
    let ns_per_event = elapsed.as_nanos() as f64 / total_events as f64;
    let mode = if snapshot_every == 0 {
        "ingest only"
    } else if snapshot_every == 1 {
        "ingest + snapshot/event"
    } else {
        "ingest + throttled snapshot"
    };
    println!(
        "{mode:<34} {:>10.0} ev/s   {:>9.1} ns/ev   rows={last_rows}",
        events_per_sec, ns_per_event
    );
}

fn run_snapshot_only_bench(symbol_count: usize, iterations: usize) {
    let markets = make_markets(symbol_count);
    let templates = make_event_templates(symbol_count);
    let mut state = EngineState::new(Arc::new(markets));
    let mut now_ms = 1_700_000_000_000_i64;

    for template in templates {
        let mut event = template;
        if let MarketEvent::Quote(quote) = &mut event {
            quote.recv_ts_ms = now_ms;
            quote.exch_ts_ms = now_ms - 2;
        }
        state.ingest_event(event, now_ms);
        now_ms = now_ms.saturating_add(1);
    }

    let warm_rows = state.ranked_snapshot(now_ms, 2_500);
    println!("preloaded visible rows: {}", warm_rows.len());

    let start = Instant::now();
    let mut total_rows = 0usize;
    for idx in 0..iterations {
        let rows = state.ranked_snapshot(now_ms + (idx % 100) as i64, 1_000_000);
        total_rows = total_rows.saturating_add(rows.len());
        black_box(rows);
    }
    let elapsed = start.elapsed();
    let per_sec = iterations as f64 / elapsed.as_secs_f64();
    let ns_per = elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "{:<34} {:>10.0} snaps/s {:>9.1} ns/snap rows/snap={}",
        "snapshot only",
        per_sec,
        ns_per,
        total_rows / iterations.max(1)
    );
}

fn benchmark_runtime_engine(symbol_count: usize, rounds: usize) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async move {
        let markets = Arc::new(make_markets(symbol_count));
        let templates = make_event_templates(symbol_count);
        let total_events = templates.len().saturating_mul(rounds);

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(262_144);
        let snapshot = Arc::new(RwLock::new(Vec::new()));
        let health = Arc::new(RwLock::new(HashMap::new()));
        let selected_route_id = Arc::new(AtomicU32::new(NO_ROUTE_SELECTED));
        let detail_snapshot: Arc<RwLock<Option<RouteHistorySnapshot>>> =
            Arc::new(RwLock::new(None));

        let config = AppConfig {
            lighter_rest_url: String::new(),
            aster_rest_url: String::new(),
            extended_rest_url: String::new(),
            edge_x_rest_url: String::new(),
            hyperliquid_rest_url: String::new(),
            grvt_rest_url: String::new(),
            apex_rest_url: String::new(),
            lighter_ws_url: String::new(),
            aster_ws_url: String::new(),
            extended_ws_url: String::new(),
            edge_x_ws_url: String::new(),
            hyperliquid_ws_url: String::new(),
            grvt_ws_url: String::new(),
            apex_ws_url: String::new(),
            apex_depth_rest_url: String::new(),
            lighter_funding_rest_url: String::new(),
            aster_funding_rest_url: String::new(),
            extended_funding_ws_url: String::new(),
            edge_x_funding_rest_url: String::new(),
            hyperliquid_funding_rest_url: String::new(),
            funding_poll_secs: 300,
            stale_ms: 2_500,
            ui_fps: 60,
            quote_channel_capacity: 262_144,
            http_timeout_secs: 10,
            discovery_refresh_secs: 600,
            collect_mode: false,
            collector_data_root: std::path::PathBuf::from("data"),
            collector_compression: CollectorCompression::Zstd,
            collector_bootstrap_timeout_ms: 45_000,
            collector_write_buffer: 16_384,
            collector_flush_interval_ms: 1_000,
            collector_max_open_files: 512,
        };

        let engine_snapshot = Arc::clone(&snapshot);
        let engine_health = Arc::clone(&health);
        let engine_selected_route_id = Arc::clone(&selected_route_id);
        let engine_detail_snapshot = Arc::clone(&detail_snapshot);
        let handle = tokio::spawn(async move {
            cross_ex_arb::engine::run_engine(
                event_rx,
                markets,
                config,
                engine_snapshot,
                engine_health,
                engine_selected_route_id,
                engine_detail_snapshot,
            )
            .await;
        });

        let start = Instant::now();
        let mut now_ms = cross_ex_arb::model::now_ms();
        for round in 0..rounds {
            let bump = ((round % 7) as f64) * 0.0001;
            for template in &templates {
                let mut event = template.clone();
                if let MarketEvent::Quote(quote) = &mut event {
                    quote.bid_px += bump;
                    quote.ask_px += bump;
                    quote.recv_ts_ms = now_ms;
                    quote.exch_ts_ms = now_ms - 2;
                }
                if event_tx.send(event).await.is_err() {
                    break;
                }
                now_ms = now_ms.saturating_add(1);
            }
        }
        drop(event_tx);
        let _ = handle.await;

        let elapsed = start.elapsed();
        let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
        let ns_per_event = elapsed.as_nanos() as f64 / total_events as f64;
        let row_count = snapshot.read().map(|g| g.len()).unwrap_or_default();
        println!(
            "{:<34} {:>10.0} ev/s   {:>9.1} ns/ev   rows={row_count}",
            "run_engine end-to-end", events_per_sec, ns_per_event
        );
    });
}

fn main() {
    let parse_iterations = 300_000usize;
    let engine_symbols = 200usize;
    let engine_rounds = 300usize;
    let snapshot_iterations = 20_000usize;

    println!("== Parser Throughput ==");
    let lighter_market_map = HashMap::from([(1_u32, "BTC".to_owned())]);
    let aster_symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);
    let extended_symbol_map = HashMap::from([("BTC-USD".to_owned(), "BTC".to_owned())]);
    let edge_market_map = HashMap::from([(101_u32, "BTC".to_owned())]);
    let hyper_symbol_map = HashMap::from([("BTC".to_owned(), "BTC".to_owned())]);
    let grvt_symbol_map = HashMap::from([("BTC_USDT_Perp".to_owned(), "BTC".to_owned())]);
    let apex_symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    bench_parse("lighter ticker", parse_iterations, |recv_ts_ms| {
        parse_ticker_message(LIGHTER_TICKER, &lighter_market_map, recv_ts_ms).is_some()
    });
    bench_parse(
        "lighter market stats funding",
        parse_iterations,
        |recv_ts_ms| {
            parse_market_stats_message(LIGHTER_STATS, &lighter_market_map, recv_ts_ms).is_some()
        },
    );
    bench_parse("aster bookTicker", parse_iterations, |recv_ts_ms| {
        parse_book_ticker_message(ASTER_BOOK, &aster_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse("aster markPrice", parse_iterations, |recv_ts_ms| {
        parse_mark_price_message(ASTER_MARK, &aster_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse("extended orderbook", parse_iterations, |recv_ts_ms| {
        parse_extended_orderbook_message(EXTENDED_BOOK, &extended_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse("extended funding", parse_iterations, |recv_ts_ms| {
        parse_extended_funding_message(EXTENDED_FUNDING, &extended_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse("edgeX depth", parse_iterations, |recv_ts_ms| {
        parse_edge_x_depth_message(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms).is_some()
    });
    bench_parse("edgeX ticker funding", parse_iterations, |recv_ts_ms| {
        parse_edge_x_ticker_funding_message(EDGE_X_TICKER, &edge_market_map, recv_ts_ms).is_some()
    });
    bench_parse(
        "edgeX ticker funding fast-path",
        parse_iterations,
        |recv_ts_ms| {
            parse_edge_x_ticker_funding_message_fast(EDGE_X_TICKER, &edge_market_map, recv_ts_ms)
                .is_some()
        },
    );
    bench_parse(
        "edgeX ticker funding fallback-value",
        parse_iterations,
        |recv_ts_ms| {
            parse_edge_x_ticker_funding_message_fallback_value(
                EDGE_X_TICKER,
                &edge_market_map,
                recv_ts_ms,
            )
            .is_some()
        },
    );
    bench_parse("edgeX depth dual-parse", parse_iterations, |recv_ts_ms| {
        let quote =
            parse_edge_x_depth_message(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms).is_some();
        let funding =
            parse_edge_x_ticker_funding_message(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms)
                .is_some();
        quote || funding
    });
    bench_parse("edgeX depth routed", parse_iterations, |recv_ts_ms| {
        parse_edge_x_depth_message(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms).is_some()
    });
    bench_parse("edgeX ticker dual-parse", parse_iterations, |recv_ts_ms| {
        let quote =
            parse_edge_x_depth_message(EDGE_X_TICKER, &edge_market_map, recv_ts_ms).is_some();
        let funding =
            parse_edge_x_ticker_funding_message(EDGE_X_TICKER, &edge_market_map, recv_ts_ms)
                .is_some();
        quote || funding
    });
    bench_parse("edgeX ticker routed", parse_iterations, |recv_ts_ms| {
        parse_edge_x_ticker_funding_message(EDGE_X_TICKER, &edge_market_map, recv_ts_ms).is_some()
    });
    bench_parse("hyperliquid bbo", parse_iterations, |recv_ts_ms| {
        parse_hyperliquid_bbo_message(HYPERLIQUID_BBO, &hyper_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse(
        "hyperliquid bbo fast-path",
        parse_iterations,
        |recv_ts_ms| {
            parse_hyperliquid_bbo_message_fast(HYPERLIQUID_BBO, &hyper_symbol_map, recv_ts_ms)
                .is_some()
        },
    );
    bench_parse(
        "hyperliquid bbo fallback-value",
        parse_iterations,
        |recv_ts_ms| {
            parse_hyperliquid_bbo_message_fallback(HYPERLIQUID_BBO, &hyper_symbol_map, recv_ts_ms)
                .is_some()
        },
    );
    bench_parse("hyperliquid l2Book", parse_iterations, |recv_ts_ms| {
        parse_hyperliquid_l2book_message(HYPERLIQUID_L2, &hyper_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse(
        "grvt ticker quote+funding",
        parse_iterations,
        |recv_ts_ms| parse_grvt_ticker_message(GRVT_TICKER, &grvt_symbol_map, recv_ts_ms).is_some(),
    );
    bench_parse("apex depth", parse_iterations, |recv_ts_ms| {
        parse_apex_depth_event(APEX_DEPTH, recv_ts_ms).is_some()
    });
    bench_parse("apex depth fast-path", parse_iterations, |recv_ts_ms| {
        parse_apex_depth_event_fast(APEX_DEPTH, recv_ts_ms).is_some()
    });
    bench_parse(
        "apex depth fallback-value",
        parse_iterations,
        |recv_ts_ms| parse_apex_depth_event_fallback(APEX_DEPTH, recv_ts_ms).is_some(),
    );
    bench_parse("apex instrument funding", parse_iterations, |recv_ts_ms| {
        parse_apex_instrument_info_message(APEX_FUNDING, &apex_symbol_map, recv_ts_ms).is_some()
    });
    bench_parse("edgeX depth fast-path", parse_iterations, |recv_ts_ms| {
        parse_edge_x_depth_message_fast(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms).is_some()
    });
    bench_parse(
        "edgeX depth fallback-value",
        parse_iterations,
        |recv_ts_ms| {
            parse_edge_x_depth_message_fallback(EDGE_X_DEPTH, &edge_market_map, recv_ts_ms)
                .is_some()
        },
    );

    println!("\n== Engine Throughput ==");
    run_engine_bench(engine_symbols, engine_rounds, 0);
    run_engine_bench(engine_symbols, engine_rounds, 1);
    run_engine_bench(engine_symbols, engine_rounds, 20);
    run_snapshot_only_bench(engine_symbols, snapshot_iterations);
    benchmark_runtime_engine(engine_symbols, 150);
}
