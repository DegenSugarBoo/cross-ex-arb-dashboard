use std::collections::HashMap;

use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::feeds::apex::{
    ApexBookState, ApplyResult, apex_symbol_map, apply_apex_delta, apply_apex_snapshot,
    best_quote_from_apex_state, parse_apex_depth_event, parse_apex_instrument_info_message,
};
use cross_ex_arb::feeds::aster::{parse_book_ticker_message, parse_mark_price_message};
use cross_ex_arb::feeds::binance::{
    parse_binance_book_ticker_message, parse_binance_mark_price_message,
};
use cross_ex_arb::feeds::bybit::{
    BybitTickerState, apply_bybit_ticker_patch, parse_bybit_ticker_message,
};
use cross_ex_arb::feeds::edge_x::{
    parse_edge_x_depth_message, parse_edge_x_ticker_funding_message,
};
use cross_ex_arb::feeds::extended::{
    parse_extended_funding_message, parse_extended_orderbook_message,
};
use cross_ex_arb::feeds::grvt::parse_grvt_ticker_message;
use cross_ex_arb::feeds::hyperliquid::{
    parse_hyperliquid_bbo_message, parse_hyperliquid_l2book_message,
};
use cross_ex_arb::feeds::lighter::{parse_market_stats_message, parse_ticker_message};
use cross_ex_arb::model::{Exchange, MarketMeta};
use ordered_float::OrderedFloat;

#[test]
fn parses_lighter_ticker_fixture() {
    let payload = include_str!("../fixtures/lighter_ticker.json");
    let market_map = HashMap::from([(1_u32, "BTC".to_owned())]);

    let update = parse_ticker_message(payload, &market_map, 1_700_000_000_999)
        .expect("lighter fixture should parse");

    assert_eq!(update.exchange, Exchange::Lighter);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.0).abs() < 1e-9);
    assert!((update.ask_px - 100.5).abs() < 1e-9);
    assert_eq!(update.exch_ts_ms, 1_700_000_000_000);
}

#[test]
fn parses_aster_book_ticker_fixture() {
    let payload = include_str!("../fixtures/aster_bookticker.json");
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    let update = parse_book_ticker_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("aster fixture should parse");

    assert_eq!(update.exchange, Exchange::Aster);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 101.0).abs() < 1e-9);
    assert!((update.ask_px - 101.2).abs() < 1e-9);
    assert_eq!(update.exch_ts_ms, 1_700_000_002_222);
}

#[test]
fn ignores_non_data_messages() {
    let ack = "{\"id\":1,\"result\":null}";
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    assert!(parse_book_ticker_message(ack, &symbol_map, 0).is_none());
}

#[test]
fn parses_aster_mark_price_update() {
    let payload = r#"{"e":"markPriceUpdate","E":1700000010000,"s":"BTCUSDT","r":"-0.00001411","T":1700006400000}"#;
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    let update =
        parse_mark_price_message(payload, &symbol_map, 1_700_000_009_999).expect("mark price");

    assert_eq!(update.exchange, Exchange::Aster);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - (-0.00001411)).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_700_006_400_000));
}

#[test]
fn parses_binance_book_ticker_fixture() {
    let payload = include_str!("../fixtures/binance_book_ticker.json");
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    let update = parse_binance_book_ticker_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("binance bookTicker");

    assert_eq!(update.exchange, Exchange::Binance);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 101.0).abs() < 1e-9);
    assert!((update.ask_px - 101.2).abs() < 1e-9);
    assert_eq!(update.exch_ts_ms, 1_700_000_002_222);
}

#[test]
fn parses_binance_mark_price_fixture() {
    let payload = include_str!("../fixtures/binance_mark_price.json");
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    let update = parse_binance_mark_price_message(payload, &symbol_map, 1_700_000_009_999)
        .expect("binance markPrice");

    assert_eq!(update.exchange, Exchange::Binance);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - (-0.00001411)).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_700_006_400_000));
}

#[test]
fn binance_ignores_irrelevant_frames() {
    let ack = "{\"stream\":\"btcusdt@markPrice\",\"data\":{\"result\":null}}";
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    assert!(parse_binance_book_ticker_message(ack, &symbol_map, 0).is_none());
    assert!(parse_binance_mark_price_message(ack, &symbol_map, 0).is_none());
}

#[test]
fn parses_lighter_market_stats_funding() {
    let payload = r#"{
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
    let market_map = HashMap::from([(1_u32, "BTC".to_owned())]);

    let update =
        parse_market_stats_message(payload, &market_map, 1_700_000_000_999).expect("market stats");

    assert_eq!(update.exchange, Exchange::Lighter);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - 0.0002).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_700_000_400_000));
}

#[test]
fn parses_extended_orderbook_message() {
    let payload = r#"{
        "timestamp":1700000000123,
        "data":{
            "m":"BTC-USD",
            "b":[["100.1","4.0"]],
            "a":[["100.3","2.5"]]
        }
    }"#;
    let symbol_map = HashMap::from([("BTC-USD".to_owned(), "BTC".to_owned())]);

    let update = parse_extended_orderbook_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("extended orderbook");

    assert_eq!(update.exchange, Exchange::Extended);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.1).abs() < 1e-9);
    assert!((update.ask_px - 100.3).abs() < 1e-9);
}

#[test]
fn parses_extended_orderbook_with_invalid_first_level() {
    let payload = r#"{
        "timestamp":1700000000123,
        "data":{
            "m":"BTC-USD",
            "b":[["0","4.0"],["100.2","3.0"]],
            "a":[["101.0","0"],["100.4","2.5"]]
        }
    }"#;
    let symbol_map = HashMap::from([("BTC-USD".to_owned(), "BTC".to_owned())]);

    let update = parse_extended_orderbook_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("extended orderbook fallback scan");

    assert_eq!(update.exchange, Exchange::Extended);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.2).abs() < 1e-9);
    assert!((update.ask_px - 100.4).abs() < 1e-9);
}

#[test]
fn parses_extended_funding_message() {
    let payload = r#"{
        "timestamp":1700000010000,
        "data":{
            "m":"BTC-USD",
            "fundingRate":"-0.00012",
            "nextFundingTime":1700003600000
        }
    }"#;
    let symbol_map = HashMap::from([("BTC-USD".to_owned(), "BTC".to_owned())]);

    let update = parse_extended_funding_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("extended funding");

    assert_eq!(update.exchange, Exchange::Extended);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - (-0.00012)).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_700_003_600_000));
}

#[test]
fn parses_edgex_depth_message() {
    let payload = r#"{
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
    let market_map = HashMap::from([(101_u32, "BTC".to_owned())]);

    let update =
        parse_edge_x_depth_message(payload, &market_map, 1_700_000_000_999).expect("edgeX depth");

    assert_eq!(update.exchange, Exchange::EdgeX);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.5).abs() < 1e-9);
    assert!((update.ask_px - 100.7).abs() < 1e-9);
}

#[test]
fn parses_edgex_ticker_funding_message() {
    let payload = r#"{
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
    let market_map = HashMap::from([(101_u32, "BTC".to_owned())]);

    let update = parse_edge_x_ticker_funding_message(payload, &market_map, 1_700_000_000_999)
        .expect("edgeX ticker funding");

    assert_eq!(update.exchange, Exchange::EdgeX);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - (-0.00002431)).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_773_547_200_000));
}

#[test]
fn parses_hyperliquid_l2book_message() {
    let payload = r#"{
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
    let symbol_map = HashMap::from([("BTC".to_owned(), "BTC".to_owned())]);

    let update = parse_hyperliquid_l2book_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("hyperliquid l2Book");

    assert_eq!(update.exchange, Exchange::Hyperliquid);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.9).abs() < 1e-9);
    assert!((update.ask_px - 101.0).abs() < 1e-9);
}

#[test]
fn parses_hyperliquid_l2book_with_invalid_first_level() {
    let payload = r#"{
        "channel":"l2Book",
        "data":{
            "coin":"BTC",
            "time":1700000030000,
            "levels":[
                [{"px":"100.9","sz":"0","n":3},{"px":"100.7","sz":"2.0","n":1}],
                [{"px":"0","sz":"2.0","n":1},{"px":"101.0","sz":"1.5","n":2}]
            ]
        }
    }"#;
    let symbol_map = HashMap::from([("BTC".to_owned(), "BTC".to_owned())]);

    let update = parse_hyperliquid_l2book_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("hyperliquid l2Book fallback scan");

    assert_eq!(update.exchange, Exchange::Hyperliquid);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.7).abs() < 1e-9);
    assert!((update.ask_px - 101.0).abs() < 1e-9);
}

#[test]
fn bybit_snapshot_emits_quote_and_funding() {
    let payload = include_str!("../fixtures/bybit_ticker_snapshot.json");
    let patch = parse_bybit_ticker_message(payload, 1_700_000_000_999).expect("bybit snapshot");
    let mut state = BybitTickerState::default();

    let (quote, funding) = apply_bybit_ticker_patch(&mut state, &patch, "BTC", 1_700_000_000_999);
    let quote = quote.expect("bybit quote");
    let funding = funding.expect("bybit funding");

    assert_eq!(quote.exchange, Exchange::Bybit);
    assert_eq!(quote.symbol_base, "BTC");
    assert!((quote.bid_px - 100.10).abs() < 1e-9);
    assert!((quote.ask_px - 100.30).abs() < 1e-9);

    assert_eq!(funding.exchange, Exchange::Bybit);
    assert_eq!(funding.symbol_base, "BTC");
    assert!((funding.funding_rate - 0.00010).abs() < 1e-12);
    assert_eq!(funding.next_funding_ts_ms, Some(1_700_003_600_000));
}

#[test]
fn bybit_delta_quote_updates_cached_top_of_book() {
    let snapshot = include_str!("../fixtures/bybit_ticker_snapshot.json");
    let delta = include_str!("../fixtures/bybit_ticker_delta_quote.json");
    let mut state = BybitTickerState::default();

    let snapshot_patch =
        parse_bybit_ticker_message(snapshot, 1_700_000_000_999).expect("bybit snapshot");
    let _ = apply_bybit_ticker_patch(&mut state, &snapshot_patch, "BTC", 1_700_000_000_999);

    let delta_patch = parse_bybit_ticker_message(delta, 1_700_000_005_999).expect("bybit delta");
    let (quote, funding) =
        apply_bybit_ticker_patch(&mut state, &delta_patch, "BTC", 1_700_000_005_999);
    let quote = quote.expect("bybit quote delta");

    assert_eq!(quote.exchange, Exchange::Bybit);
    assert!((quote.bid_px - 100.20).abs() < 1e-9);
    assert!((quote.ask_px - 100.40).abs() < 1e-9);
    assert!(funding.is_none());
}

#[test]
fn bybit_delta_funding_updates_cached_funding_only() {
    let snapshot = include_str!("../fixtures/bybit_ticker_snapshot.json");
    let delta = include_str!("../fixtures/bybit_ticker_delta_funding.json");
    let mut state = BybitTickerState::default();

    let snapshot_patch =
        parse_bybit_ticker_message(snapshot, 1_700_000_000_999).expect("bybit snapshot");
    let _ = apply_bybit_ticker_patch(&mut state, &snapshot_patch, "BTC", 1_700_000_000_999);

    let delta_patch = parse_bybit_ticker_message(delta, 1_700_000_010_999).expect("bybit delta");
    let (quote, funding) =
        apply_bybit_ticker_patch(&mut state, &delta_patch, "BTC", 1_700_000_010_999);
    let funding = funding.expect("bybit funding delta");

    assert!(quote.is_none());
    assert_eq!(funding.exchange, Exchange::Bybit);
    assert!((funding.funding_rate - 0.00012).abs() < 1e-12);
    assert_eq!(funding.next_funding_ts_ms, Some(1_700_007_200_000));
}

#[test]
fn bybit_delta_before_snapshot_is_incomplete() {
    let delta = r#"{
        "topic": "tickers.BTCUSDT",
        "type": "delta",
        "ts": 1700000005000,
        "data": {
            "symbol": "BTCUSDT",
            "bid1Price": "100.20",
            "bid1Size": "2.00"
        }
    }"#;
    let mut state = BybitTickerState::default();

    let patch = parse_bybit_ticker_message(delta, 1_700_000_005_999).expect("bybit delta");
    let (quote, funding) = apply_bybit_ticker_patch(&mut state, &patch, "BTC", 1_700_000_005_999);

    assert!(quote.is_none());
    assert!(funding.is_none());
}

#[test]
fn bybit_ignores_non_market_frames() {
    let ack = include_str!("../fixtures/bybit_ticker_subscribe_ack.json");
    let pong = include_str!("../fixtures/bybit_pong.json");

    assert!(parse_bybit_ticker_message(ack, 1_700_000_005_999).is_none());
    assert!(parse_bybit_ticker_message(pong, 1_700_000_005_999).is_none());
}

#[test]
fn parses_hyperliquid_bbo_message() {
    let payload = r#"{
        "channel":"bbo",
        "data":{
            "coin":"BTC",
            "time":1700000035000,
            "bbo":[
                {"px":"100.8","sz":"1.2","n":1},
                {"px":"101.1","sz":"2.7","n":2}
            ]
        }
    }"#;
    let symbol_map = HashMap::from([("BTC".to_owned(), "BTC".to_owned())]);

    let update = parse_hyperliquid_bbo_message(payload, &symbol_map, 1_700_000_000_999)
        .expect("hyperliquid bbo");

    assert_eq!(update.exchange, Exchange::Hyperliquid);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.bid_px - 100.8).abs() < 1e-9);
    assert!((update.ask_px - 101.1).abs() < 1e-9);
    assert!((update.bid_qty - 1.2).abs() < 1e-9);
    assert!((update.ask_qty - 2.7).abs() < 1e-9);
}

#[test]
fn parses_grvt_ticker_quote_and_funding_fixture() {
    let payload = include_str!("../fixtures/grvt_ticker_quote_funding.json");
    let symbol_map = HashMap::from([("BTC_USDT_Perp".to_owned(), "BTC".to_owned())]);

    let (quote, funding) =
        parse_grvt_ticker_message(payload, &symbol_map, 1_700_000_000_999).expect("grvt parse");
    let quote = quote.expect("grvt quote");
    let funding = funding.expect("grvt funding");

    assert_eq!(quote.exchange, Exchange::Grvt);
    assert_eq!(quote.symbol_base, "BTC");
    assert!((quote.bid_px - 100.2).abs() < 1e-9);
    assert!((quote.ask_px - 100.4).abs() < 1e-9);
    assert_eq!(quote.exch_ts_ms, 1_700_000_000_123);

    assert_eq!(funding.exchange, Exchange::Grvt);
    assert_eq!(funding.symbol_base, "BTC");
    assert!((funding.funding_rate - 0.00012).abs() < 1e-12);
    assert_eq!(funding.next_funding_ts_ms, Some(1_700_003_600_000));
}

#[test]
fn parses_grvt_current_ticker_quote_and_funding_fixture() {
    let payload = include_str!("../fixtures/grvt_ticker_quote_funding_v2.json");
    let symbol_map = HashMap::from([("BTC_USDT_Perp".to_owned(), "BTC".to_owned())]);

    let (quote, funding) =
        parse_grvt_ticker_message(payload, &symbol_map, 1_700_000_000_999).expect("grvt parse");
    let quote = quote.expect("grvt quote");
    let funding = funding.expect("grvt funding");

    assert_eq!(quote.exchange, Exchange::Grvt);
    assert_eq!(quote.symbol_base, "BTC");
    assert!((quote.bid_px - 100.2).abs() < 1e-9);
    assert!((quote.ask_px - 100.4).abs() < 1e-9);
    assert_eq!(quote.exch_ts_ms, 1_700_000_000_123);

    assert_eq!(funding.exchange, Exchange::Grvt);
    assert_eq!(funding.symbol_base, "BTC");
    assert!((funding.funding_rate - 0.00012).abs() < 1e-12);
    assert_eq!(funding.next_funding_ts_ms, Some(1_700_003_600_000));
}

#[test]
fn grvt_ignores_ack_messages() {
    let payload = include_str!("../fixtures/grvt_ticker_ack.json");
    let symbol_map = HashMap::from([("BTC_USDT_Perp".to_owned(), "BTC".to_owned())]);
    assert!(parse_grvt_ticker_message(payload, &symbol_map, 1_700_000_000_999).is_none());
}

#[test]
fn apex_snapshot_and_delta_produce_updated_best_quote() {
    let snapshot_raw = include_str!("../fixtures/apex_depth_snapshot.json");
    let delta_raw = include_str!("../fixtures/apex_depth_delta_update.json");
    let mut state = ApexBookState::default();

    let snapshot_event =
        parse_apex_depth_event(snapshot_raw, 1_700_000_040_000).expect("apex snapshot parse");
    assert!(apply_apex_snapshot(&mut state, &snapshot_event));

    let delta_event =
        parse_apex_depth_event(delta_raw, 1_700_000_040_100).expect("apex delta parse");
    assert_eq!(
        apply_apex_delta(&mut state, &delta_event),
        ApplyResult::Applied
    );

    let quote =
        best_quote_from_apex_state(&state, "BTC", delta_event.event_ts_ms, 1_700_000_040_101)
            .expect("best quote");
    assert_eq!(quote.exchange, Exchange::ApeX);
    assert_eq!(quote.symbol_base, "BTC");
    assert!((quote.bid_px - 100.6).abs() < 1e-9);
    assert!((quote.ask_px - 100.7).abs() < 1e-9);
}

#[test]
fn apex_zero_size_delta_deletes_levels() {
    let snapshot_raw = include_str!("../fixtures/apex_depth_snapshot.json");
    let delta_update_raw = include_str!("../fixtures/apex_depth_delta_update.json");
    let delta_delete_raw = include_str!("../fixtures/apex_depth_delta_delete.json");
    let mut state = ApexBookState::default();

    let snapshot_event =
        parse_apex_depth_event(snapshot_raw, 1_700_000_040_000).expect("apex snapshot parse");
    assert!(apply_apex_snapshot(&mut state, &snapshot_event));

    let delta_update =
        parse_apex_depth_event(delta_update_raw, 1_700_000_040_100).expect("apex delta update");
    assert_eq!(
        apply_apex_delta(&mut state, &delta_update),
        ApplyResult::Applied
    );

    let delta_delete_event =
        parse_apex_depth_event(delta_delete_raw, 1_700_000_040_200).expect("apex delta delete");
    assert_eq!(
        apply_apex_delta(&mut state, &delta_delete_event),
        ApplyResult::Applied
    );

    assert!(!state.bids.contains_key(&OrderedFloat(100.4)));
    assert!(!state.asks.contains_key(&OrderedFloat(100.8)));
}

#[test]
fn apex_sequence_mismatch_marks_state_for_resync() {
    let snapshot_raw = include_str!("../fixtures/apex_depth_snapshot.json");
    let bad_delta_raw = r#"{
        "topic":"orderBook200.H.BTCUSDT",
        "type":"delta",
        "data":{
            "s":"BTCUSDT",
            "b":[["100.6","1.0"]],
            "a":[["100.7","1.0"]],
            "u":"1002",
            "pu":"999"
        }
    }"#;
    let mut state = ApexBookState::default();

    let snapshot_event =
        parse_apex_depth_event(snapshot_raw, 1_700_000_040_000).expect("apex snapshot parse");
    assert!(apply_apex_snapshot(&mut state, &snapshot_event));

    let bad_delta =
        parse_apex_depth_event(bad_delta_raw, 1_700_000_040_300).expect("apex bad delta parse");
    assert_eq!(
        apply_apex_delta(&mut state, &bad_delta),
        ApplyResult::NeedsResync
    );
    assert!(state.needs_resync);
}

#[test]
fn apex_funding_parser_maps_fields() {
    let payload = include_str!("../fixtures/apex_instrument_info_funding.json");
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    let update = parse_apex_instrument_info_message(payload, &symbol_map, 1_700_000_041_999)
        .expect("apex funding parse");

    assert_eq!(update.exchange, Exchange::ApeX);
    assert_eq!(update.symbol_base, "BTC");
    assert!((update.funding_rate - (-0.000031)).abs() < 1e-12);
    assert_eq!(update.next_funding_ts_ms, Some(1_700_006_400_000));
    assert_eq!(update.recv_ts_ms, 1_700_000_041_000);
}

#[test]
fn apex_symbol_map_resolves_hyphenated_ws_symbols() {
    let mut markets = SymbolMarkets::new();
    markets.insert(
        "BTC".to_owned(),
        vec![MarketMeta {
            exchange: Exchange::ApeX,
            symbol_base: "BTC".to_owned(),
            exchange_symbol: "BTC-USDT".to_owned(),
            market_id: None,
            taker_fee_pct: 0.05,
            maker_fee_pct: 0.0,
        }],
    );

    let symbol_map = apex_symbol_map(&markets);
    let payload = r#"{
        "topic":"instrumentInfo.H.BTC-USDT",
        "data":{
            "s":"BTC-USDT",
            "fundingRate":"-0.000031",
            "nextFundingTime":"1700006400000000",
            "ts":"1700000041000"
        }
    }"#;

    let update = parse_apex_instrument_info_message(payload, &symbol_map, 1_700_000_041_999)
        .expect("apex funding parse with ws symbol");

    assert_eq!(update.exchange, Exchange::ApeX);
    assert_eq!(update.symbol_base, "BTC");
}
