use std::collections::HashMap;

use cross_ex_arb::feeds::aster::{parse_book_ticker_message, parse_mark_price_message};
use cross_ex_arb::feeds::edge_x::{
    parse_edge_x_depth_message, parse_edge_x_ticker_funding_message,
};
use cross_ex_arb::feeds::extended::{
    parse_extended_funding_message, parse_extended_orderbook_message,
};
use cross_ex_arb::feeds::hyperliquid::parse_hyperliquid_l2book_message;
use cross_ex_arb::feeds::lighter::{parse_market_stats_message, parse_ticker_message};
use cross_ex_arb::model::Exchange;

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
