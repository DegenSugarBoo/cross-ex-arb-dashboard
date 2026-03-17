use std::collections::HashSet;
use std::sync::Arc;

use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::engine::EngineState;
use cross_ex_arb::model::{Exchange, MarketEvent, MarketMeta, QuoteUpdate};

fn markets_two_exchanges() -> SymbolMarkets {
    let mut markets = SymbolMarkets::new();
    markets.insert(
        "BTC".to_owned(),
        vec![
            MarketMeta {
                exchange: Exchange::Lighter,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTC".to_owned(),
                market_id: Some(1),
                taker_fee_pct: 0.03,
                maker_fee_pct: 0.0,
            },
            MarketMeta {
                exchange: Exchange::Aster,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTCUSDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.04,
                maker_fee_pct: 0.005,
            },
        ],
    );
    markets
}

fn markets_three_exchanges() -> SymbolMarkets {
    let mut markets = SymbolMarkets::new();
    markets.insert(
        "BTC".to_owned(),
        vec![
            MarketMeta {
                exchange: Exchange::Lighter,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTC".to_owned(),
                market_id: Some(1),
                taker_fee_pct: 0.03,
                maker_fee_pct: 0.0,
            },
            MarketMeta {
                exchange: Exchange::Aster,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTCUSDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.04,
                maker_fee_pct: 0.005,
            },
            MarketMeta {
                exchange: Exchange::Extended,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTC-USD".to_owned(),
                market_id: None,
                taker_fee_pct: 0.025,
                maker_fee_pct: 0.0,
            },
        ],
    );
    markets.insert(
        "ETH".to_owned(),
        vec![
            MarketMeta {
                exchange: Exchange::Lighter,
                symbol_base: "ETH".to_owned(),
                exchange_symbol: "ETH".to_owned(),
                market_id: Some(2),
                taker_fee_pct: 0.03,
                maker_fee_pct: 0.0,
            },
            MarketMeta {
                exchange: Exchange::Aster,
                symbol_base: "ETH".to_owned(),
                exchange_symbol: "ETHUSDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.04,
                maker_fee_pct: 0.005,
            },
        ],
    );
    markets
}

fn quote(
    exchange: Exchange,
    symbol: &str,
    bid_px: f64,
    bid_qty: f64,
    ask_px: f64,
    ask_qty: f64,
    recv_ts_ms: i64,
) -> QuoteUpdate {
    QuoteUpdate {
        exchange,
        symbol_base: symbol.to_owned().into(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms: recv_ts_ms,
        recv_ts_ms,
    }
}

fn route_id_for(state: &EngineState, symbol: &str, buy_ex: Exchange, sell_ex: Exchange) -> u32 {
    state
        .route_catalog()
        .iter()
        .position(|meta| {
            meta.key.symbol.as_str() == symbol && meta.buy_ex == buy_ex && meta.sell_ex == sell_ex
        })
        .expect("route should exist") as u32
}

#[test]
fn route_catalog_contains_all_directed_routes() {
    let state = EngineState::new(Arc::new(markets_three_exchanges()));

    assert_eq!(state.route_catalog().len(), 8);
    assert_eq!(
        state.route_ids_for_symbol("BTC").expect("BTC routes").len(),
        6
    );
    assert_eq!(
        state.route_ids_for_symbol("ETH").expect("ETH routes").len(),
        2
    );

    let btc_pairs: HashSet<(Exchange, Exchange)> = state
        .route_catalog()
        .iter()
        .filter(|meta| meta.key.symbol.as_str() == "BTC")
        .map(|meta| (meta.buy_ex, meta.sell_ex))
        .collect();
    assert_eq!(btc_pairs.len(), 6);
}

#[test]
fn history_append_keeps_column_lengths_in_sync() {
    let mut state = EngineState::new(Arc::new(markets_two_exchanges()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.5,
            2.0,
            100.0,
            2.0,
            1_000,
        )),
        1_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 101.0, 2.0, 101.3, 2.0, 1_000)),
        1_000,
    );

    for route_id in state.route_ids_for_symbol("BTC").expect("BTC routes") {
        let (ts_len, raw_len, usd_len, age_len) = state
            .history_column_lengths_for_route(*route_id)
            .expect("route history should exist");
        assert_eq!(ts_len, raw_len);
        assert_eq!(ts_len, usd_len);
        assert_eq!(ts_len, age_len);
        assert_eq!(state.history_len_for_route(*route_id), Some(1));
    }
}

#[test]
fn history_prune_keeps_only_last_30_seconds() {
    let mut state = EngineState::new(Arc::new(markets_two_exchanges()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.0,
            2.0,
            100.0,
            2.0,
            1_000,
        )),
        1_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 101.0, 2.0, 101.2, 2.0, 1_000)),
        1_000,
    );

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.2,
            2.0,
            100.2,
            2.0,
            31_500,
        )),
        31_500,
    );

    let route_id = route_id_for(&state, "BTC", Exchange::Lighter, Exchange::Aster);
    let snapshot = state
        .history_snapshot_for_route(route_id, 31_500)
        .expect("snapshot should exist");

    assert!(!snapshot.points.is_empty());
    assert!(snapshot.points.iter().all(|point| point.ts_ms >= 1_500));
}

#[test]
fn selected_route_snapshot_changes_by_route_and_preserves_ordering() {
    let mut state = EngineState::new(Arc::new(markets_three_exchanges()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.5,
            2.0,
            100.0,
            2.0,
            10_000,
        )),
        10_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Aster,
            "BTC",
            101.0,
            2.0,
            101.4,
            2.0,
            10_100,
        )),
        10_100,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Extended,
            "BTC",
            100.8,
            2.0,
            101.0,
            2.0,
            10_200,
        )),
        10_200,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.7,
            2.0,
            100.1,
            2.0,
            10_300,
        )),
        10_300,
    );

    let route_a = route_id_for(&state, "BTC", Exchange::Lighter, Exchange::Aster);
    let route_b = route_id_for(&state, "BTC", Exchange::Aster, Exchange::Lighter);

    let snapshot_a = state
        .history_snapshot_for_route(route_a, 10_300)
        .expect("snapshot A should exist");
    let snapshot_b = state
        .history_snapshot_for_route(route_b, 10_300)
        .expect("snapshot B should exist");

    assert_eq!(snapshot_a.route_id, route_a);
    assert_eq!(snapshot_b.route_id, route_b);
    assert_eq!(snapshot_a.key.buy_ex, Exchange::Lighter);
    assert_eq!(snapshot_a.key.sell_ex, Exchange::Aster);
    assert_eq!(snapshot_b.key.buy_ex, Exchange::Aster);
    assert_eq!(snapshot_b.key.sell_ex, Exchange::Lighter);
    assert!(snapshot_b.revision > snapshot_a.revision);

    assert!(
        snapshot_a
            .points
            .windows(2)
            .all(|window| window[0].ts_ms <= window[1].ts_ms)
    );
    assert!(
        snapshot_b
            .points
            .windows(2)
            .all(|window| window[0].ts_ms <= window[1].ts_ms)
    );
}

#[test]
fn inactive_route_is_cleaned_by_global_prune() {
    let mut state = EngineState::new(Arc::new(markets_two_exchanges()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.0,
            2.0,
            100.0,
            2.0,
            1_000,
        )),
        1_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 101.0, 2.0, 101.3, 2.0, 1_000)),
        1_000,
    );

    let route_id = route_id_for(&state, "BTC", Exchange::Lighter, Exchange::Aster);
    assert_eq!(state.history_len_for_route(route_id), Some(1));

    state.prune_history_all(40_000);

    assert_eq!(state.history_len_for_route(route_id), Some(0));
}
