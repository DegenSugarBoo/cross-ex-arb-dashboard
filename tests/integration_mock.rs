use std::collections::HashSet;
use std::sync::Arc;

use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::engine::EngineState;
use cross_ex_arb::model::{ArbRow, Exchange, MarketEvent, MarketMeta, QuoteUpdate};

fn sample_markets() -> SymbolMarkets {
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

fn many_symbol_markets(symbol_count: usize) -> SymbolMarkets {
    let mut markets = SymbolMarkets::new();
    for idx in 0..symbol_count {
        let symbol = format!("SYM{idx:02}");
        markets.insert(
            symbol.clone(),
            vec![
                MarketMeta {
                    exchange: Exchange::Lighter,
                    symbol_base: symbol.clone(),
                    exchange_symbol: symbol.clone(),
                    market_id: Some((idx + 1) as u32),
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
            ],
        );
    }
    markets
}

fn three_way_markets() -> SymbolMarkets {
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
                exchange_symbol: "BTC_USDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.05,
                maker_fee_pct: 0.0,
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

fn seed_quotes_for_symbol(state: &mut EngineState, symbol: &str, recv_ts_ms: i64, now_ms: i64) {
    let quote_base = symbol.bytes().map(i64::from).sum::<i64>() as f64;
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            symbol,
            quote_base - 0.4,
            2.0,
            quote_base,
            2.0,
            recv_ts_ms,
        )),
        now_ms,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Aster,
            symbol,
            quote_base + 1.2,
            2.0,
            quote_base + 1.4,
            2.0,
            recv_ts_ms,
        )),
        now_ms,
    );
}

fn assert_preferred_max_by_net_then_raw(left: &ArbRow, right: &ArbRow) -> bool {
    if left.net_spread_bps != right.net_spread_bps {
        left.net_spread_bps > right.net_spread_bps
    } else {
        left.raw_spread_bps >= right.raw_spread_bps
    }
}

#[test]
fn mixed_quotes_generate_ranked_single_row_per_symbol() {
    let mut state = EngineState::new(Arc::new(sample_markets()));

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

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "ETH",
            199.0,
            3.0,
            200.0,
            3.0,
            1_050,
        )),
        1_050,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "ETH", 198.4, 3.0, 198.8, 3.0, 1_050)),
        1_050,
    );

    let rows = state.ranked_snapshot(1_100, 2_500);

    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter()
            .filter(|row| row.symbol == "BTC")
            .all(|row| row.route_id < 2),
    );
    assert!(
        rows.iter()
            .filter(|row| row.symbol == "ETH")
            .all(|row| row.route_id >= 2),
    );
}

#[test]
fn stale_leg_suppression_hides_row() {
    let mut state = EngineState::new(Arc::new(sample_markets()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.0,
            1.0,
            100.0,
            1.0,
            1_000,
        )),
        1_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 101.0, 1.0, 102.0, 1.0, 1_000)),
        1_000,
    );

    let rows = state.ranked_snapshot(3_600, 2_500);
    assert!(rows.is_empty());
}

#[test]
fn ranked_snapshot_all_includes_all_routes_for_multi_exchange_symbol() {
    let mut state = EngineState::new(Arc::new(three_way_markets()));
    let recv_ts_ms = 20_000;
    let now_ms = 20_000;

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            98.8,
            1.5,
            99.0,
            1.5,
            recv_ts_ms,
        )),
        now_ms,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Aster,
            "BTC",
            100.5,
            1.5,
            100.8,
            1.5,
            recv_ts_ms,
        )),
        now_ms,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Extended,
            "BTC",
            101.0,
            1.5,
            101.3,
            1.5,
            recv_ts_ms,
        )),
        now_ms,
    );

    let rows = state.ranked_snapshot_all(now_ms + 100, 2_500);
    let btc_rows: Vec<_> = rows.iter().filter(|row| row.symbol == "BTC").collect();

    assert_eq!(btc_rows.len(), 6);
    let route_dirs: HashSet<(Exchange, Exchange)> = btc_rows
        .iter()
        .map(|row| (row.buy_ex, row.sell_ex))
        .collect();
    assert_eq!(route_dirs.len(), 6);

    let mut best = btc_rows[0];
    for row in btc_rows.iter().skip(1) {
        if assert_preferred_max_by_net_then_raw(row, best) {
            best = row;
        }
    }
    assert_eq!(rows[0].route_id, best.route_id);
}

#[test]
fn reconnect_continuity_keeps_state_after_one_side_resumes() {
    let mut state = EngineState::new(Arc::new(sample_markets()));

    state.ingest_event(
        MarketEvent::Quote(quote(
            Exchange::Lighter,
            "BTC",
            99.0,
            1.0,
            100.0,
            1.0,
            1_000,
        )),
        1_000,
    );
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 101.0, 1.0, 102.0, 1.0, 1_000)),
        1_000,
    );

    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Lighter, "BTC", 99.4, 1.0, 99.9, 1.0, 1_500)),
        1_500,
    );

    let rows = state.ranked_snapshot(1_600, 2_500);

    assert_eq!(rows.len(), 2);
    let route_ids: HashSet<u32> = rows.iter().map(|row| row.route_id).collect();
    assert_eq!(route_ids.len(), 2);
    assert!(route_ids.contains(&0));
    assert!(route_ids.contains(&1));
    assert_eq!(rows[0].symbol, "BTC");
    assert_eq!(rows[0].route_id, 0);
    assert_eq!(rows[0].buy_ask, 99.9);
}

#[test]
fn route_catalog_includes_grvt_and_apex_pairs() {
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
                exchange: Exchange::Grvt,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTC_USDT_Perp".to_owned(),
                market_id: None,
                taker_fee_pct: 0.045,
                maker_fee_pct: 0.0,
            },
            MarketMeta {
                exchange: Exchange::ApeX,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTCUSDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.05,
                maker_fee_pct: 0.0,
            },
        ],
    );

    let state = EngineState::new(Arc::new(markets));
    let has_grvt_to_apex = state.route_catalog().iter().any(|meta| {
        meta.key.symbol.as_str() == "BTC"
            && meta.buy_ex == Exchange::Grvt
            && meta.sell_ex == Exchange::ApeX
    });
    let has_apex_to_grvt = state.route_catalog().iter().any(|meta| {
        meta.key.symbol.as_str() == "BTC"
            && meta.buy_ex == Exchange::ApeX
            && meta.sell_ex == Exchange::Grvt
    });

    assert!(has_grvt_to_apex);
    assert!(has_apex_to_grvt);
}

#[test]
fn ranked_snapshot_all_returns_all_live_rows_without_truncation() {
    let mut state = EngineState::new(Arc::new(many_symbol_markets(30)));
    for idx in 0..30 {
        seed_quotes_for_symbol(&mut state, &format!("SYM{idx:02}"), 10_000, 10_000);
    }

    let rows = state.ranked_snapshot_all(10_100, 2_500);
    let unique_symbols: HashSet<&str> = rows.iter().map(|row| row.symbol.as_str()).collect();

    assert_eq!(rows.len(), 60);
    assert_eq!(unique_symbols.len(), 30);
}

#[test]
fn ranked_snapshot_still_caps_at_twenty_and_matches_all_prefix_order() {
    let mut state = EngineState::new(Arc::new(many_symbol_markets(30)));
    for idx in 0..30 {
        seed_quotes_for_symbol(&mut state, &format!("SYM{idx:02}"), 20_000, 20_000);
    }

    let all_rows = state.ranked_snapshot_all(20_050, 2_500).to_vec();
    let top_rows = state.ranked_snapshot(20_050, 2_500).to_vec();

    assert_eq!(all_rows.len(), 60);
    assert_eq!(top_rows.len(), 20);
    for (top, all) in top_rows.iter().zip(all_rows.iter().take(20)) {
        assert_eq!(top.route_id, all.route_id);
        assert_eq!(top.symbol, all.symbol);
    }
}

#[test]
fn ranked_snapshot_all_and_top_twenty_apply_same_stale_filter_before_truncation() {
    let mut state = EngineState::new(Arc::new(many_symbol_markets(8)));
    for idx in 0..4 {
        seed_quotes_for_symbol(&mut state, &format!("SYM{idx:02}"), 50_000, 50_000);
    }
    for idx in 4..8 {
        seed_quotes_for_symbol(&mut state, &format!("SYM{idx:02}"), 40_000, 50_000);
    }

    let all_rows = state.ranked_snapshot_all(51_000, 2_000).to_vec();
    let top_rows = state.ranked_snapshot(51_000, 2_000).to_vec();

    assert_eq!(all_rows.len(), 8);
    assert_eq!(top_rows.len(), 8);
    assert!(
        all_rows
            .iter()
            .all(|row| matches!(row.symbol.as_str(), "SYM00" | "SYM01" | "SYM02" | "SYM03"))
    );
    for (top, all) in top_rows.iter().zip(all_rows.iter()) {
        assert_eq!(top.route_id, all.route_id);
        assert_eq!(top.symbol, all.symbol);
    }
}

#[test]
fn disabled_exchange_rows_disappear_and_late_events_are_ignored_until_reenabled() {
    let mut state = EngineState::new(Arc::new(sample_markets()));

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
    assert_eq!(state.ranked_snapshot(1_000, 2_500).len(), 2);

    state.ingest_event(MarketEvent::ExchangeDisabled(Exchange::Aster), 1_010);
    assert!(state.ranked_snapshot(1_010, 2_500).is_empty());

    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 102.0, 2.0, 102.3, 2.0, 1_020)),
        1_020,
    );
    assert!(state.ranked_snapshot(1_020, 2_500).is_empty());

    state.ingest_event(MarketEvent::ExchangeEnabled(Exchange::Aster), 1_030);
    state.ingest_event(
        MarketEvent::Quote(quote(Exchange::Aster, "BTC", 102.0, 2.0, 102.3, 2.0, 1_040)),
        1_040,
    );

    let rows = state.ranked_snapshot(1_040, 2_500);
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|row| row.buy_ex == Exchange::Lighter || row.sell_ex == Exchange::Lighter)
    );
}

#[test]
fn disabling_one_exchange_preserves_routes_between_remaining_exchanges() {
    let mut state = EngineState::new(Arc::new(three_way_markets()));

    for (exchange, bid_px, ask_px) in [
        (Exchange::Lighter, 99.4, 100.0),
        (Exchange::Aster, 101.1, 101.4),
        (Exchange::Extended, 100.6, 100.9),
    ] {
        state.ingest_event(
            MarketEvent::Quote(quote(exchange, "BTC", bid_px, 2.0, ask_px, 2.0, 1_000)),
            1_000,
        );
    }

    assert_eq!(state.ranked_snapshot_all(1_000, 2_500).len(), 6);

    state.ingest_event(MarketEvent::ExchangeDisabled(Exchange::Aster), 1_010);

    let rows = state.ranked_snapshot_all(1_010, 2_500);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.buy_ex != Exchange::Aster
            && row.sell_ex != Exchange::Aster
            && matches!(
                (row.buy_ex, row.sell_ex),
                (Exchange::Lighter, Exchange::Extended) | (Exchange::Extended, Exchange::Lighter)
            )
    }));
}
