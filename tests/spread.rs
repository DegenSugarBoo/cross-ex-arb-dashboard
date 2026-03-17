use cross_ex_arb::engine::{
    DirectionRowInput, compute_direction_row, select_best_direction, sort_rows,
};
use cross_ex_arb::model::{ArbRow, Exchange, QuoteUpdate};

fn quote(
    exchange: Exchange,
    symbol: &str,
    bid_px: f64,
    bid_qty: f64,
    ask_px: f64,
    ask_qty: f64,
) -> QuoteUpdate {
    QuoteUpdate {
        exchange,
        symbol_base: symbol.to_owned().into(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms: 0,
        recv_ts_ms: 1_000,
    }
}

#[test]
fn spread_formula_and_size_notional_are_correct() {
    let buy = quote(Exchange::Lighter, "BTC", 99.0, 5.0, 100.0, 3.0);
    let sell = quote(Exchange::Aster, "BTC", 101.0, 2.0, 102.0, 4.0);

    let row = compute_direction_row(DirectionRowInput {
        route_id: 7,
        symbol: "BTC",
        buy_ex: Exchange::Lighter,
        sell_ex: Exchange::Aster,
        buy_quote: &buy,
        sell_quote: &sell,
        buy_taker_fee_pct: 0.04,
        sell_taker_fee_pct: 0.04,
        now_ms: 1_200,
    })
    .expect("row should be computed");

    assert!((row.raw_spread_bps - 100.0).abs() < 1e-9);
    assert!((row.net_spread_bps - 84.0).abs() < 1e-9);
    assert!((row.max_base_qty - 2.0).abs() < 1e-9);
    assert!((row.max_usd_notional - 200.0).abs() < 1e-9);
    assert_eq!(row.age_ms, 200);
    assert_eq!(row.route_id, 7);
}

#[test]
fn best_direction_selection_prefers_higher_net_then_raw() {
    let buy_lighter = quote(Exchange::Lighter, "ETH", 99.0, 5.0, 100.0, 5.0);
    let sell_aster = quote(Exchange::Aster, "ETH", 100.5, 5.0, 101.0, 5.0);

    let buy_aster = quote(Exchange::Aster, "ETH", 98.0, 5.0, 99.0, 5.0);
    let sell_lighter = quote(Exchange::Lighter, "ETH", 99.5, 5.0, 100.0, 5.0);

    let row_a = compute_direction_row(DirectionRowInput {
        route_id: 10,
        symbol: "ETH",
        buy_ex: Exchange::Lighter,
        sell_ex: Exchange::Aster,
        buy_quote: &buy_lighter,
        sell_quote: &sell_aster,
        buy_taker_fee_pct: 0.04,
        sell_taker_fee_pct: 0.04,
        now_ms: 1_000,
    });

    let row_b = compute_direction_row(DirectionRowInput {
        route_id: 11,
        symbol: "ETH",
        buy_ex: Exchange::Aster,
        sell_ex: Exchange::Lighter,
        buy_quote: &buy_aster,
        sell_quote: &sell_lighter,
        buy_taker_fee_pct: 0.04,
        sell_taker_fee_pct: 0.04,
        now_ms: 1_000,
    });

    let best = select_best_direction(row_a, row_b).expect("one direction should win");
    assert_eq!(best.buy_ex, Exchange::Aster);
    assert_eq!(best.sell_ex, Exchange::Lighter);
    assert_eq!(best.route_id, 11);
}

#[test]
fn sorting_uses_net_then_raw_then_symbol() {
    let mut rows = vec![
        ArbRow {
            route_id: 1,
            symbol: "ETH".to_owned().into(),
            buy_ex: Exchange::Lighter,
            sell_ex: Exchange::Aster,
            buy_ask: 1.0,
            sell_bid: 1.0,
            raw_spread_bps: 100.0,
            net_spread_bps: 50.0,
            buy_funding_rate: None,
            sell_funding_rate: None,
            buy_funding_stale: false,
            sell_funding_stale: false,
            max_base_qty: 1.0,
            max_usd_notional: 1.0,
            age_ms: 1,
            latency_ms: 1,
        },
        ArbRow {
            route_id: 2,
            symbol: "BTC".to_owned().into(),
            buy_ex: Exchange::Lighter,
            sell_ex: Exchange::Aster,
            buy_ask: 1.0,
            sell_bid: 1.0,
            raw_spread_bps: 90.0,
            net_spread_bps: 50.0,
            buy_funding_rate: None,
            sell_funding_rate: None,
            buy_funding_stale: false,
            sell_funding_stale: false,
            max_base_qty: 1.0,
            max_usd_notional: 1.0,
            age_ms: 1,
            latency_ms: 1,
        },
        ArbRow {
            route_id: 3,
            symbol: "SOL".to_owned().into(),
            buy_ex: Exchange::Lighter,
            sell_ex: Exchange::Aster,
            buy_ask: 1.0,
            sell_bid: 1.0,
            raw_spread_bps: 80.0,
            net_spread_bps: 70.0,
            buy_funding_rate: None,
            sell_funding_rate: None,
            buy_funding_stale: false,
            sell_funding_stale: false,
            max_base_qty: 1.0,
            max_usd_notional: 1.0,
            age_ms: 1,
            latency_ms: 1,
        },
    ];

    sort_rows(&mut rows);

    assert_eq!(rows[0].symbol, "SOL");
    assert_eq!(rows[1].symbol, "ETH");
    assert_eq!(rows[2].symbol, "BTC");
}
