use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::engine::EngineState;
use cross_ex_arb::model::{Exchange, MarketEvent, MarketMeta, QuoteUpdate};

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
        symbol_base: symbol.to_owned(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms: recv_ts_ms,
        recv_ts_ms,
    }
}

#[test]
fn mixed_quotes_generate_ranked_single_row_per_symbol() {
    let mut state = EngineState::new(sample_markets());

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

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].symbol, "BTC");
    assert_eq!(rows[1].symbol, "ETH");
    assert_eq!(rows[0].buy_ex, Exchange::Lighter);
    assert_eq!(rows[0].sell_ex, Exchange::Aster);
    assert_eq!(rows[1].buy_ex, Exchange::Aster);
    assert_eq!(rows[1].sell_ex, Exchange::Lighter);
}

#[test]
fn stale_leg_suppression_hides_row() {
    let mut state = EngineState::new(sample_markets());

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
fn reconnect_continuity_keeps_state_after_one_side_resumes() {
    let mut state = EngineState::new(sample_markets());

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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol, "BTC");
    assert_eq!(rows[0].buy_ask, 99.9);
}
