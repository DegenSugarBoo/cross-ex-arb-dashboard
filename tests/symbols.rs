use cross_ex_arb::discovery::{
    ApexPerpetualContract, ApexSymbolsResponse, AsterSymbol, BinanceSymbol, BybitInstrument,
    ExtendedMarket, GrvtInstrument, LighterOrderBook, derive_base_from_grvt_instrument,
    is_apex_perp_trading, is_aster_perp_trading, is_binance_usdt_perp_trading,
    is_bybit_usdt_linear_perp_trading, is_extended_perp_trading, is_grvt_perp_active,
    is_lighter_perp_active, normalize_apex_symbol_base, normalize_base,
};

#[test]
fn normalize_base_uppercases_and_trims() {
    assert_eq!(normalize_base(" btc "), "BTC");
    assert_eq!(normalize_base("Eth"), "ETH");
}

#[test]
fn lighter_filter_accepts_only_active_perp() {
    let active_perp = LighterOrderBook {
        symbol: "BTC".to_owned(),
        market_id: 1,
        market_type: "perp".to_owned(),
        status: "active".to_owned(),
        taker_fee: 0.03,
        maker_fee: 0.0,
    };

    let inactive = LighterOrderBook {
        status: "paused".to_owned(),
        ..active_perp.clone()
    };

    let non_perp = LighterOrderBook {
        market_type: "spot".to_owned(),
        ..active_perp.clone()
    };

    assert!(is_lighter_perp_active(&active_perp));
    assert!(!is_lighter_perp_active(&inactive));
    assert!(!is_lighter_perp_active(&non_perp));
}

#[test]
fn aster_filter_requires_perpetual_usdt_trading() {
    let valid = AsterSymbol {
        symbol: "BTCUSDT".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
        contract_type: "PERPETUAL".to_owned(),
        status: "TRADING".to_owned(),
    };

    let wrong_quote = AsterSymbol {
        quote_asset: "USD".to_owned(),
        ..valid.clone()
    };

    let wrong_type = AsterSymbol {
        contract_type: "FUTURE".to_owned(),
        ..valid.clone()
    };

    assert!(is_aster_perp_trading(&valid));
    assert!(!is_aster_perp_trading(&wrong_quote));
    assert!(!is_aster_perp_trading(&wrong_type));
}

#[test]
fn binance_filter_requires_perpetual_usdt_trading() {
    let valid = BinanceSymbol {
        symbol: "BTCUSDT".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
        contract_type: "PERPETUAL".to_owned(),
        status: "TRADING".to_owned(),
    };

    let wrong_quote = BinanceSymbol {
        quote_asset: "BUSD".to_owned(),
        ..valid.clone()
    };

    let wrong_type = BinanceSymbol {
        contract_type: "CURRENT_QUARTER".to_owned(),
        ..valid.clone()
    };

    let halted = BinanceSymbol {
        status: "BREAK".to_owned(),
        ..valid.clone()
    };

    assert!(is_binance_usdt_perp_trading(&valid));
    assert!(!is_binance_usdt_perp_trading(&wrong_quote));
    assert!(!is_binance_usdt_perp_trading(&wrong_type));
    assert!(!is_binance_usdt_perp_trading(&halted));
}

#[test]
fn bybit_filter_requires_linear_perpetual_trading_usdt() {
    let valid = BybitInstrument {
        symbol: "BTCUSDT".to_owned(),
        base_coin: "BTC".to_owned(),
        quote_coin: "USDT".to_owned(),
        contract_type: Some("LinearPerpetual".to_owned()),
        status: Some("Trading".to_owned()),
        is_pre_listing: Some(false),
    };

    let wrong_quote = BybitInstrument {
        quote_coin: "USDC".to_owned(),
        ..valid.clone()
    };

    let wrong_type = BybitInstrument {
        contract_type: Some("LinearFutures".to_owned()),
        ..valid.clone()
    };

    let halted = BybitInstrument {
        status: Some("Settled".to_owned()),
        ..valid.clone()
    };

    let pre_listing = BybitInstrument {
        is_pre_listing: Some(true),
        ..valid.clone()
    };

    assert!(is_bybit_usdt_linear_perp_trading(&valid));
    assert!(!is_bybit_usdt_linear_perp_trading(&wrong_quote));
    assert!(!is_bybit_usdt_linear_perp_trading(&wrong_type));
    assert!(!is_bybit_usdt_linear_perp_trading(&halted));
    assert!(!is_bybit_usdt_linear_perp_trading(&pre_listing));
}

#[test]
fn extended_filter_accepts_active_markets() {
    let active = ExtendedMarket {
        name: "BTC-USD".to_owned(),
        asset_name: Some("BTC".to_owned()),
        active: Some(true),
        status: Some("ACTIVE".to_owned()),
    };

    let disabled = ExtendedMarket {
        active: Some(false),
        ..active.clone()
    };

    let halted = ExtendedMarket {
        status: Some("HALTED".to_owned()),
        ..active.clone()
    };

    assert!(is_extended_perp_trading(&active));
    assert!(!is_extended_perp_trading(&disabled));
    assert!(!is_extended_perp_trading(&halted));
}

#[test]
fn grvt_filter_and_base_derivation_work() {
    let instrument = GrvtInstrument {
        symbol: Some("BTC_USDT_Perp".to_owned()),
        instrument_type: Some("PERPETUAL".to_owned()),
        status: Some("ACTIVE".to_owned()),
        active: Some(true),
        is_active: None,
    };

    let inactive = GrvtInstrument {
        active: Some(false),
        ..instrument.clone()
    };

    assert!(is_grvt_perp_active(&instrument));
    assert!(!is_grvt_perp_active(&inactive));
    assert_eq!(
        derive_base_from_grvt_instrument("BTC_USDT_Perp"),
        Some("BTC".to_owned())
    );
}

#[test]
fn apex_filter_and_base_normalization_work() {
    let contract = ApexPerpetualContract {
        cross_symbol_name: Some("BTCUSDT".to_owned()),
        symbol: Some("BTC-USDT".to_owned()),
        base_token_id: Some("BTC".to_owned()),
        enable_trade: Some(true),
        enable_display: Some(true),
        status: Some("TRADING".to_owned()),
        contract_type: Some("UNKNOWN_CONTRACT_TYPE".to_owned()),
    };

    let disabled = ApexPerpetualContract {
        enable_trade: Some(false),
        ..contract.clone()
    };

    let fallback_parse = ApexPerpetualContract {
        base_token_id: None,
        cross_symbol_name: Some("ETHUSDT".to_owned()),
        symbol: Some("ETH-USDT".to_owned()),
        ..contract.clone()
    };

    assert!(is_apex_perp_trading(&contract));
    assert!(!is_apex_perp_trading(&disabled));
    assert_eq!(
        normalize_apex_symbol_base(&contract),
        Some("BTC".to_owned())
    );
    assert_eq!(
        normalize_apex_symbol_base(&fallback_parse),
        Some("ETH".to_owned())
    );
}

#[test]
fn apex_symbols_response_reads_contract_config_payloads() {
    let payload = r#"{
        "data": {
            "contractConfig": {
                "perpetualContract": [
                    {
                        "crossSymbolName": "BTCUSDT",
                        "symbol": "BTC-USDT",
                        "baseTokenId": "BTC",
                        "enableTrade": true,
                        "enableDisplay": true,
                        "contractType": "UNKNOWN_CONTRACT_TYPE"
                    }
                ]
            }
        }
    }"#;

    let parsed: ApexSymbolsResponse = serde_json::from_str(payload).expect("apex symbols payload");
    let contracts = parsed.contracts();

    assert_eq!(contracts.len(), 1);
    assert_eq!(contracts[0].cross_symbol_name.as_deref(), Some("BTCUSDT"));
    assert_eq!(contracts[0].symbol.as_deref(), Some("BTC-USDT"));
}
