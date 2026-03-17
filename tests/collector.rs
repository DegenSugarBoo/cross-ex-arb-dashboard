use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use compact_str::CompactString;
use cross_ex_arb::collector::{
    CollectorEnvelope, CollectorRecordType, CollectorService, CollectorSettings,
};
use cross_ex_arb::config::{AppConfig, CliArgs, CollectorCompression};
use cross_ex_arb::discovery::SymbolMarkets;
use cross_ex_arb::model::{Exchange, FundingUpdate, MarketEvent, MarketMeta, QuoteUpdate};
use tokio::sync::mpsc;

fn sample_markets() -> SymbolMarkets {
    let mut markets = HashMap::new();
    for symbol in ["BTC", "ETH"] {
        markets.insert(
            symbol.to_owned(),
            vec![
                MarketMeta {
                    exchange: Exchange::Lighter,
                    symbol_base: symbol.to_owned(),
                    exchange_symbol: symbol.to_owned(),
                    market_id: Some(1),
                    taker_fee_pct: 0.03,
                    maker_fee_pct: 0.0,
                },
                MarketMeta {
                    exchange: Exchange::Aster,
                    symbol_base: symbol.to_owned(),
                    exchange_symbol: format!("{symbol}USDT"),
                    market_id: None,
                    taker_fee_pct: 0.04,
                    maker_fee_pct: 0.0,
                },
            ],
        );
    }
    markets
}

fn collector_settings(root: &Path) -> CollectorSettings {
    CollectorSettings {
        data_root: root.to_path_buf(),
        compression: CollectorCompression::None,
        bootstrap_timeout_ms: 5_000,
        write_buffer: 1_024,
        flush_interval_ms: 20,
        max_open_files: 64,
    }
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn load_records(root: &Path) -> Vec<CollectorEnvelope> {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    files.sort();

    let mut records = Vec::new();
    for file in files {
        let Ok(raw) = fs::read_to_string(&file) else {
            continue;
        };
        for line in raw.lines().filter(|line| !line.trim().is_empty()) {
            if let Ok(record) = serde_json::from_str::<CollectorEnvelope>(line) {
                records.push(record);
            }
        }
    }
    records
}

#[tokio::test]
async fn collect_mode_persists_events_and_trade_unsupported_once_per_tuple() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let markets = sample_markets();
    let settings = collector_settings(temp_dir.path());
    let (tx, rx) = mpsc::channel::<MarketEvent>(32);

    let mut collector = CollectorService::new(settings, &markets);
    let run_handle = tokio::spawn(async move {
        collector
            .run_until_shutdown(rx, std::future::pending::<()>())
            .await
    });

    tx.send(MarketEvent::Quote(QuoteUpdate {
        exchange: Exchange::Lighter,
        symbol_base: CompactString::from("BTC"),
        bid_px: 100.0,
        bid_qty: 2.0,
        ask_px: 100.2,
        ask_qty: 3.0,
        exch_ts_ms: 1_700_000_001_000,
        recv_ts_ms: 1_700_000_001_005,
    }))
    .await
    .expect("send quote");

    tx.send(MarketEvent::Funding(FundingUpdate {
        exchange: Exchange::Aster,
        symbol_base: CompactString::from("BTC"),
        funding_rate: 0.00012,
        next_funding_ts_ms: Some(1_700_000_361_000),
        recv_ts_ms: 1_700_000_001_006,
        stale_after_ms: None,
    }))
    .await
    .expect("send funding");

    // Simulate extra post-bootstrap events; markers must remain deduplicated.
    tx.send(MarketEvent::Quote(QuoteUpdate {
        exchange: Exchange::Lighter,
        symbol_base: CompactString::from("ETH"),
        bid_px: 50.0,
        bid_qty: 5.0,
        ask_px: 50.3,
        ask_qty: 6.0,
        exch_ts_ms: 1_700_000_001_100,
        recv_ts_ms: 1_700_000_001_110,
    }))
    .await
    .expect("send quote 2");
    tx.send(MarketEvent::Funding(FundingUpdate {
        exchange: Exchange::Aster,
        symbol_base: CompactString::from("ETH"),
        funding_rate: -0.00001,
        next_funding_ts_ms: Some(1_700_000_361_100),
        recv_ts_ms: 1_700_000_001_120,
        stale_after_ms: None,
    }))
    .await
    .expect("send funding 2");
    drop(tx);

    let stats = run_handle.await.expect("join").expect("collector run");
    assert!(stats.written_records >= 8);
    assert_eq!(stats.dropped_records, 0);

    let records = load_records(temp_dir.path());
    assert!(!records.is_empty());
    assert!(
        records
            .iter()
            .any(|record| record.record_type == CollectorRecordType::Quote)
    );
    assert!(
        records
            .iter()
            .any(|record| record.record_type == CollectorRecordType::Funding)
    );

    let unsupported: Vec<_> = records
        .iter()
        .filter(|record| record.record_type == CollectorRecordType::TradeUnsupported)
        .collect();
    assert_eq!(unsupported.len(), 4, "one marker per discovered tuple");

    let marker_pairs: HashSet<(String, String)> = unsupported
        .iter()
        .map(|record| (record.symbol.clone(), record.exchange.clone()))
        .collect();
    assert_eq!(
        marker_pairs.len(),
        4,
        "no duplicate trade_unsupported markers"
    );

    let mut seqs: Vec<u64> = records.iter().map(|record| record.global_seq).collect();
    seqs.sort_unstable();
    assert!(seqs.windows(2).all(|window| window[0] < window[1]));
}

#[test]
fn collect_mode_flag_disables_ui_launch_path() {
    let cli = CliArgs::parse_from(["cross-ex-arb", "--collect-mode"]);
    let config = AppConfig::from(cli);
    assert!(config.collect_mode);
    assert!(!config.launches_ui());
}
