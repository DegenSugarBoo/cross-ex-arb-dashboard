use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{self, MissedTickBehavior};

use crate::config::CollectorCompression;
use crate::discovery::SymbolMarkets;
use crate::model::{Exchange, MarketEvent, now_ms};

const SCHEMA_VERSION: u32 = 1;
const WRITE_RETRY_ATTEMPTS: usize = 3;
const TRADE_UNSUPPORTED_REASON: &str = "trade stream not integrated in collector v1";
const TELEMETRY_LOG_INTERVAL_MS: i64 = 10_000;
const MIN_OPEN_FILE_LIMIT: usize = 16;
const FD_HEADROOM_RESERVE: usize = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectorRecordType {
    Quote,
    Funding,
    Trade,
    TradeUnsupported,
}

impl CollectorRecordType {
    fn as_path_segment(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::Funding => "funding",
            Self::Trade => "trade",
            Self::TradeUnsupported => "trade_unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CollectorPayload {
    Quote {
        bid_px: f64,
        bid_qty: f64,
        ask_px: f64,
        ask_qty: f64,
    },
    Funding {
        funding_rate: f64,
        next_funding_ts_ms: Option<i64>,
    },
    Trade {
        trade_id: Option<String>,
        trade_ts_ms: i64,
        trade_px: f64,
        trade_qty: f64,
        trade_side: String,
        trade_is_maker: Option<bool>,
        trade_notional_usd: Option<f64>,
    },
    TradeUnsupported {
        trades_supported: bool,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectorEnvelope {
    pub schema_version: u32,
    pub record_type: CollectorRecordType,
    pub global_seq: u64,
    pub exchange: String,
    pub symbol: String,
    pub symbol_base: String,
    pub exchange_symbol: Option<String>,
    pub exchange_ts_ms: i64,
    pub recv_ts_ms: i64,
    pub collected_at_ms: i64,
    #[serde(flatten)]
    pub payload: CollectorPayload,
}

#[derive(Debug, Clone)]
pub struct CollectorSettings {
    pub data_root: PathBuf,
    pub compression: CollectorCompression,
    pub bootstrap_timeout_ms: u64,
    pub write_buffer: usize,
    pub flush_interval_ms: u64,
    pub max_open_files: usize,
}

#[derive(Debug, Clone, Default)]
pub struct CollectorStats {
    pub written_records: u64,
    pub dropped_records: u64,
    pub write_errors: u64,
    pub gate_opened_by_timeout: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    symbol: String,
    exchange: String,
    record_type: CollectorRecordType,
    yyyy_mm_dd: String,
    hour: u8,
}

#[derive(Debug, Clone)]
struct DiscoveredTuple {
    symbol: String,
    exchange: Exchange,
    exchange_symbol: String,
}

struct WriterState {
    writer: CodecWriter,
    path: PathBuf,
    last_flush_ms: i64,
    last_write_ms: i64,
}

enum CodecWriter {
    Plain(BufWriter<File>),
    Zstd(zstd::stream::write::Encoder<'static, BufWriter<File>>),
    Lz4(lz4::Encoder<BufWriter<File>>),
}

impl CodecWriter {
    fn new(file: File, compression: CollectorCompression, write_buffer: usize) -> io::Result<Self> {
        let capacity = write_buffer.max(1);
        let buffered = BufWriter::with_capacity(capacity, file);

        match compression {
            CollectorCompression::None => Ok(Self::Plain(buffered)),
            CollectorCompression::Zstd => {
                let mut encoder =
                    zstd::stream::write::Encoder::new(buffered, 6).map_err(io::Error::other)?;
                encoder.include_checksum(true).map_err(io::Error::other)?;
                Ok(Self::Zstd(encoder))
            }
            CollectorCompression::Lz4hc => {
                let encoder = lz4::EncoderBuilder::new()
                    .level(16)
                    .build(buffered)
                    .map_err(io::Error::other)?;
                Ok(Self::Lz4(encoder))
            }
        }
    }

    fn write_line(&mut self, line: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain(writer) => {
                writer.write_all(line)?;
                writer.write_all(b"\n")
            }
            Self::Zstd(writer) => {
                writer.write_all(line)?;
                writer.write_all(b"\n")
            }
            Self::Lz4(writer) => {
                writer.write_all(line)?;
                writer.write_all(b"\n")
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Zstd(writer) => writer.flush(),
            Self::Lz4(writer) => writer.flush(),
        }
    }

    fn finish(self) -> io::Result<()> {
        match self {
            Self::Plain(mut writer) => writer.flush(),
            Self::Zstd(mut writer) => {
                writer.flush()?;
                writer.finish().map_err(io::Error::other)?;
                Ok(())
            }
            Self::Lz4(writer) => {
                let (mut inner, result) = writer.finish();
                inner.flush()?;
                result.map_err(io::Error::other)?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BootstrapGate {
    expected: HashSet<Exchange>,
    ready: HashSet<Exchange>,
    deadline_ms: i64,
    open: bool,
    opened_by_timeout: bool,
}

impl BootstrapGate {
    fn new(markets: &SymbolMarkets, timeout_ms: u64, start_ms: i64) -> Self {
        let expected: HashSet<Exchange> = markets
            .values()
            .flat_map(|entries| entries.iter().map(|meta| meta.exchange))
            .collect();
        let open = expected.is_empty();

        Self {
            expected,
            ready: HashSet::new(),
            deadline_ms: start_ms.saturating_add(timeout_ms as i64),
            open,
            opened_by_timeout: false,
        }
    }

    fn mark_ready(&mut self, exchange: Exchange) -> bool {
        if self.open {
            return false;
        }

        if self.expected.contains(&exchange) {
            self.ready.insert(exchange);
        }
        self.try_open_by_readiness()
    }

    fn maybe_open_on_timeout(&mut self, now_ms: i64) -> bool {
        if self.open || now_ms < self.deadline_ms {
            return false;
        }
        self.open = true;
        self.opened_by_timeout = true;
        true
    }

    fn force_open(&mut self) -> bool {
        if self.open {
            return false;
        }
        self.open = true;
        true
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn missing_exchanges(&self) -> Vec<Exchange> {
        let mut missing: Vec<Exchange> = self.expected.difference(&self.ready).copied().collect();
        missing.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        missing
    }

    fn try_open_by_readiness(&mut self) -> bool {
        if self.open {
            return false;
        }
        if self.expected.is_subset(&self.ready) {
            self.open = true;
            return true;
        }
        false
    }
}

pub struct CollectorService {
    settings: CollectorSettings,
    effective_max_open_files: usize,
    bootstrap_gate: BootstrapGate,
    gate_open_handled: bool,
    discovered_tuples: Vec<DiscoveredTuple>,
    emitted_markers: HashSet<(String, Exchange)>,
    exchange_symbol_lookup: HashMap<String, HashMap<Exchange, String>>,
    pre_gate_buffer: VecDeque<MarketEvent>,
    writers: HashMap<PartitionKey, WriterState>,
    global_seq: u64,
    stats: CollectorStats,
    last_telemetry_log_ms: i64,
}

impl CollectorService {
    pub fn new(settings: CollectorSettings, markets: &SymbolMarkets) -> Self {
        let mut discovered_tuples = Vec::new();
        let mut emitted_pairs = HashSet::new();
        let mut exchange_symbol_lookup: HashMap<String, HashMap<Exchange, String>> = HashMap::new();

        for (symbol, metas) in markets {
            let symbol_lookup = exchange_symbol_lookup.entry(symbol.clone()).or_default();
            for meta in metas {
                symbol_lookup
                    .entry(meta.exchange)
                    .or_insert_with(|| meta.exchange_symbol.clone());
                if emitted_pairs.insert((symbol.clone(), meta.exchange)) {
                    discovered_tuples.push(DiscoveredTuple {
                        symbol: symbol.clone(),
                        exchange: meta.exchange,
                        exchange_symbol: meta.exchange_symbol.clone(),
                    });
                }
            }
        }

        discovered_tuples.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| left.exchange.as_str().cmp(right.exchange.as_str()))
        });

        let start_ms = now_ms();
        let bootstrap_gate = BootstrapGate::new(markets, settings.bootstrap_timeout_ms, start_ms);
        let effective_max_open_files = compute_effective_max_open_files(settings.max_open_files);
        tracing::info!(
            configured_max_open_files = settings.max_open_files,
            effective_max_open_files,
            "collector open-file budget"
        );

        Self {
            settings,
            effective_max_open_files,
            bootstrap_gate,
            gate_open_handled: false,
            discovered_tuples,
            emitted_markers: HashSet::new(),
            exchange_symbol_lookup,
            pre_gate_buffer: VecDeque::new(),
            writers: HashMap::new(),
            global_seq: 0,
            stats: CollectorStats::default(),
            last_telemetry_log_ms: start_ms,
        }
    }

    pub fn stats(&self) -> &CollectorStats {
        &self.stats
    }

    pub async fn run_until_shutdown<F>(
        &mut self,
        mut event_rx: mpsc::Receiver<MarketEvent>,
        shutdown: F,
    ) -> Result<CollectorStats>
    where
        F: Future<Output = ()>,
    {
        let flush_interval_ms = self.settings.flush_interval_ms.max(100);
        let mut flush_tick = time::interval(Duration::from_millis(flush_interval_ms));
        flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(shutdown);

        if self.bootstrap_gate.is_open() {
            if let Err(err) = self.on_gate_open("no discovery tuples require bootstrap wait") {
                tracing::error!(error = %err, "collector gate-open initialization failed");
            }
        }

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("collector shutdown signal received");
                    break;
                }
                maybe_event = event_rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            if let Err(err) = self.handle_market_event(event) {
                                tracing::error!(error = %err, "collector event handling failed");
                            }
                        }
                        None => {
                            tracing::info!("collector channel closed; draining and shutting down");
                            break;
                        }
                    }
                }
                _ = flush_tick.tick() => {
                    if let Err(err) = self.on_periodic_tick() {
                        tracing::error!(error = %err, "collector periodic tick failed");
                    }
                }
            }
        }

        if self.bootstrap_gate.force_open() {
            if let Err(err) = self.on_gate_open("forced open during shutdown") {
                tracing::error!(error = %err, "collector gate-open shutdown drain failed");
            }
        }
        if let Err(err) = self.flush_all_writers() {
            tracing::error!(error = %err, "collector flush during shutdown failed");
        }
        self.close_all_writers();

        tracing::info!(
            written_records = self.stats.written_records,
            dropped_records = self.stats.dropped_records,
            write_errors = self.stats.write_errors,
            gate_opened_by_timeout = self.stats.gate_opened_by_timeout,
            "collector shutdown complete"
        );

        Ok(self.stats.clone())
    }

    fn handle_market_event(&mut self, event: MarketEvent) -> Result<()> {
        if self.bootstrap_gate.is_open() {
            return self.write_market_event(event);
        }

        let exchange = match &event {
            MarketEvent::Quote(update) => update.exchange,
            MarketEvent::Funding(update) => update.exchange,
        };
        self.bootstrap_gate.mark_ready(exchange);
        self.push_pre_gate_event(event);

        if self.bootstrap_gate.is_open() {
            self.on_gate_open("all expected exchanges observed first event")?;
        }

        Ok(())
    }

    fn push_pre_gate_event(&mut self, event: MarketEvent) {
        let cap = self.settings.write_buffer.max(1);
        if self.pre_gate_buffer.len() >= cap {
            let _ = self.pre_gate_buffer.pop_front();
            self.stats.dropped_records = self.stats.dropped_records.saturating_add(1);
            tracing::warn!(
                cap,
                "collector bootstrap buffer overflow, dropping oldest event"
            );
        }
        self.pre_gate_buffer.push_back(event);
    }

    fn on_periodic_tick(&mut self) -> Result<()> {
        let tick_now_ms = now_ms();

        if !self.bootstrap_gate.is_open() && self.bootstrap_gate.maybe_open_on_timeout(tick_now_ms)
        {
            self.stats.gate_opened_by_timeout = true;
            self.on_gate_open("bootstrap timeout elapsed")?;
        }

        if self.bootstrap_gate.is_open() {
            self.flush_all_writers()?;
        }

        if tick_now_ms - self.last_telemetry_log_ms >= TELEMETRY_LOG_INTERVAL_MS {
            self.last_telemetry_log_ms = tick_now_ms;
            tracing::info!(
                gate_open = self.bootstrap_gate.is_open(),
                pre_gate_buffer_len = self.pre_gate_buffer.len(),
                open_writers = self.writers.len(),
                written_records = self.stats.written_records,
                dropped_records = self.stats.dropped_records,
                write_errors = self.stats.write_errors,
                "collector telemetry"
            );
        }

        Ok(())
    }

    fn on_gate_open(&mut self, reason: &str) -> Result<()> {
        if self.gate_open_handled {
            return Ok(());
        }
        self.gate_open_handled = true;

        let missing = self
            .bootstrap_gate
            .missing_exchanges()
            .into_iter()
            .map(|exchange| exchange.as_str().to_owned())
            .collect::<Vec<_>>();
        tracing::info!(
            reason,
            missing = ?missing,
            buffered = self.pre_gate_buffer.len(),
            "collector bootstrap gate opened"
        );

        while let Some(event) = self.pre_gate_buffer.pop_front() {
            if let Err(err) = self.write_market_event(event) {
                tracing::warn!(error = %err, "collector failed to write buffered bootstrap event");
            }
        }
        self.emit_trade_unsupported_markers();
        Ok(())
    }

    fn emit_trade_unsupported_markers(&mut self) {
        let tuples = self.discovered_tuples.clone();
        for tuple in tuples {
            if !self
                .emitted_markers
                .insert((tuple.symbol.clone(), tuple.exchange))
            {
                continue;
            }
            let symbol_for_log = tuple.symbol.clone();
            let exchange_for_log = tuple.exchange;
            let ts_ms = now_ms();
            let envelope = CollectorEnvelope {
                schema_version: SCHEMA_VERSION,
                record_type: CollectorRecordType::TradeUnsupported,
                global_seq: self.next_global_seq(),
                exchange: tuple.exchange.as_str().to_owned(),
                symbol: tuple.symbol.clone(),
                symbol_base: tuple.symbol,
                exchange_symbol: Some(tuple.exchange_symbol),
                exchange_ts_ms: ts_ms,
                recv_ts_ms: ts_ms,
                collected_at_ms: ts_ms,
                payload: CollectorPayload::TradeUnsupported {
                    trades_supported: false,
                    reason: TRADE_UNSUPPORTED_REASON.to_owned(),
                },
            };
            if let Err(err) = self.write_envelope(envelope) {
                tracing::warn!(
                    symbol = %symbol_for_log,
                    exchange = %exchange_for_log,
                    error = %err,
                    "failed to write trade_unsupported marker"
                );
            }
        }
    }

    fn write_market_event(&mut self, event: MarketEvent) -> Result<()> {
        let envelope = match event {
            MarketEvent::Quote(update) => CollectorEnvelope {
                schema_version: SCHEMA_VERSION,
                record_type: CollectorRecordType::Quote,
                global_seq: self.next_global_seq(),
                exchange: update.exchange.as_str().to_owned(),
                symbol: update.symbol_base.to_string(),
                symbol_base: update.symbol_base.to_string(),
                exchange_symbol: self.lookup_exchange_symbol(&update.symbol_base, update.exchange),
                exchange_ts_ms: update.exch_ts_ms,
                recv_ts_ms: update.recv_ts_ms,
                collected_at_ms: now_ms(),
                payload: CollectorPayload::Quote {
                    bid_px: update.bid_px,
                    bid_qty: update.bid_qty,
                    ask_px: update.ask_px,
                    ask_qty: update.ask_qty,
                },
            },
            MarketEvent::Funding(update) => CollectorEnvelope {
                schema_version: SCHEMA_VERSION,
                record_type: CollectorRecordType::Funding,
                global_seq: self.next_global_seq(),
                exchange: update.exchange.as_str().to_owned(),
                symbol: update.symbol_base.to_string(),
                symbol_base: update.symbol_base.to_string(),
                exchange_symbol: self.lookup_exchange_symbol(&update.symbol_base, update.exchange),
                exchange_ts_ms: update.recv_ts_ms,
                recv_ts_ms: update.recv_ts_ms,
                collected_at_ms: now_ms(),
                payload: CollectorPayload::Funding {
                    funding_rate: update.funding_rate,
                    next_funding_ts_ms: update.next_funding_ts_ms,
                },
            },
        };
        self.write_envelope(envelope)
    }

    fn lookup_exchange_symbol(&self, symbol_base: &str, exchange: Exchange) -> Option<String> {
        self.exchange_symbol_lookup
            .get(symbol_base)
            .and_then(|by_exchange| by_exchange.get(&exchange))
            .cloned()
    }

    fn next_global_seq(&mut self) -> u64 {
        self.global_seq = self.global_seq.saturating_add(1);
        self.global_seq
    }

    fn write_envelope(&mut self, envelope: CollectorEnvelope) -> Result<()> {
        let key = partition_key_from_envelope(&envelope)?;
        let line =
            serde_json::to_vec(&envelope).context("failed to serialize collector envelope")?;
        match self.write_line_with_retry(&key, &line) {
            Ok(()) => {
                self.stats.written_records = self.stats.written_records.saturating_add(1);
                Ok(())
            }
            Err(err) => {
                self.stats.dropped_records = self.stats.dropped_records.saturating_add(1);
                Err(err)
            }
        }
    }

    fn write_line_with_retry(&mut self, key: &PartitionKey, line: &[u8]) -> Result<()> {
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..WRITE_RETRY_ATTEMPTS {
            match self.try_write_line(key, line) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.stats.write_errors = self.stats.write_errors.saturating_add(1);
                    if is_too_many_open_files(&err) {
                        // Free multiple least-recently-used writers to relieve FD pressure.
                        for _ in 0..4 {
                            let _ = self.evict_one_writer();
                        }
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = WRITE_RETRY_ATTEMPTS,
                        path = %self.partition_path(key).display(),
                        error = %err,
                        "collector write attempt failed"
                    );
                    self.drop_writer(key);
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("collector write failed with unknown error")))
    }

    fn try_write_line(&mut self, key: &PartitionKey, line: &[u8]) -> Result<()> {
        self.ensure_writer(key)?;
        let now_ms = now_ms();
        let writer = self
            .writers
            .get_mut(key)
            .ok_or_else(|| anyhow!("writer missing after ensure"))?;
        writer.writer.write_line(line).with_context(|| {
            format!(
                "failed writing collector line to {}",
                writer.path.to_string_lossy()
            )
        })?;
        writer.last_write_ms = now_ms;
        Ok(())
    }

    fn ensure_writer(&mut self, key: &PartitionKey) -> Result<()> {
        if self.writers.contains_key(key) {
            return Ok(());
        }

        let max_open_files = self.effective_max_open_files.max(1);
        if self.writers.len() >= max_open_files {
            self.evict_one_writer()?;
        }

        let path = self.partition_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed creating collector directory {}",
                    parent.to_string_lossy()
                )
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed opening collector file {}", path.to_string_lossy()))?;

        let state = WriterState {
            writer: CodecWriter::new(
                file,
                self.settings.compression,
                self.settings.write_buffer.max(1),
            )
            .with_context(|| {
                format!(
                    "failed creating codec writer for {}",
                    path.to_string_lossy()
                )
            })?,
            path,
            last_flush_ms: now_ms(),
            last_write_ms: now_ms(),
        };

        self.writers.insert(key.clone(), state);
        Ok(())
    }

    fn evict_one_writer(&mut self) -> Result<()> {
        let evict_key = self
            .writers
            .iter()
            .min_by_key(|(_, state)| state.last_write_ms)
            .map(|(key, _)| key.clone());

        let Some(evict_key) = evict_key else {
            return Ok(());
        };
        self.drop_writer(&evict_key);
        Ok(())
    }

    fn drop_writer(&mut self, key: &PartitionKey) {
        if let Some(state) = self.writers.remove(key) {
            if let Err(err) = state.writer.finish() {
                tracing::warn!(
                    path = %state.path.display(),
                    error = %err,
                    "failed to finalize collector writer"
                );
            }
        }
    }

    fn flush_all_writers(&mut self) -> Result<()> {
        let mut failed_keys = Vec::new();
        let now_ms = now_ms();

        for (key, state) in &mut self.writers {
            if let Err(err) = state.writer.flush() {
                self.stats.write_errors = self.stats.write_errors.saturating_add(1);
                tracing::warn!(
                    path = %state.path.display(),
                    error = %err,
                    "collector writer flush failed"
                );
                failed_keys.push(key.clone());
            } else {
                state.last_flush_ms = now_ms;
            }
        }

        for key in failed_keys {
            self.drop_writer(&key);
        }
        Ok(())
    }

    fn close_all_writers(&mut self) {
        let keys: Vec<PartitionKey> = self.writers.keys().cloned().collect();
        for key in keys {
            self.drop_writer(&key);
        }
    }

    fn partition_path(&self, key: &PartitionKey) -> PathBuf {
        build_partition_path(&self.settings.data_root, key, self.settings.compression)
    }
}

fn partition_key_from_envelope(envelope: &CollectorEnvelope) -> Result<PartitionKey> {
    let dt = Utc
        .timestamp_millis_opt(envelope.collected_at_ms)
        .single()
        .ok_or_else(|| anyhow!("invalid collected_at_ms: {}", envelope.collected_at_ms))?;

    Ok(PartitionKey {
        symbol: envelope.symbol.clone(),
        exchange: envelope.exchange.clone(),
        record_type: envelope.record_type,
        yyyy_mm_dd: format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        hour: dt.hour() as u8,
    })
}

fn build_partition_path(
    data_root: &Path,
    key: &PartitionKey,
    compression: CollectorCompression,
) -> PathBuf {
    let symbol = sanitize_path_component(&key.symbol);
    let exchange = sanitize_path_component(&key.exchange.to_lowercase());
    let mut filename = format!("{:02}.jsonl", key.hour);
    if let Some(ext) = compression.file_extension() {
        filename.push('.');
        filename.push_str(ext);
    }

    data_root
        .join(symbol)
        .join(exchange)
        .join(key.record_type.as_path_segment())
        .join(&key.yyyy_mm_dd)
        .join(filename)
}

fn sanitize_path_component(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_owned()
    } else {
        sanitized
    }
}

#[cfg(unix)]
fn soft_fd_limit() -> Option<usize> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: libc::getrlimit only writes to the provided struct.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if result != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    usize::try_from(limit.rlim_cur).ok()
}

#[cfg(not(unix))]
fn soft_fd_limit() -> Option<usize> {
    None
}

fn compute_effective_max_open_files(configured: usize) -> usize {
    let configured = configured.max(MIN_OPEN_FILE_LIMIT);
    let Some(soft_limit) = soft_fd_limit() else {
        return configured;
    };

    // Keep room for sockets + runtime internals.
    let cap = soft_limit
        .saturating_sub(FD_HEADROOM_RESERVE)
        .max(MIN_OPEN_FILE_LIMIT);
    configured.min(cap)
}

fn is_too_many_open_files(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        source
            .downcast_ref::<io::Error>()
            .is_some_and(|io_err| io_err.raw_os_error() == Some(24))
    })
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::Utc;
    use compact_str::CompactString;

    use super::*;
    use crate::model::{FundingUpdate, MarketEvent, QuoteUpdate};

    fn sample_markets() -> SymbolMarkets {
        use crate::model::MarketMeta;

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
                    maker_fee_pct: 0.0,
                },
            ],
        );
        markets
    }

    fn sample_settings(tmp: &Path) -> CollectorSettings {
        CollectorSettings {
            data_root: tmp.to_path_buf(),
            compression: CollectorCompression::None,
            bootstrap_timeout_ms: 1_000,
            write_buffer: 8_192,
            flush_interval_ms: 10,
            max_open_files: 32,
        }
    }

    #[test]
    fn partition_path_generation_matches_layout() {
        let envelope = CollectorEnvelope {
            schema_version: SCHEMA_VERSION,
            record_type: CollectorRecordType::Quote,
            global_seq: 1,
            exchange: "Aster".to_owned(),
            symbol: "BTC".to_owned(),
            symbol_base: "BTC".to_owned(),
            exchange_symbol: Some("BTCUSDT".to_owned()),
            exchange_ts_ms: 1_700_000_000_000,
            recv_ts_ms: 1_700_000_000_100,
            collected_at_ms: 1_700_000_000_123,
            payload: CollectorPayload::Quote {
                bid_px: 100.0,
                bid_qty: 1.0,
                ask_px: 100.1,
                ask_qty: 2.0,
            },
        };

        let key = partition_key_from_envelope(&envelope).expect("partition key");
        let path = build_partition_path(Path::new("/tmp/data"), &key, CollectorCompression::Zstd);
        assert_eq!(
            path.to_string_lossy(),
            "/tmp/data/BTC/aster/quote/2023-11-14/22.jsonl.zst"
        );
    }

    #[test]
    fn partition_key_rotates_across_hour_and_day() {
        let day_a = Utc
            .with_ymd_and_hms(2026, 3, 16, 23, 59, 59)
            .single()
            .expect("day_a");
        let day_b = Utc
            .with_ymd_and_hms(2026, 3, 17, 0, 0, 0)
            .single()
            .expect("day_b");

        let envelope_a = CollectorEnvelope {
            schema_version: SCHEMA_VERSION,
            record_type: CollectorRecordType::Funding,
            global_seq: 10,
            exchange: "Lighter".to_owned(),
            symbol: "ETH".to_owned(),
            symbol_base: "ETH".to_owned(),
            exchange_symbol: Some("ETH".to_owned()),
            exchange_ts_ms: day_a.timestamp_millis(),
            recv_ts_ms: day_a.timestamp_millis(),
            collected_at_ms: day_a.timestamp_millis(),
            payload: CollectorPayload::Funding {
                funding_rate: 0.0001,
                next_funding_ts_ms: None,
            },
        };
        let envelope_b = CollectorEnvelope {
            collected_at_ms: day_b.timestamp_millis(),
            exchange_ts_ms: day_b.timestamp_millis(),
            recv_ts_ms: day_b.timestamp_millis(),
            ..envelope_a.clone()
        };

        let key_a = partition_key_from_envelope(&envelope_a).expect("key_a");
        let key_b = partition_key_from_envelope(&envelope_b).expect("key_b");

        assert_eq!(key_a.yyyy_mm_dd, "2026-03-16");
        assert_eq!(key_a.hour, 23);
        assert_eq!(key_b.yyyy_mm_dd, "2026-03-17");
        assert_eq!(key_b.hour, 0);
    }

    #[test]
    fn envelope_serialization_roundtrip() {
        let original = CollectorEnvelope {
            schema_version: SCHEMA_VERSION,
            record_type: CollectorRecordType::TradeUnsupported,
            global_seq: 42,
            exchange: "GRVT".to_owned(),
            symbol: "BTC".to_owned(),
            symbol_base: "BTC".to_owned(),
            exchange_symbol: Some("BTC_USDT_Perp".to_owned()),
            exchange_ts_ms: 1234,
            recv_ts_ms: 1235,
            collected_at_ms: 1236,
            payload: CollectorPayload::TradeUnsupported {
                trades_supported: false,
                reason: "trade stream not integrated in collector v1".to_owned(),
            },
        };

        let encoded = serde_json::to_string(&original).expect("serialize");
        let decoded: CollectorEnvelope = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, original);
    }

    #[test]
    fn codec_writer_roundtrip_none_zstd_lz4() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let payload = br#"{"hello":"world"}"#;
        for codec in [
            CollectorCompression::None,
            CollectorCompression::Zstd,
            CollectorCompression::Lz4hc,
        ] {
            let path = match codec {
                CollectorCompression::None => temp_dir.path().join("test.jsonl"),
                CollectorCompression::Zstd => temp_dir.path().join("test.jsonl.zst"),
                CollectorCompression::Lz4hc => temp_dir.path().join("test.jsonl.lz4"),
            };

            let file = File::create(&path).expect("create file");
            let mut writer = CodecWriter::new(file, codec, 1024).expect("writer");
            writer.write_line(payload).expect("write");
            writer.finish().expect("finish");

            let mut decoded = String::new();
            match codec {
                CollectorCompression::None => {
                    File::open(&path)
                        .expect("open plain")
                        .read_to_string(&mut decoded)
                        .expect("read plain");
                }
                CollectorCompression::Zstd => {
                    let mut reader = zstd::stream::read::Decoder::new(
                        File::open(&path).expect("open zstd file"),
                    )
                    .expect("zstd reader");
                    reader.read_to_string(&mut decoded).expect("read zstd");
                }
                CollectorCompression::Lz4hc => {
                    let mut reader =
                        lz4::Decoder::new(File::open(&path).expect("open lz4 file")).expect("lz4");
                    reader.read_to_string(&mut decoded).expect("read lz4");
                }
            }

            assert_eq!(decoded.trim_end(), "{\"hello\":\"world\"}");
        }
    }

    #[test]
    fn bootstrap_gate_state_machine() {
        let markets = sample_markets();
        let mut gate = BootstrapGate::new(&markets, 1_000, 10_000);
        assert!(!gate.is_open());
        assert!(!gate.mark_ready(Exchange::Lighter));
        assert!(!gate.is_open());
        assert!(gate.mark_ready(Exchange::Aster));
        assert!(gate.is_open());

        let markets = sample_markets();
        let mut timeout_gate = BootstrapGate::new(&markets, 100, 10_000);
        assert!(!timeout_gate.maybe_open_on_timeout(10_099));
        assert!(timeout_gate.maybe_open_on_timeout(10_100));
        assert!(timeout_gate.is_open());
    }

    #[tokio::test]
    async fn buffered_events_are_persisted_after_gate_open() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let markets = sample_markets();
        let settings = sample_settings(temp_dir.path());
        let (tx, rx) = mpsc::channel(8);

        tx.send(MarketEvent::Quote(QuoteUpdate {
            exchange: Exchange::Lighter,
            symbol_base: CompactString::from("BTC"),
            bid_px: 100.0,
            bid_qty: 1.0,
            ask_px: 100.1,
            ask_qty: 1.2,
            exch_ts_ms: 1_700_000_000_000,
            recv_ts_ms: 1_700_000_000_001,
        }))
        .await
        .expect("send quote");
        tx.send(MarketEvent::Funding(FundingUpdate {
            exchange: Exchange::Aster,
            symbol_base: CompactString::from("BTC"),
            funding_rate: 0.0001,
            next_funding_ts_ms: Some(1_700_000_500_000),
            recv_ts_ms: 1_700_000_000_002,
            stale_after_ms: None,
        }))
        .await
        .expect("send funding");
        drop(tx);

        let mut collector = CollectorService::new(settings, &markets);
        let stats = collector
            .run_until_shutdown(rx, std::future::pending::<()>())
            .await
            .expect("collector run");
        assert!(stats.written_records >= 4);
    }
}
