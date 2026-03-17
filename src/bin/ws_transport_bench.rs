use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use cross_ex_arb::feeds::aster::parse_book_ticker_message;
use cross_ex_arb::ws_fast::connect_fast_websocket;
use fastwebsockets::OpCode;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

const ASTER_BOOK: &str = r#"{"e":"bookTicker","s":"BTCUSDT","b":"101.0","B":"1.50","a":"101.2","A":"0.80","T":1700000001111,"E":1700000002222}"#;
const WARMUP_FRAMES: usize = 25_000;
const MEASURE_FRAMES: usize = 200_000;
const ROUNDS: usize = 3;

#[derive(Clone, Copy)]
enum Mode {
    TransportOnly,
    TransportAndParse,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::TransportOnly => "transport-only",
            Self::TransportAndParse => "transport+parse",
        }
    }
}

async fn spawn_text_server(
    frames: usize,
    payload: &'static str,
) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind local benchmark listener")?;
    let addr = listener
        .local_addr()
        .context("failed to read local benchmark listener address")?;
    let payload_message = Message::Text(payload.to_owned().into());
    let url = format!("ws://{addr}/stream");

    let handle = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .context("benchmark server failed to accept client")?;
        let mut ws = accept_async(stream)
            .await
            .context("benchmark server websocket handshake failed")?;

        for _ in 0..frames {
            ws.send(payload_message.clone())
                .await
                .context("benchmark server failed to send websocket frame")?;
        }

        let _ = ws.close(None).await;
        Ok(())
    });

    Ok((url, handle))
}

fn consume_text(mode: Mode, raw: &str, symbol_map: &HashMap<String, String>) -> anyhow::Result<()> {
    match mode {
        Mode::TransportOnly => {
            black_box(raw.len());
        }
        Mode::TransportAndParse => {
            let recv_ts_ms = 1_700_000_000_000i64;
            if parse_book_ticker_message(raw, symbol_map, recv_ts_ms).is_none() {
                bail!("failed to parse Aster benchmark payload");
            }
        }
    }
    Ok(())
}

async fn run_round_tungstenite(
    frames: usize,
    mode: Mode,
    symbol_map: &HashMap<String, String>,
) -> anyhow::Result<Duration> {
    let (url, server_handle) = spawn_text_server(frames, ASTER_BOOK).await?;
    let (mut ws, _) = connect_async(&url)
        .await
        .context("tungstenite benchmark client failed to connect")?;

    let started = Instant::now();
    let mut received = 0usize;
    while received < frames {
        let Some(message_result) = ws.next().await else {
            bail!("tungstenite benchmark stream closed early");
        };
        let message = message_result.context("tungstenite benchmark read failed")?;
        if let Message::Text(text) = message {
            consume_text(mode, text.as_ref(), symbol_map)?;
            received = received.saturating_add(1);
        }
    }
    let elapsed = started.elapsed();

    server_handle
        .await
        .context("tungstenite benchmark server task join failed")??;

    Ok(elapsed)
}

async fn run_round_fastwebsockets(
    frames: usize,
    mode: Mode,
    symbol_map: &HashMap<String, String>,
) -> anyhow::Result<Duration> {
    let (url, server_handle) = spawn_text_server(frames, ASTER_BOOK).await?;
    let mut ws = connect_fast_websocket(&url)
        .await
        .context("fastwebsockets benchmark client failed to connect")?;

    let started = Instant::now();
    let mut received = 0usize;
    while received < frames {
        let frame = ws
            .read_frame()
            .await
            .context("fastwebsockets benchmark read failed")?;
        if frame.opcode != OpCode::Text {
            if frame.opcode == OpCode::Close {
                bail!("fastwebsockets benchmark stream closed early");
            }
            continue;
        }
        let raw = std::str::from_utf8(&frame.payload)
            .context("fastwebsockets benchmark text frame was invalid UTF-8")?;
        consume_text(mode, raw, symbol_map)?;
        received = received.saturating_add(1);
    }
    let elapsed = started.elapsed();

    server_handle
        .await
        .context("fastwebsockets benchmark server task join failed")??;

    Ok(elapsed)
}

fn median_duration(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn print_result_row(name: &str, frames: usize, elapsed: Duration) {
    let ns_per_msg = elapsed.as_nanos() as f64 / frames as f64;
    let msg_per_s = frames as f64 / elapsed.as_secs_f64();
    println!(
        "{name:<36} {:>10.0} msg/s {:>9.1} ns/msg",
        msg_per_s, ns_per_msg
    );
}

async fn run_mode_benchmarks(
    mode: Mode,
    symbol_map: &HashMap<String, String>,
) -> anyhow::Result<()> {
    println!();
    println!("mode: {}", mode.label());

    let _ = run_round_tungstenite(WARMUP_FRAMES, mode, symbol_map).await?;
    let _ = run_round_fastwebsockets(WARMUP_FRAMES, mode, symbol_map).await?;

    let mut tungstenite_samples = Vec::with_capacity(ROUNDS);
    let mut fastwebsockets_samples = Vec::with_capacity(ROUNDS);

    for round in 0..ROUNDS {
        println!("  round {}/{}", round + 1, ROUNDS);
        tungstenite_samples.push(
            run_round_tungstenite(MEASURE_FRAMES, mode, symbol_map)
                .await
                .context("tungstenite benchmark round failed")?,
        );
        fastwebsockets_samples.push(
            run_round_fastwebsockets(MEASURE_FRAMES, mode, symbol_map)
                .await
                .context("fastwebsockets benchmark round failed")?,
        );
    }

    let tungstenite_median = median_duration(tungstenite_samples);
    let fastwebsockets_median = median_duration(fastwebsockets_samples);

    print_result_row("tungstenite", MEASURE_FRAMES, tungstenite_median);
    print_result_row("fastwebsockets", MEASURE_FRAMES, fastwebsockets_median);

    let delta = (fastwebsockets_median.as_secs_f64() / tungstenite_median.as_secs_f64()) - 1.0;
    println!("delta (fast vs tung): {:+.2}%", delta * 100.0);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!(
        "ws transport benchmark (Aster fixture): warmup={} measure={} rounds={}",
        WARMUP_FRAMES, MEASURE_FRAMES, ROUNDS
    );
    let symbol_map = HashMap::from([("BTCUSDT".to_owned(), "BTC".to_owned())]);

    run_mode_benchmarks(Mode::TransportOnly, &symbol_map).await?;
    run_mode_benchmarks(Mode::TransportAndParse, &symbol_map).await?;
    Ok(())
}
