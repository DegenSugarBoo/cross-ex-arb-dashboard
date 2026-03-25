use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;
use cross_ex_arb::ws_fast::connect_fast_websocket;
use fastwebsockets::{Frame, OpCode};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "wss://api.hyperliquid.xyz/ws")]
    ws_url: String,
    #[arg(long, value_delimiter = ',', required = true)]
    coins: Vec<String>,
    #[arg(long, default_value_t = 5)]
    window_secs: u64,
}

fn subscribe_payload(coin: &str) -> String {
    format!(
        "{{\"method\":\"subscribe\",\"subscription\":{{\"type\":\"bbo\",\"coin\":\"{coin}\"}}}}"
    )
}

fn install_rustls_crypto_provider() -> anyhow::Result<()> {
    use tokio_rustls::rustls::crypto::{CryptoProvider, ring};

    if CryptoProvider::get_default().is_some() {
        return Ok(());
    }

    ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_rustls_crypto_provider()?;
    let args = Args::parse();
    if args.coins.is_empty() {
        bail!("no coins provided");
    }

    println!(
        "probing {} coins: {}",
        args.coins.len(),
        args.coins.join(",")
    );
    let mut ws = connect_fast_websocket(&args.ws_url)
        .await
        .context("connect failed")?;

    for coin in &args.coins {
        ws.write_frame(Frame::text(subscribe_payload(coin).into_bytes().into()))
            .await
            .with_context(|| format!("subscribe send failed for {coin}"))?;
    }
    ws.flush().await.context("subscribe flush failed")?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.window_secs);
    let mut text_frames = 0usize;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            println!("ok text_frames={text_frames}");
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(now);
        match tokio::time::timeout(remaining, ws.read_frame()).await {
            Ok(Ok(frame)) if frame.opcode == OpCode::Text => {
                text_frames = text_frames.saturating_add(1);
            }
            Ok(Ok(frame)) if frame.opcode == OpCode::Ping => {
                ws.write_frame(Frame::pong(frame.payload.to_vec().into()))
                    .await
                    .context("pong send failed")?;
                ws.flush().await.context("pong flush failed")?;
            }
            Ok(Ok(frame)) if frame.opcode == OpCode::Close => {
                bail!("remote close after text_frames={text_frames}");
            }
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                bail!("read failed after text_frames={text_frames}: {err}");
            }
            Err(_) => {
                println!("ok text_frames={text_frames}");
                return Ok(());
            }
        }
    }
}
