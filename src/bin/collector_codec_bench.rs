use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use serde::Serialize;

#[derive(Parser, Debug, Clone)]
#[command(name = "collector_codec_bench")]
struct Args {
    #[arg(long, default_value_t = 200_000)]
    events: usize,
    #[arg(long, default_value_t = 16_384)]
    write_buffer: usize,
}

#[derive(Debug, Clone, Copy)]
enum BenchCodec {
    None,
    Zstd,
    Lz4hc,
}

impl BenchCodec {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
            Self::Lz4hc => "lz4hc",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::None => "jsonl",
            Self::Zstd => "jsonl.zst",
            Self::Lz4hc => "jsonl.lz4",
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchResult {
    codec: String,
    events: usize,
    compressed_bytes: u64,
    elapsed_secs: f64,
    events_per_sec: f64,
}

enum BenchWriter {
    Plain(BufWriter<File>),
    Zstd(zstd::stream::write::Encoder<'static, BufWriter<File>>),
    Lz4(lz4::Encoder<BufWriter<File>>),
}

impl BenchWriter {
    fn new(file: File, codec: BenchCodec, write_buffer: usize) -> io::Result<Self> {
        let buffered = BufWriter::with_capacity(write_buffer.max(1), file);
        match codec {
            BenchCodec::None => Ok(Self::Plain(buffered)),
            BenchCodec::Zstd => {
                let mut encoder =
                    zstd::stream::write::Encoder::new(buffered, 6).map_err(io::Error::other)?;
                encoder.include_checksum(true).map_err(io::Error::other)?;
                Ok(Self::Zstd(encoder))
            }
            BenchCodec::Lz4hc => {
                let encoder = lz4::EncoderBuilder::new()
                    .level(16)
                    .build(buffered)
                    .map_err(io::Error::other)?;
                Ok(Self::Lz4(encoder))
            }
        }
    }

    fn write_line(&mut self, idx: usize) -> io::Result<()> {
        match self {
            Self::Plain(writer) => write_synthetic_record(writer, idx),
            Self::Zstd(writer) => write_synthetic_record(writer, idx),
            Self::Lz4(writer) => write_synthetic_record(writer, idx),
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

fn write_synthetic_record<W: Write>(writer: &mut W, idx: usize) -> io::Result<()> {
    writeln!(
        writer,
        "{{\"schema_version\":1,\"record_type\":\"quote\",\"global_seq\":{},\"exchange\":\"Aster\",\"symbol\":\"BTC\",\"symbol_base\":\"BTC\",\"exchange_symbol\":\"BTCUSDT\",\"exchange_ts_ms\":1700000000000,\"recv_ts_ms\":1700000000001,\"collected_at_ms\":1700000000002,\"bid_px\":100.01,\"bid_qty\":2.5,\"ask_px\":100.02,\"ask_qty\":1.7}}",
        idx + 1
    )
}

fn run_once(codec: BenchCodec, args: &Args) -> anyhow::Result<BenchResult> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir for benchmark")?;
    let file_path: PathBuf = temp_dir
        .path()
        .join(format!("collector_bench.{}", codec.extension()));
    let file = File::create(&file_path)
        .with_context(|| format!("failed to create {}", file_path.display()))?;
    let mut writer =
        BenchWriter::new(file, codec, args.write_buffer).context("failed to initialize writer")?;

    let start = Instant::now();
    for idx in 0..args.events {
        writer
            .write_line(idx)
            .context("failed writing benchmark row")?;
    }
    writer
        .finish()
        .context("failed finishing benchmark writer")?;
    let elapsed = start.elapsed();

    let compressed_bytes = fs::metadata(&file_path)
        .with_context(|| format!("failed reading metadata for {}", file_path.display()))?
        .len();
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let events_per_sec = args.events as f64 / elapsed_secs;

    Ok(BenchResult {
        codec: codec.as_str().to_owned(),
        events: args.events,
        compressed_bytes,
        elapsed_secs,
        events_per_sec,
    })
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    for codec in [BenchCodec::None, BenchCodec::Zstd, BenchCodec::Lz4hc] {
        let result = run_once(codec, &args)?;
        println!("{}", serde_json::to_string(&result)?);
    }
    Ok(())
}
