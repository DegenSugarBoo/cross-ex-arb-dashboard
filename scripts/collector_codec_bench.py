#!/usr/bin/env python3
import argparse
import json
import statistics
import subprocess
import sys
from collections import defaultdict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score collector compression codecs.")
    parser.add_argument("--runs", type=int, default=3, help="Number of benchmark runs.")
    parser.add_argument(
        "--events",
        type=int,
        default=200_000,
        help="Number of synthetic events per codec per run.",
    )
    parser.add_argument(
        "--write-buffer",
        type=int,
        default=16_384,
        help="Writer buffer size in bytes.",
    )
    return parser.parse_args()


def run_benchmark(events: int, write_buffer: int) -> list[dict]:
    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "collector_codec_bench",
        "--",
        "--events",
        str(events),
        "--write-buffer",
        str(write_buffer),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if {"codec", "compressed_bytes", "events_per_sec"}.issubset(payload.keys()):
            rows.append(payload)
    if not rows:
        raise RuntimeError("No benchmark rows parsed from collector codec bench output.")
    return rows


def main() -> int:
    args = parse_args()
    per_codec: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"compressed_bytes": [], "events_per_sec": []}
    )

    for run_idx in range(args.runs):
        rows = run_benchmark(args.events, args.write_buffer)
        for row in rows:
            codec = row["codec"]
            per_codec[codec]["compressed_bytes"].append(float(row["compressed_bytes"]))
            per_codec[codec]["events_per_sec"].append(float(row["events_per_sec"]))
        print(f"run {run_idx + 1}/{args.runs}: captured {len(rows)} codec rows")

    medians = {}
    for codec, metrics in per_codec.items():
        medians[codec] = {
            "compressed_bytes": statistics.median(metrics["compressed_bytes"]),
            "events_per_sec": statistics.median(metrics["events_per_sec"]),
        }

    none_bytes = medians.get("none", {}).get("compressed_bytes")
    if not none_bytes:
        raise RuntimeError("Missing `none` codec benchmark row, cannot score size reduction.")

    max_throughput = max(m["events_per_sec"] for m in medians.values())
    scored = []
    for codec, metrics in medians.items():
        size_reduction = max(0.0, (none_bytes - metrics["compressed_bytes"]) / none_bytes)
        throughput_norm = (
            metrics["events_per_sec"] / max_throughput if max_throughput > 0 else 0.0
        )
        weighted = 0.6 * size_reduction + 0.4 * throughput_norm
        scored.append(
            {
                "codec": codec,
                "compressed_bytes": metrics["compressed_bytes"],
                "events_per_sec": metrics["events_per_sec"],
                "size_reduction": size_reduction,
                "throughput_norm": throughput_norm,
                "weighted_score": weighted,
            }
        )

    scored.sort(key=lambda row: row["weighted_score"], reverse=True)
    winner = scored[0]

    print("\nCodec scorecard (weight: 60% size + 40% throughput):")
    for row in scored:
        print(
            f"- {row['codec']:<6} "
            f"bytes={row['compressed_bytes']:.0f} "
            f"eps={row['events_per_sec']:.0f} "
            f"size_reduction={row['size_reduction']:.3f} "
            f"throughput_norm={row['throughput_norm']:.3f} "
            f"score={row['weighted_score']:.3f}"
        )

    print(f"\nRecommended default codec: {winner['codec']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"collector codec bench failed: {exc}", file=sys.stderr)
        raise
