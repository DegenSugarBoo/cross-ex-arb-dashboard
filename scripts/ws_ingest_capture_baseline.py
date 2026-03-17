#!/usr/bin/env python3
"""Capture a repeatable websocket ingest baseline from ws_exchange_ingest_bench output."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import statistics
import subprocess
from pathlib import Path
from typing import Dict, List

LINE_RE = re.compile(
    r"^(?P<name>.+?)\s+"
    r"(?P<throughput>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<throughput_unit>msg/s)\s+"
    r"(?P<ns>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<ns_unit>ns/msg)$"
)


def parse_metrics(output: str) -> Dict[str, Dict[str, float | str]]:
    metrics: Dict[str, Dict[str, float | str]] = {}
    for line in output.splitlines():
        match = LINE_RE.match(line.strip())
        if not match:
            continue
        name = match.group("name").strip()
        metrics[name] = {
            "throughput": float(match.group("throughput")),
            "throughput_unit": match.group("throughput_unit"),
            "ns_per_unit": float(match.group("ns")),
            "ns_unit": match.group("ns_unit"),
        }
    return metrics


def run_ws_ingest_bench(command: List[str], cwd: Path) -> Dict[str, Dict[str, float | str]]:
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    )
    parsed = parse_metrics(proc.stdout)
    if not parsed:
        raise RuntimeError("No metrics parsed from ws_exchange_ingest_bench output.")
    return parsed


def aggregate(runs: List[Dict[str, Dict[str, float | str]]]) -> Dict[str, Dict[str, float | str]]:
    names = set(runs[0].keys())
    for run in runs[1:]:
        names &= set(run.keys())
    if not names:
        raise RuntimeError("No common metrics found across runs.")

    result: Dict[str, Dict[str, float | str]] = {}
    for name in sorted(names):
        ns_values = [float(run[name]["ns_per_unit"]) for run in runs]
        throughput_values = [float(run[name]["throughput"]) for run in runs]
        ns_unit = str(runs[0][name]["ns_unit"])
        throughput_unit = str(runs[0][name]["throughput_unit"])
        result[name] = {
            "ns_per_unit": statistics.median(ns_values),
            "ns_stddev": statistics.pstdev(ns_values) if len(ns_values) > 1 else 0.0,
            "ns_unit": ns_unit,
            "throughput": statistics.median(throughput_values),
            "throughput_stddev": (
                statistics.pstdev(throughput_values) if len(throughput_values) > 1 else 0.0
            ),
            "throughput_unit": throughput_unit,
        }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="How many ws_exchange_ingest_bench runs to aggregate (median). Default: 3",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmarks/ws_ingest_baseline_post_ws_rollout.json"),
        help="Baseline JSON path.",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("."),
        help="Project root directory.",
    )
    args = parser.parse_args()

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    command = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "ws_exchange_ingest_bench",
        "--features",
        "ws-compare-tungstenite",
    ]
    all_runs = []
    for idx in range(args.runs):
        print(f"[capture] run {idx + 1}/{args.runs}")
        all_runs.append(run_ws_ingest_bench(command, args.workdir))

    baseline = {
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "runs": args.runs,
        "command": command,
        "metrics": aggregate(all_runs),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
    print(f"[capture] wrote baseline to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
