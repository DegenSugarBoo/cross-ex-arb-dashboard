#!/usr/bin/env python3
"""Compare current websocket ingest metrics against a saved baseline."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

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
        result[name] = {
            "ns_per_unit": statistics.median(ns_values),
            "ns_unit": str(runs[0][name]["ns_unit"]),
            "throughput": statistics.median(throughput_values),
            "throughput_unit": str(runs[0][name]["throughput_unit"]),
        }
    return result


def compare(
    baseline: Dict[str, Dict[str, float | str]],
    current: Dict[str, Dict[str, float | str]],
    allowed_slowdown: float,
) -> Tuple[bool, List[str]]:
    lines: List[str] = []
    ok = True

    baseline_names = sorted(baseline.keys())
    for name in baseline_names:
        if name not in current:
            ok = False
            lines.append(f"[missing] {name} (not present in current run)")
            continue

        base_ns = float(baseline[name]["ns_per_unit"])
        cur_ns = float(current[name]["ns_per_unit"])
        slowdown = (cur_ns / base_ns) - 1.0
        status = "OK"
        if slowdown > allowed_slowdown:
            status = "REGRESSION"
            ok = False

        lines.append(
            f"[{status:<10}] {name:<34} "
            f"base={base_ns:>9.1f}ns cur={cur_ns:>9.1f}ns "
            f"delta={slowdown * 100:>6.2f}%"
        )

    return ok, lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("benchmarks/ws_ingest_baseline_post_ws_rollout.json"),
        help="Baseline JSON path.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="How many ws_exchange_ingest_bench runs to aggregate (median). Default: 3",
    )
    parser.add_argument(
        "--allowed-slowdown-pct",
        type=float,
        default=10.0,
        help="Allowed slowdown in percent per metric (based on ns/msg). Default: 10.0",
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
    if not args.baseline.exists():
        raise SystemExit(f"baseline file not found: {args.baseline}")

    baseline_doc = json.loads(args.baseline.read_text())
    baseline_metrics = baseline_doc.get("metrics", {})
    if not baseline_metrics:
        raise SystemExit("baseline file has no metrics")

    command = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "ws_exchange_ingest_bench",
        "--features",
        "ws-compare-tungstenite",
    ]
    runs = []
    for idx in range(args.runs):
        print(f"[check] run {idx + 1}/{args.runs}")
        runs.append(run_ws_ingest_bench(command, args.workdir))
    current = aggregate(runs)

    allowed = args.allowed_slowdown_pct / 100.0
    ok, lines = compare(baseline_metrics, current, allowed)
    for line in lines:
        print(line)

    if ok:
        print("[check] PASS: no metric exceeded slowdown threshold")
        return 0

    print("[check] FAIL: at least one metric regressed past threshold")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
