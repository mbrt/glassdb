#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Reproduce the GlassDB README performance graphs from rtbench CSV output.

It reads the four CSV files produced by ``rtbench -test-name=rw9010`` and
``rtbench -test-name=deadlock`` (``samples.csv``, ``stats.csv``,
``throughput.csv`` and ``deadlock.csv``) from a local directory -- typically
``hack/aws-bench/out``, where ``deploy.sh results`` downloads them. The large
``samples.csv`` may be stored compressed as ``samples.csv.xz``; it is read
transparently. It renders the five README figures:

* ``tx-throughput.png`` -- total transactions/sec per transaction type.
* ``retries.png``       -- transaction retries per committed transaction.
* ``ops-latency.png``   -- per-operation (per-key) latency.
* ``tx-latency.png``    -- per-transaction latency.
* ``deadlock-latency.png`` -- latency under heavy single-key contention.

Throughput aggregation: ``throughput.csv`` holds the rate each individual DB
sustained. The total system throughput at a given concurrency level is the sum
across DBs, estimated here as ``num_db * median(per-db rate)``; the 10th-90th
percentile band comes from the spread between DBs (which widens with conflicts).
The four rw9010 figures share an x-axis of "concurrent transactions" =
``num_db * concurrency-per-db`` (10 by default), matching the README.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# 10th-90th percentile band, matching the README error bands.
ERRORBAR = ("pi", 80)
CONC_LABEL = "Concurrent transactions"


def read_csv(input_dir: Path, name: str) -> pd.DataFrame | None:
    """Load a result CSV, returning None (with a warning) when it is missing.

    Accepts either ``<name>`` or a compressed ``<name>.xz`` (e.g. the large
    ``samples.csv`` that ``deploy.sh results`` compresses); pandas infers the
    compression from the extension.
    """
    for path in (input_dir / name, input_dir / f"{name}.xz"):
        if path.exists():
            return pd.read_csv(path)
    print(f"warning: {input_dir / name}[.xz] not found, skipping", file=sys.stderr)
    return None


def _save(fig: plt.Figure, out_dir: Path, name: str, extra_dirs: list[Path]) -> None:
    for directory in [out_dir, *extra_dirs]:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        fig.savefig(path, dpi=120, bbox_inches="tight")
        print(f"wrote {path}")
    plt.close(fig)


def plot_throughput(
    df: pd.DataFrame, conc_per_db: int, out_dir: Path, extra: list[Path]
) -> None:
    data = df.copy()
    data["concurrent"] = data["num-db"] * conc_per_db
    # Per-DB rate scaled by the number of DBs estimates total system throughput.
    data["total-tps"] = data["tx-per-sec"] * data["num-db"]

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="total-tps",
        hue="tx-type",
        estimator="median",
        errorbar=ERRORBAR,
        ax=ax,
    )
    ax.set_title("Transaction throughput")
    ax.set_xlabel(CONC_LABEL)
    ax.set_ylabel("Transactions / sec")
    ax.legend(title="Transaction type")
    _save(fig, out_dir, "tx-throughput.png", extra)


def plot_tx_latency(
    df: pd.DataFrame, conc_per_db: int, out_dir: Path, extra: list[Path]
) -> None:
    data = df.copy()
    data["concurrent"] = data["num-db"] * conc_per_db

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="latency",
        hue="tx-type",
        estimator="median",
        errorbar=ERRORBAR,
        ax=ax,
    )
    ax.set_title("Transaction latency")
    ax.set_xlabel(CONC_LABEL)
    ax.set_ylabel("Latency (ms)")
    ax.legend(title="Transaction type")
    _save(fig, out_dir, "tx-latency.png", extra)


def plot_ops_latency(
    df: pd.DataFrame, conc_per_db: int, out_dir: Path, extra: list[Path]
) -> None:
    data = df.copy()
    data["concurrent"] = data["num-db"] * conc_per_db
    data["op-latency"] = data["latency"] / data["ops"]

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="op-latency",
        hue="tx-type",
        estimator="median",
        errorbar=ERRORBAR,
        ax=ax,
    )
    ax.set_title("Per-operation latency")
    ax.set_xlabel(CONC_LABEL)
    ax.set_ylabel("Latency per key op (ms)")
    ax.legend(title="Transaction type")
    _save(fig, out_dir, "ops-latency.png", extra)


def plot_retries(
    df: pd.DataFrame, conc_per_db: int, out_dir: Path, extra: list[Path]
) -> None:
    data = df.copy()
    data["concurrent"] = data["num-db"] * conc_per_db
    data["retries-per-tx"] = data["num-retries"] / data["num-tx"].where(
        data["num-tx"] > 0
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="retries-per-tx",
        estimator="median",
        errorbar=ERRORBAR,
        ax=ax,
    )
    ax.set_title("Transaction retries")
    ax.set_xlabel(CONC_LABEL)
    ax.set_ylabel("Retries per transaction")
    _save(fig, out_dir, "retries.png", extra)


def plot_deadlock(df: pd.DataFrame, out_dir: Path, extra: list[Path]) -> None:
    # The README scenario is 100% overlap (all workers hammer the same keys).
    data = df[df["overlap-pct"] == 100].copy()
    if data.empty:
        data = df.copy()

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="num-keys",
        y="latency-ms",
        estimator="median",
        errorbar=ERRORBAR,
        marker="o",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Latency under contention")
    ax.set_xlabel("Contended keys (5 workers, 100% overlap)")
    ax.set_ylabel("Transaction latency (ms, log scale)")
    _save(fig, out_dir, "deadlock-latency.png", extra)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_dir = Path(__file__).resolve().parent / "out"
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=default_dir,
        help="directory containing the result CSVs (default: ./out)",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=Path,
        default=default_dir,
        help="output directory for the PNGs (default: ./out)",
    )
    parser.add_argument(
        "--concurrency-per-db",
        type=int,
        default=10,
        help="parallel transactions per DB, used to scale the x-axis (default: 10)",
    )
    parser.add_argument(
        "--write-docs",
        action="store_true",
        help="also write the PNGs into docs/img/",
    )
    args = parser.parse_args()

    sns.set_theme(style="whitegrid", context="talk")

    extra: list[Path] = []
    if args.write_docs:
        extra.append(Path(__file__).resolve().parents[2] / "docs" / "img")

    samples = read_csv(args.input, "samples.csv")
    stats = read_csv(args.input, "stats.csv")
    throughput = read_csv(args.input, "throughput.csv")
    deadlock = read_csv(args.input, "deadlock.csv")

    if throughput is not None:
        plot_throughput(throughput, args.concurrency_per_db, args.out, extra)
    if samples is not None:
        plot_tx_latency(samples, args.concurrency_per_db, args.out, extra)
        plot_ops_latency(samples, args.concurrency_per_db, args.out, extra)
    if stats is not None:
        plot_retries(stats, args.concurrency_per_db, args.out, extra)
    if deadlock is not None:
        plot_deadlock(deadlock, args.out, extra)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
