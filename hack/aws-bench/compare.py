#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Compare two rtbench result sets and report how closely they match.

This is used to check that the "fake" backend (in-memory + simulated S3
latencies, ``rtbench -backend=memory -delays=s3``) reproduces a real Amazon S3
run (``rtbench -backend=s3``). It reads the CSVs produced by both runs from two
directories -- ``--real`` (the reference, default ``hack/aws-bench/out``) and
``--fake`` (default ``hack/aws-bench/out-fake``) -- and prints, for each of the
comparable metrics, a side-by-side table with the ratio ``fake / real``:

* transaction throughput per type (total tx/s = ``num_db * median(per-db rate)``);
* retries per committed transaction;
* deadlock latency p50/p90 at 100% overlap, per contended-key count.

A ratio near 1.0 means the fake backend matches. It also writes overlay PNGs
(``cmp-tx-throughput.png``, ``cmp-retries.png``, ``cmp-deadlock-latency.png``)
into ``--out`` so the curves can be eyeballed together.
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

REAL = "real"
FAKE = "fake"


def read_csv(input_dir: Path, name: str) -> pd.DataFrame | None:
    """Load a result CSV, accepting an optional ``.xz`` compression."""
    for path in (input_dir / name, input_dir / f"{name}.xz"):
        if path.exists():
            return pd.read_csv(path)
    print(f"warning: {input_dir / name}[.xz] not found, skipping", file=sys.stderr)
    return None


def _ratio(fake: float, real: float) -> float:
    return float("nan") if real == 0 else fake / real


def throughput_table(real: pd.DataFrame, fake: pd.DataFrame, conc_per_db: int):
    """Total tx/s per (concurrency, tx-type): num_db * median(per-db rate)."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        g = (
            df.groupby(["num-db", "tx-type"])["tx-per-sec"]
            .median()
            .reset_index()
        )
        g["total-tps"] = g["tx-per-sec"] * g["num-db"]
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    r = agg(real)
    f = agg(fake)
    merged = r.merge(
        f,
        on=["num-db", "tx-type", "concurrent"],
        suffixes=(f"_{REAL}", f"_{FAKE}"),
    )
    merged["ratio"] = merged.apply(
        lambda row: _ratio(row[f"total-tps_{FAKE}"], row[f"total-tps_{REAL}"]), axis=1
    )
    return merged


def retries_table(real: pd.DataFrame, fake: pd.DataFrame, conc_per_db: int):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        d["retries-per-tx"] = d["num-retries"] / d["num-tx"].where(d["num-tx"] > 0)
        g = d.groupby("num-db")["retries-per-tx"].median().reset_index()
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    r = agg(real)
    f = agg(fake)
    merged = r.merge(f, on=["num-db", "concurrent"], suffixes=(f"_{REAL}", f"_{FAKE}"))
    merged["ratio"] = merged.apply(
        lambda row: _ratio(
            row[f"retries-per-tx_{FAKE}"], row[f"retries-per-tx_{REAL}"]
        ),
        axis=1,
    )
    return merged


def deadlock_table(real: pd.DataFrame, fake: pd.DataFrame):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = df[df["overlap-pct"] == 100]
        if d.empty:
            d = df
        g = (
            d.groupby("num-keys")["latency-ms"]
            .quantile([0.5, 0.9])
            .unstack()
            .reset_index()
        )
        g.columns = ["num-keys", "p50", "p90"]
        return g

    r = agg(real)
    f = agg(fake)
    merged = r.merge(f, on="num-keys", suffixes=(f"_{REAL}", f"_{FAKE}"))
    for pct in ("p50", "p90"):
        merged[f"{pct}-ratio"] = merged.apply(
            lambda row, p=pct: _ratio(row[f"{p}_{FAKE}"], row[f"{p}_{REAL}"]), axis=1
        )
    return merged


def print_table(title: str, df: pd.DataFrame) -> None:
    print(f"\n## {title}\n")
    if df.empty:
        print("(no overlapping data)")
        return
    with pd.option_context(
        "display.max_rows", None, "display.width", 200, "display.float_format", "{:.3f}".format
    ):
        print(df.to_string(index=False))


def summarize_ratios(name: str, ratios: pd.Series) -> str:
    r = ratios.dropna()
    if r.empty:
        return f"{name}: no data"
    return (
        f"{name}: ratio fake/real "
        f"min={r.min():.2f} median={r.median():.2f} max={r.max():.2f} "
        f"(geomean={_geomean(r):.2f})"
    )


def _geomean(s: pd.Series) -> float:
    import numpy as np

    s = s[s > 0]
    if s.empty:
        return float("nan")
    return float(np.exp(np.log(s).mean()))


def _tidy_throughput(real: pd.DataFrame, fake: pd.DataFrame, conc_per_db: int):
    frames = []
    for src, df in ((REAL, real), (FAKE, fake)):
        d = df.copy()
        d["concurrent"] = d["num-db"] * conc_per_db
        d["total-tps"] = d["tx-per-sec"] * d["num-db"]
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_retries(real: pd.DataFrame, fake: pd.DataFrame, conc_per_db: int):
    frames = []
    for src, df in ((REAL, real), (FAKE, fake)):
        d = df.copy()
        d["concurrent"] = d["num-db"] * conc_per_db
        d["retries-per-tx"] = d["num-retries"] / d["num-tx"].where(d["num-tx"] > 0)
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_deadlock(real: pd.DataFrame, fake: pd.DataFrame):
    frames = []
    for src, df in ((REAL, real), (FAKE, fake)):
        d = df[df["overlap-pct"] == 100].copy()
        if d.empty:
            d = df.copy()
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def plot_overlay_throughput(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="total-tps",
        hue="tx-type",
        style="source",
        estimator="median",
        errorbar=None,
        ax=ax,
    )
    ax.set_title("Transaction throughput: real vs fake")
    ax.set_xlabel("Concurrent transactions")
    ax.set_ylabel("Transactions / sec")
    _save(fig, out_dir, "cmp-tx-throughput.png")


def plot_overlay_retries(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="retries-per-tx",
        style="source",
        estimator="median",
        errorbar=None,
        marker="o",
        ax=ax,
    )
    ax.set_title("Transaction retries: real vs fake")
    ax.set_xlabel("Concurrent transactions")
    ax.set_ylabel("Retries per transaction")
    _save(fig, out_dir, "cmp-retries.png")


def plot_overlay_deadlock(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="num-keys",
        y="latency-ms",
        style="source",
        estimator="median",
        errorbar=("pi", 80),
        marker="o",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Latency under contention: real vs fake")
    ax.set_xlabel("Contended keys (5 workers, 100% overlap)")
    ax.set_ylabel("Transaction latency (ms, log scale)")
    _save(fig, out_dir, "cmp-deadlock-latency.png")


def _save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=120, bbox_inches="tight")
    print(f"wrote {path}")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent
    parser.add_argument("--real", type=Path, default=base / "out")
    parser.add_argument("--fake", type=Path, default=base / "out-fake")
    parser.add_argument("--out", type=Path, default=base / "out-fake")
    parser.add_argument("--concurrency-per-db", type=int, default=10)
    parser.add_argument(
        "--no-plots", action="store_true", help="skip writing overlay PNGs"
    )
    args = parser.parse_args()

    sns.set_theme(style="whitegrid", context="talk")

    print(f"# rtbench comparison: fake={args.fake} vs real={args.real}")

    summaries: list[str] = []

    real_tp = read_csv(args.real, "throughput.csv")
    fake_tp = read_csv(args.fake, "throughput.csv")
    if real_tp is not None and fake_tp is not None:
        tbl = throughput_table(real_tp, fake_tp, args.concurrency_per_db)
        cols = [
            "concurrent",
            "tx-type",
            f"total-tps_{REAL}",
            f"total-tps_{FAKE}",
            "ratio",
        ]
        print_table("Throughput (total tx/s, fake/real ratio)", tbl[cols])
        for tx_type, grp in tbl.groupby("tx-type"):
            summaries.append(summarize_ratios(f"throughput[{tx_type}]", grp["ratio"]))

    real_st = read_csv(args.real, "stats.csv")
    fake_st = read_csv(args.fake, "stats.csv")
    if real_st is not None and fake_st is not None:
        tbl = retries_table(real_st, fake_st, args.concurrency_per_db)
        cols = [
            "concurrent",
            f"retries-per-tx_{REAL}",
            f"retries-per-tx_{FAKE}",
            "ratio",
        ]
        print_table("Retries per transaction (fake/real ratio)", tbl[cols])
        summaries.append(summarize_ratios("retries", tbl["ratio"]))

    real_dl = read_csv(args.real, "deadlock.csv")
    fake_dl = read_csv(args.fake, "deadlock.csv")
    if real_dl is not None and fake_dl is not None:
        tbl = deadlock_table(real_dl, fake_dl)
        print_table("Deadlock latency at 100% overlap (ms, fake/real ratio)", tbl)
        summaries.append(summarize_ratios("deadlock-p50", tbl["p50-ratio"]))
        summaries.append(summarize_ratios("deadlock-p90", tbl["p90-ratio"]))

    print("\n## Summary (closer to 1.0 = better match)\n")
    for s in summaries:
        print(f"- {s}")

    if not args.no_plots:
        if real_tp is not None and fake_tp is not None:
            plot_overlay_throughput(
                _tidy_throughput(real_tp, fake_tp, args.concurrency_per_db), args.out
            )
        if real_st is not None and fake_st is not None:
            plot_overlay_retries(
                _tidy_retries(real_st, fake_st, args.concurrency_per_db), args.out
            )
        if real_dl is not None and fake_dl is not None:
            plot_overlay_deadlock(_tidy_deadlock(real_dl, fake_dl), args.out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
