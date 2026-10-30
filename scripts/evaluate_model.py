"""Compares the old (v1) and current (v2) anomaly models on a recorded trade file.

    python scripts/evaluate_model.py data/history-60k.jsonl

Two numbers per model:
- flag rate: share of real trades flagged. Real data has no labels, so this is the "how
  often does it raise an alert" cost.
- spikes caught: we copy the data, plant clearly abnormal trades at random positions
  (a price move 3x the symbol's 99th-percentile move, or a trade 5x its 99th-percentile
  size), and count how many of those each model flags.

v1 is kept here exactly as it was (last 30 raw prices/sizes INCLUDING the current trade,
mean/stdev z-score, threshold 3.0) so the before/after numbers stay reproducible. v2 is the
model in anomaly_detector.py. Both are run per symbol, the way the pipeline sees them.
"""

import argparse
import json
from itertools import pairwise
import math
import random
import statistics
import sys
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import anomaly_detector  # noqa: E402

SPIKES_PER_KIND = 200  # per symbol


def v1_z_score(value, values):
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    if stdev == 0:
        return 0.0
    return (value - mean) / stdev


def v1_classify(price_z, vol_z, threshold=3.0):
    if abs(price_z) > threshold and abs(vol_z) > threshold:
        return "both"
    if abs(price_z) > threshold:
        return "price"
    if abs(vol_z) > threshold:
        return "volume"
    return None


def run_v1(trades, window_size=30):
    """{(symbol, trade_id): anomaly_type} for every trade v1 scores."""
    windows = {}
    flags = {}
    for t in trades:
        window = windows.setdefault(t["s"], deque(maxlen=window_size))
        price, quantity = float(t["p"]), float(t["q"])
        window.append((price, quantity))
        if len(window) < window_size:
            continue  # v1 never even published warm-up trades
        price_z = v1_z_score(price, [p for p, _ in window])
        vol_z = v1_z_score(quantity, [q for _, q in window])
        flags[(t["s"], t["t"])] = v1_classify(price_z, vol_z)
    return flags


def run_v2(trades):
    windows = {}
    flags = {}
    for t in trades:
        window = windows.setdefault(t["s"], anomaly_detector.new_window())
        price_z, size_ratio = anomaly_detector.score_trade(window, float(t["p"]), float(t["q"]))
        if price_z is None and size_ratio is None:
            continue  # warming up: published, but not scored
        flags[(t["s"], t["t"])] = anomaly_detector.classify_anomaly(price_z, size_ratio)
    return flags


def plant_spikes(trades, seed):
    """Copy of `trades` with abnormal trades planted; returns (trades, {key: kind})."""
    rng = random.Random(seed)
    by_symbol = {}
    for t in trades:
        by_symbol.setdefault(t["s"], []).append(t)
    planted = {}
    for symbol, ts in by_symbol.items():
        prices = [float(t["p"]) for t in ts]
        moves = sorted(abs(math.log(b / a)) for a, b in pairwise(prices))
        sizes = sorted(float(t["q"]) for t in ts)
        p99_move, p99_size = moves[int(0.99 * len(moves))], sizes[int(0.99 * len(sizes))]
        # skip the first 400 so both models are past warm-up
        positions = rng.sample(range(400, len(ts)), 2 * SPIKES_PER_KIND)
        for i, pos in enumerate(positions):
            kind = "price" if i < SPIKES_PER_KIND else "volume"
            value = p99_move * 3 if kind == "price" else p99_size * 5
            planted[(symbol, ts[pos]["t"])] = (kind, value)

    out = []
    for t in trades:
        spike = planted.get((t["s"], t["t"]))
        if spike:
            t = dict(t)
            kind, value = spike
            if kind == "price":
                t["p"] = str(float(t["p"]) * math.exp(value))
            else:
                t["q"] = str(value)
        out.append(t)
    return out, {k: kind for k, (kind, _) in planted.items()}


def evaluate(name, model, trades, spiked, planted):
    clean = model(trades)
    dirty = model(spiked)
    flag_rate = sum(1 for v in clean.values() if v) / len(clean) * 100

    def caught(kind):
        hits = [k for k, planted_kind in planted.items() if planted_kind == kind]
        return sum(1 for k in hits if dirty.get(k) in (kind, "both")) / len(hits) * 100

    print(f"{name:<44} {flag_rate:6.2f}% {caught('price'):12.1f}% {caught('volume'):12.1f}%")
    return {"flag_rate": flag_rate, "price_caught": caught("price"), "size_caught": caught("volume")}


def main():
    parser = argparse.ArgumentParser(description="Compare the v1 and v2 anomaly models on recorded trades")
    parser.add_argument("file", type=Path)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    trades = [json.loads(line) for line in args.file.open()]
    spiked, planted = plant_spikes(trades, args.seed)
    print(f"{args.file}: {len(trades)} real trades, symbols {sorted({t['s'] for t in trades})}, "
          f"{len(planted)} planted spikes\n")
    print(f"{'model':<44} {'flag rate':>7} {'price spikes':>13} {'size spikes':>13}")
    evaluate("v1: raw price/size, mean/stdev, 30 trades", run_v1, trades, spiked, planted)
    evaluate(f"v2: log return z>{anomaly_detector.PRICE_Z_THRESHOLD:g}, "
             f"size>={anomaly_detector.SIZE_RATIO_THRESHOLD:g}x median, "
             f"{anomaly_detector.WINDOW_SIZE} trades",
             run_v2, trades, spiked, planted)


if __name__ == "__main__":
    main()
