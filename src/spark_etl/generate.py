"""Generate a synthetic, deliberately messy trade feed plus an instrument reference file.

The feed is skewed (a few symbols carry most of the volume), contains duplicate
trade ids, and includes malformed rows, so the pipeline has real work to do.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

INSTRUMENTS = [
    ("AAPL", "Apple Inc", "Technology", "USD"),
    ("MSFT", "Microsoft Corp", "Technology", "USD"),
    ("JPM", "JPMorgan Chase & Co", "Financials", "USD"),
    ("XOM", "Exxon Mobil Corp", "Energy", "USD"),
    ("JNJ", "Johnson & Johnson", "Health Care", "USD"),
    ("TSLA", "Tesla Inc", "Consumer Discretionary", "USD"),
    ("BAC", "Bank of America Corp", "Financials", "USD"),
    ("NVDA", "NVIDIA Corp", "Technology", "USD"),
]
BASE_PRICE = {"AAPL": 190, "MSFT": 410, "JPM": 200, "XOM": 115, "JNJ": 155, "TSLA": 240, "BAC": 38, "NVDA": 120}
# Heavy skew: AAPL and NVDA dominate the feed.
WEIGHTS = [30, 8, 6, 4, 3, 6, 4, 39]


def generate(out_dir: Path, n_trades: int = 50_000, days: int = 5, seed: int = 7) -> tuple[Path, Path]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    ref_path = out_dir / "instruments.csv"
    with ref_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "name", "sector", "currency"])
        w.writerows(INSTRUMENTS)

    trades_path = out_dir / "trades.csv"
    start = datetime(2026, 10, 5, 13, 30)
    symbols = [i[0] for i in INSTRUMENTS]
    with trades_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trade_id", "symbol", "side", "price", "quantity", "trade_ts", "venue"])
        for n in range(n_trades):
            sym = rng.choices(symbols, WEIGHTS)[0]
            ts = start + timedelta(days=rng.randrange(days), seconds=rng.randrange(6 * 3600 + 1800))
            price = round(BASE_PRICE[sym] * (1 + rng.gauss(0, 0.01)), 4)
            row = [f"T{n:08d}", sym, rng.choice(["BUY", "SELL"]), price, rng.randint(1, 2000),
                   ts.isoformat(), rng.choice(["XNYS", "XNAS", "ARCX", "BATS"])]
            roll = rng.random()
            if roll < 0.01:
                row[3] = "N/A"          # unparseable price
            elif roll < 0.02:
                row[4] = -row[4]        # negative quantity
            elif roll < 0.025:
                row[1] = "ZZZZ"         # unknown instrument
            w.writerow(row)
            if rng.random() < 0.02:     # duplicate delivery from upstream
                w.writerow(row)
    return trades_path, ref_path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="data/raw")
    p.add_argument("--trades", type=int, default=50_000)
    p.add_argument("--days", type=int, default=5)
    a = p.parse_args()
    t, r = generate(Path(a.out), a.trades, a.days)
    print(f"wrote {t} and {r}")


if __name__ == "__main__":
    main()
