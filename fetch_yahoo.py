#!/usr/bin/env python3
"""
fetch_yahoo.py
--------------
Fetch last ~3 months of DAILY OHLCV from Yahoo Finance for symbols listed in
configs/symbols.json, and save results into:

    data/ohlc_tv.csv   (all symbols stacked, like TradingView export)
    data/ohlc_all.json (backup as JSON)

Usage:
    python fetch_yahoo.py --symbols configs/symbols.json
"""

import argparse
import json
import os
from pathlib import Path
import pandas as pd


def _require_yfinance():
    try:
        import yfinance as yf  # noqa
    except Exception as e:
        raise SystemExit(
            "Missing dependency 'yfinance'. Install it with:\n"
            "    pip install yfinance"
        ) from e


def load_symbols_config(path: str) -> dict:
    """Load configs/symbols.json"""
    print(f"[DEBUG] Loading symbols config from {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    print("[DEBUG] Loaded symbols config:", cfg)
    return cfg


def fetch_symbol(symbol: str) -> pd.DataFrame:
    """Fetch OHLCV data for one symbol from Yahoo Finance"""
    _require_yfinance()
    import yfinance as yf

    print(f"\n[INFO] Fetching {symbol} ...")
    df = yf.download(symbol, period="3mo", interval="1d",
                     auto_adjust=False, progress=False)

    print(f"[DEBUG] Raw shape for {symbol}: {df.shape}")
    print("[DEBUG] Raw columns:", df.columns.tolist())

    if df.empty:
        raise RuntimeError(f"[ERROR] No data returned for {symbol}")

    df = df.reset_index()

    # Flatten MultiIndex if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        print("[DEBUG] Flattened MultiIndex columns ->", df.columns.tolist())

    # Rename columns
    col_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "close",  # fallback
        "Volume": "volume",
    }
    df.rename(columns=col_map, inplace=True)
    print("[DEBUG] After renaming columns ->", df.columns.tolist())

    # Keep required cols
    expected = ["date", "open", "high", "low", "close", "volume"]
    df = df[[c for c in expected if c in df.columns]]

    # Fix missing
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"[WARN] {symbol} missing columns: {missing}")

    # ✅ FIX 1 — Proper datetime handling
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        # keep ISO string (yyyy-mm-dd) so JSON is safe
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Add symbol column
    df["symbol"] = symbol

    print(f"[INFO] Cleaned rows for {symbol}: {len(df)}")
    print(df.head())

    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch OHLCV from Yahoo Finance")
    parser.add_argument("--symbols", default="configs/symbols.json",
                        help="Path to symbols JSON")
    args = parser.parse_args()

    symbols_cfg = load_symbols_config(args.symbols)
    symbols = symbols_cfg.get("symbols", [])
    if not symbols:
        raise ValueError("No 'symbols' key found in symbols.json")

    all_frames = []
    for sym in symbols:
        if isinstance(sym, dict) and "symbol" in sym:
            symbol = sym["symbol"]
        elif isinstance(sym, str):
            symbol = sym
        else:
            raise ValueError(f"Unrecognized entry in symbols.json: {sym}")

        try:
            df = fetch_symbol(symbol)
            all_frames.append(df)
        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol}: {e}")

    # Combine everything
    if not all_frames:
        raise RuntimeError("No data fetched for any symbol!")

    combined = pd.concat(all_frames, ignore_index=True)

    # Save CSV like TradingView
    out_csv = "data/ohlc_tv.csv"
    Path(os.path.dirname(out_csv)).mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_csv, index=False)
    print(f"\n[INFO] Wrote {out_csv} with {len(combined)} rows.")

    # Also save JSON backup
    out_json = "data/ohlc_all.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(combined.to_dict(orient="records"), f, indent=2)
    print(f"[INFO] Wrote {out_json}")


if __name__ == "__main__":
    main()
