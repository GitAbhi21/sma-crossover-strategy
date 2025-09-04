#!/usr/bin/env python3
"""
run_strategy.py
---------------
Reads a JSON config, loads a *cleaned* CSV (data/ohlc_clean.csv by default),
runs SMA crossover (long/flat, next day's open), and writes outputs/orders.xlsx.

Run:
    python run_strategy.py --config configs/strategy.json

Deps:
    pip install pandas numpy openpyxl python-dateutil
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import pandas as pd
import numpy as np
import os
from pathlib import Path

@dataclass
class StrategyParams:
    fast: int
    slow: int

@dataclass
class ExecutionParams:
    fill: str  # "next_open"
    qty: int

@dataclass
class Config:
    data_file: str
    timezone: str
    start_equity: float
    strategy_name: str
    strategy_params: StrategyParams
    execution: ExecutionParams

def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    strat = raw["strategy"]
    exe = raw["execution"]
    return Config(
        data_file=raw["data_file"],
        timezone=raw["timezone"],
        start_equity=float(raw["start_equity"]),
        strategy_name=strat["name"],
        strategy_params=StrategyParams(
            fast=int(strat["params"]["fast"]),
            slow=int(strat["params"]["slow"]),
        ),
        execution=ExecutionParams(
            fill=str(exe["fill"]),
            qty=int(exe["qty"]),
        ),
    )

def ensure_clean_csv_exists(raw_csv="data/ohlc_tv.csv",
                            cleaned_csv="data/ohlc_clean.csv",
                            report_path="outputs/validation_report.txt") -> None:
    if os.path.exists(cleaned_csv):
        return
    if not os.path.exists(raw_csv):
        raise FileNotFoundError(
            f"Missing raw CSV '{raw_csv}'. Run the Yahoo fetch first (python fetch_yahoo.py)."
        )

    df = pd.read_csv(raw_csv)
    df.columns = [c.lower() for c in df.columns]
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Input CSV missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    before = len(df)

    df = df.sort_values("date").drop_duplicates().reset_index(drop=True)

    # Drop rows missing any of O/H/L/C (volume may be NaN)
    ohlc = ["open", "high", "low", "close"]
    miss_mask = df[ohlc].isna().any(axis=1) | df["date"].isna()
    dropped = int(miss_mask.sum())
    df = df.loc[~miss_mask].copy()

    start_date = df["date"].iloc[0] if len(df) else None
    end_date = df["date"].iloc[-1] if len(df) else None
    coverage_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days if len(df) > 1 else None

    Path(os.path.dirname(cleaned_csv)).mkdir(parents=True, exist_ok=True)
    df.to_csv(cleaned_csv, index=False)

    Path(os.path.dirname(report_path)).mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("Validation Report\n")
        f.write("=================\n")
        f.write(f"Start date: {start_date}\n")
        f.write(f"End date:   {end_date}\n")
        f.write(f"Coverage (days): {coverage_days}\n")
        f.write(f"Rows before: {before}\n")
        f.write(f"Rows dropped (missing OHLC/date): {dropped}\n")
        f.write(f"Rows after:  {len(df)}\n")

def run_sma_crossover(cfg: Config) -> pd.DataFrame:
    ensure_clean_csv_exists()

    df = pd.read_csv(cfg.data_file)
    df.columns = [c.lower() for c in df.columns]
    for col in ["date", "open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"Cleaned CSV missing required column: {col}")
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.date

    df = df.sort_values("date").reset_index(drop=True)

    fast, slow = cfg.strategy_params.fast, cfg.strategy_params.slow
    if fast <= 0 or slow <= 0:
        raise ValueError("SMA window lengths must be positive.")
    if fast >= slow:
        print("Warning: fast >= slow; crossover may be degenerate.")

    df["sma_fast"] = df["close"].rolling(window=fast, min_periods=fast).mean()
    df["sma_slow"] = df["close"].rolling(window=slow, min_periods=slow).mean()

    fast_gt = df["sma_fast"] > df["sma_slow"]
    cross_up = fast_gt & (~fast_gt).shift(1, fill_value=False)      # fast crosses above slow
    cross_dn = (~fast_gt) & fast_gt.shift(1, fill_value=False)      # fast crosses below slow

    raw_signal = pd.Series(np.where(cross_up, 1, np.where(cross_dn, -1, 0)), index=df.index)
    signal = raw_signal.shift(1).fillna(0)  # no look-ahead: act next bar

    orders = []
    position = 0
    entry_price: Optional[float] = None
    entry_dt: Optional[datetime] = None
    entry_idx: Optional[int] = None
    qty = int(cfg.execution.qty)

    for i in range(len(df) - 1):  # can only execute on next day's open
        sig = int(signal.iloc[i])
        next_open = float(df["open"].iloc[i + 1])
        next_date = df["date"].iloc[i + 1]

        if position == 0 and sig == 1:
            position = 1
            entry_price = next_open
            entry_dt = next_date
            entry_idx = i + 1

        elif position == 1 and sig == -1:
            exit_price = next_open
            exit_dt = next_date
            bars_held = (i + 1) - entry_idx if entry_idx is not None else None
            pnl = (exit_price - float(entry_price)) * qty

            orders.append({
                "entry_dt": pd.Timestamp(entry_dt).date().isoformat() if entry_dt else "",
                "entry_price": float(entry_price) if entry_price is not None else np.nan,
                "qty": qty,
                "exit_dt": pd.Timestamp(exit_dt).date().isoformat(),
                "exit_price": float(exit_price),
                "pnl": float(pnl),
                "bars_held": int(bars_held) if bars_held is not None else np.nan,
            })
            position = 0
            entry_price = None
            entry_dt = None
            entry_idx = None

    if position == 1:  # open at final bar: include with blank exit
        orders.append({
            "entry_dt": pd.Timestamp(entry_dt).date().isoformat() if entry_dt else "",
            "entry_price": float(entry_price) if entry_price is not None else np.nan,
            "qty": qty,
            "exit_dt": "",
            "exit_price": np.nan,
            "pnl": np.nan,
            "bars_held": np.nan,
        })

    orders_df = pd.DataFrame(orders, columns=[
        "entry_dt","entry_price","qty","exit_dt","exit_price","pnl","bars_held"
    ])
    if not orders_df.empty:
        orders_df = orders_df.sort_values("entry_dt").reset_index(drop=True)
    return orders_df

def write_orders_excel(orders: pd.DataFrame, out_path="outputs/orders.xlsx") -> None:
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        orders.to_excel(writer, sheet_name="orders", index=False)

def main():
    parser = argparse.ArgumentParser(description="Run SMA crossover backtest and write outputs/orders.xlsx")
    parser.add_argument("--config", required=True, help="Path to strategy JSON")
    args = parser.parse_args()

    cfg = load_config(args.config)
    orders = run_sma_crossover(cfg)
    write_orders_excel(orders, out_path="outputs/orders.xlsx")
    print("Done. Wrote outputs/orders.xlsx")

if __name__ == "__main__":
    main()
