#!/usr/bin/env python3
"""
clean_data.py
-------------
Cleans the raw Yahoo Finance export stored in data/ohlc_tv.csv and
saves a cleaned version in data/ohlc_clean.csv.

Steps:
1. Reads data/ohlc_tv.csv
2. Parses the date column as DD-MM-YYYY (dayfirst=True)
3. Sorts ascending by date
4. Keeps all rows (no duplicate removal)
5. Drops rows missing open/high/low/close
6. Reorders columns -> date, symbol, open, high, low, close, volume
7. Saves cleaned CSV as data/ohlc_clean.csv
8. Writes outputs/validation_report.txt
"""

import os
from pathlib import Path
import pandas as pd


def main():
    in_csv = "data/ohlc_tv.csv"
    out_csv = "data/ohlc_clean.csv"
    report_file = "outputs/validation_report.txt"

    # Ensure outputs folder
    Path("outputs").mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Reading {in_csv} ...")
    if not os.path.exists(in_csv):
        raise FileNotFoundError(f"[ERROR] File not found: {in_csv}")

    df = pd.read_csv(in_csv)
    print(f"[DEBUG] Loaded {len(df)} rows from {in_csv}")

    # Parse date column with dayfirst=True
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    before_rows = len(df)

    # Drop rows with bad dates
    df = df.dropna(subset=["date"])

    # Sort ascending by date
    df = df.sort_values("date").reset_index(drop=True)

    # Drop rows missing OHLC
    before_ohlc_rows = len(df)
    df = df.dropna(subset=["open", "high", "low", "close"])
    after_ohlc_rows = len(df)

    # Reorder columns
    col_order = ["date", "symbol", "open", "high", "low", "close", "volume"]
    existing = [c for c in col_order if c in df.columns]
    df = df[existing]

    # Final stats
    start_date = df["date"].min()
    end_date = df["date"].max()
    days = (end_date - start_date).days
    rows_kept = len(df)

    # Save cleaned CSV
    Path(os.path.dirname(out_csv)).mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"[INFO] Wrote cleaned file -> {out_csv} with {rows_kept} rows")

    # Write validation report
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("Validation Report\n")
        f.write("=================\n")
        f.write(f"Start date: {start_date.date()}\n")
        f.write(f"End date:   {end_date.date()}\n")
        f.write(f"Days span:  {days} days (~{days/30:.1f} months)\n")
        f.write(f"Rows before: {before_rows}\n")
        f.write(f"Rows after dropping bad OHLC: {after_ohlc_rows}\n")
        f.write(f"Final rows kept: {rows_kept}\n")

    print(f"[INFO] Validation report written -> {report_file}")


if __name__ == "__main__":
    main()
