# SMA Crossover Strategy

## 📂 Contents
- `fetch_yahoo.py`: Fetches raw OHLCV data from Yahoo Finance
- `clean_data.py`: Cleans and validates raw data into `ohlc_clean.csv`
- `run_strategy.py`: Main script to run SMA crossover backtest
- `configs/symbols.json`: List of stock symbols to fetch
- `configs/strategy.json`: Strategy parameters
- `data/ohlc_tv.csv`: Raw OHLC data (Yahoo/TradingView-like export)
- `data/ohlc_clean.csv`: Cleaned OHLC data
- `outputs/validation_report.txt`: Data validation summary
- `outputs/orders.xlsx`: Backtest trade results
- `docs/tv_export.png`: Screenshot of export/fetch settings

## 📊 How Data Was Exported  
Here, data is fetched automatically using **Yahoo Finance (`yfinance`)**.  

## ▶️ How to Run
```bash
# 1. Fetch OHLC data
python fetch_yahoo.py --symbols configs/symbols.json

# 2. Clean and validate raw data
python clean_data.py

# 3. Run SMA crossover backtest
python run_strategy.py --config configs/strategy.json
