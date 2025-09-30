import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("yfinance not installed. Install with: pip install yfinance", file=sys.stderr)
    raise


def load_tickers(path: Path) -> list[str]:
    return pd.read_csv(path)["ticker"].astype(str).tolist()


def fetch(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    data = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)["Close"]
    if isinstance(data, pd.Series):
        data = data.to_frame()
    return data


def to_long(close_wide: pd.DataFrame) -> pd.DataFrame:
    long = close_wide.reset_index().melt(id_vars=["Date"], var_name="symbol", value_name="close")
    long = long.rename(columns={"Date": "date"}).sort_values(["symbol", "date"]).reset_index(drop=True)
    return long


def main():
    root = Path(__file__).resolve().parents[1]
    tickers_file = root / "data" / "tickers-top20-us.csv"
    out_csv = root / "data" / "prices-top20.csv"
    tickers = load_tickers(tickers_file)
    end = datetime.utcnow().date()
    start = end - timedelta(days=365*3)
    close = fetch(tickers, start=start.isoformat(), end=end.isoformat())
    long = to_long(close)
    long.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} with {len(long):,} rows and {long['symbol'].nunique()} tickers")


if __name__ == "__main__":
    main()
