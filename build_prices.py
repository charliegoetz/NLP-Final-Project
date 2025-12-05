import os
import pandas as pd
import yfinance as yf
from datasets import load_dataset

# 1. Load the transcripts dataset
print("Loading S&P 500 transcripts dataset from Hugging Face...")
ds = load_dataset("glopardo/sp500-earnings-transcripts", split="train")
df = ds.to_pandas()

# Keep only what we need
df = df[["ticker", "earnings_date", "transcript"]].copy()

# Convert earnings_date to datetime
df["earnings_date"] = pd.to_datetime(df["earnings_date"], errors="coerce")

# 2. Work out universe of tickers and date range, ignoring missing dates
valid_dates = df["earnings_date"].dropna()
tickers = sorted(df["ticker"].dropna().unique().tolist())
min_date = valid_dates.min()
max_date = valid_dates.max()

print(f"Found {len(tickers)} tickers, from {min_date.date()} to {max_date.date()}")

# Add a small buffer around dates
start = (min_date - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
end = (max_date + pd.Timedelta(days=5)).strftime("%Y-%m-%d")
print(f"Downloading price data from {start} to {end}")

# 3. Download price data for each ticker and store in dict
price_data = {}

for t in tickers:
    print(f"Downloading prices for {t}...")
    try:
        hist = yf.download(t, start=start, end=end, progress=False)
        if hist.empty:
            print(f"  [WARN] No data for {t}, skipping")
            continue

        # Ensure index is datetime and sorted
        hist = hist.sort_index()
        price_data[t] = hist
    except Exception as e:
        print(f"  [ERROR] Failed for {t}: {e}")

print(f"Downloaded data for {len(price_data)} tickers")


def get_pre_post_close(prices: pd.DataFrame, call_date: pd.Timestamp):
    """
    pre_close: close at call_date if trading day, otherwise last trading day before call_date
    post_close: close on the next trading day after the pre_close date
    """
    idx = prices.index

    # Find insertion point for call_date
    pos = idx.searchsorted(call_date)

    # pre index: if exact date match, use it, else previous trading day
    if pos < len(idx) and idx[pos].normalize() == call_date.normalize():
        pre_idx = pos
    else:
        pre_idx = pos - 1

    if pre_idx < 0:
        return None, None

    pre_close = prices["Close"].iloc[pre_idx]

    # post is next trading day after pre_idx
    post_idx = pre_idx + 1
    if post_idx >= len(idx):
        post_close = None
    else:
        post_close = prices["Close"].iloc[post_idx]

    pre_val = float(pre_close) if pre_close is not None else None
    post_val = float(post_close) if post_close is not None else None
    return pre_val, post_val


# 5. Loop through transcripts and attach pre and post closes
pre_closes = []
post_closes = []

missing_price_tickers = set()

for i, row in df.iterrows():
    t = row["ticker"]
    call_date = row["earnings_date"]

    # If earnings_date is missing, we cannot compute prices
    if pd.isna(call_date):
        pre_closes.append(None)
        post_closes.append(None)
        continue

    call_date = call_date.normalize()

    if t not in price_data:
        pre_closes.append(None)
        post_closes.append(None)
        missing_price_tickers.add(t)
        continue

    prices = price_data[t]
    pre, post = get_pre_post_close(prices, call_date)
    pre_closes.append(pre)
    post_closes.append(post)

df["pre_close"] = pre_closes
df["post_close"] = post_closes

print(f"Number of rows with missing prices: {df['pre_close'].isna().sum()}")

if missing_price_tickers:
    print("Tickers with no price data:", sorted(missing_price_tickers))

# 6. Save to Desktop as CSV
out_path = "/Users/CharlieGoetzAir/Desktop/sp500_transcripts_with_prices.csv"
df.to_csv(out_path, index=False)
print(f"Saved merged dataset with prices to: {out_path}")
