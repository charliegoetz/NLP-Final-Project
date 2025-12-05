import os
from datasets import load_dataset
import re

print("Loading S&P 500 transcripts dataset...")
ds = load_dataset("glopardo/sp500-earnings-transcripts", split="train")

# Folder on your Desktop where all .txt files will go
output_dir = "/Users/CharlieGoetzAir/Desktop/sp500_transcripts"
os.makedirs(output_dir, exist_ok=True)
print(f"Saving transcripts into: {output_dir}")


def sanitize(s: str) -> str:
    """Keep only safe characters for filenames."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", s)


for i, row in enumerate(ds):
    ticker = row.get("ticker") or "UNKNOWN"
    year_val = row.get("year")
    quarter_val = row.get("quarter")

    # Handle year (can be float, int, or None)
    if year_val is None:
        year_str = "NA"
    elif isinstance(year_val, (int, float)):
        year_str = str(int(year_val))
    else:
        year_str = sanitize(str(year_val))

    # Handle quarter (can be float, int, or None)
    if quarter_val is None:
        quarter_str = "NA"
    elif isinstance(quarter_val, (int, float)):
        quarter_str = str(int(quarter_val))
    else:
        quarter_str = sanitize(str(quarter_val))

    text = row.get("transcript") or ""
    ticker_clean = sanitize(str(ticker))

    # Example filename: A_2014_Q1_0.txt or A_NA_QNA_123.txt if missing
    filename = f"{ticker_clean}_{year_str}_Q{quarter_str}_{i}.txt"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)

    if i % 500 == 0:
        print(f"Saved {i} files...")

print("All transcripts exported successfully.")
