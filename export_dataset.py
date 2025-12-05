import os
import pandas as pd

# Root folder that will contain everything
OUTPUT_ROOT = "/Users/CharlieGoetzAir/Desktop/sp500_transcripts_dataset"
TRANSCRIPTS_DIR = os.path.join(OUTPUT_ROOT, "transcripts")
METADATA_CSV = os.path.join(OUTPUT_ROOT, "metadata.csv")

# This is the file created by build_prices.py
MERGED_CSV = "/Users/CharlieGoetzAir/Desktop/sp500_transcripts_with_prices.csv"


def main():
    os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)

    print(f"Loading merged dataset with prices from:\n  {MERGED_CSV}")
    df = pd.read_csv(MERGED_CSV)
    print(f"Rows in merged dataset: {len(df)}")

    # Make sure earnings_date is a datetime
    df["earnings_date"] = pd.to_datetime(df["earnings_date"], errors="coerce")

    records = []
    total_written = 0

    for idx, row in df.iterrows():
        ticker = str(row["ticker"]).strip()
        transcript = row.get("transcript", "")

        if not isinstance(transcript, str) or transcript.strip() == "":
            continue

        # clean transcript text
        transcript = transcript.lstrip("\ufeff").strip()

        call_date = row["earnings_date"]
        if pd.isna(call_date):
            # no date, skip
            continue
        date_str = call_date.strftime("%Y-%m-%d")

        # transcripts/TICKER/TICKER_YYYY-MM-DD.txt
        ticker_dir = os.path.join(TRANSCRIPTS_DIR, ticker)
        os.makedirs(ticker_dir, exist_ok=True)

        filename = f"{ticker}_{date_str}.txt"
        rel_path = os.path.join("transcripts", ticker, filename)
        abs_path = os.path.join(OUTPUT_ROOT, rel_path)

        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(transcript)

        records.append(
            {
                "ticker": ticker,
                "company": row.get("company", None),
                "earnings_date": date_str,
                "year": row.get("year", None),
                "quarter": row.get("quarter", None),
                "pre_close": row.get("pre_close", None),
                "post_close": row.get("post_close", None),
                "text_path": rel_path,  # relative, so it works on any machine
            }
        )

        total_written += 1
        if total_written % 500 == 0:
            print(f"Wrote {total_written} transcripts so far...")

    meta_df = pd.DataFrame(records)
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    meta_df.to_csv(METADATA_CSV, index=False)

    print(f"\nDone. Wrote {total_written} transcript files.")
    print(f"Metadata saved to: {METADATA_CSV}")
    print(f"Root folder to share or zip: {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
