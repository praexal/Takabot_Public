import os
import pandas as pd

CSV_PATH = os.path.join("trade_logs", "orb_920_backtest.csv")
TARGET_PREMIUM = 100.0

def deduplicate_csv():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print("CSV not found or empty.")
        return

    df = pd.read_csv(CSV_PATH)
    before = len(df)

    # Auto-detect entry column (supports both old and new schemas)
    entry_col = next((c for c in ["soft_entry", "loose_entry", "hard_entry", "lcl_entry"] if c in df.columns), None)
    if not entry_col:
        print("No entry price column found.")
        return

    df["_diff"] = (pd.to_numeric(df[entry_col], errors="coerce") - TARGET_PREMIUM).abs()
    df["_row"] = range(len(df))

    # Closest to 100 first; on exact tie, keep the latest run
    df = (
        df.sort_values(by=["date", "_diff", "_row"], ascending=[True, True, False])
        .drop_duplicates(subset=["date"], keep="first")
        .drop(columns=["_diff", "_row"])
        .sort_values(by="date")
        .reset_index(drop=True)
    )

    df.to_csv(CSV_PATH, index=False)
    print(f"Done: {before} -> {len(df)} rows (removed {before - len(df)} duplicates).")

if __name__ == "__main__":
    deduplicate_csv()