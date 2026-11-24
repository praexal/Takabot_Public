import pandas as pd
import os

INPUT_CSV = "trade_logs/ml_raw_data.csv"
OUTPUT_CSV = "trade_logs/ml_labeled_data.csv"

# --- LOGIC SETTINGS ---
LOOKAHEAD_MINS = 15   # Max minutes to wait for target/SL (logger runs every 60s)
SL_MULTIPLIER  = 0.6  # Initial Stop Loss = 0.6 * ATR
TGT_MULTIPLIER = 1.2  # Minimum Target    = 1.2 * ATR (1:2 Risk-Reward)
MAX_ATR_CAP    = 10.0 # Caps ATR at 10

def label_group(group):
    group = group.sort_values("timestamp").reset_index(drop=True)
    ltps = group["ltp"].values
    atrs = group["atr"].values
    n = len(group)

    labels, maes, mfes = [], [], []

    for i in range(n):
        if i + LOOKAHEAD_MINS >= n:
            labels.append(-1)
            maes.append(0.0)
            mfes.append(0.0)
            continue

        entry = ltps[i]
        raw_atr = atrs[i] if atrs[i] > 0 else 10.0
        capped_atr = min(raw_atr, MAX_ATR_CAP)

        target = entry + (capped_atr * TGT_MULTIPLIER)
        sl     = entry - (capped_atr * SL_MULTIPLIER)

        outcome = 0
        min_price = entry
        max_price = entry

        for j in range(1, LOOKAHEAD_MINS + 1):
            future_price = ltps[i + j]
            if future_price < min_price:
                min_price = future_price
            if future_price > max_price:
                max_price = future_price

            if future_price <= sl:
                if outcome == 0:
                    break  # Hit SL before ever reaching target
            if future_price >= target:
                outcome = 1  # Winner; keep scanning to record full peak (MFE)

        mae_atr = round((entry - min_price) / capped_atr, 2) if outcome == 1 else SL_MULTIPLIER
        mfe_atr = round((max_price - entry) / capped_atr, 2) if outcome == 1 else TGT_MULTIPLIER

        labels.append(outcome)
        maes.append(mae_atr)
        mfes.append(mfe_atr)

    group["label"] = labels
    group["mae_atr"] = maes
    group["mfe_atr"] = mfes

    return group[group["label"] != -1]

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found. Run data_logger.py during market hours first.")
        return

    df = pd.read_csv(INPUT_CSV)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    labeled_df = df.groupby(["date", "token"], group_keys=False).apply(label_group)
    labeled_df = labeled_df.drop(columns=["date"])

    labeled_df.to_csv(OUTPUT_CSV, index=False)

    wins = (labeled_df["label"] == 1).sum()
    total = len(labeled_df)
    print(f"Done! Saved {total} labeled rows to {OUTPUT_CSV}.")
    if total > 0:
        print(f"Wins (1): {wins} ({wins/total*100:.1f}%) | Losses/Chop (0): {total - wins}")

if __name__ == "__main__":
    main()