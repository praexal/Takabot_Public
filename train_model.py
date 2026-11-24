import os
import pandas as pd
import xgboost as xgb
from sklearn.metrics import precision_score, mean_absolute_error

INPUT_CSV = "trade_logs/ml_labeled_data.csv"

FEATURES = [
    "minute_of_day", "opt_type_num", "prev_rsi", "live_rsi", "opp_live_rsi",
    "ema_spread", "atr", "dist_vwap", "volume", "vol_war_ratio",
    "spot_rsi5", "spot_ema_spread", "dist_poc", "in_ob"
]

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found. Run label_data.py first.")
        return

    df = pd.read_csv(INPUT_CSV).sort_values("timestamp").reset_index(drop=True)
    df["opt_type_num"] = (df["opt_type"] == "CE").astype(int)

    # Strict 80/20 Chronological Split (No Shuffling)
    split = int(len(df) * 0.80)
    train_df, test_df = df.iloc[:split], df.iloc[split:]

    # ==========================================
    # BRAIN 1: ENTRY CLASSIFIER (Win vs Loss)
    # ==========================================
    X_train, y_train = train_df[FEATURES], train_df["label"]
    X_test,  y_test  = test_df[FEATURES],  test_df["label"]

    weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    entry_model = xgb.XGBClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, scale_pos_weight=weight,
        eval_metric="logloss", random_state=42
    )
    entry_model.fit(X_train, y_train)
    entry_model.save_model("ai_entry_brain.json")

    probs = entry_model.predict_proba(X_test)[:, 1]
    print("=== BRAIN 1: ENTRY CLASSIFIER (TEST SET) ===")
    for thresh in [0.65, 0.70, 0.75, 0.80]:
        preds = (probs >= thresh).astype(int)
        if preds.sum() > 0:
            win_rate = precision_score(y_test, preds) * 100
            print(f"Confidence >= {thresh:.2f} | Trades: {preds.sum():4d} | Win Rate: {win_rate:.1f}%")
        else:
            print(f"Confidence >= {thresh:.2f} | Trades:    0")

    # ==========================================
    # BRAIN 2: DYNAMIC ATR SL REGRESSOR (MAE)
    # ==========================================
    win_train = train_df[train_df["label"] == 1]
    win_test  = test_df[test_df["label"] == 1]

    if len(win_train) < 10:
        print("\nNot enough winning trades yet to train SL and Trail brains.")
        return

    sl_model = xgb.XGBRegressor(
        n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42
    )
    sl_model.fit(win_train[FEATURES], win_train["mae_atr"])
    sl_model.save_model("ai_sl_brain.json")

    if len(win_test) > 0:
        sl_preds = sl_model.predict(win_test[FEATURES])
        err_sl = mean_absolute_error(win_test["mae_atr"], sl_preds)
        print(f"\n=== BRAIN 2: DYNAMIC SL (MAE) ===")
        print(f"Avg SL Prediction Error: +/- {err_sl:.2f} ATR")

    # ==========================================
    # BRAIN 3: DYNAMIC TRAIL / PEAK REGRESSOR (MFE)
    # ==========================================
    trail_model = xgb.XGBRegressor(
        n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42
    )
    trail_model.fit(win_train[FEATURES], win_train["mfe_atr"])
    trail_model.save_model("ai_trail_brain.json")

    if len(win_test) > 0:
        mfe_preds = trail_model.predict(win_test[FEATURES])
        err_mfe = mean_absolute_error(win_test["mfe_atr"], mfe_preds)
        print(f"\n=== BRAIN 3: DYNAMIC TRAIL (MFE) ===")
        print(f"Avg Peak Prediction Error: +/- {err_mfe:.2f} ATR")

    print("\nSaved: ai_entry_brain.json, ai_sl_brain.json, ai_trail_brain.json")

if __name__ == "__main__":
    main()