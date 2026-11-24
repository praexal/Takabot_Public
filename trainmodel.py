import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, precision_score

# 1. Load labeled data and sort chronologically
df = pd.read_csv("trade_logs/ml_labeled_data.csv")
df = df.sort_values("timestamp").reset_index(drop=True)

# Encode CE/PE as 1/0
df["opt_type_num"] = (df["opt_type"] == "CE").astype(int)

# 2. Define exact features (must match what aislave.py will feed live)
FEATURES = [
    "minute_of_day", "opt_type_num", "prev_rsi", "live_rsi", "opp_live_rsi",
    "ema_spread", "atr", "dist_vwap", "volume", "vol_war_ratio",
    "spot_rsi5", "spot_ema_spread", "dist_poc", "in_ob"
]

X = df[FEATURES]
y = df["label"]

# 3. Strict 80/20 Chronological Split (NO SHUFFLING)
split_idx = int(len(df) * 0.80)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# 4. Calculate class weight for imbalanced data
neg_count = (y_train == 0).sum()
pos_count = max((y_train == 1).sum(), 1)
weight = neg_count / pos_count

# 5. Train XGBoost (Shallow trees prevent overfitting)
model = xgb.XGBClassifier(
    n_estimators=150,
    max_depth=3,          # Keep shallow (3-4) for noisy market data
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=weight,
    eval_metric="logloss",
    random_state=42
)

model.fit(X_train, y_train)

# 6. Evaluate at High Confidence Thresholds
probs = model.predict_proba(X_test)[:, 1]

print("=== THRESHOLD EVALUATION (TEST SET) ===")
for thresh in [0.60, 0.70, 0.75, 0.80]:
    preds = (probs >= thresh).astype(int)
    trades_taken = preds.sum()
    if trades_taken > 0:
        prec = precision_score(y_test, preds)
        print(f"Threshold {thresh:.2f} | Trades: {trades_taken:4d} | Win Rate (Precision): {prec*100:.1f}%")
    else:
        print(f"Threshold {thresh:.2f} | Trades:    0")

# 7. Check which features actually matter
print("\n=== TOP FEATURES ===")
imp = pd.Series(model.feature_importances_, index=FEATURES).sort_values(ascending=False)
print(imp.head(8))

# 8. Save the trained brain
model.save_model("ai_brain.json")
print("\nSaved model to ai_brain.json")