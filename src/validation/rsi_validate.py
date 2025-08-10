# validate_rsi.py
from __future__ import annotations
import os, math
from typing import Deque, Dict, Any, List
from collections import deque

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from mongo_io import MongoConfig, MongoWrapper

# --------- use some financial libs, TA first, then pandas ---------
def compute_rsi_lib(df_k: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Calculate RSI using financial libraries.
    """
    close = pd.to_numeric(df_k["close"], errors="coerce")
    rsi = None

    # 1) TA-Lib if applicable
    try:
        import talib  # type: ignore
        rsi = talib.RSI(close.values.astype(float), timeperiod=period)
    except Exception:
        print("TA-Lib.RSI failed, trying pandas_ta.")
        pass

    # 2) pandas_ta fallback
    if rsi is None:
        try:
            import pandas_ta as ta  # type: ignore
            rsi = ta.rsi(close, length=period).values
        except Exception as e:
            print(f"pandas_ta.rsi failed: {e}")
            raise RuntimeError("Both TA-Lib and pandas_ta are unavailable for RSI calculation.") from e

    out = pd.DataFrame({
        "starttime": df_k["starttime"].astype(int).values,
        "rsi_lib": pd.to_numeric(rsi, errors="coerce"),
    })
    out = out.dropna().reset_index(drop=True)
    return out


# --------- C++ style RSI implementation (consistent with ChomoSyncer Indicators) ---------
def compute_rsi_cpp_style(df_k: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    assert {"starttime", "endtime", "close"}.issubset(df_k.columns), "kline df 缺列"
    df = df_k.sort_values("starttime").reset_index(drop=True)

    gains: Deque[float] = deque()
    losses: Deque[float] = deque()
    avg_gain = 0.0
    avg_loss = 0.0
    initialized = False
    prev_close = None

    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        close = float(row["close"])

        if not initialized:
            prev_close = close
            initialized = True
            continue

        change = close - prev_close
        gain = change if change > 0 else 0.0
        loss = -change if change < 0 else 0.0

        gains.append(gain)
        losses.append(loss)

        if len(gains) > period:
            gains.popleft(); losses.popleft()

        if len(gains) == period:
            if avg_gain == 0.0 and avg_loss == 0.0:
                avg_gain = sum(gains) / period
                avg_loss = sum(losses) / period
            else:
                avg_gain = (avg_gain * (period - 1) + gain) / period
                avg_loss = (avg_loss * (period - 1) + loss) / period

            rsi = 100.0 if avg_loss == 0.0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

            rows.append({
                "starttime": int(row["starttime"]),
                "endtime": int(row["endtime"]),
                "rsi_cpp": float(rsi),
            })

        prev_close = close

    return pd.DataFrame(rows)

def main():
    # ==== Parameters ====
    period = 14
    symbol = "BTCUSDT"
    interval = "5m"
    outlier_eps = 1e-6   # You can also adjust to 1e-3/1e-2 for rough consistency
    start_ms = None
    end_ms = None
    limit = None

    # ==== Connect and Read ====
    if not os.getenv("MONGODB_URI"):
        raise RuntimeError("MONGODB_URI environment variable not detected. Please set it in the current terminal first.")
    cfg = MongoConfig.from_env()
    mw = MongoWrapper(cfg)

    # Read RSI (full) + K-line (full or enough)
    rsi_docs = mw.fetch_rsi_docs(symbol, interval, period=period, start_ms=start_ms, end_ms=end_ms, limit=limit)
    df_r = mw.rsi_docs_to_df(rsi_docs)
    if df_r.empty:
        raise RuntimeError("RSI collection is empty or column names do not match.")
    print(f"Fetched {len(df_r)} RSI docs from {mw.rsi_col(period, symbol, interval)}")

    if "period" in df_r.columns:
        df_r = df_r[df_r["period"] == period].copy()

    klines = mw.fetch_klines(symbol, interval, start_ms=start_ms, end_ms=end_ms, limit=limit)
    df_k = mw.klines_to_df(klines)
    print(f"Fetched {len(df_k)} klines from {mw.kline_col(symbol, interval)}")
    if df_k.empty:
        raise RuntimeError("Kline collection is empty.")

    # ==== re-calculate RSI with C++ style and financial libraries ====
    # For C++ version warmup, K-line uses full data (or ensures coverage of RSI earliest time >= period)
    df_cpp = compute_rsi_cpp_style(df_k, period=period)
    df_lib = compute_rsi_lib(df_k, period=period)

    # ==== Align (only compare where RSI timestamps exist) ====
    # Standardize DB RSI column names
    if "rsi_value" in df_r.columns:
        df_r = df_r.rename(columns={"rsi_value": "rsi_db"})
    elif "rsi" in df_r.columns:
        df_r = df_r.rename(columns={"rsi": "rsi_db"})
    else:
        raise RuntimeError("RSI document does not contain 'rsi' or 'rsi_value' field.")

    # Drop duplicates by starttime
    for d, name in [(df_r,"df_r"), (df_cpp,"df_cpp"), (df_lib,"df_lib")]:
        dup = d["starttime"].duplicated().sum()
        if dup:
            print(f"[warn] {name} has {dup} duplicated starttime; dropping dups")
            d.drop_duplicates("starttime", keep="last", inplace=True)

    tmp = df_r[["starttime","rsi_db"]].merge(df_cpp[["starttime","rsi_cpp"]],
                                            on="starttime", how="inner", validate="one_to_one")
    merged = tmp.merge(df_lib[["starttime","rsi_lib"]],
                    on="starttime", how="inner", validate="one_to_one") \
                .sort_values("starttime").reset_index(drop=True)

    print(f"After merging on starttime, the final docs have {len(merged)} entries.")

    # ==== Error Statistics ====
    merged["diff_cpp_db"]  = merged["rsi_cpp"] - merged["rsi_db"]
    merged["diff_lib_db"]  = merged["rsi_lib"] - merged["rsi_db"]
    merged["diff_cpp_lib"] = merged["rsi_cpp"] - merged["rsi_lib"]

    def stats(name, s):
        mae = s.abs().mean()
        rmse = math.sqrt((s ** 2).mean())
        mx = s.abs().max()
        print(f"{name}: n={len(s)}  MAE={mae:.8f}  RMSE={rmse:.8f}  Max|diff|={mx:.8f}")

    print("\n=== Pairwise diff stats ===")
    stats("cpp - db ", merged["diff_cpp_db"])
    stats("lib - db ", merged["diff_lib_db"])
    stats("cpp - lib", merged["diff_cpp_lib"])

    # Outlier printing (relative to DB)
    outliers = merged[merged["diff_cpp_db"].abs() > outlier_eps]
    print(f"\nOutliers vs DB (|cpp-db|>{outlier_eps}): {len(outliers)}")
    if len(outliers):
        print(outliers[["starttime", "rsi_db", "rsi_cpp", "rsi_lib", "diff_cpp_db"]].head(10))

    # ==== Plotting ====
    try:
        fig = plt.figure(figsize=(10, 7))
        ax1 = fig.add_subplot(2, 1, 1)
        ax1.plot(merged["starttime"], merged["rsi_db"],  label="RSI DB")
        ax1.plot(merged["starttime"], merged["rsi_cpp"], label="RSI CPP-style")
        ax1.plot(merged["starttime"], merged["rsi_lib"], label="RSI Finance-lib")
        ax1.set_title(f"RSI {symbol} {interval} (period={period})")
        ax1.legend(loc="best")

        ax2 = fig.add_subplot(2, 1, 2)
        ax2.plot(merged["starttime"], merged["diff_cpp_db"],  label="cpp - db")
        ax2.plot(merged["starttime"], merged["diff_cpp_lib"], label="cpp - lib")
        ax2.plot(merged["starttime"], merged["diff_lib_db"],  label="lib - db")
        ax2.set_xlabel("starttime (ms)")
        ax2.set_ylabel("difference")
        ax2.legend(loc="best")

        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Plotting failed (can be ignored): {e}")


if __name__ == "__main__":
    main()
