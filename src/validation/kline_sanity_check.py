# kline_sanity_check.py
# pip install pymongo python-dateutil

from pymongo import MongoClient
from datetime import datetime, timezone
from dateutil import tz
import os
from dotenv import load_dotenv

# ========= config =========
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
print("MongoDB URI:", MONGO_URI)
DB_NAME   = "market_info"
COLL_NAME = "BTCUSDT_15m_Binance"     # Example collection name
INTERVAL_MS = 15 * 60 * 1000          # Interval in milliseconds (5m)
# Field names (change according to your schema)
F_START = "starttime"
F_OPEN  = "open"
F_HIGH  = "high"
F_LOW   = "low"
F_CLOSE = "close"
F_VOL   = "volume"
F_QVOL  = "quotevolume"
F_END   = "endtime"                  # Optional, only if you store it
# ==================================

def to_float(x):
    """Convert value to float, return None if not possible."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x))
    except Exception:
        return None

def ms_to_local(ms, tzname="Europe/Amsterdam"):
    """Convert milliseconds timestamp to human-readable local time."""
    return datetime.fromtimestamp(ms/1000, tz=tz.gettz(tzname)).strftime("%Y-%m-%d %H:%M:%S")

def main():
    cli = MongoClient(MONGO_URI)
    col = cli[DB_NAME][COLL_NAME]

    total = col.count_documents({})  # Count all documents
    print(f"[Collection] {DB_NAME}.{COLL_NAME}, approx {total} docs")

    # ---- 1) Uniqueness: check duplicate starttime ----
    dup = list(col.aggregate([
        {"$group": {"_id": f"${F_START}", "cnt": {"$sum": 1}}},
        {"$match": {"cnt": {"$gt": 1}}},
        {"$sort": {"_id": 1}}
    ], allowDiskUse=True))
    if dup:
        print(f"[Duplicate] Found {len(dup)} duplicate starttime values. Sample:")
        for d in dup[:5]:
            print("  -", d["_id"], ms_to_local(d["_id"]), "x", d["cnt"])
    else:
        print("[Duplicate] OK: No duplicate starttime found")

    # ---- 2) Continuity: check constant interval between starttimes ----
    cursor = col.find({}, {F_START: 1}).sort(F_START, 1)
    times = [doc[F_START] for doc in cursor if isinstance(doc.get(F_START), int)]
    gaps_bad = []
    for i in range(1, len(times)):
        gap = times[i] - times[i-1]
        if gap != INTERVAL_MS:
            gaps_bad.append((times[i-1], times[i], gap))
    if gaps_bad:
        print(f"[Continuity] FAIL: Found {len(gaps_bad)} interval issues. Sample:")
        for a, b, g in gaps_bad[:5]:
            print(f"  - {a}({ms_to_local(a)}) -> {b}({ms_to_local(b)}), gap={g} ms")
    else:
        print("[Continuity] OK: starttime interval is constant")

    # ---- 3) Range coverage: check if there are missing bars ----
    if times:
        print(f"[Range] First kline: {times[0]} ({ms_to_local(times[0])})")
        print(f"[Range] Last  kline: {times[-1]} ({ms_to_local(times[-1])})")
        expect_cnt = (times[-1] - times[0]) // INTERVAL_MS + 1
        miss = expect_cnt - len(times)
        if miss == 0:
            print("[Range] OK: No missing bars in range")
        else:
            print(f"[Range] FAIL: Expected {expect_cnt} bars, found {len(times)}, missing {miss}")

    # ---- 4) OHLC logic and non-negative volumes ----
    proj = {F_START: 1, F_OPEN: 1, F_HIGH: 1, F_LOW: 1, F_CLOSE: 1, F_VOL: 1, F_QVOL: 1}
    bad_ohlc, bad_vol, bad_type = [], [], []
    batch_size = 10000
    skip = 0
    while True:
        batch = list(col.find({}, proj).sort(F_START, 1).skip(skip).limit(batch_size))
        if not batch:
            break
        for d in batch:
            st = d.get(F_START)
            o = to_float(d.get(F_OPEN))
            h = to_float(d.get(F_HIGH))
            l = to_float(d.get(F_LOW))
            c = to_float(d.get(F_CLOSE))
            v = to_float(d.get(F_VOL)) if F_VOL in d else 0.0
            qv = to_float(d.get(F_QVOL)) if F_QVOL in d else 0.0

            # Type/Null check
            if None in (o, h, l, c):
                bad_type.append(st)
                continue

            # Logical OHLC check: high >= max(open, close, low) and low <= min(open, close, high)
            if not (h >= max(o, c, l) and l <= min(o, c, h) and h >= l):
                bad_ohlc.append(st)

            # Non-negative volume and quote volume
            if (v is not None and v < 0) or (qv is not None and qv < 0):
                bad_vol.append(st)

        skip += batch_size

    if bad_type:
        print(f"[Fields] FAIL: {len(bad_type)} bars have missing/non-numeric OHLC. Sample: {bad_type[:5]}")
    else:
        print("[Fields] OK: OHLC are numeric and present")

    if bad_ohlc:
        print(f"[OHLC] FAIL: {len(bad_ohlc)} bars have inconsistent high/low values. Sample: {bad_ohlc[:5]}")
    else:
        print("[OHLC] OK: High/low values consistent")

    if bad_vol:
        print(f"[Volume] FAIL: {len(bad_vol)} bars have negative volume/quote volume. Sample: {bad_vol[:5]}")
    else:
        print("[Volume] OK: Non-negative volume/quote volume")

    # ---- 5) Endtime check (if applicable) ----
    if F_END in proj:
        bad_end = list(col.find({
            "$expr": {"$ne": [ {"$subtract": [f"${F_END}", f"${F_START}"]}, INTERVAL_MS-1 ]}
        }, {F_START:1, F_END:1}).limit(5))
        if bad_end:
            print("[Endtime] FAIL: endtime - starttime != interval-1. Sample:")
            for x in bad_end:
                print("  -", x)
        else:
            print("[Endtime] OK: Matches interval")

    print("\n[Done] Basic data consistency check completed.")

if __name__ == "__main__":
    main()
