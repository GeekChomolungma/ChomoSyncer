from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any, Tuple

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import pandas as pd

# ------------------------------
# Config & Client
# ------------------------------

@dataclass
class MongoConfig:
    uri: str
    db_market_name: str = "market_info"
    db_indicators_name: str = "indicators"
    server_selection_timeout_ms: int = 5000

    @staticmethod
    def from_env() -> "MongoConfig":
        load_dotenv()
        uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        print("Connection destination set, URI:", uri)
        return MongoConfig(uri=uri)


class MongoWrapper:
    def __init__(self, cfg: MongoConfig):
        self.cfg = cfg
        self.client = MongoClient(
            cfg.uri,
            serverSelectionTimeoutMS=cfg.server_selection_timeout_ms
        )
        
        # handshake to ensure connection is valid
        try:
            _ = self.client.admin.command("ping")
        except ServerSelectionTimeoutError as e:
            raise RuntimeError(
                f"Cannot connect to MongoDB: {e}\nURI: {cfg.uri}"
            ) from e

        self.db_market = self.client[cfg.db_market_name]
        self.db_indicators = self.client[cfg.db_indicators_name]

    # --------------------------
    # Collection name helpers
    # --------------------------
    @staticmethod
    def kline_col(symbol: str, interval: str) -> str:
        # e.g. BTCUSDT_1h_Binance
        return f"{symbol.upper()}_{interval}_Binance"

    @staticmethod
    def rsi_col(symbol: str, interval: str) -> str:
        # e.g. rsi_BTCUSDT_1h_Binance
        print(f"Creating RSI collection name for symbol: {symbol}, interval: {interval}")
        return f"rsi_{symbol.upper()}_{interval}_Binance"

    # --------------------------
    # Query helpers
    # --------------------------
    def _col(self, db: str, colName: str) -> Collection:
        if db == "market":
            return self.db_market[colName]
        elif db == "indicators":
            return self.db_indicators[colName]
        else:
            print(f"Invalid database name: {db}")
            raise ValueError("db must be 'market' or 'indicators'")

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        retrieve Kline documents from MongoDB.
        """
        col_name = self.kline_col(symbol, interval)
        col = self._col("market", col_name)

        q: Dict[str, Any] = {}
        if start_ms is not None or end_ms is not None:
            time_range: Dict[str, Any] = {}
            if start_ms is not None:
                time_range["$gte"] = start_ms
            if end_ms is not None:
                time_range["$lte"] = end_ms
            q["starttime"] = time_range

        cursor = col.find(q, projection={"_id": False}).sort("starttime", 1) # 1 means ascending order
        if limit:
            cursor = cursor.limit(limit)
        docs = list(cursor)
        return docs

    def fetch_rsi_docs(
        self,
        symbol: str,
        interval: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Retrieve RSI documents for a given symbol and interval.
        If no documents found, will try with typo in collection name.
        """
        col_name = self.rsi_col(symbol, interval)
        col = self._col("indicators", col_name)
        q: Dict[str, Any] = {}
        if start_ms is not None or end_ms is not None:
            tr: Dict[str, Any] = {}
            if start_ms is not None:
                tr["$gte"] = start_ms
            if end_ms is not None:
                tr["$lte"] = end_ms
            q["starttime"] = tr
        cursor = col.find(q, projection={"_id": False}).sort("starttime", 1)
        
        if limit:
            cursor = cursor.limit(limit)
        docs = list(cursor)
        return docs

    # --------------------------
    # DataFrame helpers
    # --------------------------
    @staticmethod
    def klines_to_df(klines: Iterable[Dict[str, Any]]) -> pd.DataFrame:
        """
        convert Kline documents to a DataFrame.
        """
        df = pd.DataFrame(klines)
        if df.empty:
            return df
        # convert string columns to float
        for col in ["open", "high", "low", "close", "volume", "quotevolume",
                    "activebuyvolume", "activebuyquotevolume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # handle timestamps in milliseconds
        if "starttime" in df.columns:
            df = df.sort_values("starttime").reset_index(drop=True)
        return df

    @staticmethod
    def rsi_docs_to_df(docs: Iterable[Dict[str, Any]]) -> pd.DataFrame:
        """
        convert RSI documents to a DataFrame.
        """
        df = pd.DataFrame(docs)
        if df.empty:
            return df
        # try to identify RSI value fields
        possible_keys = [k for k in df.columns if k.lower() == "rsi"] or \
                        [k for k in df.columns if k.lower() == "value"]
        if possible_keys:
            value_key = possible_keys[0]
        else:
            # if no specific RSI value key found, assume a generic numeric column
            meta = {"starttime", "endtime", "name", "period", "_id"}
            numeric_candidates = [c for c in df.columns if c not in meta]
            value_key = numeric_candidates[0] if numeric_candidates else None

        if value_key:
            df = df.rename(columns={value_key: "rsi_value"})
            df["rsi_value"] = pd.to_numeric(df["rsi_value"], errors="coerce")
        return df.sort_values("starttime").reset_index(drop=True)


# ------------------------------
# Example usage (for quick test)
# ------------------------------

if __name__ == "__main__":
    cfg = MongoConfig.from_env()
    mw = MongoWrapper(cfg)

    symbol = "BTCUSDT"
    interval = "1h"

    # the oldest 1000 klines
    klines = mw.fetch_klines(symbol, interval, start_ms=None, end_ms=None, limit=1000)
    print(f"Fetched klines: {len(klines)} from {mw.kline_col(symbol, interval)}")

    rsi_docs = mw.fetch_rsi_docs(symbol, interval, start_ms=None, end_ms=None, limit=1000)
    print(f"Fetched RSI docs: {len(rsi_docs)} from {mw.rsi_col(symbol, interval)}")

    df_k = mw.klines_to_df(klines)
    df_r = mw.rsi_docs_to_df(rsi_docs)

    print(df_k.head(3))
    print(df_r.head(3))
