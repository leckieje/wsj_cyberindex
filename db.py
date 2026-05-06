import json
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "cyberindex.db"


def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS volatility_events (
            ticker_date  TEXT PRIMARY KEY,
            summary      TEXT,
            raw_results  TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS article_cache (
            cache_key   TEXT PRIMARY KEY,
            articles    TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.commit()
    con.close()


def get_cached_summary(ticker_date: str) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT summary, raw_results FROM volatility_events WHERE ticker_date = ?",
        (ticker_date,),
    ).fetchone()
    con.close()
    if row is None:
        return None
    return {"summary": row[0], "raw_results": json.loads(row[1]) if row[1] else []}


def save_summary(ticker_date: str, summary: str, raw_results: list):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """
        INSERT INTO volatility_events (ticker_date, summary, raw_results, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker_date) DO UPDATE SET
            summary    = excluded.summary,
            raw_results = excluded.raw_results,
            created_at  = excluded.created_at
        """,
        (ticker_date, summary, json.dumps(raw_results), datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()


# ── Sentiment cache ──────────────────────────────────────────────────────────

def _sentiment_cache_key(company: str, start_date: str, end_date: str) -> str:
    return f"{company}_{start_date}_{end_date}"


def get_cached_sentiment(company: str, start_date: str, end_date: str) -> dict | None:
    key = _sentiment_cache_key(company, start_date, end_date)
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT articles FROM article_cache WHERE cache_key = ?", (key,)
    ).fetchone()
    con.close()
    if row is None:
        return None
    return json.loads(row[0])


def save_sentiment(company: str, start_date: str, end_date: str, data: dict):
    key = _sentiment_cache_key(company, start_date, end_date)
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """
        INSERT INTO article_cache (cache_key, articles, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            articles   = excluded.articles,
            created_at = excluded.created_at
        """,
        (key, json.dumps(data), datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()
