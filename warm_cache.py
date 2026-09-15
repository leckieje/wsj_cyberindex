"""
Pre-warm the GCS price cache for all NYSE trading days in a date range.

Only fills gaps — skips dates that already have cached data.

Usage:
    python warm_cache.py                        # Sep 2025 → yesterday
    python warm_cache.py 2025-09-01 2026-09-12  # custom range
"""

import sys
import json
from datetime import date, datetime

import lseg.data as ld
import pandas as pd
import pandas_market_calendars as mcal
import pytz

from data_pull import (
    IDS, _get_snapshot_cached, _cache_snapshot, _get_shares_cached,
    _cache_shares, _latest_shares_outstanding,
)
from price_cache import (
    get_prices, _get_bucket, _CACHE_BASE, _BLOB_PREFIX, _DAILY_BLOB_PREFIX,
    _INTRADAY_BOUNDARY, _is_settled, _ET,
)

DEFAULT_START = date(2025, 9, 1)


def _get_all_cached_dates(bucket):
    """List all dates that have at least one cached price blob (intraday + daily)."""
    cached = set()

    for prefix in (_BLOB_PREFIX, _DAILY_BLOB_PREFIX):
        for blob in bucket.list_blobs(prefix=prefix):
            parts = blob.name.split("/")
            if len(parts) >= 2:
                cached.add(parts[-2])

    return cached


def _get_cached_snapshot_dates(bucket):
    cached = set()
    prefix = f"{_CACHE_BASE}/snapshot/"
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name.split("/")[-1].replace(".json", "")
        cached.add(name)
    return cached


def _get_cached_shares_dates(bucket):
    cached = set()
    prefix = f"{_CACHE_BASE}/shares/"
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name.split("/")[-1].replace(".json", "")
        cached.add(name)
    return cached


def warm(start: date, end: date):
    nyse = mcal.get_calendar("NYSE")
    now_et = datetime.now(pytz.timezone("America/New_York"))

    all_trading = nyse.valid_days(start_date=str(start), end_date=str(end))
    all_days = [d.date() for d in all_trading if _is_settled(d.date(), now_et)]

    if not all_days:
        print("No settled trading days in range.")
        return

    print(f"Trading days in range: {len(all_days)}  ({all_days[0]} → {all_days[-1]})")

    bucket = _get_bucket()
    print("Scanning GCS for existing cache entries...")
    cached_prices = _get_all_cached_dates(bucket)
    cached_snapshots = _get_cached_snapshot_dates(bucket)
    cached_shares = _get_cached_shares_dates(bucket)

    missing_prices = [d for d in all_days if d.isoformat() not in cached_prices]
    missing_snapshots = [d for d in all_days if d.isoformat() not in cached_snapshots]
    missing_shares = [d for d in all_days if d.isoformat() not in cached_shares]

    print(f"  Prices:    {len(all_days) - len(missing_prices)} cached, {len(missing_prices)} missing")
    print(f"  Snapshots: {len(all_days) - len(missing_snapshots)} cached, {len(missing_snapshots)} missing")
    print(f"  Shares:    {len(all_days) - len(missing_shares)} cached, {len(missing_shares)} missing")

    total_missing = len(missing_prices) + len(missing_snapshots) + len(missing_shares)
    if total_missing == 0:
        print("\nCache is fully warm — nothing to do.")
        return

    print(f"\nOpening LSEG session...")
    try:
        ld.close_session()
    except Exception:
        pass
    ld.open_session("platform.ldp")

    # --- Snapshots ---
    for i, day in enumerate(missing_snapshots, 1):
        print(f"  Snapshot {i}/{len(missing_snapshots)}: {day}")
        try:
            top = ld.get_data(
                universe=IDS,
                fields=['TR.CommonName', 'TR.TickerSymbol', 'TR.CompanyMarketCap',
                        'TR.PriceClose', 'TR.PriceDate', 'TR.ExchangeName'],
                parameters={'SDate': str(day), 'EDate': str(day)},
            )
            _cache_snapshot(day, top)
        except Exception as e:
            print(f"    FAILED: {e}")

    # --- Shares ---
    for i, day in enumerate(missing_shares, 1):
        print(f"  Shares {i}/{len(missing_shares)}: {day}")
        try:
            shares = _latest_shares_outstanding(IDS, day, day)
            _cache_shares(day, shares)
        except Exception as e:
            print(f"    FAILED: {e}")

    # --- Prices (batch by contiguous runs to reduce LSEG calls) ---
    if missing_prices:
        print(f"\n  Fetching prices for {len(missing_prices)} days...")
        batches = _split_into_batches(missing_prices, batch_size=5)
        for i, batch in enumerate(batches, 1):
            print(f"  Price batch {i}/{len(batches)}: {batch[0]} → {batch[-1]} ({len(batch)} days)")
            try:
                get_prices(instruments=IDS, start=batch[0], end=batch[-1], today=batch[-1])
            except Exception as e:
                print(f"    FAILED: {e}")

    ld.close_session()
    print("\nDone.")


def _split_into_batches(days, batch_size=5):
    """Split a sorted list of dates into batches of at most batch_size."""
    return [days[i:i + batch_size] for i in range(0, len(days), batch_size)]


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s = date.fromisoformat(sys.argv[1])
        e = date.fromisoformat(sys.argv[2])
    else:
        s = DEFAULT_START
        e = date.today()
    warm(s, e)
