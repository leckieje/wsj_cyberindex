import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, time, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pytz
from google.cloud import storage

import lseg.data as ld
import pandas_market_calendars as mcal

_BUCKET_NAME = os.environ.get("PRICE_CACHE_BUCKET", "dj-newsroom-stag-shared")
_CACHE_BASE = os.environ.get("PRICE_CACHE_BASE", "jon_leckie/cyberindex-price-cache")
_BLOB_PREFIX = f"{_CACHE_BASE}/prices"
_DAILY_BLOB_PREFIX = f"{_CACHE_BASE}/daily"
_VERIFY_BLOB = f"{_CACHE_BASE}/verify_state.json"
_ET = pytz.timezone("America/New_York")
_MARKET_CLOSE = time(16, 0)
_TOLERANCE = 0.0001  # 0.01%
_INTRADAY_BOUNDARY = date(2025, 5, 5)

_gcs_client: Optional[storage.Client] = None
_bucket: Optional[storage.Bucket] = None


def _get_bucket() -> storage.Bucket:
    global _gcs_client, _bucket
    if _bucket is None:
        _gcs_client = storage.Client()
        _bucket = _gcs_client.bucket(_BUCKET_NAME)
    return _bucket


def _is_settled(day: date, now_et: datetime) -> bool:
    if day < now_et.date():
        return True
    if day == now_et.date() and now_et.time() >= _MARKET_CLOSE:
        return True
    return False


def _blob_path(trading_date: date, instrument: str) -> str:
    return f"{_BLOB_PREFIX}/{trading_date.isoformat()}/{instrument}.json"


def _get_cached_instruments_for_day(day: date) -> set:
    bucket = _get_bucket()
    prefix = f"{_BLOB_PREFIX}/{day.isoformat()}/"
    blobs = bucket.list_blobs(prefix=prefix)
    return {blob.name.split("/")[-1].replace(".json", "") for blob in blobs}


def _read_cached_day(trading_date: date, instrument: str, skip_exists=False) -> Optional[pd.DataFrame]:
    bucket = _get_bucket()
    blob = bucket.blob(_blob_path(trading_date, instrument))
    if not skip_exists and not blob.exists():
        return None
    try:
        data = json.loads(blob.download_as_text())
    except Exception:
        return None
    if not data.get("prices"):
        return None
    df = pd.DataFrame(data["prices"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").rename(columns={"TRDPRC_1": instrument})
    return df


def _write_cached_day(trading_date: date, instrument: str, df: pd.DataFrame):
    bucket = _get_bucket()
    records = []
    for ts, row in df.iterrows():
        val = row[instrument] if instrument in row.index else row.iloc[0]
        records.append({
            "timestamp": ts.isoformat(),
            "TRDPRC_1": float(val) if pd.notna(val) else None,
        })
    payload = {
        "instrument": instrument,
        "date": trading_date.isoformat(),
        "interval": "10min",
        "prices": records,
    }
    blob = bucket.blob(_blob_path(trading_date, instrument))
    blob.upload_from_string(json.dumps(payload), content_type="application/json")


def _daily_blob_path(trading_date: date, instrument: str) -> str:
    return f"{_DAILY_BLOB_PREFIX}/{trading_date.isoformat()}/{instrument}.json"


def _read_daily_cached(trading_date: date, instrument: str, skip_exists=False) -> Optional[pd.DataFrame]:
    bucket = _get_bucket()
    blob = bucket.blob(_daily_blob_path(trading_date, instrument))
    if not skip_exists and not blob.exists():
        return None
    try:
        data = json.loads(blob.download_as_text())
    except Exception:
        return None
    if not data.get("price"):
        return None
    price = data["price"]
    ts = pd.Timestamp(f"{trading_date} 16:00:00", tz="America/New_York").tz_convert("UTC")
    df = pd.DataFrame([{instrument: price}], index=pd.DatetimeIndex([ts], name="timestamp"))
    return df


def _write_daily_cached(trading_date: date, instrument: str, price: float):
    bucket = _get_bucket()
    payload = {
        "instrument": instrument,
        "date": trading_date.isoformat(),
        "interval": "daily",
        "price": price,
    }
    blob = bucket.blob(_daily_blob_path(trading_date, instrument))
    blob.upload_from_string(json.dumps(payload), content_type="application/json")


def _contiguous_ranges(sorted_days: list) -> list:
    if not sorted_days:
        return []
    ranges = []
    range_start = sorted_days[0]
    prev = sorted_days[0]
    for d in sorted_days[1:]:
        if (d - prev).days > 4:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges


def _normalize_lseg_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1] if c[1] else c[0] for c in df.columns]
    df.columns = [
        "timestamp" if str(c).lower() in ("timestamp", "date", "index") else c
        for c in df.columns
    ]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    # Drop any leftover TRDPRC_1 column (appears when LSEG returns partial results)
    if "TRDPRC_1" in df.columns:
        df = df.drop(columns=["TRDPRC_1"])
    return df


# ── Verification logic ───────────────────────────────────────────────────────

def _read_verify_state() -> dict:
    try:
        bucket = _get_bucket()
        blob = bucket.blob(_VERIFY_BLOB)
        if not blob.exists():
            return {"last_verified": {}, "pending_verify": []}
        return json.loads(blob.download_as_text())
    except Exception:
        return {"last_verified": {}, "pending_verify": []}


def _write_verify_state(state: dict):
    try:
        bucket = _get_bucket()
        blob = bucket.blob(_VERIFY_BLOB)
        blob.upload_from_string(json.dumps(state), content_type="application/json")
    except Exception:
        pass


def _pick_next_verification(instruments: list, settled_days: list, state: dict) -> list:
    """Pick 1-2 days to verify on the next request based on tiered schedule."""
    now = datetime.now(_ET)
    today = now.date()
    last_verified = state.get("last_verified", {})
    picks = []

    nyse = mcal.get_calendar("NYSE")
    recent_cutoff = today - timedelta(days=7)
    medium_cutoff = today - timedelta(days=45)

    for day in sorted(settled_days, reverse=True):
        day_str = day.isoformat()
        last_check = last_verified.get(day_str)
        if last_check:
            last_dt = datetime.fromisoformat(last_check)
        else:
            last_dt = None

        if day >= recent_cutoff:
            # Recent: verify if not checked since last request (always eligible)
            if last_dt is None or (now - last_dt).total_seconds() > 300:
                picks.append({"date": day_str, "instruments": instruments})
                if len(picks) >= 1:
                    break
        elif day >= medium_cutoff:
            # Medium: verify weekly
            if last_dt is None or (now - last_dt).days >= 7:
                picks.append({"date": day_str, "instruments": instruments})
                if len(picks) >= 2:
                    break
        else:
            # Old: verify monthly
            if last_dt is None or (now - last_dt).days >= 30:
                picks.append({"date": day_str, "instruments": instruments})
                if len(picks) >= 2:
                    break

    return picks


def _verify_day(day_str: str, instruments: list) -> int:
    """Verify one cached day against LSEG. Returns number of corrections made."""
    day = date.fromisoformat(day_str)
    corrections = 0

    try:
        fetched = ld.get_history(
            universe=instruments,
            fields=["TRDPRC_1"],
            interval="10min",
            start=str(day),
            end=f"{day} 23:59:59",
        )
        fetched = _normalize_lseg_df(fetched)
        if fetched.index.tz is None:
            fetched.index = fetched.index.tz_localize("UTC")
        fetched.index = fetched.index.tz_convert("America/New_York")
    except Exception:
        return 0

    for inst in instruments:
        if inst not in fetched.columns:
            continue

        cached = _read_cached_day(day, inst)
        if cached is None:
            # Not cached yet — write it
            inst_df = fetched[[inst]]
            day_mask = inst_df.index.date == day
            day_df = inst_df[day_mask]
            if not day_df.empty:
                try:
                    _write_cached_day(day, inst, day_df)
                    corrections += 1
                except Exception:
                    pass
            continue

        # Compare cached vs fresh
        fresh_series = fetched[[inst]]
        day_mask = fresh_series.index.date == day
        fresh_day = fresh_series[day_mask]
        if fresh_day.empty:
            continue

        # Align to UTC for comparison
        cached_utc = cached.copy()
        if cached_utc.index.tz is None:
            cached_utc.index = cached_utc.index.tz_localize("UTC")
        fresh_utc = fresh_day.copy()
        fresh_utc.index = fresh_utc.index.tz_convert("UTC")

        # Check if values differ beyond tolerance
        merged = cached_utc.join(fresh_utc, lsuffix="_cached", rsuffix="_fresh", how="inner")
        if merged.empty:
            continue

        col_cached = f"{inst}_cached"
        col_fresh = f"{inst}_fresh"
        if col_cached not in merged.columns or col_fresh not in merged.columns:
            continue

        cached_vals = merged[col_cached].values.astype(float)
        fresh_vals = merged[col_fresh].values.astype(float)

        # Relative difference
        with np.errstate(divide="ignore", invalid="ignore"):
            rel_diff = np.abs((fresh_vals - cached_vals) / cached_vals)
        max_diff = np.nanmax(rel_diff) if len(rel_diff) > 0 else 0

        if max_diff > _TOLERANCE:
            print(f"Cache correction: {inst} on {day_str} (max diff: {max_diff:.4%})")
            try:
                _write_cached_day(day, inst, fresh_day)
                corrections += 1
            except Exception:
                pass

    return corrections


def _run_pending_verification():
    """Run any pending verifications queued by the previous request."""
    state = _read_verify_state()
    pending = state.get("pending_verify", [])
    if not pending:
        return

    for entry in pending:
        day_str = entry["date"]
        instruments = entry["instruments"]
        _verify_day(day_str, instruments)
        state["last_verified"][day_str] = datetime.now(_ET).isoformat()

    state["pending_verify"] = []
    _write_verify_state(state)


def _queue_next_verification(instruments: list, settled_days: list):
    """Pick next day(s) to verify and save to pending state for next request."""
    state = _read_verify_state()
    picks = _pick_next_verification(instruments, settled_days, state)
    if picks:
        state["pending_verify"] = picks
        _write_verify_state(state)


# ── Public entry point ───────────────────────────────────────────────────────

def run_verification():
    """Public: run pending verifications. Call separately from the main data path."""
    try:
        _run_pending_verification()
    except Exception:
        pass


def get_prices(instruments: list, start: date, end: date, today: date) -> tuple:
    """Returns (prices_df, includes_daily)."""
    now_et = datetime.now(_ET)

    nyse = mcal.get_calendar("NYSE")
    all_trading_days = nyse.valid_days(start_date=str(start), end_date=str(end))
    all_days = [d.date() for d in all_trading_days]

    settled_days = [d for d in all_days if _is_settled(d, now_et)]
    live_days = [d for d in all_days if not _is_settled(d, now_et)]

    # Split settled into daily (pre-boundary) and intraday (on/after boundary)
    daily_days = [d for d in settled_days if d < _INTRADAY_BOUNDARY]
    intraday_days = [d for d in settled_days if d >= _INTRADAY_BOUNDARY]
    includes_daily = len(daily_days) > 0

    all_frames = []

    # --- Daily days (before intraday boundary): check cache, fetch missing ---
    if daily_days:
        daily_missing = {}
        try:
            months_needed = sorted(set(f"{d.year}-{d.month:02d}" for d in daily_days))
            bucket = _get_bucket()
            daily_cached_set = set()

            def _list_daily_month(ym):
                prefix = f"{_DAILY_BLOB_PREFIX}/{ym}"
                return list(bucket.list_blobs(prefix=prefix))

            with ThreadPoolExecutor(max_workers=10) as pool:
                month_results = pool.map(_list_daily_month, months_needed)

            for blobs in month_results:
                for b in blobs:
                    parts = b.name.split("/")
                    daily_cached_set.add((parts[-2], parts[-1].replace(".json", "")))

            to_read = []
            for day in daily_days:
                day_str = day.isoformat()
                for inst in instruments:
                    if (day_str, inst) in daily_cached_set:
                        to_read.append((day, inst))
                    else:
                        daily_missing.setdefault(inst, []).append(day)

            if to_read:
                with ThreadPoolExecutor(max_workers=50) as pool:
                    results = list(pool.map(
                        lambda pair: _read_daily_cached(pair[0], pair[1], skip_exists=True), to_read))
                for i, df in enumerate(results):
                    if df is not None:
                        all_frames.append(df)
                    else:
                        day, inst = to_read[i]
                        daily_missing.setdefault(inst, []).append(day)
        except Exception:
            daily_missing = {inst: list(daily_days) for inst in instruments}

        if daily_missing:
            all_missing_days = sorted(set(d for days in daily_missing.values() for d in days))
            ranges = _contiguous_ranges(all_missing_days)

            for range_start, range_end in ranges:
                insts_needed = [
                    inst for inst, days in daily_missing.items()
                    if any(range_start <= d <= range_end for d in days)
                ]

                fetched = ld.get_history(
                    universe=insts_needed,
                    fields=["TRDPRC_1"],
                    interval="daily",
                    start=str(range_start),
                    end=str(range_end),
                )
                fetched = _normalize_lseg_df(fetched)

                # Daily data comes without time — assign 16:00 ET (market close)
                if fetched.index.tz is None:
                    new_idx = []
                    for ts in fetched.index:
                        d = ts.date() if hasattr(ts, 'date') else pd.Timestamp(ts).date()
                        new_idx.append(pd.Timestamp(f"{d} 16:00:00", tz="America/New_York"))
                    fetched.index = pd.DatetimeIndex(new_idx, name="timestamp")

                fetched_utc = fetched.copy()
                fetched_utc.index = fetched_utc.index.tz_convert("UTC")

                # Cache each day/instrument
                for inst in insts_needed:
                    if inst not in fetched_utc.columns:
                        continue
                    for day in sorted(d for d in daily_missing[inst] if range_start <= d <= range_end):
                        close_ts = pd.Timestamp(f"{day} 16:00:00", tz="America/New_York").tz_convert("UTC")
                        if close_ts in fetched_utc.index:
                            val = fetched_utc.at[close_ts, inst]
                            if pd.notna(val):
                                try:
                                    _write_daily_cached(day, inst, float(val))
                                except Exception:
                                    pass

                all_frames.append(fetched_utc)

    # --- Intraday settled days: check cache, fetch missing ---
    missing = {}
    try:
        if intraday_days:
            months_needed = sorted(set(f"{d.year}-{d.month:02d}" for d in intraday_days))
            bucket = _get_bucket()
            cached_set = set()

            def _list_month(ym):
                prefix = f"{_BLOB_PREFIX}/{ym}"
                return list(bucket.list_blobs(prefix=prefix))

            with ThreadPoolExecutor(max_workers=10) as pool:
                month_results = pool.map(_list_month, months_needed)

            for blobs in month_results:
                for b in blobs:
                    parts = b.name.split("/")
                    cached_set.add((parts[-2], parts[-1].replace(".json", "")))

            to_read = []
            for day in intraday_days:
                day_str = day.isoformat()
                for inst in instruments:
                    if (day_str, inst) in cached_set:
                        to_read.append((day, inst))
                    else:
                        missing.setdefault(inst, []).append(day)

            if to_read:
                with ThreadPoolExecutor(max_workers=50) as pool:
                    results = list(pool.map(lambda pair: _read_cached_day(pair[0], pair[1], skip_exists=True), to_read))

                for i, df in enumerate(results):
                    if df is not None:
                        all_frames.append(df)
                    else:
                        day, inst = to_read[i]
                        missing.setdefault(inst, []).append(day)
    except Exception:
        missing = {inst: list(intraday_days) for inst in instruments}

    if missing:
        all_missing_days = sorted(set(d for days in missing.values() for d in days))
        ranges = _contiguous_ranges(all_missing_days)

        for range_start, range_end in ranges:
            insts_needed = [
                inst for inst, days in missing.items()
                if any(range_start <= d <= range_end for d in days)
            ]

            fetched = ld.get_history(
                universe=insts_needed,
                fields=["TRDPRC_1"],
                interval="10min",
                start=str(range_start),
                end=f"{range_end} 23:59:59",
            )
            fetched = _normalize_lseg_df(fetched)

            if fetched.index.tz is None:
                fetched.index = fetched.index.tz_localize("UTC")

            fetched_et = fetched.copy()
            fetched_et.index = fetched_et.index.tz_convert("America/New_York")

            for inst in insts_needed:
                if inst not in fetched_et.columns:
                    continue
                inst_df = fetched_et[[inst]]
                for day in sorted(d for d in missing[inst] if range_start <= d <= range_end):
                    day_mask = inst_df.index.date == day
                    day_df = inst_df[day_mask]
                    if not day_df.empty:
                        try:
                            _write_cached_day(day, inst, day_df)
                        except Exception:
                            pass

            all_frames.append(fetched)

    # --- Live days: always fetch from LSEG ---
    for day in live_days:
        kwargs = dict(
            universe=instruments,
            fields=["TRDPRC_1"],
            interval="10min",
            start=str(day),
        )
        if day != date.today():
            kwargs["end"] = f"{day} 23:59:59"

        live_df = ld.get_history(**kwargs)
        live_df = _normalize_lseg_df(live_df)
        if live_df.index.tz is None:
            live_df.index = live_df.index.tz_localize("UTC")
        else:
            live_df.index = live_df.index.tz_convert("UTC")
        all_frames.append(live_df)

    # --- Combine ---
    if not all_frames:
        return pd.DataFrame(), includes_daily

    normalized = []
    for f in all_frames:
        if f.index.tz is None:
            f.index = f.index.tz_localize("UTC")
        elif str(f.index.tz) != "UTC":
            f.index = f.index.tz_convert("UTC")
        normalized.append(f)

    result = pd.concat(normalized, axis=1)
    result = result.T.groupby(level=0).first().T.sort_index()

    # Queue next verification for the following request
    try:
        _queue_next_verification(instruments, intraday_days)
    except Exception:
        pass

    return result, includes_daily
