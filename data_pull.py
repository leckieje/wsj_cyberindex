import json
import re

import lseg.data as ld
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import pytz
from datetime import date, time, timedelta, datetime

from price_cache import get_prices, _get_bucket, _is_settled, _ET, _CACHE_BASE

IDS = [
    'ALLT.OQ', 'INTZ.OQ', 'MITK.OQ', 'OSPN.OQ', 'CISO.OQ', 'CSCO.OQ',
    'PANW.OQ', 'CRWD.OQ', 'FTNT.OQ', 'ZS.OQ',   'CHKP.OQ', 'OKTA.OQ',
    'FFIV.OQ', 'AKAM.OQ', 'SAIL.OQ', 'VRNS.OQ',  'QLYS.OQ',
    'TENB.OQ', 'NTCT.OQ', 'RDWR.OQ', 'RPD.OQ',   'TLS.OQ',  'HUBC.OQ',
    'CYCU.OQ', 'NET.N',   'LDOS.N',  'RBRK.N',   'S.N',     'LUMN.N',
    'ATEN.N',  'CVLT.OQ',
]

_MARKET_OPEN  = time(9, 30)
_MARKET_CLOSE = time(16, 0)


# ── Date helpers ──────────────────────────────────────────────────────────────

def _get_trading_days(n: int, end_date: date) -> list:
    nyse = mcal.get_calendar('NYSE')
    trading_days = nyse.valid_days(
        start_date=end_date - timedelta(days=n * 2),
        end_date=end_date,
    )
    if len(trading_days) >= n:
        return trading_days[-n:]
    extended = nyse.valid_days(
        start_date=end_date - timedelta(days=n * 3),
        end_date=end_date,
    )
    return extended[-n:]


# ── Math helpers ──────────────────────────────────────────────────────────────

def _pct_change(val1: float, val2: float) -> float:
    if pd.isna(val1) or pd.isna(val2) or val1 == 0:
        return float('nan')
    return (val2 - val1) / val1


def _get_changes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    val1s = list(df.iloc[0])
    for i in range(len(df.columns)):
        val2s = list(df.iloc[:, i])
        out[df.columns[i]] = [_pct_change(val1s[i], x) for x in val2s]
    return out


# ── Format helpers ────────────────────────────────────────────────────────────

def _format_currency(value) -> str:
    if pd.isna(value):
        return ''
    return '${:,.2f}'.format(value)


def _format_currency_round(value) -> str:
    if pd.isna(value):
        return ''
    return '${:,.0f}'.format(value)


def _format_percentage(val) -> str:
    if pd.isna(val):
        return ''
    return '{:.2f}%'.format(val * 100)


def _format_int(val) -> str:
    if pd.isna(val):
        return ''
    return '{:,}'.format(int(val))


def _format_to_excel_ap(dt: datetime) -> str:
    base = dt.strftime('%m/%d/%Y %I:%M:%S')
    ap = 'a' if dt.hour < 12 else 'p'
    return f"{base} {ap}"


def _short_exchange(ex_lst) -> dict:
    ex_map = {}
    for ex in ex_lst:
        if ex not in ex_map:
            if 'nasdaq' in str(ex).lower():
                ex_map[ex] = 'Nasdaq'
            elif 'new york stock exchange' in str(ex).lower():
                ex_map[ex] = 'NYSE'
            else:
                ex_map[ex] = ex
    return ex_map


# ── GCS cache helpers (snapshot + shares) ────────────────────────────────────

def _get_snapshot_cached(today: date) -> pd.DataFrame | None:
    try:
        bucket = _get_bucket()
        blob = bucket.blob(f"{_CACHE_BASE}/snapshot/{today.isoformat()}.json")
        if not blob.exists():
            return None
        data = json.loads(blob.download_as_text())
        return pd.DataFrame(data["records"])
    except Exception:
        return None


def _cache_snapshot(today: date, df: pd.DataFrame):
    try:
        bucket = _get_bucket()
        blob = bucket.blob(f"{_CACHE_BASE}/snapshot/{today.isoformat()}.json")
        records = df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None
                elif isinstance(v, (np.integer,)):
                    r[k] = int(v)
                elif isinstance(v, (np.floating,)):
                    r[k] = float(v)
        blob.upload_from_string(
            json.dumps({"date": today.isoformat(), "records": records}),
            content_type="application/json",
        )
    except Exception:
        pass


def _get_shares_cached(today: date) -> dict | None:
    try:
        bucket = _get_bucket()
        blob = bucket.blob(f"{_CACHE_BASE}/shares/{today.isoformat()}.json")
        if not blob.exists():
            return None
        data = json.loads(blob.download_as_text())
        return {k: int(v) for k, v in data["shares"].items()}
    except Exception:
        return None


def _cache_shares(today: date, shares: dict):
    try:
        bucket = _get_bucket()
        blob = bucket.blob(f"{_CACHE_BASE}/shares/{today.isoformat()}.json")
        blob.upload_from_string(
            json.dumps({"date": today.isoformat(), "shares": shares}),
            content_type="application/json",
        )
    except Exception:
        pass


# ── LSEG data helpers ─────────────────────────────────────────────────────────

def _latest_shares_outstanding(ids: list, start: date, end: date) -> dict:
    df = ld.get_history(
        universe=ids,
        fields=['TR.F.ComShrOutsTot'],
        interval='10min',
        start=str(start),
        end=str(end),
    )
    result = {}
    for col in df.columns:
        nums = [v for v in df[col] if np.issubdtype(type(v), np.number) and pd.notna(v)]
        if nums:
            result[col] = int(nums[-1])
        else:
            print(f"Warning: no shares data for {col}")
            result[col] = 0
    return result


def _intraday_mkt_cap(price_df: pd.DataFrame, shares: dict) -> dict:
    ids = [c for c in price_df.columns if str(c).lower() not in ('date', 'timestamp')]
    return {i: shares[i] * price_df[i] for i in ids}


def _get_avg_price(price_df: pd.DataFrame, shares: dict) -> pd.DataFrame:
    mkt_cap_dict = _intraday_mkt_cap(price_df, shares)
    mkt_cap = pd.DataFrame(mkt_cap_dict).apply(pd.to_numeric, errors='coerce')

    total = mkt_cap.sum(axis=1)
    weights = mkt_cap.div(total, axis=0)

    prices = price_df.select_dtypes(include=['number']).reset_index(drop=True)
    weight_avg = prices.mul(weights.reset_index(drop=True))
    weight_avg['CyberIndex'] = weight_avg.sum(axis=1)
    weight_avg.index = mkt_cap.index
    return weight_avg


# ── Public entry point ────────────────────────────────────────────────────────

def run_data_pull(n_days: int = 5, end_date=None, start_date=None) -> tuple:
    try:
        nyse = mcal.get_calendar('NYSE')
        if start_date and end_date:
            # Custom date range: use all NYSE trading days between the two dates
            s = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
            e = date.fromisoformat(end_date)   if isinstance(end_date,   str) else end_date
            trading = nyse.valid_days(start_date=str(s), end_date=str(e))
            if len(trading) == 0:
                raise RuntimeError("No trading days found in the selected range.")
            past  = trading[0].date()
            today = trading[-1].date()
        else:
            today = (date.fromisoformat(end_date) if isinstance(end_date, str) else end_date) or date.today()
            past  = _get_trading_days(n_days, today)[0].date()

        # ── 1. Snapshot: top-20 by market cap + company names ────────────────
        now_et = datetime.now(pytz.timezone("America/New_York"))
        today_is_settled = _is_settled(today, now_et)

        cached_snapshot = _get_snapshot_cached(today) if today_is_settled else None
        if cached_snapshot is not None:
            top = cached_snapshot
        else:
            top = ld.get_data(
                universe=IDS,
                fields=['TR.CommonName', 'TR.TickerSymbol', 'TR.CompanyMarketCap',
                        'TR.PriceClose', 'TR.PriceDate', 'TR.ExchangeName'],
                parameters={'SDate': str(today), 'EDate': str(today)},
            )
            if today_is_settled:
                _cache_snapshot(today, top)

        ex_map = _short_exchange(top['Exchange Name'])
        top['Exchange Name'] = top['Exchange Name'].map(ex_map)
        ordered = top.sort_values('Company Market Cap', ascending=False).reset_index(drop=True)
        top_20      = ordered.head(20)
        instruments = list(top_20['Instrument'])

        # Build company-name map from the snapshot — no extra round-trip needed
        _name_strip_re = re.compile(r' Software Technologies| Technologies| Systems| Holdings| Ltd| Inc', re.IGNORECASE)
        com_name_map = {
            inst: _name_strip_re.sub('', name).strip()
            for inst, name in zip(top['Instrument'], top['Company Common Name'])
            if pd.notna(name)
        }

        # ── 2+3. Prices (cached historical + live intraday) ────────────────
        concatted, includes_daily = get_prices(
            instruments=instruments, start=past, end=today, today=today,
        )

        # ── 4. Shares outstanding ─────────────────────────────────────────────
        cached_shares = _get_shares_cached(today) if today_is_settled else None
        if cached_shares is not None and all(inst in cached_shares for inst in instruments):
            shares = {inst: cached_shares[inst] for inst in instruments}
        else:
            shares = _latest_shares_outstanding(instruments, past, today)
            if today_is_settled:
                _cache_shares(today, shares)

        # ── 5. Localise to NY, filter to trading hours ────────────────────────
        if concatted.index.tz is None:
            concatted.index = concatted.index.tz_localize('UTC')
        concatted.index = concatted.index.tz_convert('America/New_York')

        # Filter to trading hours BEFORE ffill so pre/post market prices don't bleed
        time_mask = (
            (concatted.index.time >= _MARKET_OPEN) &
            (concatted.index.time <= _MARKET_CLOSE)
        )
        concatted = concatted[time_mask]
        if concatted.empty:
            raise RuntimeError(
                f"No trading-hours data found for {past} to {today}. "
                "The market may not have opened yet today."
            )
        concatted = concatted.ffill().bfill()

        # ── 6. CyberIndex (market-cap-weighted average price) ─────────────────
        weight_avg = _get_avg_price(concatted, shares)
        concatted['CyberIndex'] = weight_avg['CyberIndex']
        cyber_index_close = float(weight_avg['CyberIndex'].iloc[-1])

        # ── 7. % changes from period start ────────────────────────────────────
        changes = _get_changes(concatted)
        changes = changes[
            (changes.index.time >= _MARKET_OPEN) &
            (changes.index.time <= _MARKET_CLOSE)
        ].reset_index()

        # Normalise column names
        changes.columns = [
            'Date' if str(c).lower() in ('date', 'timestamp', "('trdprc_1', 'date')")
            else (c[1] if isinstance(c, tuple) else c)
            for c in changes.columns
        ]
        changes['Date'] = pd.to_datetime(changes['Date'])

        # Period change per instrument (keyed by RIC, before renaming columns)
        wkly_change = changes.set_index('Date').iloc[-1]

        time_changes = changes.rename(columns=com_name_map)

        # ── 8. Summary table ──────────────────────────────────────────────────
        # Re-apply time mask on concatted (may have grown after CyberIndex col was added)
        concatted = concatted[
            (concatted.index.time >= _MARKET_OPEN) &
            (concatted.index.time <= _MARKET_CLOSE)
        ]

        last_prices = concatted.iloc[-1:].T.reset_index()
        last_prices.columns = ['Instrument', 'Price Close']

        out_table = top_20[['Instrument', 'Company Common Name', 'Ticker Symbol',
                             'Exchange Name', 'Company Market Cap']].copy()
        out_table['Date'] = str(today)
        out_table = pd.merge(out_table, last_prices, on='Instrument', how='left')

        out_table['Period Change'] = out_table['Instrument'].map(wkly_change)

        col_map = {
            'Company Common Name': 'Company',
            'Ticker Symbol':       'Ticker',
            'Exchange Name':       'Exchange',
            'Date':                'Date',
            'Price Close':         'Price Close',
            'Period Change':       'Period Change',
            'Company Market Cap':  'Market Cap',
        }
        top_20_out = out_table[[k for k in col_map]].copy()
        top_20_out.rename(columns=col_map, inplace=True)
        # Keep Market Cap, Period Change, Price Close as numeric for Excel export.
        # Formatting for web display is applied in app.py.
        top_20_out['Company']       = (
            top_20_out['Company']
            .str.replace(r' ltd| inc| Software Technologies| Technologies| Systems| Holdings', '', case=False, regex=True)
            .str.strip()
        )
        top_20_out.index = range(1, len(top_20_out) + 1)

        return time_changes, top_20_out, past, today, cyber_index_close, includes_daily

    except Exception as exc:
        raise RuntimeError(f"Data pull failed: {exc}") from exc
