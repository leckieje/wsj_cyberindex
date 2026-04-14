import lseg.data as ld
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import pytz
from datetime import date, time, timedelta, datetime

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

def run_data_pull(n_days: int = 5, end_date: date = None) -> tuple:
    """
    Execute the full CyberIndex pipeline.

    Args:
        n_days:   Number of NYSE trading days to look back (default 5).
        end_date: End date for the range (default today).

    Returns:
        (time_changes, top_20_out, past, today) — DataFrames plus the
        resolved start and end dates.

    Raises:
        RuntimeError on any failure, with a human-readable message.
    """
    try:
        today = end_date or date.today()
        past  = _get_trading_days(n_days, today)[0].date()

        # ── 1. Get today's snapshot for the full universe ─────────────────────
        top = ld.get_data(
            universe=IDS,
            fields=['TR.CommonName', 'TR.TickerSymbol', 'TR.CompanyMarketCap',
                    'TR.PriceClose', 'TR.PriceDate'],
            parameters={'SDate': str(today), 'EDate': str(today)},
        )
        ordered = top.sort_values('Company Market Cap', ascending=False).reset_index(drop=True)
        top_20  = ordered.head(20)
        instruments = list(top_20['Instrument'])

        # ── 2. Historical prices (5 trading days up to yesterday) ─────────────
        hist = ld.get_history(
            universe=instruments,
            fields=['TRDPRC_1'],
            interval='10min',
            start=str(past),
            end=f"{today} 00:00:00",
        )
        hist = hist.reset_index()
        hist.rename(columns={'Timestamp': 'date'}, inplace=True)

        # ── 3. Intraday prices (today) ────────────────────────────────────────
        intra = ld.get_history(
            universe=instruments,
            fields=['TRDPRC_1'],
            interval='10min',
            start=str(today),
        )
        intra = intra.reset_index()
        intra.rename(columns={'Timestamp': 'date'}, inplace=True)

        # ── 4. Combine, localise to NY, filter to trading hours FIRST ─────────
        concatted = pd.concat([hist, intra], ignore_index=False)
        concatted['date'] = pd.to_datetime(concatted['date'])
        concatted = concatted.set_index('date').sort_index()

        if concatted.index.tz is None:
            concatted.index = concatted.index.tz_localize('UTC')
        concatted.index = concatted.index.tz_convert('America/New_York')

        # Filter to trading hours BEFORE ffill so pre/post market prices
        # don't bleed into the calculation
        time_mask = (
            (concatted.index.time >= _MARKET_OPEN) &
            (concatted.index.time <= _MARKET_CLOSE)
        )
        concatted = concatted[time_mask]

        # Now ffill/bfill within trading hours only
        concatted = concatted.ffill().bfill()

        # ── 5. Shares outstanding (fetched once, reused below) ────────────────
        shares = _latest_shares_outstanding(instruments, past, today)

        # ── 6. CyberIndex (market-cap-weighted average price) ─────────────────
        weight_avg = _get_avg_price(concatted, shares)
        concatted['CyberIndex'] = weight_avg['CyberIndex']

        # ── 7. % changes from week start ──────────────────────────────────────
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
        changes['Date'] = pd.to_datetime(changes['Date']).apply(_format_to_excel_ap)

        # Weekly change per instrument (keyed by RIC, before renaming columns)
        wkly_change = changes.set_index('Date').iloc[-1]  # index is RIC codes here

        # Map LSEG instrument codes → common company names
        com_name = ld.get_data(universe=IDS, fields=['TR.CommonName'])
        com_name_map = {
            inst: name.replace(' Inc', '').replace(' Ltd', '')
            for inst, name in zip(com_name['Instrument'], com_name['Company Common Name'])
        }
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
                             'Company Market Cap']].copy()
        out_table['Date'] = str(today)
        out_table = pd.merge(out_table, last_prices, on='Instrument', how='left')

        out_table['Period Change'] = out_table['Instrument'].map(wkly_change)

        col_map = {
            'Company Common Name': 'Company',
            'Ticker Symbol':       'Ticker',
            'Date':                'Date',
            'Price Close':         'Price Close',
            'Period Change':       'Period Change',
            'Company Market Cap':  'Market Cap',
        }
        top_20_out = out_table[[k for k in col_map]].copy()
        top_20_out.rename(columns=col_map, inplace=True)
        top_20_out['Market Cap']    = top_20_out['Market Cap'].apply(_format_currency_round)
        top_20_out['Period Change'] = top_20_out['Period Change'].apply(_format_percentage)
        top_20_out['Price Close']   = top_20_out['Price Close'].apply(_format_currency)
        top_20_out['Company']       = (
            top_20_out['Company']
            .str.replace(r' ltd| inc', '', case=False, regex=True)
            .str.strip()
        )
        top_20_out.index = range(1, len(top_20_out) + 1)

        return time_changes, top_20_out, past, today

    except Exception as exc:
        raise RuntimeError(f"Data pull failed: {exc}") from exc
