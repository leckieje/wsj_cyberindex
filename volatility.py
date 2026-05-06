import pandas as pd

VOLATILITY_THRESHOLD = 0.03


def detect_volatility_events(
    time_changes: pd.DataFrame,
    ticker_to_company: dict,
    threshold: float = VOLATILITY_THRESHOLD,
) -> list[dict]:
    """
    For each (stock, trading_day) pair, compute first-to-last % change using
    9:30 AM open and 4:00 PM close from the cumulative-from-period-start series.

    Day change = (last_val - first_val) / (1 + first_val)

    Returns events where abs(change) >= threshold, sorted by date asc then
    abs(pct_change) desc.
    """
    stock_cols = [c for c in time_changes.columns if c not in ("Date", "CyberIndex")]
    company_to_ticker = {v: k for k, v in ticker_to_company.items()}

    df = time_changes.copy()
    df["_date"] = df["Date"].dt.date

    events = []
    for day, day_df in df.groupby("_date"):
        day_df = day_df.sort_values("Date")
        if len(day_df) < 2:
            continue
        first_row = day_df.iloc[0]
        last_row  = day_df.iloc[-1]

        for col in stock_cols:
            first_val = first_row[col]
            last_val  = last_row[col]

            if pd.isna(first_val) or pd.isna(last_val):
                continue

            day_change = (last_val - first_val) / (1 + first_val)

            if abs(day_change) >= threshold:
                ticker = company_to_ticker.get(col, col)
                events.append({
                    "ticker":     ticker,
                    "company":    col,
                    "date":       str(day),
                    "pct_change": round(day_change, 6),
                    "direction":  "up" if day_change > 0 else "down",
                    "summary":    None,
                })

    events.sort(key=lambda e: (e["date"], -abs(e["pct_change"])))
    return events
