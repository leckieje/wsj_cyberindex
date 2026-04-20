import io
import threading
import traceback
from datetime import date

import openpyxl
import pandas as pd

import lseg.data as ld
from flask import Flask, Response, jsonify, render_template, request

import data_pull

app = Flask(__name__)

# ── LSEG session ──────────────────────────────────────────────────────────────
# Opened once at import time so gunicorn workers have a live session immediately.
# lseg-data reads lseg-data.config.json from the working directory automatically.
# Run gunicorn from the project directory:
#   gunicorn -w 1 -b 0.0.0.0:5000 --timeout 120 app:app
# -w 1 is REQUIRED — the LSEG session is process-level state and must not be
# forked across multiple workers.

def _open_lseg_session():
    ld.open_session("platform.ldp")

_open_lseg_session()

# ── In-memory result cache ────────────────────────────────────────────────────
# Holds the two DataFrames from the last successful pull.
# Protected by _cache_lock so concurrent requests don't race.

_cache_lock = threading.Lock()
_result_cache: dict = {
    "time_changes":      None,
    "top_20_out":        None,
    "run_date":          None,
    "cyber_index_close": None,
}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


def _fmt_date(d: date) -> str:
    return d.strftime("%B %-d, %Y")


@app.route("/run", methods=["POST"])
def run():
    body       = request.get_json(silent=True) or {}
    start_date = body.get("start_date") or None   # "YYYY-MM-DD" or None
    end_date   = body.get("end_date")   or None   # "YYYY-MM-DD" or None
    n_days     = max(1, min(int(body.get("n_days", 5)), 252))

    with _cache_lock:
        try:
            time_changes, top_20_out, past, today, cyber_index_close = data_pull.run_data_pull(
                n_days=n_days, start_date=start_date, end_date=end_date)
        except RuntimeError as exc:
            # One reconnect attempt in case the LSEG session timed out
            try:
                _open_lseg_session()
                time_changes, top_20_out, past, today, cyber_index_close = data_pull.run_data_pull(
                    n_days=n_days, start_date=start_date, end_date=end_date)
            except Exception as retry_exc:
                traceback.print_exc()
                return jsonify({"status": "error", "message": str(retry_exc)}), 500
        except Exception as exc:
            traceback.print_exc()
            return jsonify({"status": "error", "message": f"Unexpected error: {exc}"}), 500

        run_date = str(today)
        _result_cache["time_changes"]      = time_changes
        _result_cache["top_20_out"]        = top_20_out
        _result_cache["run_date"]          = run_date
        _result_cache["cyber_index_close"] = cyber_index_close

        # Build CyberIndex summary row and prepend it to the table
        # Drop Instrument column — it's an internal RIC code not useful for display
        display_cols = ["Company", "Ticker", "Date", "Price Close", "Period Change", "Market Cap"]
        cyber_period_change = time_changes["CyberIndex"].iloc[-1]
        ci_row = pd.DataFrame([{
            "Company":       "CyberIndex",
            "Ticker":        "",
            "Date":          str(today),
            "Price Close":   f"${cyber_index_close:,.2f}",
            "Period Change": f"{cyber_period_change * 100:.2f}%",
            "Market Cap":    "",
        }])
        display_table = pd.concat([ci_row, top_20_out[display_cols].reset_index(drop=True)], ignore_index=True)
        table_html = display_table.to_html(classes=["data-table"], border=0, index=False)

        # Identify biggest gainer and loser by final % change (exclude Date and CyberIndex)
        stock_cols = [c for c in time_changes.columns if c not in ("Date", "CyberIndex")]
        final_row  = time_changes[stock_cols].iloc[-1]
        gainer_name = final_row.idxmax()
        loser_name  = final_row.idxmin()

        # Pass CyberIndex, gainer, loser, and all company series to the frontend
        chart_data = {
            "labels":        time_changes["Date"].dt.strftime('%Y-%m-%d %H:%M').tolist(),
            "cyber_index":   time_changes["CyberIndex"].tolist(),
            "gainer_name":   gainer_name,
            "gainer_values": time_changes[gainer_name].tolist(),
            "loser_name":    loser_name,
            "loser_values":  time_changes[loser_name].tolist(),
            "all_series":    {
                col: [round(v, 6) for v in time_changes[col].tolist()]
                for col in stock_cols
            },
        }

        return jsonify({
            "status":       "ok",
            "table_html":   table_html,
            "run_date":     run_date,
            "date_range":   f"{_fmt_date(past)} \u2013 {_fmt_date(today)}",
            "chart_data":   chart_data,
        })


@app.route("/download/charting", methods=["GET"])
def download_charting():
    with _cache_lock:
        df       = _result_cache["time_changes"]
        run_date = _result_cache["run_date"]

    if df is None:
        return "No data available — run a pull first.", 404

    df_out = df.copy()
    # Convert decimal % changes → rounded percentages (e.g. 0.0123 → 1.23)
    for col in df_out.columns:
        if col != 'Date':
            df_out[col] = (df_out[col] * 100).round(2)
    # Strip timezone so openpyxl can write datetime cells (Excel has no tz support)
    df_out['Date'] = df_out['Date'].dt.tz_localize(None).dt.to_pydatetime()

    buf = io.BytesIO()
    df_out.to_excel(buf, index=False, engine='openpyxl')
    buf.seek(0)

    wb = openpyxl.load_workbook(buf)
    ws = wb.active
    for row in ws.iter_rows(min_row=2, min_col=1, max_col=1):
        for cell in row:
            cell.number_format = 'MM/DD/YYYY HH:MM:SS AM/PM'
    buf2 = io.BytesIO()
    wb.save(buf2)
    buf2.seek(0)

    filename = f"cyberIndex_ChartingData_{run_date.replace('-', '_')}.xlsx"
    return Response(
        buf2.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/download/table", methods=["GET"])
def download_table():
    with _cache_lock:
        df       = _result_cache["top_20_out"]
        run_date = _result_cache["run_date"]

    if df is None:
        return "No data available — run a pull first.", 404

    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine='openpyxl')
    buf.seek(0)

    filename = f"cyberIndex_Table_{run_date.replace('-', '_')}.xlsx"
    return Response(
        buf.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
