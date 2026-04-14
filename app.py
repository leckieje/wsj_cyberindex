import io
import threading
import traceback
from datetime import date

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
    "time_changes": None,
    "top_20_out":   None,
    "run_date":     None,
}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


def _fmt_date(d: date) -> str:
    return d.strftime("%B %-d, %Y")


@app.route("/run", methods=["POST"])
def run():
    body   = request.get_json(silent=True) or {}
    n_days = max(1, min(int(body.get("n_days", 5)), 252))

    with _cache_lock:
        try:
            time_changes, top_20_out, past, today = data_pull.run_data_pull(n_days=n_days)
        except RuntimeError as exc:
            # One reconnect attempt in case the LSEG session timed out
            try:
                _open_lseg_session()
                time_changes, top_20_out, past, today = data_pull.run_data_pull(n_days=n_days)
            except Exception as retry_exc:
                traceback.print_exc()
                return jsonify({"status": "error", "message": str(retry_exc)}), 500
        except Exception as exc:
            traceback.print_exc()
            return jsonify({"status": "error", "message": f"Unexpected error: {exc}"}), 500

        run_date = str(today)
        _result_cache["time_changes"] = time_changes
        _result_cache["top_20_out"]   = top_20_out
        _result_cache["run_date"]     = run_date

        table_html = top_20_out.to_html(classes=["data-table"], border=0)

        # Identify biggest gainer and loser by final % change (exclude Date and CyberIndex)
        stock_cols = [c for c in time_changes.columns if c not in ("Date", "CyberIndex")]
        final_row  = time_changes[stock_cols].iloc[-1]
        gainer_name = final_row.idxmax()
        loser_name  = final_row.idxmin()

        # Pass CyberIndex, gainer, and loser series to the frontend
        chart_data = {
            "labels":       time_changes["Date"].tolist(),
            "cyber_index":  time_changes["CyberIndex"].tolist(),
            "gainer_name":  gainer_name,
            "gainer_values": time_changes[gainer_name].tolist(),
            "loser_name":   loser_name,
            "loser_values": time_changes[loser_name].tolist(),
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

    filename = f"cyberIndex_ChartingData_{run_date.replace('-', '_')}.csv"
    buf = io.StringIO()
    df.to_csv(buf, float_format='%.6f', index=False)
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/download/table", methods=["GET"])
def download_table():
    with _cache_lock:
        df       = _result_cache["top_20_out"]
        run_date = _result_cache["run_date"]

    if df is None:
        return "No data available — run a pull first.", 404

    filename = f"cyberIndex_Table_{run_date.replace('-', '_')}.csv"
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
