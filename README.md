# WSJ Pro CyberIndex

A market-cap-weighted index tracking the top 20 publicly traded cybersecurity companies. Built as a Flask web app with real-time data from LSEG (Refinitiv), deployed on Google Cloud Functions (gen2).

## Features

- **CyberIndex**: Market-cap-weighted average price across the top 20 cyber stocks
- **Interactive charting**: Period % change with per-company drill-down
- **Custom date ranges**: Calendar picker with NYSE trading day awareness
- **Excel exports**: Download charting data or summary table as `.xlsx`
- **Access-gated**: Session-based login with a shared access key

## Architecture

```
main.py          → functions-framework entry point + auth gate
app.py           → Flask routes, LSEG session management, caching
data_pull.py     → LSEG data fetching, index calculation
templates/       → Jinja2 HTML (single-page app)
static/          → Company logos
Dockerfile       → Container build for Cloud Run / Cloud Functions gen2
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your ACCESS_KEY and SECRET_KEY

# Run locally
python main.py
# App available at http://localhost:8080
```

Requires `lseg-data.config.json` in the project root with valid LSEG platform credentials.

## Deployment

Deployed as a Google Cloud Function (gen2) backed by Cloud Run:

```bash
gcloud functions deploy wsj-pro-data-cyberindex \
  --gen2 \
  --runtime python312 \
  --entry-point cyberindex_entry_point \
  --source . \
  --trigger-http \
  --allow-unauthenticated \
  --memory 1Gi \
  --timeout 120s \
  --set-env-vars ACCESS_KEY=<key>,SECRET_KEY=<key>
```

## Data Sources

- **LSEG Data Library** (formerly Refinitiv): Real-time and historical pricing, market cap, shares outstanding
- **NYSE calendar**: Trading day validation via `pandas_market_calendars`

## Index Constituents

31 cybersecurity companies are tracked; the top 20 by market cap on any given day form the index. Includes large-caps (PANW, CRWD, FTNT, ZS, CSCO) through small-caps (ALLT, INTZ, CYCU).
