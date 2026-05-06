# Sidebar Implementation Guide

Instructions for re-implementing the Factiva article sidebar. This was removed to unblock deployment while Factiva retrieval issues are resolved.

## Overview

The sidebar is a slide-in panel on the right side of the page that shows Factiva news articles and sentiment scores for the chart's biggest gainer and loser companies. Articles are fetched in a background thread as soon as the user clicks "Get Data", running in parallel with the LSEG data pull.

## Architecture

```
User clicks "Get Data"
    ├── LSEG data pull (blocks, returns table + chart)
    └── Factiva sentiment extraction (background thread)
            ├── Priority batch: top gainer + loser (first 2)
            └── Remaining batch: all other KNOWN_COMPANIES
```

The frontend polls `/articles/status` to know when data is ready, then fetches articles on demand when the user opens the sidebar.

## Files to modify

### 1. `app.py` — Backend

#### Dependencies to add back
```python
import db
import sentiment_client
import llm
import pandas_market_calendars as mcal
```

#### Constants
```python
KNOWN_COMPANIES = [
    "Palo Alto Networks", "CrowdStrike", "Fortinet", "Zscaler", "Check Point Software",
    "Okta", "F5", "Akamai Technologies", "SailPoint", "Varonis Systems",
    "Qualys", "Tenable", "NetScout Systems", "Radware", "Rapid7",
    "Telos", "HUB Cyber Security", "CyberArk Software", "Cloudflare", "Leidos",
    "Rubrik", "SentinelOne", "Lumen Technologies", "A10 Networks", "Commvault Systems",
    "Cisco Systems", "Allegro MicroSystems", "Intrusion", "Mitek Systems", "OneSpan",
    "CISO Global",
]

LSEG_TO_KNOWN = {
    "Check Point Software Technologies": "Check Point Software",
    "CrowdStrike Holdings": "CrowdStrike",
    "Tenable Holdings": "Tenable",
    "Leidos Holdings": "Leidos",
    "Netscout Systems": "NetScout Systems",
}
```

#### State to add
```python
_article_lock = threading.Lock()
_article_status: dict = {}  # {company_name: "ready" | "loading" | "error" | "pending"}
```

Also call `db.init_db()` at module level.

#### Date range helper
Add `_compute_date_range(n_days, start_date, end_date)` — uses `pandas_market_calendars` to compute the trading-day window for the sentiment query without calling LSEG. This is needed because the sentiment thread starts before the LSEG response returns dates.

#### Background extraction logic
Two functions:
- `_run_sentiment_batch(companies, start_date, end_date, label)` — checks DB cache first, then calls `sentiment_client.extract_sentiment_batch()` for uncached companies. Updates `_article_status` per company.
- `_extract_sentiment_background(companies, start_date, end_date)` — orchestrator that runs priority batch (first 2 companies = gainer/loser) then the rest.

#### Changes to `/run` route
Before the LSEG call:
1. Compute date range with `_compute_date_range()`
2. Clear `_article_status` and set all companies to "pending"
3. Store `_result_cache["article_date_range"] = (start_date_str, end_date_str)`
4. Spawn a daemon thread running `_extract_sentiment_background`

In the response JSON, add:
```python
"name_map": LSEG_TO_KNOWN,
```

#### New routes
- `GET /articles/status` — returns `{"status": "ok", "companies": {name: status_str, ...}}`
- `GET /articles/<company>` — returns cached sentiment data (scores + articles) from DB
- `POST /chat` — LLM chat endpoint (uses `llm.chat(messages, context)`)

### 2. `templates/index.html` — Frontend

#### CSS to add
- `.main` needs `transition: padding-right 0.28s cubic-bezier(0.4,0,0.2,1);`
- `.main.sidebar-open { padding-right: calc(420px + 24px); }` — shifts content left when sidebar opens
- Full sidebar styles: `#article-sidebar` (fixed, right:0, width 420px), `.sidebar-header`, `.sidebar-body`, `.article-section`, `.article-item`, `.sentiment-badge`, `.score-summary`, `.article-loading`, etc.

#### HTML to add (after `</main>`, before `<script>`)
```html
<div id="article-sidebar">
  <div class="sidebar-header">
    <div>
      <div class="sidebar-title">News Feed</div>
      <div class="sidebar-meta" id="sidebar-meta">Factiva Sentiment Signals</div>
    </div>
    <button id="sidebar-close" title="Close">&#10005;</button>
  </div>
  <div class="sidebar-body" id="sidebar-body"></div>
</div>
```

#### JavaScript to add

**Global state:**
```javascript
let _articlePollInterval = null;
let _currentGainer = null;
let _currentLoser = null;
let _lastChartData = null;
let _sidebarCompanies = {};
let _nameMap = {};
let _bgPollInterval = null;
let _companyReadiness = {};
```

**Background readiness polling:**
- `startBackgroundReadinessPolling()` — called when "Get Data" is clicked, polls `/articles/status` every 3s
- `stopBackgroundReadinessPolling()` — stops when all companies are "ready" or "error"
- `_pollReadiness()` — fetches status, updates `_companyReadiness`

**Sidebar open/close:**
- `openArticleSidebarForCompany(companyName, role)` — opens sidebar, adds a section for the company, calls `_tryLoadOrPoll`
- `closeSidebar()` — hides sidebar, stops polling, resets state
- Bind close button click and Escape key to `closeSidebar()`

**Article loading:**
- `_tryLoadOrPoll(companyName, role)` — checks readiness, loads if ready or starts polling
- `startArticlePolling()` / `stopArticlePolling()` — poll `/articles/status` every 5s
- `checkAndLoadArticles()` — checks status for gainer/loser, loads when ready
- `loadArticlesForSection(company, role)` — fetches `/articles/<company>`, renders score summary + article list

**Chart integration:**
- In `renderChart()`, make gainer/loser end-labels clickable:
  ```javascript
  if (s.name === cd.gainer_name || s.name === cd.loser_name) {
    lbl.style.cursor = 'pointer';
    lbl.addEventListener('click', () => {
      const role = s.name === cd.gainer_name ? 'gainer' : 'loser';
      openArticleSidebarForCompany(s.name, role);
    });
  }
  ```

**In pull handler (`pullBtn` click):**
- Call `startBackgroundReadinessPolling()` right after `showSpinner()`
- After successful response, store: `_lastChartData = data.chart_data; _nameMap = data.name_map || {};`

## Supporting files

- **`db.py`** — SQLite cache for sentiment results. Functions: `init_db()`, `get_cached_sentiment(company, start, end)`, `save_sentiment(company, start, end, data)`
- **`sentiment_client.py`** — Calls Factiva Sentiment Service. Function: `extract_sentiment_batch(companies, start_date, end_date)` → returns `{company: data_or_exception}`
- **`llm.py`** — LLM chat handler. Function: `chat(messages, context)` → returns reply string
- **`factiva_client.py`** — Lower-level Factiva API wrapper used by sentiment_client

## Key design decisions

1. **Two-pass extraction**: Priority batch (gainer + loser) runs first so the sidebar can show data quickly, then remaining companies load in the background for future sidebar opens.
2. **DB caching**: Sentiment results are cached by (company, start_date, end_date) so repeated queries don't re-hit Factiva.
3. **Name mapping**: LSEG returns names like "CrowdStrike Holdings" but Factiva uses "CrowdStrike". The `LSEG_TO_KNOWN` map + `_nameMap` in JS handle this translation.
4. **Non-blocking**: The sidebar never blocks the main data pull. If Factiva is slow or broken, the chart/table still loads normally.

## Git reference

The full working implementation (before removal) exists in the working tree at commit `1758d40` plus uncommitted changes. To see the exact diff of what was removed, run:
```bash
git diff HEAD -- app.py templates/index.html
```
