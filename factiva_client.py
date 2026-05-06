import asyncio
import os
import sys
import time as _time
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/Documents/sandbox/factiva_analytics/factiva/src"))
from factiva import FactivaClient


def _run_async(coro):
    """Run an async coroutine from synchronous Flask code."""
    return asyncio.run(coro)


async def _extract(company: str, start_date: str, end_date: str, limit: int = 5) -> list[dict]:
    api_key = os.environ.get("FACTIVA_API_KEY")
    if not api_key:
        raise EnvironmentError("FACTIVA_API_KEY is not set")

    safe_name = company.replace('"', "")
    fql = f'la=en AND IN=isecpri AND ("{safe_name}")'

    async with FactivaClient(api_key=api_key) as client:
        articles = await client.extract_articles(fql, limit=limit)

    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    end_ts = int(end_dt.timestamp() * 1000)

    filtered = []
    for a in articles:
        pub_date = a.get("publication_date", 0)
        if start_ts <= pub_date <= end_ts:
            filtered.append(a)

    filtered.sort(key=lambda a: a.get("publication_date", 0), reverse=True)
    return filtered


async def _extract_batch(companies: list[str], start_date: str, end_date: str, limit_per_company: int = 5) -> dict:
    """Extract articles for multiple companies in a single Factiva job.
    Combines companies into one OR query, then distributes results.
    Returns {company_name: [articles]} dict.
    """
    api_key = os.environ.get("FACTIVA_API_KEY")
    if not api_key:
        raise EnvironmentError("FACTIVA_API_KEY is not set")

    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    end_ts = int(end_dt.timestamp() * 1000)

    # Build a single query: English, cybersecurity industry, any of the companies
    or_parts = " OR ".join(f'"{c.replace(chr(34), "")}"' for c in companies)
    fql = f"la=en AND IN=isecpri AND ({or_parts})"
    total_limit = limit_per_company * len(companies)

    results = {c: [] for c in companies}

    async with FactivaClient(api_key=api_key) as client:
        try:
            t0 = _time.time()
            print(f"[factiva] Starting extraction: limit={total_limit}, companies={len(companies)}", flush=True)
            print(f"[factiva] FQL ({len(fql)} chars): {fql[:200]}...", flush=True)
            articles = await client.extract_articles(fql, limit=total_limit)
            print(f"[factiva] Extraction complete: {len(articles)} articles in {_time.time()-t0:.1f}s", flush=True)
        except Exception as exc:
            print(f"[factiva] Extraction FAILED after {_time.time()-t0:.1f}s: {exc}", flush=True)
            return {c: exc for c in companies}

    # Distribute articles to companies by checking if company name appears in title or body
    for a in articles:
        pub_date = a.get("publication_date", 0)
        if isinstance(pub_date, str):
            try:
                pub_date = int(datetime.strptime(pub_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
            except (ValueError, TypeError):
                continue
        if not (start_ts <= pub_date <= end_ts):
            continue
        title = (a.get("title") or "").lower()
        body = (a.get("body") or "").lower()
        snippet = (a.get("snippet") or "").lower()
        text = f"{title} {body} {snippet}"

        for company in companies:
            if company.lower() in text:
                results[company].append(a)

    # Sort each company's articles by date descending and cap at limit
    for company in companies:
        results[company].sort(key=lambda a: a.get("publication_date", 0), reverse=True)
        results[company] = results[company][:limit_per_company]

    return results


def extract_articles_for_company(company: str, start_date: str, end_date: str, limit: int = 5) -> list[dict]:
    """Synchronous wrapper. Returns Factiva articles for a company in a date range."""
    return _run_async(_extract(company, start_date, end_date, limit))


def extract_articles_batch(companies: list[str], start_date: str, end_date: str, limit_per_company: int = 5) -> dict:
    """Extract articles for multiple companies in a single Factiva job.
    Returns {company_name: list[dict] | Exception}.
    """
    return _run_async(_extract_batch(companies, start_date, end_date, limit_per_company))
