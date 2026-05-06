import asyncio
import os
import sys
import time as _time

sys.path.insert(0, os.path.expanduser("~/Documents/sandbox/factiva_analytics/factiva_sentiment/src"))
from factiva_sentiment import SentimentClient

COMPANY_TO_CODE = {
    "Palo Alto Networks": "PALTNK",
    "CrowdStrike": "PGXXRC",
    "Fortinet": "FORNT",
    "Zscaler": "ZSECSF",
    "Check Point Software": "CHCKSF",
    "Okta": "OKTAIU",
    "F5": "FFIVE",
    "Akamai Technologies": "AKMT",
    "SailPoint": "SAILPZ",
    "Varonis Systems": "VRNSSI",
    "Qualys": "QUALY",
    "Tenable": "SYJEJV",
    "NetScout Systems": "FRNSFT",
    "Radware": "RADWAE",
    "Rapid7": "RPDSLL",
    "Telos": "TLOS",
    "HUB Cyber Security": "ALDALD",
    "CyberArk Software": "CYBARK",
    "Cloudflare": "CLDFLR",
    "Leidos": "SCIAPP",
    "Rubrik": "IJDGER",
    "SentinelOne": "JXJFFT",
    "Lumen Technologies": "CTLE",
    "A10 Networks": "ATENTW",
    "Commvault Systems": "CMMVLT",
    "Cisco Systems": "CISCOS",
    "Allegro MicroSystems": "ALLGMI",
    "Intrusion": "OPDYS",
    "Mitek Systems": "MTKSYS",
    "OneSpan": "VSCDTR",
    "CISO Global": "CCYBSN",
}
CODE_TO_COMPANY = {v: k for k, v in COMPANY_TO_CODE.items()}


def extract_sentiment_batch(companies: list[str], start_date: str, end_date: str) -> dict:
    """Extract sentiment scores + article links for multiple companies.
    Returns {company_name: {"scores": [...], "articles": [...]} | Exception}.
    """
    try:
        return asyncio.run(_extract(companies, start_date, end_date))
    except Exception as exc:
        return {c: exc for c in companies}


async def _extract(companies: list[str], start_date: str, end_date: str) -> dict:
    api_key = os.environ.get("FACTIVA_SENTIMENT_API_KEY")
    if not api_key:
        raise EnvironmentError("FACTIVA_SENTIMENT_API_KEY is not set")

    codes = []
    code_to_name = {}
    skipped = []
    for c in companies:
        code = COMPANY_TO_CODE.get(c)
        if code:
            codes.append(code)
            code_to_name[code] = c
        else:
            skipped.append(c)

    results = {c: {"scores": [], "articles": []} for c in companies}
    for s in skipped:
        results[s] = {"scores": [], "articles": []}

    if not codes:
        return results

    t0 = _time.time()
    print(f"[sentiment] Starting FSS extraction: {len(codes)} companies, {start_date} to {end_date}", flush=True)

    async with SentimentClient(api_key=api_key) as client:
        try:
            records = await client.extract(codes, start_date, end_date, articles=True)
            print(f"[sentiment] Extraction complete: {len(records)} records in {_time.time()-t0:.1f}s", flush=True)
        except Exception as exc:
            print(f"[sentiment] Extraction FAILED after {_time.time()-t0:.1f}s: {exc}", flush=True)
            return {c: exc for c in companies}

    # Index records by company code for fast grouping
    t1 = _time.time()
    by_code = {code: [] for code in codes}
    for r in records:
        cc = r.get("_company_code")
        if cc in by_code:
            by_code[cc].append(r)

    for code in codes:
        company = code_to_name[code]
        company_records = by_code[code]

        scores_by_date = {}
        article_candidates = []

        for r in company_records:
            sd = r.get("score_date", "")
            if sd and sd not in scores_by_date:
                scores_by_date[sd] = {
                    "score_date": sd,
                    "score": r.get("score", 0),
                    "signal": r.get("signal", ""),
                    "daily_percentage_change": r.get("daily_percentage_change", 0),
                    "weekly_percentage_change": r.get("weekly_percentage_change", 0),
                    "total_article_count": r.get("total_article_count", "0"),
                    "negative_article_count": r.get("negative_article_count", "0"),
                    "positive_article_count": r.get("positive_article_count", "0"),
                }

            an = r.get("_an")
            rank = r.get("article_rank", 0) or 0
            if an:
                article_candidates.append((rank, an, r.get("theme", ""), r.get("sentiment_flag", ""), sd))

        # Keep only the top 10 most relevant articles (highest article_rank)
        article_candidates.sort(key=lambda x: x[0], reverse=True)
        seen = set()
        articles = []
        for rank, an, theme, sentiment_flag, sd in article_candidates:
            if an in seen:
                continue
            seen.add(an)
            articles.append({
                "url": SentimentClient.article_url(an),
                "theme": theme,
                "sentiment_flag": sentiment_flag,
                "score_date": sd,
            })
            if len(articles) >= 10:
                break

        results[company] = {
            "scores": sorted(scores_by_date.values(), key=lambda x: x["score_date"], reverse=True),
            "articles": articles,
        }

    print(f"[sentiment] Post-processing took {_time.time()-t1:.1f}s", flush=True)
    return results
