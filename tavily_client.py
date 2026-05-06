import os

from tavily import TavilyClient


def _client() -> TavilyClient:
    key = os.environ.get("TAVILY_API_KEY")
    if not key:
        raise EnvironmentError("TAVILY_API_KEY is not set")
    return TavilyClient(api_key=key)


def search_news(ticker: str, company: str, date: str) -> list[dict]:
    """
    Search for news around a stock volatility event.

    Returns a list of result dicts with keys: title, url, content, score.
    """
    query = f"{company} ({ticker}) stock {date} cybersecurity news"
    resp  = _client().search(
        query=query,
        search_depth="advanced",
        topic="news",
        days=3,
        max_results=5,
    )
    return resp.get("results", [])
