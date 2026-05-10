"""Modo Energy API client.

Auth: X-Token header (public API token).
Base URL: https://api.modoenergy.com/pub/v1
Rate limit: 1000 req/min.
"""
import os
import time
import requests

BASE_URL = "https://api.modoenergy.com/pub/v1"
_DEFAULT_PAGE_SIZE = 1000


class ModoClient:
    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or os.environ["MODO_API_KEY"]
        self._session = requests.Session()
        self._session.headers.update({"X-Token": self._api_key})

    def get(self, path: str, params: dict | None = None) -> list[dict]:
        """Fetch all pages for a paginated endpoint; return flat list of records.

        For non-paginated endpoints (list response without count/results wrapper),
        returns the list directly.
        """
        params = dict(params or {})
        url = f"{BASE_URL}{path}"

        # First request
        resp = self._request(url, {**params, "limit": _DEFAULT_PAGE_SIZE, "offset": 0})

        # Non-paginated: list response
        if isinstance(resp, list):
            return resp

        # Paginated: {count, next, previous, results}
        results = list(resp.get("results", []))
        offset = _DEFAULT_PAGE_SIZE
        while resp.get("next"):
            resp = self._request(url, {**params, "limit": _DEFAULT_PAGE_SIZE, "offset": offset})
            results.extend(resp.get("results", []))
            offset += _DEFAULT_PAGE_SIZE

        return results

    def _request(self, url: str, params: dict) -> dict | list:
        for attempt in range(4):
            r = self._session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 60 / (attempt + 1)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Modo API rate-limited after retries: {url}")
