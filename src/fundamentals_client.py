from __future__ import annotations
import time, logging, requests
from .rate_limit import MinuteRateLimiter
from . import config

log = logging.getLogger(__name__)

class FundamentalsClient:
    def __init__(self, session: requests.Session | None = None, rpm: int = config.REQUESTS_PER_MINUTE):
        self.session = session or requests.Session()
        self.limiter = MinuteRateLimiter(rpm)

    def _get(self, base_url: str, symbol: str):
        params = {"symbol": symbol, "apikey": config.API_KEY}
        for attempt in range(5):
            self.limiter.acquire()
            try:
                r = self.session.get(base_url, params=params, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if data is None:
                        return []
                    return data if isinstance(data, list) else [data]
                elif r.status_code in (429, 503, 504):
                    time.sleep(2 ** attempt)
                else:
                    log.error("HTTP %s for %s: %s", r.status_code, symbol, r.text[:200])
                    return []
            except requests.RequestException as e:
                log.warning("Request error %s (try %d): %s", symbol, attempt+1, e)
                time.sleep(2 ** attempt)
        return []

    def balance_sheet(self, symbol: str):    
        return self._get(config.FMP_BALANCE_SHEET_URL, symbol)
    def income_statement(self, symbol: str):
        return self._get(config.FMP_INCOME_STATEMENT_URL, symbol)
    def key_metrics(self, symbol: str):
        return self._get(config.FMP_KEY_METRICS_URL, symbol)
    def ratios(self, symbol: str):
        return self._get(config.FMP_RATIOS_URL, symbol)
    def revenue_segments(self, symbol: str):
        return self._get(config.FMP_REVENUE_SEGMENTS_URL, symbol)
