from __future__ import annotations
import time, logging
import requests
from .rate_limit import MinuteRateLimiter
from . import config

log = logging.getLogger(__name__)

class PricesClient:
    def __init__(self, session: requests.Session | None = None, rpm: int = config.REQUESTS_PER_MINUTE):
        self.session = session or requests.Session()
        self.limiter = MinuteRateLimiter(rpm)

    def historical_divadj(self, symbol: str) -> list[dict]:
        params = {"symbol": symbol, "apikey": config.API_KEY}
        for attempt in range(5):
            self.limiter.acquire()
            try:
                r = self.session.get(config.FMP_PRICES_URL, params=params, timeout=30)
                if r.status_code == 200:
                    data = r.json() or []
                    return data if isinstance(data, list) else []
                elif r.status_code in (429, 503, 504):
                    time.sleep(2 ** attempt)
                else:
                    log.error("HTTP %s %s: %s", r.status_code, symbol, r.text[:200])
                    return []
            except requests.RequestException as e:
                log.warning("Request error %s (try %d): %s", symbol, attempt+1, e)
                time.sleep(2 ** attempt)
        return []
