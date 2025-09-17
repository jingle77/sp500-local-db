from __future__ import annotations
import threading, time
from collections import deque

class MinuteRateLimiter:
    def __init__(self, max_per_minute: int):
        self.max = max_per_minute
        self.q = deque()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            cutoff = now - 60.0
            while self.q and self.q[0] < cutoff:
                self.q.popleft()
            if len(self.q) >= self.max:
                sleep_for = 60.0 - (now - self.q[0])
                if sleep_for > 0:
                    time.sleep(sleep_for)
                # cleanup after sleep
                now = time.time()
                cutoff = now - 60.0
                while self.q and self.q[0] < cutoff:
                    self.q.popleft()
            self.q.append(time.time())
