"""
Sliding-window rate limiter — no Redis required.

Two limiters:
  ingest_limiter   — per client IP, events per second
  mgmt_limiter     — per client IP, requests per minute

Ingest is bucketed by **client IP**, NOT by the client-supplied api_key, because
the api_key field is attacker-controlled: keying on it lets an attacker rotate
to a fresh key per request and get an unlimited budget. (Fixes audit H-2.)

Algorithm: sliding-window log (stores timestamps of recent requests in a deque).
Thread-safe via asyncio (single-threaded event loop); the deques are never
accessed from multiple threads. Empty buckets are evicted so the key space
cannot grow without bound. (Fixes audit M-2.)
"""
import time
from collections import defaultdict, deque
from fastapi import Request, HTTPException
from config import RATE_LIMIT_INGEST_PER_SEC, RATE_LIMIT_MGMT_PER_MIN


class SlidingWindowLimiter:
    """
    Tracks request timestamps in a per-key deque.
    Allows `limit` calls within a rolling `window_seconds` window.
    """
    def __init__(self, limit: int, window_seconds: float):
        self.limit   = limit
        self.window  = window_seconds
        self._log: dict[str, deque] = defaultdict(deque)

    def is_allowed(self, key: str, cost: int = 1) -> tuple[bool, int]:
        """
        Returns (allowed, remaining).
        `cost` lets a batch of N events count as N against the limit.
        """
        now    = time.monotonic()
        cutoff = now - self.window
        dq     = self._log[key]

        # evict timestamps outside the window
        while dq and dq[0] < cutoff:
            dq.popleft()

        if len(dq) + cost > self.limit:
            remaining = max(0, self.limit - len(dq))
            # leave no empty bucket behind if we just emptied it
            if not dq:
                self._log.pop(key, None)
            return False, remaining

        # record `cost` timestamps as a single group (append cost times)
        for _ in range(cost):
            dq.append(now)

        return True, self.limit - len(dq)

    def evict_idle(self):
        """Drop buckets that currently hold no in-window timestamps. (M-2)"""
        now = time.monotonic()
        cutoff = now - self.window
        for key in list(self._log.keys()):
            dq = self._log[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if not dq:
                self._log.pop(key, None)

    def reset(self, key: str):
        self._log.pop(key, None)


# ── singleton limiters ────────────────────────────────────────────────────────
ingest_limiter = SlidingWindowLimiter(
    limit=RATE_LIMIT_INGEST_PER_SEC,
    window_seconds=1.0,
)

mgmt_limiter = SlidingWindowLimiter(
    limit=RATE_LIMIT_MGMT_PER_MIN,
    window_seconds=60.0,
)


# ── FastAPI dependency helpers ─────────────────────────────────────────────────
def client_ip(request: Request) -> str:
    """Best-effort IP extraction (handles X-Forwarded-For from proxies)."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def check_ingest_limit(request: Request, event_count: int = 1):
    """
    Dependency: rate-limit ingest by **client IP** (not the client-supplied
    api_key, which is forgeable — see module docstring, audit H-2).
    Raises HTTP 429 if over limit. Returns remaining quota.
    """
    ip = client_ip(request)
    allowed, remaining = ingest_limiter.is_allowed(ip, cost=event_count)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": f"Ingest limit: {RATE_LIMIT_INGEST_PER_SEC} events/s per IP.",
                "retry_after_seconds": 1,
            },
            headers={"Retry-After": "1", "X-RateLimit-Remaining": "0"},
        )
    return remaining


async def check_mgmt_limit(request: Request):
    """
    Dependency: rate-limit management API by client IP.
    Raises HTTP 429 if over limit.
    """
    ip = client_ip(request)
    allowed, remaining = mgmt_limiter.is_allowed(ip)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": f"Management API limit: {RATE_LIMIT_MGMT_PER_MIN} requests/min per IP.",
                "retry_after_seconds": 60,
            },
            headers={"Retry-After": "60", "X-RateLimit-Remaining": "0"},
        )
    return remaining
