"""
Sliding-window rate limiter — no Redis required.

Two limiters:
  ingest_limiter   — per api_key, events per second
  mgmt_limiter     — per client IP, requests per minute

Algorithm: sliding-window log (stores timestamps of recent requests in a deque).
Thread-safe via asyncio (single-threaded event loop); the deques are never
accessed from multiple threads.
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
            return False, remaining

        # record `cost` timestamps as a single group (append cost times)
        for _ in range(cost):
            dq.append(now)

        return True, self.limit - len(dq)

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


async def check_ingest_limit(request: Request, api_key: str, event_count: int = 1):
    """
    Dependency: rate-limit ingest by api_key.
    Raises HTTP 429 if over limit. Returns remaining quota.
    """
    allowed, remaining = ingest_limiter.is_allowed(api_key or "anonymous", cost=event_count)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": f"Ingest limit: {RATE_LIMIT_INGEST_PER_SEC} events/s per API key.",
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
