"""
Rate limit tests — run against live server at http://localhost:8001
"""
import sys
import pathlib
import time
import uuid
import httpx
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "backend"))

BASE = "http://localhost:8001"

# Each test run uses a unique key so it never shares quota with other tests or
# the demo project.  The key doesn't need to be registered — the ingest endpoint
# accepts any non-empty api_key and the rate limiter tracks by key string.
_RLTEST_KEY = f"sf_rl_test_{uuid.uuid4().hex[:8]}"


def _event(key=None):
    return {
        "event_id":   str(uuid.uuid4()),
        "project":    "demo",
        "api_key":    key or _RLTEST_KEY,
        "session_id": str(uuid.uuid4()),
        "user_id":    "rl_test",
        "event_name": "page_view",
        "path":       "/rl-test",
        "url":        "http://x.com",
        "title":      "T",
        "timestamp":  time.time(),
    }


class TestRateLimitHeaders:
    def test_rate_limit_policy_header_on_ingest(self):
        r = httpx.post(f"{BASE}/ingest/event", json=_event())
        assert "x-ratelimit-policy" in r.headers
        policy = r.headers["x-ratelimit-policy"]
        assert "ingest=" in policy
        assert "mgmt=" in policy

    def test_rate_limit_policy_header_on_mgmt(self):
        r = httpx.get(f"{BASE}/api/projects")
        assert "x-ratelimit-policy" in r.headers

    def test_429_returns_json_error_body(self):
        """Send 600 events in one batch to blow the 500/s ingest limit."""
        key = f"sf_rl_blow_{uuid.uuid4().hex[:6]}"
        events = [_event(key) for _ in range(600)]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        assert r.status_code == 429
        body = r.json()
        assert body["detail"]["error"] == "rate_limit_exceeded"
        assert "retry_after_seconds" in body["detail"]

    def test_429_has_retry_after_header(self):
        key = f"sf_rl_hdr_{uuid.uuid4().hex[:6]}"
        events = [_event(key) for _ in range(600)]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        assert r.status_code == 429
        assert "retry-after" in r.headers

    def test_different_keys_have_independent_limits(self):
        """Two different api keys should not share quota."""
        key_a = f"sf_ind_a_{uuid.uuid4().hex[:4]}"
        key_b = f"sf_ind_b_{uuid.uuid4().hex[:4]}"
        r_a = httpx.post(f"{BASE}/ingest/batch", json={"events": [_event(key_a) for _ in range(300)]})
        r_b = httpx.post(f"{BASE}/ingest/batch", json={"events": [_event(key_b) for _ in range(300)]})
        assert r_a.status_code == 200
        assert r_b.status_code == 200

    def test_limit_resets_after_window(self):
        """After 1 s the ingest window resets and requests succeed again."""
        key = f"sf_rl_reset_{uuid.uuid4().hex[:6]}"
        events = [_event(key) for _ in range(600)]
        r1 = httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        assert r1.status_code == 429
        time.sleep(1.1)
        r2 = httpx.post(f"{BASE}/ingest/event", json=_event(key))
        assert r2.status_code == 200


class TestRateLimitSlidingWindow:
    def test_sliding_window_not_fixed_bucket(self):
        """
        Send 400 events, wait for window to slide, then send 200 more — both
        should succeed because the first 400 have aged out of the 1-second window.
        """
        key = f"sf_sliding_{uuid.uuid4().hex[:6]}"
        r1 = httpx.post(f"{BASE}/ingest/batch", json={"events": [_event(key) for _ in range(400)]})
        assert r1.status_code == 200
        time.sleep(1.1)
        r2 = httpx.post(f"{BASE}/ingest/batch", json={"events": [_event(key) for _ in range(200)]})
        assert r2.status_code == 200


class TestRateLimitUnit:
    """Unit tests for the SlidingWindowLimiter class directly."""

    def test_limiter_allows_up_to_limit(self):
        from rate_limit import SlidingWindowLimiter
        lim = SlidingWindowLimiter(limit=10, window_seconds=60)
        for _ in range(10):
            allowed, _ = lim.is_allowed("k")
            assert allowed

    def test_limiter_blocks_over_limit(self):
        from rate_limit import SlidingWindowLimiter
        lim = SlidingWindowLimiter(limit=5, window_seconds=60)
        for _ in range(5):
            lim.is_allowed("k")
        allowed, remaining = lim.is_allowed("k")
        assert not allowed
        assert remaining == 0

    def test_limiter_cost_parameter(self):
        from rate_limit import SlidingWindowLimiter
        lim = SlidingWindowLimiter(limit=10, window_seconds=60)
        allowed, remaining = lim.is_allowed("k", cost=7)
        assert allowed
        assert remaining == 3
        allowed2, _ = lim.is_allowed("k", cost=4)   # 7+4=11 > 10
        assert not allowed2

    def test_limiter_independent_keys(self):
        from rate_limit import SlidingWindowLimiter
        lim = SlidingWindowLimiter(limit=3, window_seconds=60)
        for _ in range(3):
            lim.is_allowed("a")
        allowed_a, _ = lim.is_allowed("a")
        allowed_b, _ = lim.is_allowed("b")
        assert not allowed_a
        assert allowed_b

    def test_limiter_reset_clears_key(self):
        from rate_limit import SlidingWindowLimiter
        lim = SlidingWindowLimiter(limit=2, window_seconds=60)
        lim.is_allowed("k"); lim.is_allowed("k")
        assert not lim.is_allowed("k")[0]
        lim.reset("k")
        assert lim.is_allowed("k")[0]
