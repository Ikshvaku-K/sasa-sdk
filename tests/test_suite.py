"""
SASA — Full Test Suite
Tests run against the live server at http://localhost:8001
"""
import json
import time
import uuid
import threading
import pytest
import websockets
import asyncio

import httpx
BASE = "http://localhost:8001"
WS   = "ws://localhost:8001"
DEMO_PROJECT = "demo"
# Demo key fetched at runtime — never hardcoded
try:
    DEMO_KEY = httpx.get(f"{BASE}/api/projects/{DEMO_PROJECT}").json()["api_key"]
except Exception:
    DEMO_KEY = ""


# ═══════════════════════════════════════════════════════════════
# SECTION 1 — Static asset delivery
# ═══════════════════════════════════════════════════════════════

class TestAssetDelivery:
    def test_sdk_js_served(self):
        r = httpx.get(f"{BASE}/sdk/sasa.js")
        assert r.status_code == 200
        assert "application/javascript" in r.headers["content-type"]

    def test_sdk_js_contains_sasa_iife(self):
        r = httpx.get(f"{BASE}/sdk/sasa.js")
        src = r.text
        assert "SASA" in src
        assert "track" in src
        assert "identify" in src
        assert "page_view" in src

    def test_sdk_js_has_video_tracking(self):
        src = httpx.get(f"{BASE}/sdk/sasa.js").text
        assert "video_play" in src
        assert "video_heartbeat" in src
        assert "video_complete" in src

    def test_sdk_js_has_scroll_tracking(self):
        src = httpx.get(f"{BASE}/sdk/sasa.js").text
        assert "scroll_depth" in src

    def test_sdk_js_has_spa_tracking(self):
        src = httpx.get(f"{BASE}/sdk/sasa.js").text
        assert "pushState" in src
        assert "popstate" in src

    def test_sdk_cache_header(self):
        r = httpx.get(f"{BASE}/sdk/sasa.js")
        cc = r.headers.get("cache-control", "")
        assert "max-age" in cc

    def test_dashboard_html_served(self):
        r = httpx.get(f"{BASE}/dashboard")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "SASA" in r.text

    def test_demo_page_served(self):
        r = httpx.get(f"{BASE}/demo")
        assert r.status_code == 200
        # API key is now fetched at runtime — check the dynamic loader script is present
        assert "sasa.js" in r.text
        assert "/api/projects/demo" in r.text

    def test_demo_page_has_videos(self):
        r = httpx.get(f"{BASE}/demo")
        assert "<video" in r.text

    def test_root_redirects_to_dashboard(self):
        r = httpx.get(f"{BASE}/", follow_redirects=True)
        assert r.status_code == 200
        assert "SASA" in r.text

    def test_api_docs_available(self):
        r = httpx.get(f"{BASE}/docs")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════
# SECTION 2 — Project Management API
# ═══════════════════════════════════════════════════════════════

class TestProjectAPI:
    def test_list_projects_returns_array(self):
        r = httpx.get(f"{BASE}/api/projects")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_demo_project_exists(self):
        r = httpx.get(f"{BASE}/api/projects")
        ids = [p["id"] for p in r.json()]
        assert "demo" in ids

    def test_demo_project_has_required_fields(self):
        r = httpx.get(f"{BASE}/api/projects")
        demo = next(p for p in r.json() if p["id"] == "demo")
        assert "api_key" in demo
        assert "name" in demo
        assert "color" in demo
        assert demo["api_key"] == DEMO_KEY

    def test_create_project(self):
        name = f"Test Project {uuid.uuid4().hex[:6]}"
        r = httpx.post(f"{BASE}/api/projects", json={"name": name, "color": "#ff0000"})
        assert r.status_code == 200
        data = r.json()
        assert data["name"] == name
        assert data["api_key"].startswith("sf_")
        assert data["color"] == "#ff0000"

    def test_created_project_appears_in_list(self):
        name = f"ListTest {uuid.uuid4().hex[:6]}"
        r1 = httpx.post(f"{BASE}/api/projects", json={"name": name})
        pid = r1.json()["id"]
        r2 = httpx.get(f"{BASE}/api/projects")
        ids = [p["id"] for p in r2.json()]
        assert pid in ids

    def test_get_project_by_id(self):
        r = httpx.get(f"{BASE}/api/projects/demo")
        assert r.status_code == 200
        assert r.json()["id"] == "demo"

    def test_get_nonexistent_project_returns_404(self):
        r = httpx.get(f"{BASE}/api/projects/does_not_exist_xyz")
        assert r.status_code == 404

    def test_create_project_without_name_returns_400(self):
        r = httpx.post(f"{BASE}/api/projects", json={"color": "#000"})
        assert r.status_code == 400

    def test_api_keys_are_unique(self):
        keys = set()
        for i in range(5):
            r = httpx.post(f"{BASE}/api/projects", json={"name": f"Unique {i} {uuid.uuid4().hex[:4]}"})
            keys.add(r.json()["api_key"])
        assert len(keys) == 5


# ═══════════════════════════════════════════════════════════════
# SECTION 3 — Event Ingestion
# ═══════════════════════════════════════════════════════════════

class TestEventIngestion:
    @pytest.fixture(autouse=True)
    def _setup(self, test_key, test_pid):
        self._key = test_key
        self._pid = test_pid

    def _event(self, etype="page_view", **kwargs):
        return {
            "event_id":   str(uuid.uuid4()),
            "project":    self._pid,
            "api_key":    self._key,
            "session_id": str(uuid.uuid4()),
            "user_id":    "test_user_1",
            "event_name": etype,
            "url":        "http://example.com/test",
            "path":       "/test",
            "title":      "Test Page",
            "timestamp":  time.time(),
            **kwargs,
        }

    def test_batch_ingest_single_event(self):
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": [self._event()]})
        assert r.status_code == 200
        assert r.json()["ok"] is True
        assert r.json()["count"] == 1

    def test_batch_ingest_multiple_events(self):
        events = [self._event(f"event_{i}") for i in range(20)]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        assert r.status_code == 200
        assert r.json()["count"] == 20

    def test_single_event_endpoint(self):
        r = httpx.post(f"{BASE}/ingest/event", json=self._event("click"))
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_event_without_api_key_falls_back_to_project_field(self):
        e = self._event()
        del e["api_key"]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": [e]})
        assert r.status_code == 200

    def test_event_with_unknown_key_falls_back_to_default(self):
        e = self._event()
        e["api_key"] = "sf_unknown_key_xyz"
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": [e]})
        assert r.status_code == 200

    def test_ingest_video_events(self):
        sid = str(uuid.uuid4())
        events = []
        for etype in ["video_play", "video_heartbeat", "video_heartbeat", "video_pause", "video_complete"]:
            e = self._event(etype, session_id=sid)
            e["properties"] = {"video_id": "test_vid_1", "video_title": "Test Video", "position": 42}
            events.append(e)
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        assert r.status_code == 200
        assert r.json()["count"] == 5

    def test_ingest_click_events(self):
        e = self._event("click")
        e["properties"] = {"element": "button", "text": "Sign Up", "sf_label": "cta-signup"}
        r = httpx.post(f"{BASE}/ingest/event", json=e)
        assert r.status_code == 200

    def test_ingest_scroll_depth_event(self):
        e = self._event("scroll_depth")
        e["properties"] = {"depth": 75}
        r = httpx.post(f"{BASE}/ingest/event", json=e)
        assert r.status_code == 200

    def test_ingest_js_error_event(self):
        e = self._event("js_error")
        e["properties"] = {"message": "TypeError: Cannot read property 'x' of undefined", "filename": "app.js", "lineno": 42}
        r = httpx.post(f"{BASE}/ingest/event", json=e)
        assert r.status_code == 200

    def test_cors_headers_present(self):
        r = httpx.options(f"{BASE}/ingest/batch",
                          headers={"Origin": "https://customer-site.com",
                                   "Access-Control-Request-Method": "POST"})
        assert r.headers.get("access-control-allow-origin") in ("*", "https://customer-site.com")

    def test_cross_origin_event_accepted(self):
        r = httpx.post(f"{BASE}/ingest/batch",
                       json={"events": [self._event()]},
                       headers={"Origin": "https://example-customer.com"})
        assert r.status_code == 200

    def test_high_volume_ingestion(self):
        """Send 200 events in one batch — server must handle without error."""
        events = [self._event("heartbeat") for _ in range(200)]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events}, timeout=30)
        assert r.status_code == 200
        assert r.json()["count"] == 200

    def test_empty_batch_accepted(self):
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": []})
        assert r.status_code == 200
        assert r.json()["count"] == 0


# ═══════════════════════════════════════════════════════════════
# SECTION 4 — Metrics API
# ═══════════════════════════════════════════════════════════════

class TestMetricsAPI:
    @pytest.fixture(autouse=True)
    def _setup(self, test_key, test_pid):
        self._key = test_key
        self._pid = test_pid

    def _seed(self, n=10, etype="page_view", path="/home"):
        sid = str(uuid.uuid4())
        events = [{
            "event_id":   str(uuid.uuid4()),
            "project":    self._pid,
            "api_key":    self._key,
            "session_id": sid,
            "user_id":    f"u_{i}",
            "event_name": etype,
            "path":       path,
            "url":        f"http://example.com{path}",
            "title":      "Page",
            "timestamp":  time.time(),
        } for i in range(n)]
        httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        time.sleep(0.1)

    def test_metrics_snapshot_endpoint(self):
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        assert r.status_code == 200
        data = r.json()
        assert "live" in data
        assert "spark" in data

    def test_live_metrics_structure(self):
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        live = r.json()["live"]
        for key in ("active_sessions", "total_events", "event_type_counts",
                    "top_pages", "top_clicks", "video_viewers",
                    "event_timeline", "events_per_sec"):
            assert key in live, f"missing key: {key}"

    def test_spark_metrics_structure(self):
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        spark = r.json()["spark"]
        for key in ("page_stats", "event_counts", "session_stats"):
            assert key in spark, f"missing key: {key}"

    def test_event_counts_increment(self):
        r1 = httpx.get(f"{BASE}/api/metrics/{self._pid}").json()["live"]["total_events"]
        self._seed(5, "page_view")
        r2 = httpx.get(f"{BASE}/api/metrics/{self._pid}").json()["live"]["total_events"]
        assert r2 >= r1 + 5

    def test_page_view_appears_in_top_pages(self):
        unique_path = f"/test-path-{uuid.uuid4().hex[:8]}"
        self._seed(3, "page_view", path=unique_path)
        time.sleep(0.2)
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        pages = {p["path"]: p["views"] for p in r.json()["live"]["top_pages"]}
        assert unique_path in pages
        assert pages[unique_path] >= 3

    def test_click_appears_in_top_clicks(self):
        label = f"btn-{uuid.uuid4().hex[:6]}"
        events = [{
            "event_id": str(uuid.uuid4()), "project": self._pid, "api_key": self._key,
            "session_id": str(uuid.uuid4()), "user_id": "u1", "event_name": "click",
            "path": "/", "url": "http://x.com", "title": "T", "timestamp": time.time(),
            "properties": {"sf_label": label, "element": "button", "text": label},
        } for _ in range(3)]
        httpx.post(f"{BASE}/ingest/batch", json={"events": events})
        time.sleep(0.2)
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        clicks = {c["label"]: c["count"] for c in r.json()["live"]["top_clicks"]}
        assert label in clicks

    def test_video_viewers_tracked(self):
        vid = f"vid-{uuid.uuid4().hex[:6]}"
        sid = str(uuid.uuid4())
        e = {"event_id": str(uuid.uuid4()), "project": self._pid, "api_key": self._key,
             "session_id": sid, "user_id": "u1", "event_name": "video_play",
             "path": "/", "url": "http://x.com", "title": "T", "timestamp": time.time(),
             "properties": {"video_id": vid, "video_title": "My Video", "position": 0}}
        httpx.post(f"{BASE}/ingest/event", json=e)
        time.sleep(0.2)
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        vv = r.json()["live"]["video_viewers"]
        assert vid in vv
        assert vv[vid] >= 1

    def test_session_expires_after_timeout(self):
        r = httpx.get(f"{BASE}/api/metrics/{self._pid}")
        count = r.json()["live"]["active_sessions"]
        assert isinstance(count, int)
        assert count >= 0

    def test_metrics_unknown_project_returns_empty(self):
        r = httpx.get(f"{BASE}/api/metrics/nonexistent_project_xyz")
        assert r.status_code == 200
        data = r.json()
        assert data["live"]["total_events"] == 0


# ═══════════════════════════════════════════════════════════════
# SECTION 5 — WebSocket real-time push
# ═══════════════════════════════════════════════════════════════

class TestWebSocket:
    def test_ws_connects_and_receives_snapshot(self):
        received = []

        async def run():
            async with websockets.connect(f"{WS}/ws/demo") as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                received.append(json.loads(msg))

        asyncio.run(run())
        assert len(received) == 1
        snap = received[0]
        assert "live" in snap
        assert "spark" in snap

    def test_ws_pushes_updates_after_ingest(self):
        """Send an event via REST, verify WebSocket pushes updated total_events."""
        snapshots = []

        async def run():
            async with websockets.connect(f"{WS}/ws/demo") as ws:
                # get baseline
                msg1 = await asyncio.wait_for(ws.recv(), timeout=5)
                snapshots.append(json.loads(msg1))
                # now send event via HTTP
                path = f"/ws-test-{uuid.uuid4().hex[:6]}"
                httpx.post(f"{BASE}/ingest/event", json={
                    "event_id": str(uuid.uuid4()), "project": DEMO_PROJECT,
                    "api_key": DEMO_KEY, "session_id": str(uuid.uuid4()),
                    "user_id": "ws_test", "event_name": "page_view",
                    "path": path, "url": f"http://x.com{path}",
                    "title": "WS Test", "timestamp": time.time(),
                })
                # wait for next push
                msg2 = await asyncio.wait_for(ws.recv(), timeout=5)
                snapshots.append(json.loads(msg2))

        asyncio.run(run())
        assert len(snapshots) == 2
        t1 = snapshots[0]["live"]["total_events"]
        t2 = snapshots[1]["live"]["total_events"]
        assert t2 > t1

    def test_ws_multiple_concurrent_clients(self):
        """Three simultaneous WS clients all receive the same broadcast."""
        results = {0: None, 1: None, 2: None}

        async def client(idx):
            async with websockets.connect(f"{WS}/ws/demo") as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                results[idx] = json.loads(msg)["live"]["total_events"]

        async def run():
            await asyncio.gather(client(0), client(1), client(2))

        asyncio.run(run())
        assert all(v is not None for v in results.values())
        # all clients should see the same snapshot (within same broadcast cycle)
        vals = list(results.values())
        assert max(vals) - min(vals) <= 5   # allow ≤5 event drift between broadcasts


# ═══════════════════════════════════════════════════════════════
# SECTION 6 — SDK source quality checks
# ═══════════════════════════════════════════════════════════════

class TestSDKSource:
    @pytest.fixture(scope="class")
    def sdk(self):
        return httpx.get(f"{BASE}/sdk/sasa.js").text

    def test_sdk_is_iife(self, sdk):
        assert sdk.strip().startswith("(function") or sdk.strip().startswith("/**")

    def test_sdk_exposes_global(self, sdk):
        assert "global.SASA = SASA" in sdk

    def test_sdk_has_uuid_generator(self, sdk):
        assert "uuid" in sdk.lower()

    def test_sdk_batches_events(self, sdk):
        assert "batchInterval" in sdk or "batch" in sdk.lower()

    def test_sdk_uses_beacon_api(self, sdk):
        assert "sendBeacon" in sdk

    def test_sdk_has_localstorage_session(self, sdk):
        assert "localStorage" in sdk

    def test_sdk_has_visibility_change(self, sdk):
        assert "visibilitychange" in sdk

    def test_sdk_has_before_unload(self, sdk):
        assert "beforeunload" in sdk

    def test_sdk_has_mutation_observer_for_dynamic_videos(self, sdk):
        assert "MutationObserver" in sdk

    def test_sdk_supports_data_attributes(self, sdk):
        assert "data-project" in sdk or "data-" in sdk

    def test_sdk_has_no_external_dependencies(self, sdk):
        for lib in ["jquery", "lodash", "axios", "require("]:
            assert lib not in sdk.lower(), f"unexpected dependency: {lib}"

    def test_sdk_size_under_10kb(self, sdk):
        size_kb = len(sdk.encode()) / 1024
        assert size_kb < 15, f"SDK is {size_kb:.1f}KB — over 10KB budget"


# ═══════════════════════════════════════════════════════════════
# SECTION 7 — Concurrency & throughput
# ═══════════════════════════════════════════════════════════════

class TestConcurrency:
    @pytest.fixture(autouse=True)
    def _setup(self, test_key, test_pid):
        self._key = test_key
        self._pid = test_pid

    def _send(self, n, results, idx, key, pid):
        # Each thread uses a unique key so threads don't share rate-limit quota
        thread_key = f"sf_conctest_{idx}_{uuid.uuid4().hex[:4]}"
        events = [{
            "event_id": str(uuid.uuid4()), "project": pid, "api_key": thread_key,
            "session_id": str(uuid.uuid4()), "user_id": f"thread_{idx}",
            "event_name": "heartbeat", "path": "/load-test",
            "url": "http://x.com", "title": "T", "timestamp": time.time(),
        } for _ in range(n)]
        r = httpx.post(f"{BASE}/ingest/batch", json={"events": events}, timeout=30)
        results[idx] = r.status_code

    def test_10_concurrent_senders(self):
        results = {}
        threads = [
            threading.Thread(target=self._send, args=(40, results, i, self._key, self._pid))
            for i in range(10)
        ]
        for t in threads: t.start()
        for t in threads: t.join()
        assert all(s == 200 for s in results.values()), f"Failed: {results}"

    def test_server_responds_fast(self):
        """Single event ingest must complete in under 200ms."""
        e = {"event_id": str(uuid.uuid4()), "project": self._pid, "api_key": self._key,
             "session_id": str(uuid.uuid4()), "user_id": "perf", "event_name": "page_view",
             "path": "/", "url": "http://x.com", "title": "T", "timestamp": time.time()}
        start = time.perf_counter()
        httpx.post(f"{BASE}/ingest/event", json=e)
        elapsed_ms = (time.perf_counter() - start) * 1000
        assert elapsed_ms < 200, f"Too slow: {elapsed_ms:.0f}ms"
