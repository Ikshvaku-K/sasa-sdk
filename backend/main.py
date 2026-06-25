"""
SASA Analytics — Backend API

Endpoints
---------
POST /ingest/batch           Receive batched events from the SDK  (rate-limited per api_key)
POST /ingest/event           Single-event shortcut                (rate-limited per api_key)

GET  /api/projects           List projects                        (rate-limited per IP)
POST /api/projects           Create project                       (rate-limited per IP)
GET  /api/projects/{id}      Get project info + API key          (rate-limited per IP)
GET  /api/metrics/{id}       Live + Spark metrics snapshot        (rate-limited per IP)

WS   /ws/{project_id}        Real-time metrics push (1 s cadence)

GET  /sdk/sasa.js            Serve the SDK
GET  /dashboard              Analytics dashboard SPA
GET  /demo                   Demo page showing SDK integration
"""

import asyncio
import csv
import io
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import config
from auth import require_admin
from metrics_store import full_snapshot, record_event
from projects import create_project, get_project, list_projects, validate_key
from rate_limit import check_ingest_limit, check_mgmt_limit, client_ip

log = logging.getLogger("sasa")

BASE     = Path(__file__).parent.parent
SDK_DIR  = BASE / "sdk"
DASH_DIR = BASE / "dashboard"
DEMO_DIR = BASE / "demo"

config.SPARK_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

# ── WebSocket manager ─────────────────────────────────────────────────────────
class WsManager:
    def __init__(self):
        self._sockets: dict[str, list[WebSocket]] = {}

    async def connect(self, project_id: str, ws: WebSocket):
        await ws.accept()
        self._sockets.setdefault(project_id, []).append(ws)
        # Send immediate snapshot so client doesn't wait up to 1 s
        try:
            await ws.send_json(full_snapshot(project_id))
        except Exception as e:
            log.debug("ws initial snapshot failed for %s: %s", project_id, e)

    def disconnect(self, project_id: str, ws: WebSocket):
        lst = self._sockets.get(project_id, [])
        if ws in lst:
            lst.remove(ws)

    async def broadcast(self, project_id: str, data: dict):
        dead = []
        for ws in self._sockets.get(project_id, []):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(project_id, ws)

    def all_project_ids(self):
        return list(self._sockets.keys())

manager = WsManager()

# ── event file buffer ─────────────────────────────────────────────────────────
_event_buffer: list[dict] = []
_last_flush = time.time()

def _write_to_spark(event: dict):
    _event_buffer.append(event)
    global _last_flush
    now = time.time()
    if now - _last_flush >= 2 or len(_event_buffer) >= 100:
        fname = config.SPARK_EVENTS_DIR / f"batch_{int(now*1000)}_{uuid.uuid4().hex[:6]}.json"
        with open(fname, "w") as f:
            for e in _event_buffer:
                f.write(json.dumps(e) + "\n")
        _event_buffer.clear()
        _last_flush = now

# ── retention cleanup ─────────────────────────────────────────────────────────
async def retention_cleanup():
    """Delete event files older than EVENT_FILE_RETENTION_DAYS (if set > 0)."""
    days = config.EVENT_FILE_RETENTION_DAYS
    if days <= 0:
        return
    while True:
        await asyncio.sleep(3600)   # run hourly
        cutoff = time.time() - days * 86400
        deleted = 0
        for f in config.SPARK_EVENTS_DIR.glob("*.json"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except Exception as e:
                log.warning("retention: could not remove %s: %s", f, e)
        if deleted:
            print(f"[retention] deleted {deleted} event files older than {days} days")

# ── broadcaster ───────────────────────────────────────────────────────────────
async def broadcaster():
    while True:
        for pid in manager.all_project_ids():
            try:
                await manager.broadcast(pid, full_snapshot(pid))
            except Exception as e:
                log.debug("broadcast failed for %s: %s", pid, e)
        await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(broadcaster())
    asyncio.create_task(retention_cleanup())
    yield

# ── app ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SASA Analytics API",
    description="Drop-in real-time analytics SDK backend.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── body size limit middleware (M-3) ──────────────────────────────────────────
@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    cl = request.headers.get("content-length")
    if cl:
        try:
            if int(cl) > config.MAX_BODY_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"error": "payload_too_large",
                             "message": f"Max body size is {config.MAX_BODY_BYTES} bytes."},
                )
        except ValueError:
            pass
    return await call_next(request)

# ── rate limit headers middleware ─────────────────────────────────────────────
@app.middleware("http")
async def rate_limit_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-RateLimit-Policy"] = (
        f"ingest={config.RATE_LIMIT_INGEST_PER_SEC}/s; mgmt={config.RATE_LIMIT_MGMT_PER_MIN}/min"
    )
    return response

# ── ingest ────────────────────────────────────────────────────────────────────
async def _parse_json_body(request: Request) -> dict:
    """
    Parse the request body as JSON regardless of Content-Type.

    The browser SDK delivers events via navigator.sendBeacon, which always sets
    Content-Type: text/plain. FastAPI's typed body params only JSON-decode
    application/json, so a typed param would 422 every beacon and silently drop
    all events. We therefore read the raw body and json-decode it ourselves.
    text/plain is also a CORS-safe content type, so beacons work cross-origin
    without a preflight.
    """
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=400, detail={"error": "invalid_json"})
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail={"error": "expected_json_object"})
    return data


@app.post("/ingest/batch")
async def ingest_batch(request: Request):
    payload = await _parse_json_body(request)
    events = payload.get("events", [])
    if not events:
        return {"ok": True, "count": 0}

    # Reject oversized batches before doing any work. (M-3)
    if len(events) > config.MAX_BATCH_EVENTS:
        raise HTTPException(
            status_code=413,
            detail={"error": "batch_too_large",
                    "message": f"Max {config.MAX_BATCH_EVENTS} events per batch."},
        )

    # Rate-limit by client IP (not the forgeable api_key field). (H-2)
    await check_ingest_limit(request, event_count=len(events))

    for event in events:
        if not isinstance(event, dict):
            continue
        key        = event.get("api_key", "")
        project_id = validate_key(key) or event.get("project", "default")
        event["project_id"] = project_id
        event.setdefault("event_id",    str(uuid.uuid4()))
        event.setdefault("ingested_at", time.time())
        record_event(project_id, event)
        _write_to_spark(event)

    return {"ok": True, "count": len(events)}


@app.post("/ingest/event")
async def ingest_event(request: Request):
    event = await _parse_json_body(request)
    api_key    = event.get("api_key", "")
    await check_ingest_limit(request, event_count=1)

    project_id = validate_key(api_key) or event.get("project", "default")
    event["project_id"] = project_id
    event.setdefault("event_id",    str(uuid.uuid4()))
    event.setdefault("ingested_at", time.time())
    record_event(project_id, event)
    _write_to_spark(event)
    return {"ok": True}


# ── project management ────────────────────────────────────────────────────────
# These expose/issue api_keys, so they require the admin token when ADMIN_SECRET
# is configured (no-op in local dev). See auth.require_admin. (H-1)
@app.get("/api/projects")
def api_list_projects(request: Request, _=Depends(check_mgmt_limit), __=Depends(require_admin)):
    return [{"id": p.id, "name": p.name, "api_key": p.api_key, "color": p.color}
            for p in list_projects()]


@app.post("/api/projects")
def api_create_project(body: dict, request: Request, _=Depends(check_mgmt_limit), __=Depends(require_admin)):
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "name required")
    color = body.get("color", "#4f8ef7")
    p = create_project(name, color)
    return {"id": p.id, "name": p.name, "api_key": p.api_key, "color": p.color}


@app.get("/api/projects/{project_id}")
def api_get_project(project_id: str, request: Request, _=Depends(check_mgmt_limit), __=Depends(require_admin)):
    p = get_project(project_id)
    if not p:
        raise HTTPException(404, "project not found")
    return {"id": p.id, "name": p.name, "api_key": p.api_key, "color": p.color}


@app.get("/api/metrics/{project_id}")
def api_metrics(project_id: str, request: Request, _=Depends(check_mgmt_limit)):
    return full_snapshot(project_id)


# ── CSV export ────────────────────────────────────────────────────────────────
# Standard columns emitted for every event; any extra per-event fields are
# preserved as a JSON string in the trailing "properties" column so the file
# stays a fixed-width, analytics-friendly CSV (load straight into pandas/Excel).
EXPORT_COLUMNS = [
    "event_id", "project_id", "session_id", "user_id", "event_name",
    "path", "url", "title", "referrer", "screen_w", "screen_h",
    "user_agent", "timestamp", "ingested_at", "properties",
]


def _iter_project_events(project_id: str):
    """Yield every stored event for a project (flushed files + in-memory buffer)."""
    for f in sorted(config.SPARK_EVENTS_DIR.glob("*.json")):
        try:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except (json.JSONDecodeError, ValueError):
                        continue
                    if ev.get("project_id") == project_id:
                        yield ev
        except FileNotFoundError:
            continue
    # include events still buffered in memory (not yet flushed to disk)
    for ev in list(_event_buffer):
        if ev.get("project_id") == project_id:
            yield ev


@app.get("/api/export/{project_id}.csv")
def export_csv(
    project_id: str,
    request: Request,
    event_name: Optional[str] = None,
    since: Optional[float] = None,
    until: Optional[float] = None,
    _=Depends(check_mgmt_limit),
):
    """
    Stream all events for a project as a CSV download for offline analytics.

    Optional query filters:
      ?event_name=click        only that event type
      ?since=<unix_ts>         events at/after this timestamp
      ?until=<unix_ts>         events at/before this timestamp
    """
    def rows():
        buf = io.StringIO()
        writer = csv.writer(buf)

        def flush():
            data = buf.getvalue()
            buf.seek(0)
            buf.truncate(0)
            return data

        writer.writerow(EXPORT_COLUMNS)
        yield flush()

        for ev in _iter_project_events(project_id):
            if event_name and ev.get("event_name") != event_name:
                continue
            ts = ev.get("timestamp")
            if since is not None and (ts is None or ts < since):
                continue
            if until is not None and (ts is None or ts > until):
                continue
            props = ev.get("properties")
            row = [ev.get(c, "") for c in EXPORT_COLUMNS[:-1]]
            row.append(json.dumps(props, ensure_ascii=False) if props else "")
            writer.writerow(row)
            yield flush()

    filename = f"sasa_{project_id}_{int(time.time())}.csv"
    return StreamingResponse(
        rows(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws/{project_id}")
async def ws_metrics(websocket: WebSocket, project_id: str):
    await manager.connect(project_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(project_id, websocket)


# ── static / frontend ─────────────────────────────────────────────────────────
@app.get("/sdk/sasa.js")
def serve_sdk():
    return FileResponse(
        str(SDK_DIR / "sasa.js"),
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=300"},
    )

app.mount("/dashboard/static", StaticFiles(directory=str(DASH_DIR)), name="dash_static")

@app.get("/dashboard")
@app.get("/dashboard/{rest:path}")
def serve_dashboard(rest: str = ""):
    return FileResponse(str(DASH_DIR / "index.html"))

app.mount("/demo/static", StaticFiles(directory=str(DEMO_DIR)), name="demo_static")

@app.get("/demo")
def serve_demo():
    return FileResponse(str(DEMO_DIR / "index.html"))

@app.get("/")
def root():
    return FileResponse(str(DASH_DIR / "index.html"))
