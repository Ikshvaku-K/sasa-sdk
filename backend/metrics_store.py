"""
Per-project in-memory metrics + Spark output reader.
"""
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import config
SPARK_OUT = config.SPARK_OUTPUT_DIR
log = logging.getLogger("sasa")


def _bump(counter: dict, key: str):
    """
    Increment counter[key], but cap the number of DISTINCT keys so that
    attacker-controlled values (paths/labels/event names) can't grow the dict
    without bound. (Fixes audit M-2.) New keys past the cap are folded into a
    single "__other__" bucket.
    """
    if key in counter or len(counter) < config.MAX_DISTINCT_KEYS:
        counter[key] += 1
    else:
        counter["__other__"] += 1


@dataclass
class ProjectMetrics:
    project_id: str

    # live counters
    total_events: int = 0
    active_sessions: dict = field(default_factory=dict)   # session_id → {last_seen, path, user_id}
    event_type_counts: dict = field(default_factory=lambda: defaultdict(int))
    page_counts: dict = field(default_factory=lambda: defaultdict(int))
    video_viewers: dict = field(default_factory=lambda: defaultdict(set))
    event_timeline: deque = field(default_factory=lambda: deque(maxlen=120))
    _bucket_ts: float = field(default_factory=time.time)
    _bucket_count: int = 0
    top_clicks: dict = field(default_factory=lambda: defaultdict(int))

    def record(self, event: dict):
        now      = time.time()
        ename    = event.get("event_name", "unknown")
        sid      = event.get("session_id", "")
        uid      = event.get("user_id", "")
        path     = event.get("path", "/")
        props    = event.get("properties", {})
        vid      = props.get("video_id") or event.get("video_id", "")

        self.total_events += 1
        _bump(self.event_type_counts, ename)

        # bucket timeline
        if now - self._bucket_ts >= 1.0:
            self.event_timeline.append({"ts": int(self._bucket_ts), "count": self._bucket_count})
            self._bucket_count = 0
            self._bucket_ts    = now
        self._bucket_count += 1

        # session tracking
        self.active_sessions[sid] = {"last_seen": now, "path": path, "user_id": uid}

        # page counts
        if ename == "page_view":
            _bump(self.page_counts, path[:200])

        # click labels
        if ename == "click":
            label = props.get("sf_label") or props.get("text") or props.get("element") or "unknown"
            _bump(self.top_clicks, label[:60])

        # video
        if vid and ename.startswith("video_"):
            self.video_viewers[vid].add(sid)

        # expire sessions > 5 min idle
        stale = [s for s, v in self.active_sessions.items() if now - v["last_seen"] > 300]
        for s in stale:
            del self.active_sessions[s]

    def snapshot(self) -> dict:
        top_pages   = sorted(self.page_counts.items(), key=lambda x: -x[1])[:10]
        top_clicks  = sorted(self.top_clicks.items(),  key=lambda x: -x[1])[:10]
        video_vw    = {v: len(s) for v, s in self.video_viewers.items()}
        return {
            "project_id":        self.project_id,
            "active_sessions":   len(self.active_sessions),
            "total_events":      self.total_events,
            "event_type_counts": dict(self.event_type_counts),
            "top_pages":         [{"path": p, "views": c} for p, c in top_pages],
            "top_clicks":        [{"label": l, "count": c} for l, c in top_clicks],
            "video_viewers":     video_vw,
            "event_timeline":    list(self.event_timeline),
            "events_per_sec":    self._bucket_count,
            "ts":                time.time(),
        }


# ── global registry ──────────────────────────────────────────────────────────
_stores: dict[str, ProjectMetrics] = {}


def get_store(project_id: str) -> ProjectMetrics:
    if project_id not in _stores:
        _stores[project_id] = ProjectMetrics(project_id=project_id)
    return _stores[project_id]


def record_event(project_id: str, event: dict):
    get_store(project_id).record(event)


# ── Spark output reader ───────────────────────────────────────────────────────
_seen_files: set[str] = set()
_spark_cache: dict[str, list] = defaultdict(list)


def _read_spark(subdir: str) -> list[dict]:
    rows = []
    path = SPARK_OUT / subdir
    if not path.exists():
        return rows
    for f in sorted(path.glob("*.json")):
        key = str(f)
        if key in _seen_files:
            continue
        try:
            for line in f.read_text().splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            _seen_files.add(key)
        except Exception as e:
            log.warning("could not read spark output %s: %s", f, e)
    return rows


def refresh_spark():
    for subdir in ("page_stats", "event_counts", "session_stats"):
        new = _read_spark(subdir)
        if new:
            _spark_cache[subdir].extend(new)
            _spark_cache[subdir] = _spark_cache[subdir][-200:]


def full_snapshot(project_id: str) -> dict:
    refresh_spark()
    live = get_store(project_id).snapshot()

    # filter spark data by project
    def proj_filter(rows):
        return [r for r in rows if not r.get("project_id") or r["project_id"] == project_id]

    return {
        "live":  live,
        "spark": {
            "page_stats":    proj_filter(_spark_cache["page_stats"])[-40:],
            "event_counts":  proj_filter(_spark_cache["event_counts"])[-40:],
            "session_stats": proj_filter(_spark_cache["session_stats"])[-40:],
        },
    }
