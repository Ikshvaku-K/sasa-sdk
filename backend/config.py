"""
Central config — reads from environment variables with safe defaults.
Load a .env file by running: pip install python-dotenv  and calling load_dotenv()
before importing this module, or export vars in your shell / Docker entrypoint.
"""
import os
from pathlib import Path

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

# ── Server ────────────────────────────────────────────────────────────────────
# Default to loopback so the service isn't accidentally exposed on every
# interface during local dev. Set HOST=0.0.0.0 explicitly for containers. (L-2)
HOST = _env("HOST", "127.0.0.1")
PORT = _env_int("PORT", 8001)

# ── Security ──────────────────────────────────────────────────────────────────
# When set to a non-empty value, the management API (/api/*) requires
# `Authorization: Bearer <ADMIN_SECRET>`. Leave empty ONLY for local dev. (H-1)
ADMIN_SECRET = _env("ADMIN_SECRET", "")   # empty = admin auth disabled in dev

# ── Request size limits (M-3) ─────────────────────────────────────────────────
MAX_BODY_BYTES   = _env_int("MAX_BODY_BYTES",   1_048_576)  # 1 MB max request body
MAX_BATCH_EVENTS = _env_int("MAX_BATCH_EVENTS", 1_000)      # max events per batch

# ── Metric cardinality cap (M-2) ──────────────────────────────────────────────
# Hard ceiling on the number of distinct keys retained per metric dict, to stop
# attacker-controlled values (paths/labels/event names) growing memory forever.
MAX_DISTINCT_KEYS = _env_int("MAX_DISTINCT_KEYS", 5_000)

# ── Demo project ──────────────────────────────────────────────────────────────
# Loaded from env so the key is never committed to source control.
# In production, set DEMO_API_KEY to a real random value.
DEMO_API_KEY = _env("DEMO_API_KEY", "sf_demo_key_dev_only")

# ── Rate limits ───────────────────────────────────────────────────────────────
RATE_LIMIT_INGEST_PER_SEC  = _env_int("RATE_LIMIT_INGEST", 500)   # events/s per api_key
RATE_LIMIT_MGMT_PER_MIN    = _env_int("RATE_LIMIT_MGMT", 60)      # requests/min per IP

# ── Spark paths ───────────────────────────────────────────────────────────────
BASE_DIR            = Path(__file__).parent.parent
SPARK_EVENTS_DIR    = BASE_DIR / _env("SPARK_EVENTS_DIR",    "spark/data/events")
SPARK_OUTPUT_DIR    = BASE_DIR / _env("SPARK_OUTPUT_DIR",    "spark/output")
SPARK_CHECKPOINT_DIR= BASE_DIR / _env("SPARK_CHECKPOINT_DIR","spark/checkpoints")

SPARK_MAX_FILES     = _env_int("SPARK_MAX_FILES_PER_TRIGGER", 20)
SPARK_TRIGGER_SECS  = _env_int("SPARK_TRIGGER_INTERVAL", 10)

# ── Retention ─────────────────────────────────────────────────────────────────
EVENT_FILE_RETENTION_DAYS = _env_int("EVENT_FILE_RETENTION_DAYS", 7)
