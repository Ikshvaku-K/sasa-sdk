# SASA — Spark Analytics & Streaming Architecture / API

A **drop-in, self-hosted product-analytics platform**. Add **one `<script>` tag**
to any website and SASA automatically captures page views, clicks, scroll depth,
video engagement, sessions, and JavaScript errors — then streams them through an
**Apache Spark** pipeline into a **live real-time dashboard**.

Think of it as a self-hosted, developer-friendly alternative to Segment /
Mixpanel / Plausible: your data stays on your own infrastructure, there are no
per-event fees, and the whole stack runs from a single command.

```
┌────────────┐   events    ┌──────────────┐   NDJSON    ┌───────────────┐
│  Your site │ ──────────▶ │ FastAPI      │ ──────────▶ │ Apache Spark  │
│ + sasa.js  │  HTTP/Beacon│ ingest API   │   files     │ Structured    │
└────────────┘             │              │             │ Streaming     │
      ▲                    │  in-memory   │ ◀────────── │ aggregations  │
      │  one script tag    │  live metrics│   results   └───────────────┘
      │                    └──────┬───────┘
      │                           │ WebSocket (1 Hz push)
      │                    ┌──────▼───────┐
      └─ live dashboard ◀──│  Dashboard   │  (Chart.js SPA)
                           └──────────────┘
```

---

## Table of Contents

1. [Why SASA](#1-why-sasa)
2. [Architecture & How It Works](#2-architecture--how-it-works)
3. [Project Layout](#3-project-layout)
4. [Quick Start](#4-quick-start)
5. [Installing the SDK on Your Site](#5-installing-the-sdk-on-your-site)
6. [What Gets Tracked Automatically](#6-what-gets-tracked-automatically)
7. [Manual Tracking API](#7-manual-tracking-api)
8. [Multiple Projects](#8-multiple-projects)
9. [HTTP API Reference](#9-http-api-reference)
10. [Configuration](#10-configuration)
11. [Security Model](#11-security-model)
12. [Testing](#12-testing)
13. [Deploying to Production](#13-deploying-to-production)
14. [Troubleshooting](#14-troubleshooting)
15. [Glossary](#15-glossary)

---

## 1. Why SASA

| | |
|---|---|
| **One script tag** | No build step, no npm install on the client. Paste and go. |
| **Self-hosted** | Your analytics data never leaves your infrastructure. |
| **Real-time** | Events appear on the dashboard within ~1 second (live) and are aggregated by Spark within ~10 seconds. |
| **No client dependencies** | The browser SDK is a ~12 KB dependency-free IIFE — no jQuery, no frameworks. |
| **Multi-project** | One server hosts many sites, each with its own API key and isolated dashboard. |
| **Batteries included** | Auto-captures page views, clicks, scroll, video, sessions, and JS errors out of the box. |

---

## 2. Architecture & How It Works

SASA has four cooperating parts:

### a) The Browser SDK — `sdk/sasa.js`
A self-contained **IIFE** (Immediately-Invoked Function Expression — a script
that runs the moment it loads, exposing nothing but a single `SASA` global). On
load it:
- reads its configuration from `data-*` attributes on its own `<script>` tag,
- assigns each visitor a **session id** and **user id** (stored in
  `localStorage`, with a 30-minute idle timeout that starts a new session),
- listens for user activity and **batches** events, flushing them every couple
  of seconds (or immediately on page-unload via the **Beacon API**, which
  reliably delivers a final payload even as the tab closes).

### b) The Ingest API — `backend/main.py` (FastAPI)
An async web server that receives event batches at `POST /ingest/batch`. For
each event it:
- updates an **in-memory live-metrics store** (for the instant dashboard view),
- appends the event to **NDJSON** files on disk (newline-delimited JSON — one
  event per line), which act as a simple, durable message queue for Spark.

### c) The Spark Job — `spark/streaming_job.py`
An **Apache Spark Structured Streaming** job watches the event directory and
continuously computes windowed aggregations (events per type, top pages, session
stats) using event-time **tumbling windows**. Results are written back out as
JSON for the dashboard to read. This is the component that scales the analytics
to large volumes.

### d) The Dashboard — `dashboard/`
A single-page app (vanilla JS + **Chart.js**) that opens a **WebSocket** to the
server and receives a fresh metrics snapshot once per second, rendering live
counters, charts, top-pages/clicks tables, an event feed, and per-video
engagement.

> **Data flow in one sentence:** the SDK batches events → the API records them in
> memory (for the live view) and to disk (for Spark) → Spark aggregates them →
> the dashboard streams both the live and Spark views over a WebSocket.

---

## 3. Project Layout

```
sasa-sdk/
├── sdk/
│   └── sasa.js              # The drop-in browser SDK (IIFE, no deps)
├── backend/
│   ├── main.py             # FastAPI app: ingest, metrics, WebSocket, static
│   ├── config.py           # Env-var driven configuration
│   ├── auth.py             # Optional admin-token gate for the management API
│   ├── rate_limit.py       # Sliding-window rate limiter (by client IP)
│   ├── projects.py         # In-memory project + API-key registry
│   ├── metrics_store.py    # Live in-memory metrics + Spark output reader
│   └── requirements.txt
├── spark/
│   └── streaming_job.py    # Spark Structured Streaming aggregations
├── dashboard/
│   ├── index.html          # Dashboard SPA
│   ├── css/style.css
│   └── js/dashboard.js     # WebSocket client + Chart.js rendering
├── demo/
│   └── index.html          # Example "customer site" instrumented with SASA
├── tests/
│   ├── test_suite.py       # 71 functional/integration tests
│   ├── test_rate_limits.py # Rate-limit unit + live tests
│   └── conftest.py         # Pytest session fixtures
├── run.sh                  # Start Spark + the API server together
├── .env.example            # Documented configuration template
└── README.md
```

---

## 4. Quick Start

### Prerequisites
- **Python 3.10+**
- (Optional) **Java 8/11/17** + `pyspark` if you want the Spark aggregation
  layer. The live dashboard works without Spark; only the "Spark" panels need it.

### Run it

```bash
# 1. Clone
git clone https://github.com/Ikshvaku-K/sasa-sdk.git
cd sasa-sdk

# 2. Create an isolated environment and install deps
python3 -m venv venv && source venv/bin/activate
pip install -r backend/requirements.txt
#   (lighter install without Spark:)
#   pip install fastapi "uvicorn[standard]" aiofiles python-multipart

# 3. (Optional) configure
cp .env.example .env        # edit values as needed

# 4. Start the server
cd backend && uvicorn main:app --host 127.0.0.1 --port 8001
#   …or run Spark + server together from the repo root:  ./run.sh
```

Then open:

| URL | What it is |
|---|---|
| http://localhost:8001/dashboard | The live analytics dashboard |
| http://localhost:8001/demo | A sample instrumented site — interact with it and watch the dashboard update |
| http://localhost:8001/docs | Auto-generated OpenAPI/Swagger API docs |
| http://localhost:8001/sdk/sasa.js | The SDK file your sites will load |

Open `/demo` and `/dashboard` side by side, click around the demo, play a video —
the dashboard updates in real time.

---

## 5. Installing the SDK on Your Site

Add **one tag** to your site's `<head>`. That's the entire integration:

```html
<script
  src="https://your-sasa-server/sdk/sasa.js"
  data-project="my-app"
  data-api-key="YOUR_PROJECT_API_KEY"
  data-track-videos="true"
  data-track-clicks="true"
  data-track-scroll="true">
</script>
```

| Attribute | Default | Meaning |
|---|---|---|
| `data-project` | `default` | Project id this site reports to |
| `data-api-key` | — | The project's API key (from the dashboard) |
| `data-api-base` | `<host>:8000` | Override the ingest server base URL |
| `data-track-videos` | `true` | Auto-track `<video>` events |
| `data-track-clicks` | `true` | Auto-track clicks on links/buttons |
| `data-track-scroll` | `true` | Auto-track scroll-depth milestones |
| `data-batch-interval` | `2000` | Flush interval in ms |
| `data-debug` | `false` | Log SDK activity to the console |

> **Tip:** the bundled `/demo` page fetches its key from the API at runtime
> rather than hard-coding it — see `demo/index.html` for that pattern.

---

## 6. What Gets Tracked Automatically

With the three `data-track-*` flags on, **no extra code** is needed to capture:

- **Page views** — including SPA navigations (the SDK wraps `history.pushState`
  / `replaceState` and listens for `popstate`, so React/Vue/Next route changes
  are tracked).
- **Clicks** — on links, buttons, and anything with `role="button"` or
  `data-sf-track`; captures text, href, id, and an optional `data-sf-label`.
- **Scroll depth** — fires milestones at 25 / 50 / 75 / 90 / 100 %.
- **Video** — play, pause, seek, buffer start/end, complete, plus a heartbeat
  every 5 s while playing; works for `<video>` elements added dynamically later
  (via a `MutationObserver`).
- **Sessions** — a session id with a 30-minute idle timeout.
- **Time on page & exits** — via `visibilitychange` and `beforeunload`.
- **JavaScript errors** — global `error` events (message, file, line, column).

---

## 7. Manual Tracking API

For custom business events, the SDK exposes a tiny global, `SASA`:

```js
// Track a custom event with arbitrary properties
SASA.track('purchase', { plan: 'pro', amount: 49 });

// Attach an identity + traits to all future events
SASA.identify('user_123', { email: 'jane@co.com', plan: 'pro' });

// Manually record a page view (e.g. a virtual page name)
SASA.page('Checkout');
```

---

## 8. Multiple Projects

One server can host analytics for many sites. Each **project** has its own id,
colour, and **API key**.

- Create projects from the dashboard UI ("New Project"), or via
  `POST /api/projects`.
- Each project gets an isolated metrics store and its own dashboard view.
- Events are routed to a project by their `api_key` (falling back to the
  `project` field, then to `"default"`).

---

## 9. HTTP API Reference

| Method & Path | Auth | Description |
|---|---|---|
| `POST /ingest/batch` | public | Ingest an array of events `{ "events": [...] }` |
| `POST /ingest/event` | public | Ingest a single event |
| `GET  /api/projects` | admin* | List projects (includes API keys) |
| `POST /api/projects` | admin* | Create a project `{ "name", "color" }` |
| `GET  /api/projects/{id}` | admin* | Get one project (includes API key) |
| `GET  /api/metrics/{id}` | public | Live + Spark metrics snapshot for a project |
| `WS   /ws/{project_id}` | public | Real-time metrics push (~1 Hz) |
| `GET  /sdk/sasa.js` | public | Serve the SDK |
| `GET  /dashboard`, `/demo` | public | Front-end pages |

\* *“admin” routes require `Authorization: Bearer <ADMIN_SECRET>` **only when
`ADMIN_SECRET` is configured**. In local dev (no secret set) they are open. See
[Security Model](#11-security-model).*

**Rate limits** (defaults): ingest = **500 events/sec per client IP**;
management = **60 requests/min per IP**. Exceeding either returns `429` with a
`Retry-After` header. Every response carries an `X-RateLimit-Policy` header.

---

## 10. Configuration

All configuration is via environment variables (or a `.env` file). See
[`.env.example`](.env.example) for the full annotated list. Highlights:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` / `PORT` | `127.0.0.1` / `8001` | Bind address. Use `0.0.0.0` only in a container. |
| `ADMIN_SECRET` | *(empty)* | **Set in production** to lock down the management API. |
| `DEMO_API_KEY` | `sf_demo_key_dev_only` | Key for the built-in demo project. |
| `RATE_LIMIT_INGEST` | `500` | Ingest events/sec per IP. |
| `RATE_LIMIT_MGMT` | `60` | Management requests/min per IP. |
| `MAX_BODY_BYTES` | `1048576` | Max request body (1 MB). |
| `MAX_BATCH_EVENTS` | `1000` | Max events per batch. |
| `MAX_DISTINCT_KEYS` | `5000` | Cap on distinct keys per metric dict (memory guard). |
| `EVENT_FILE_RETENTION_DAYS` | `7` | Auto-delete event files older than N days. |
| `SPARK_*` | see file | Spark input/output/checkpoint dirs and trigger timing. |

Secrets are **never** hard-coded — the repo ships `.env.example` and `.gitignore`
excludes `.env` and all runtime data.

---

## 11. Security Model

SASA underwent a security audit; the hardening below is built in. **Two
deliberate trust boundaries:**

- **Ingest is public by design.** Any website must be able to POST events (that's
  the whole point of an analytics SDK), so `/ingest/*` is open and CORS-enabled.
  It is protected by **per-IP rate limiting** and **request-size limits**.
- **Management is privileged.** `/api/projects*` can expose API keys, so it is
  gated behind an **admin bearer token** whenever `ADMIN_SECRET` is set.

Built-in protections:

| Protection | How |
|---|---|
| **Admin auth** | `Authorization: Bearer <ADMIN_SECRET>` on `/api/*` (constant-time compare). |
| **Ingest rate limiting** | Sliding window keyed by **client IP** — rotating the `api_key` cannot mint fresh budget. |
| **Request/batch size caps** | `413` for bodies > `MAX_BODY_BYTES` or batches > `MAX_BATCH_EVENTS`. |
| **Bounded memory** | Metric dictionaries and limiter buckets are capped/evicted so high-cardinality input can't exhaust RAM. |
| **XSS-safe dashboard** | All untrusted values (project names, paths, labels, colours) are HTML-escaped before rendering. |
| **No secrets in source** | All keys via env vars; `.env` git-ignored. |
| **Loopback by default** | Binds `127.0.0.1` unless you opt into `0.0.0.0`. |

> ⚠️ **Production checklist:** set a strong `ADMIN_SECRET`, set a random
> `DEMO_API_KEY` (or remove the demo project), run behind a reverse proxy that
> sets a correct `X-Forwarded-For`, and serve over HTTPS.

A full write-up of the audit findings and fixes lives in the `testing/` folder
(`SECURITY_AND_BUGS_AUDIT.md`).

---

## 12. Testing

```bash
# Start the server first (tests run against a live instance on :8001)
cd backend && uvicorn main:app --port 8001 &

# Run the full suite from the repo root
pytest tests/test_suite.py tests/test_rate_limits.py -v
```

The suite covers asset delivery, the project API, event ingestion, the metrics
API, WebSocket streaming, SDK source invariants, concurrency, and rate limiting
(**72 tests**). There is also an optional adversarial probe suite and a
test-data generator under `testing/` for security regression checking and for
populating the dashboard with realistic traffic.

---

## 13. Deploying to Production

1. **Environment:** set `ADMIN_SECRET`, a random `DEMO_API_KEY`, and
   `HOST=0.0.0.0` (inside a container).
2. **Process:** run under a production ASGI setup, e.g.
   `uvicorn main:app --host 0.0.0.0 --port 8001 --workers 4` (note: the
   in-memory metrics store is per-process; for multi-worker or multi-host
   deployments, move projects/metrics to a shared store such as Redis/Postgres).
3. **Reverse proxy:** terminate TLS and forward `X-Forwarded-For` so per-IP rate
   limiting sees real client IPs.
4. **Spark:** run `spark/streaming_job.py` as a long-lived job pointed at the
   same `SPARK_*` directories.
5. **Retention:** tune `EVENT_FILE_RETENTION_DAYS` to control disk usage.

---

## 14. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Dashboard shows no data | Confirm the SDK's `data-api-base` points at your server and the project/api-key match. Check the browser console with `data-debug="true"`. |
| `401` on `/api/projects` | `ADMIN_SECRET` is set — send `Authorization: Bearer <secret>`. |
| `429` responses | You're over the ingest/management rate limit; back off or raise `RATE_LIMIT_*`. |
| `413` responses | Batch or body too large; reduce size or raise `MAX_BATCH_EVENTS` / `MAX_BODY_BYTES`. |
| Spark panels empty | Spark job not running, or `pyspark`/Java not installed. The live panels still work without Spark. |
| All clients share one rate-limit bucket | You're behind a proxy not setting `X-Forwarded-For`; configure it. |

---

## 15. Glossary

| Term | Meaning |
|---|---|
| **IIFE** | Immediately-Invoked Function Expression — a JS pattern that runs on load and keeps its internals private. |
| **Beacon API** | A browser API (`navigator.sendBeacon`) that reliably sends a final payload as a page unloads. |
| **NDJSON** | Newline-Delimited JSON — one JSON object per line; used here as a simple file-based event queue. |
| **Structured Streaming** | Apache Spark's engine for continuous, incremental computation over streaming data. |
| **Tumbling window** | A fixed, non-overlapping time bucket used to aggregate events (e.g. counts per 10 s). |
| **Sliding-window rate limit** | Limits requests over a rolling time window (vs. resetting on fixed boundaries). |
| **WebSocket** | A persistent two-way browser↔server connection; used to push live metrics. |
| **ASGI** | Asynchronous Server Gateway Interface — the async Python web standard FastAPI/uvicorn implement. |

---

*Built with FastAPI, Apache Spark Structured Streaming, and Chart.js.*

---

<p align="center">
  Built by <strong>Ikshvaku</strong> and <strong>Opus</strong>
</p>
