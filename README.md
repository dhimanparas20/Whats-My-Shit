# WMS — WhatsApp Messaging Service

A self-hosted, Twilio-like **WhatsApp-first messaging platform**. FastAPI-style
APIs on the front, background workers in the back, and real WhatsApp delivery
through a linked WhatsApp session — no per-message pricing.

> **Status: early development (pre-Phase 1).** The core send pipeline,
> task tracking, and Docker infrastructure are working end-to-end. The HTTP
> API layer, OTP flows, and webhooks are next. See
> [Roadmap](#roadmap--progress) for exactly where things stand.
>
> There is currently **an `AGENTS.md` in this repo — LLM agents working on
> this codebase must read and follow it first.**

---

## Why WMS?

| | WMS | Twilio / Mailgun / Brevo |
|---|---|---|
| Delivery channel | Your own WhatsApp number via a linked session | WhatsApp Business API (rent per conversation) |
| Cost | Server + phone number. That's it. | Per-message/conversation pricing |
| Ops | Self-hosted, one `docker compose up` | Managed SaaS |
| Scope | Outbound + OTP + scheduling on WhatsApp | Multi-channel, heavyweight |

**Trade-off:** WMS rides on the unofficial WhatsApp Web protocol (via
[WAHA](https://waha.devlike.pro/)). That means zero per-message cost but
**number-ban risk if you spam**. Build rate limits/consent flows before
blast-messaging anyone. Designed for OTP and transactional traffic, not
marketing blasts.

---

## Architecture

```
                ┌──────────────┐
   clients ───▶ │   FastAPI    │  Phase 1 — HTTP API (planned)
                └──────┬───────┘
                       │ enqueue
                       ▼
                ┌──────────────┐     ┌─────────────┐
                │     Huey     │───▶ │    Redis    │  queues · results · task tracker
                │  (workers)   │     │  (ephemeral)│  future: OTP · idempotency · rate limits
                └──────┬───────┘     └─────────────┘
                       │ send
                       ▼
                ┌──────────────┐
                │     WAHA     │  WhatsApp HTTP API (devlikeapro)
                │  (WEBJS)     │  linked WhatsApp session
                └──────────────┘
```

- **FastAPI** (`api/`) — the public HTTP surface (Phase 1, in progress)
- **Huey** (`tasks/`) — lightweight Redis-backed task queue executing sends
  with automatic retries, replacing a full Celery stack
- **Redis** — queues plus all ephemeral state (task tracker today; OTP,
  idempotency keys, rate limits next)
- **WAHA** — self-hosted WhatsApp HTTP API; sends/receives over your linked
  WhatsApp Web session

---

## Current features

- ✅ **WhatsApp send pipeline** — `send_whatsapp_message` Huey task wrapping
  WAHA's `/sendText`, with configurable retries (`2` attempts / `60s` delay)
- ✅ **WhatsApp OTP flow (Phase 2 core)** — `send_whatsapp_otp`: cryptographically
  random code, HMAC-SHA256-hashed with an `OTP_PEPPER` secret (plaintext never
  stored or logged), one active code per phone (`OTP:<country_code><phone>`),
  per-key TTL, and `verify_otp()` with attempt counting, expiry handling and
  lockout after `OTP_MAX_VERIFY_ATTEMPTS` wrong tries
- ✅ **User service (Phase 5 groundwork)** — `UserService`: registration with
  duplicate-email protection, bcrypt-hashed passwords, `wms_`-prefixed API
  keys **stored only as sha256 hashes** (plaintext shown once at
  create/rotation) with O(1) hashed-index lookups (ready for `X-Api-Key`
  auth), key rotation, password reset (forgot-password primitive),
  soft-disable (`is_active`), and clean deletes
- ✅ **Task tracker** — every task records its terminal state
  (`TaskStatus` JSON) to Redis under `HUEY_TASK_TRACKER:<job_id>`; survives
  result-TTL expiry and is readable without touching Huey's pickle store
- ✅ **Job status lookup** — `get_job_status(job_id)` resolves in priority
  order: tracker → Huey result store → pending queue → `not_found`
- ✅ **Structured logging** — JSON or pretty, redaction filter, trace ids,
  rotation (`modules/logger.py`)
- ✅ **Typed config** — one `BaseSettings` class per domain with env prefixes,
  single cached `get_settings()`, absolute `.env` resolution (CWD-independent)
- ✅ **Client singletons** — `configs/clients.py`: shared sync Redis, cached
  `RedisStringUtil` / `RedisHashUtil` wrappers, WAHA client
- ✅ **Docker Compose stack** — WAHA (with `/health` API-key healthcheck) +
  Huey consumer (with Redis-ping healthcheck)
- ✅ **In-container IPython dev loop** — `docker exec wms-huey-1 bash` →
  `ipython`, with a redis client `r` and autoreload injected; history
  persists across rebuilds via the `./.ipython` volume
- ✅ **Production WAHA client** (`modules/wa_util.py`) — text/media/voice senders,
  phone normalization, session management, webhook HMAC helpers (ready for Phase 3)

## Not here yet

- ❌ HTTP API (`POST /messages`, status endpoint) — Phase 1, next up
- ❌ OTP cooldown / resend rate limiting + HTTP verify endpoint — remaining Phase 2 work
- ❌ Inbound webhooks + delivery-tracking — Phase 3
- ❌ Scheduled sends — Phase 4
- ❌ API keys / user auth / quotas — Phase 5

---

## Quickstart

Prereqs: Docker + Docker Compose, a phone with WhatsApp you can scan a QR with.

```bash
# 1. Configure
cp .env.sample .env
#    → set WAHA_API_KEY to anything strong; adjust REDIS_URL if not local.

# 2. Bring the stack up
docker compose up -d --build

# 3. Watch until healthy
docker compose ps          # waha and wms-huey-1 both "healthy"
docker compose logs -f huey

# 4. Link your WhatsApp number: open the dashboard and scan the QR
#    http://localhost:3000  (login with WAHA_DASHBOARD_* creds from .env)

# 5. Peek inside the running worker (dev)
docker exec wms-huey-1 bash
ipython                    # `r` = connected redis client, autoreload on
```

### Sending your first message (from a REPL for now)

```python
from tasks import send_whatsapp_message, get_job_status

result = send_whatsapp_message("91XXXXXXXXXX", "Hello from WMS!")
job_id = result.task.id

get_job_status(job_id)
# {'status': 'success', 'data': {'job_id': ..., 'success': True,
#   'result': {'from': ..., 'to': ..., 'id': ...}, ...}}
```

---

## Environment variables

`.env` is the single source of truth (gitignored; see `.env.sample`).
Resolution order: **real env vars → `.env` → code defaults**. Keys are matched
case-insensitively against `configs/config.py` field names + prefixes.

### WMS app keys

| Key | Default | Used by | Purpose |
|---|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Huey, clients | Redis connection (queue + state) |
| `WAHA_API_URL` | `http://localhost:3000` | WAHA client | How **this app** reaches WAHA (`http://waha:3000` in Docker) |
| `WAHA_API_KEY` | *(none)* | WAHA client | `X-Api-Key` auth for WAHA requests |
| `WAHA_SESSION` | `default` | WAHA client | Which session to send through |
| `WAHA_DEFAULT_COUNTRY_CODE` | `91` | WAHA client | Prefix for bare phone numbers |
| `API_DEBUG` / `API_PORT` / `API_HOST` | `false` / `8000` / `0.0.0.0` | FastAPI (Phase 1) | Reserved |
| `OTP_PEPPER` | *(empty)* | OTP tasks | HMAC secret for hashing codes — **required before real OTP traffic** |
| `OTP_TTL` | `1800` | OTP tasks | Seconds a code stays valid |
| `OTP_LENGTH` | `6` | OTP tasks | Code length in digits (4–12) |
| `OTP_MAX_VERIFY_ATTEMPTS` | `5` | `verify_otp` | Wrong-code tries before lockout |
| `HUEY_TASK_TRACKER` | `HUEY_TASK_TRACKER` | tasks | Redis key prefix for task-status records |
| `OTP` | `OTP` | OTP service | Redis key prefix for OTP records (`OTP:<cc><phone>`) |
| `USER` | `USER` | user service | Redis key prefix for user records + indexes |
| `LOG_LEVEL` | `DEBUG` | logger | stdlib levels |
| `LOG_FORMAT` | `json` | logger | `json` or pretty |

### WAHA container keys (read by WAHA, not by WMS code)

`WAHA_BASE_URL`, `WHATSAPP_DEFAULT_ENGINE` (`WEBJS` default), `WAHA_NAMESPACE`,
`WHATSAPP_START_SESSION`, `WHATSAPP_RESTART_ALL_SESSIONS`, `WAHA_MEDIA_STORAGE`,
`WHATSAPP_FILES_FOLDER`, `WAHA_LOG_*`, `WAHA_PRINT_QR`, `WAHA_DASHBOARD_*`.

> ⚠️ **Do not confuse `WAHA_API_URL` with `WAHA_BASE_URL`** — the first is
> app→waha, the second is WAHA's own public URL for webhooks/files.

---

## Project layout

```
├── configs/        BaseSettings classes + client singletons (get_* + lru_cache)
│   ├── config.py     .env → typed Settings (one class per domain, env_prefix)
│   └── clients.py    get_redis / get_redis_str / get_redis_hash / get_waha
├── models/         Pure Pydantic models (TaskStatus, OTPData, User, WAHASendResult) + enums
├── modules/        Generic utils: logger, IST time, Redis toolkit, WAHA client
├── services/       Business logic: OTPService (generate/save/verify OTP lifecycle)
├── tasks/          Huey instance + THIN task wrappers + get_job_status
├── api/            FastAPI routers (Phase 1 — currently empty)
├── compose.yml     waha + huey services with healthchecks
├── Dockerfile      Python 3.14-slim + uv sync (code is volume-mounted, dev)
├── .dockerignore   Keeps build context at ~750KB (excludes sessions/, .venv, .env)
├── AGENTS.md       ⚠️ READ FIRST if you're an LLM agent — all conventions live there
└── .env.sample     Template for .env
```

`AGENTS.md` documents the rules in depth: where new configs/clients/models/
tasks go, the task-tracker pattern, Huey conventions (`context=True`,
`task.id`, `overwrite=True`), Docker gotchas, and a learned-pitfalls list.

---

## Development loop

Code is **volume-mounted** — Python changes need only a restart, not a rebuild:

```bash
docker compose restart huey          # after editing tasks/ or modules/
docker compose up -d --build huey    # only after Dockerfile / dependency changes
```

Handy checks:

```bash
docker compose logs -f huey                       # worker logs (JSON)
docker exec -it wms-huey-1 bash                   # then: ipython  ('r' = redis client)

# host-side sanity (see AGENTS.md §12 for the full list)
.venv/bin/python -c "from configs import get_settings; s=get_settings(); print(s.redis.redis_url, s.waha.api_url, s.task)"
.venv/bin/python -c "from tasks import huey, send_whatsapp_message; print(type(huey).__name__)"
```

---

## Roadmap & progress

- [x] Config system, client singletons, utils (logger/time/redis/WAHA)
- [x] Huey send task with retries + Redis task tracker + `get_job_status()`
- [x] Docker: WAHA + huey with healthchecks, IPython dev loop, `.dockerignore`
- [ ] **Phase 1 — Core send API**: `POST /messages` (idempotency-Key),
      `GET /messages/{id}` via `get_job_status`, message persistence
- [ ] **Phase 2 — OTP**: ✅ send (hashed, peppered) + `verify_otp()` with
      attempts/lockout/expiry — cooldown & rate limiting + HTTP endpoints remain
- [ ] **Phase 3 — Inbound webhooks + delivery tracking**: WAHA HMAC-verified
      receiver, message-ack correlation, `session.status` auto-restart
- [ ] **Phase 4 — Scheduling**: `send_at` (ISO or natural language), cancel,
      list scheduled
- [ ] **Phase 5 — Users & auth**: API keys (hashed), quotas
- [ ] **Phase 6 — Outbound webhooks + media/broadcast**
- [ ] **Phase 7 — Hardening**: metrics, **TLS on Redis** (⚠️ current Redis
      endpoint is public-IP + password — must be fixed before real OTP traffic),
      session failover, DB backups

---

## Security notes

- `.env` is gitignored — never commit real credentials. `.dockerignore` also
  keeps it out of image builds.
- `sessions/` holds **WhatsApp authorization data** — treat it like a password;
  it is excluded from build contexts and git.
- This project depends on the unofficial WhatsApp Web protocol. Automated
  messaging can violate WhatsApp's Terms of Service; risk of number bans is
  real. Use consented, transactional flows — OTP and notifications, not spam.
- Redis with no TLS + public IP is a known gap before Phase 7.
