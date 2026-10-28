# AGENTS.md — Context for LLM Agents working on WMS

> **READ THIS FILE FIRST, EVERY SESSION.** It is the single source of truth for
> architecture, conventions, and hard rules. If this file and the code disagree,
> the code is wrong — fix one of them before continuing.

---

## §0. MANDATORY SELF-MAINTENANCE RULES (NON-NEGOTIABLE)

**Rule 0.1 — Update docs side-by-side with code.**
Every time you make ANY change to the codebase during a session (new file,
modified function, new settings key, new dependency, renamed module, new
container, changed env var…), you MUST update `AGENTS.md` and `README.md`
in the SAME session, side by side with the code change:

- Added/changed a **settings class, env var, or default** → update §3 of this
  file AND the env table in `README.md` AND `.env.sample`.
- Added/changed a **client getter / singleton** → update §4 and `configs/clients.py` docstring header.
- Added/changed a **service** → update §5a (services table) and `services/__init__.py` re-exports.
- Added/changed a **model** (Pydantic) → update §6 module reference.
- Added/changed a **task** → update §8 (Huey conventions) and README feature list.
- Added/changed a **container, volume, healthcheck, or build step** → update §9.
- Discovered a **new gotcha** → add it to §11 (Learned Pitfalls) with the reason.
- **Changed anything a user should know about → `README.md`** feature/progress sections.

Do not batch this to "later". Later never happens. If you skip it, the next
agent session inherits wrong context and repeats your mistakes.

**Rule 0.2 — Never commit secrets.** `.env` is gitignored. Never paste real
Redis passwords, WAHA API keys, or OTP codes into code, logs, or docs. The
RedactionFilter in `modules/logger.py` helps but is not a license.

**Rule 0.3 — Match existing style.** Google-style docstrings with Args/Returns,
`from __future__ import annotations` preferred, `lower_snake_case` everywhere,
module docstrings at top of every file.

---

## §1. What is WMS?

**WMS = WhatsApp Messaging Service** (working name).

A self-hosted, Twilio-like messaging (WhatsApp-first) delivery platform:
FastAPI exposes the API (users, tokens, webhooks), **Huey** (Redis-backed,
lightweight Celery alternative) executes send/OTP/scheduling work, **Redis**
stores ephemeral state (OTPs with TTL, idempotency keys, rate limits, task
tracker), and **WAHA** (WhatsApp HTTP API, unofficial) performs actual
WhatsApp transport via linked sessions.

Positioning: a lightweight alternative to Mailgun/Twilio/Brevo for
WhatsApp. Delivered in phases — see §13 Roadmap.

Stack: Python 3.14 · FastAPI · Huey 3 · pydantic v2 + pydantic-settings ·
redis-py (sync + asyncio) · requests · uv · Docker Compose · WAHA (WEBJS
engine by default).

---

## §2. Layered Architecture & Placement Rules (WHERE things go)

Mental model — strictly ordered layers, dependencies only point downward
(left → right):

```
api/ (FastAPI)  →  tasks/ (Huey)  →  services/  →  configs/  →  modules/
                       models/          └─────────────┘
```

| Layer | Directory | What belongs there | What does NOT belong there |
|---|---|---|---|
| **Settings** | `configs/config.py` | `BaseSettings` subclasses grouped by domain, aggregated into `Settings`, cached `get_settings()` | clients, connections, I/O |
| **Clients** | `configs/clients.py` | `get_*()` singletons: raw redis, `RedisStringUtil`/`RedisHashUtil` wrappers, WAHA client, (future: HTTP clients) | business logic, raw env access |
| **Re-exports** | `configs/__init__.py` | public surface: `from configs import get_settings, get_waha, ...` | new classes |
| **Generic utils** | `modules/` | pure, project-agnostic helpers (logging, datetime, redis utils, WAHA client util) | app-specific business logic |
| **Pydantic models** | `models/` | request/response models, domain records (e.g. `TaskStatus`, `OTPData`), domain enums (`models/enums.py`), re-exported from `models/__init__.py` | redis I/O, direct calls to clients |
| **Services** | `services/` | domain/business logic spanning multiple Redis stores/prefixes or with lifecycle rules — state machines, cooldowns, rate limits (e.g. `OTPService`). Classes with module-level singletons | huey decorators, enqueueing; importing from tasks/api (cycles) |
| **Tasks** | `tasks/` | Huey instance + THIN task functions (send → track → raise) + `get_job_status()` | business/domain logic — delegate to services |
| **API** | `api/` | FastAPI routers/endpoints, dependencies, auth, webhook receivers (planned; empty now) | sending logic (enqueue only); call services/tasks |
| **Tests** | *(not yet created)* | pytest tests mirroring the layer layout | — |

**Placement checklists:**

*New settings/env need?*
1. Small class in `configs/config.py` with `env_prefix` (§3) — e.g. `TaskSettings`, `RedisKeys`.
2. Add to aggregate `Settings` as a typed field (`task: TaskSettings = TaskSettings()`).
3. Add key to `.env.sample` with comment.
4. Update §3 + README env table (Rule 0.1).

*New external client need?*
1. Getter in `configs/clients.py` with `functools.lru_cache`.
2. Re-export from `configs/__init__.py`.
3. Never construct clients ad-hoc in modules/tasks.

*New task need?*
1. Task functions live in `tasks/tasks.py` — THE task module — following §8 conventions.
2. Register in `tasks/__init__.py` re-exports (REQUIRED — the consumer
   imports `tasks.huey` via `tasks/__init__.py`).
3. Import-time side-effect: tasks module instantiates `huey` and db clients —
   keep it import-safe (no network literal calls needed at import).

*New service need?*
1. `services/<domain>.py` — business logic spanning multiple stores/prefixes
   or with lifecycle rules. Class-based with a module-level singleton
   (`otp_service = OTPService()`), like client getters.
2. Re-export the class + singleton from `services/__init__.py`.
3. Services import configs/modules/models ONLY — never tasks/api (import
   cycles). Tasks and api import services downward.

*New model need?*
1. `models/<domain>.py` with a module docstring.
2. Re-export from `models/__init__.py` — call sites always do
   `from models import TaskStatus`, never `from models.tasks import ...`.

---

## §3. Config System (`configs/config.py`)

~ Single source of truth. Everything reads from `.env` at repo root.

**Resolution order (highest wins):**
1. Real environment variables (Docker `-e`, shell export)
2. `.env` file — resolved ABSOLUTELY: `ENV_FILE = BASE_DIR / ".env"` (CWD-independent; never use a relative `env_file="..."`)
3. Field defaults

**Conventions:**
- One `BaseSettings` class per service/domain, each with an **`env_prefix`**
  (so keys are namespaced in `.env`):
  - `RedisSettings` — no prefix — `REDIS_URL`
  - `WASettings` — `env_prefix="waha_"` — `WAHA_API_URL`, `WAHA_API_KEY`, `WAHA_SESSION`, `WAHA_DEFAULT_COUNTRY_CODE`
  - `FastAPISettings` — `env_prefix="api_"` — `API_DEBUG`, `API_PORT`, `API_HOST`
  - `TaskSettings` — no prefix yet (defaults `retry_count=2`, `delay=60`; consider
    `env_prefix="task_"` if growing)
  - `OTPSettings` — `env_prefix="otp_"` — `OTP_PEPPER` (HMAC secret, REQUIRED
    for prod; empty default logs a loud warning + insecure dev fallback),
    `OTP_TTL` (1800s), `OTP_LENGTH` (6), `OTP_MAX_VERIFY_ATTEMPTS` (5)
  - `RedisKeys` — configured key-name constants (e.g. `huey_task_tracker="HUEY_TASK_TRACKER"`,
    `otp="OTP"`, `user="USER"`)
- Aggregate: `Settings` holds typed instances (`redis: RedisSettings`, `waha: WASettings`, …).
- Access: `get_settings()` — `@lru_cache` — call freely; never construct `Settings()` ad-hoc.
  Aggregate fields: `redis`, `waha`, `api`, `task`, `otp`, `redis_keys`.
- Env names match `env_prefix + fieldname` **case-insensitively** (`redis_url` ← `REDIS_URL`).

**Purpose of `WAHA_API_URL` vs `WAHA_BASE_URL` (do not mix):**
- `WAHA_API_URL` — how THIS app reaches WAHA (`http://waha:3000` in Docker, `http://localhost:3000` locally). `WASettings.api_url` reads this.
- `WAHA_BASE_URL` — consumed by the **WAHA container itself** (public URL used for webhooks/files). Do NOT read it in app settings.

**Runtime data keys (constants, also in `.env`):** `RedisKeys.huey_task_tracker`
(defaults `"HUEY_TASK_TRACKER"`) — see §7.

---

## §4. Client Singletons (`configs/clients.py`)

All external connections are built here. **Rules:**
- Always `get_*()` functions with `@functools.lru_cache`; lazy — a down Redis
  only breaks the first actual call, not `import configs.clients`.
- Never construct clients inline in modules/tasks/api.
- Sync/async split (see §10): Huey workers are sync; FastAPI endpoints are async.

**Current surface:**
| Getter | Returns | Notes |
|---|---|---|
| `get_redis_url()` | `str` | raw URL from settings (cheap, no connection) |
| `get_redis()` | `redis.Redis` | shared SYNC client (decode_responses=True). Scripts/direct cmds. |
| `get_redis_str(prefix="WMS", default_ttl=None)` | `RedisStringUtil` | **cached per (prefix, default_ttl)** — each constructor PINGs, so reuse per-name |
| `get_redis_hash(prefix="WMS", default_ttl=None)` | `RedisHashUtil` | same, HASH-backed generic records |
| `get_waha()` | `WhatsAppUtil` | sync requests-based WAHA client from `WAHA_*` settings |

**Named stores in use (via `get_redis_str`/`get_redis_hash`):**
- `HUEY_TASK_TRACKER` prefix — `TaskStatus` records (§7).
- `OTP:<country_code><phone>` prefix (one STRING per phone, `TaskSettings`-independent
  per-key TTL) — `OTPData` records (§7).
- `USER:<user_id>` HASH — user records (uuid7, no TTL) + O(1) indexes
  `USER:EMAIL:<sha256(email)>` / `USER:APIKEY:<sha256(api_key)>` STRING maps
  (PII/keys never in key names, never SCAN'd — §5a).
- `QUOTA:<DAILY|MONTHLY>:<user_id>` STRING — `QuotaUsage` JSON (window FIRST,
  so `SCAN QUOTA:DAILY:*` = everyone's daily usage), TTL anchored
  to each window's end (§5a QuotaService).
- `WMS:*` namespaces reserved for future use (idempotency).
- CONVENTION: all new Redis keys use `UPPER_SNAKE` or `WMS:<SUB>:<ID>` prefixes,
  IDs are UUID4 from huey/str(uuid4).

⚠️ **Careful with which util class has which static helpers:** the pure crypto
helpers (`generate_random_number` for OTPs, `generate_uuid7`,
`hash_sensitive_data`, `hash_sensitive_data_hmac`, `hash_password`,
`verify_password`) are static methods on `RedisHashUtil` ONLY —
`RedisStringUtil` does NOT have them and does NOT inherit from it. Call them
class-style (`RedisHashUtil.generate_random_number(6)`), never via a
`get_redis_str()` instance, unless that module also defines them.

---

## §5a. Services (`services/`)

Business-logic layer (AGENTS §2) — domain logic spanning multiple Redis
stores/prefixes or with lifecycle rules. Classes with module-level singletons;
import from `services` re-exports, never tasks/api cycles.

| File | Service | Key API |
|---|---|---|
| `services/otp.py` | `OTPService` (singleton `otp_service`) | `generate(phone, length?, ttl?)` → `(plaintext, OTPData)`; `save(record, user_id, country_code?)`; `get(user_id, phone, country_code?)` → `OTPData\|None`; `delete(user_id, phone, ...)`; **`verify_otp(user_id, phone, code, country_code?)` — classmethod** (routes through the singleton's store/settings, overridable; callable as `OTPService.verify_otp(...)`, `otp_service.verify_otp(...)`, or flat `verify_otp` alias). Keys are user-scoped: `OTP:<user_id>:<cc><phone>` — different users hold independent codes for the same phone. Owns: pepper (warn-once), one-active-code-per-(user,phone), attempt counting/lockout, expiry reclaim |
| `services/user.py` | `UserService` (singleton `user_service`) | Storage: `USER:<user_id>` HASH (uuid7, no TTL) + O(1) indexes `USER:EMAIL:<sha256(email)>` and `USER:APIKEY:<sha256(api_key)>` (STRING maps — PII/keys never in key names, never SCAN'd). API: `create(full_name, email, password, webhook_url?, extra_info?)` → `(User, plaintext_api_key)` — dup email raises, plaintext shown ONCE, only sha256 persisted; `get(id)`; `find_by_email(email)`; `authenticate_api_key(key)` (respects `is_active`); `authenticate(email, password)` (bcrypt `verify_password`); `update(id, **fields)` — whitelist `full_name/webhook_url/is_active/extra_info` only, stamps `updated_at`, raises on credential fields; `update_password(id, new)` — no old password required = forgot-password/reset primitive (API layer verifies identity via OTP/token first); `rotate_api_key(id)` → `(User, plaintext)` (old key dies instantly, index killed via stored hash — no plaintext needed); `delete(id)` (hard — indexes cleaned). Passwords = bcrypt; api keys = sha256 (see §11 pitfall 18) |
| `services/tracking.py` | (module, no singleton) | The ONLY writer of the task tracker + the quota gate. Exports: `task_tracker` (shared `RedisStringUtil`), `tracker_key(user_id, job_id)` (→ `HUEY_TASK_TRACKER:<user_id>:<job_id>`), `write_status(user_id, job_id, status)`, **`quota_gate(user_id, message_type, job_id, task)`** — checks quota, and when blocked: builds the terminal `quota_blocked` TaskStatus, writes it, returns it (task must `return` it — NO raise); returns `None` when allowed. `get_job_status` (in tasks/tasks.py) reads through it. Direct `task_tracker.set` outside this module is a violation |
| `services/quota.py` | `QuotaService` (singleton `quota_service`) | Storage: `QUOTA:<DAILY\|MONTHLY>:<user_id>` STRING (QuotaUsage JSON — window FIRST: `SCAN QUOTA:DAILY:*` = all daily records), TTL = window end (IST midnight / month start) + 60s buffer — **lazy reset via expiry**, `window_end` field is the authority for the race window. Tracking unit = user/api key (NOT phone). API: `get_or_create(user_id, window, messages_limit?, otp_limit?)`; `check(user_id, message_type)` — pre-send gate for the API layer (BOTH windows must allow; returns `{allowed, reason, daily, monthly}` usage dicts, no mutation); `increment(user_id, message_type, amount=1)` — call ONLY after successful WAHA send (failed sends never consume quota), bumps BOTH windows, stamps **per-type** first/last (`first_message_sent_at`/`last_message_sent_at` vs `first_otp_sent_at`/`last_otp_sent_at`); `set_limits(user_id, messages?, otps?, window?)` — per-user caps, `window=None` = both; `reset(user_id, window)` / `delete(...)`; lazy-expired reads return None → treated as fresh window. Default caps: 1000/100 daily, 10000/1000 monthly (service constants) |

## §5b. Module Reference (`modules/`)

| File | Purpose | Key API |
|---|---|---|
| `logger.py` | Structured JSON/colorlogger, SUCCESS level, redaction, trace_id, rotation | `get_logger(__name__, show_time=True)`; env `LOG_LEVEL`, `LOG_FORMAT` |
| `generics.py` | IST time helpers | `now_ist()`, `get_timestamp()`, `now_iso()`, `format_timestamp()`, `parse_datetime()` |
| `datetime_util.py` | dateparser-based parsing + 100+ tz aliases (IST default) | `convert_datetime()`, `string_to_timestamp()`, `format_iso()` — for scheduling UIs (Phase 4) |
| `redis_util.py` | Big-but-battle-tested generic Redis toolkit | see below |
| `wa_util.py` | Production WAHA HTTP client + webhook HMAC helpers + phone normalization | see below |
| `ipython_startup.py` | IPython profile startup (redis client `r` in shell, autoreload, cls) — mounted into container IPython dir via compose | `docker exec wms-huey-1 bash` then `ipython` |

**`redis_util.py` — which class when:**
- `RedisStringUtil` — STRING type (SET/GET), one JSON value per key, TTL. Use for
  ephemeral per-key state on hot paths (OTP, idempotency, tracker).
- `RedisHashUtil` — HASH type, many fields per record, secondary indexes,
  SCAN-based search/export. Use for richer records.
- ⚠️ `search`/`get_all`/`list_ids` do full-keyspace SCAN — fine for small data,
  NEVER on request hot paths. Real querying needs a DB (see §13).
- ⚠️ `set(key, ...)` raises `ValueError` when key exists and `overwrite=False`
  (default). Retries/multi-write flows MUST pass `overwrite=True`.
- Uses `decode_responses=True`; only store UTF-8 JSON inside it — binary/pickle
  data (e.g. Huey's own keys) needs a raw client (see §11).

**`wa_util.py` — `WhatsAppUtil`:**
- Constructor args: `base_url, api_key, session, default_country_code, timeout, max_retries, retry_delay` (env fallbacks `WAHA_*`).
- Senders: `send_text` (chat_id, text, link_preview, country_code), `send_image/video/file/voice/sticker/...`, phone→chat id normalization (`phone_to_chat_id`, default cc 91).
- Sessions/QR: `create_session/get_qr_code/start/stop/restart/logout`.
- Webhooks: `WAHAWebhook.build_config / verify_request` (HMAC-SHA512, header
  `X-Webhook-Hmac`), event names `WahaWebhookEvents` enum. Use for Phase 3
  webhook receiver — REQUIRED for inbound+delivery tracking.
- WAHA response shape: `resp["_data"]` holds the sent message (`from`, `to`,
  `viewed`, `id` — a dict with `_serialized`).

---

## §6. Models (`models/`)

- `TaskStatus` (models/tasks.py) — record written to the task tracker. Fields:
  `job_id`, `success` (default True), `result: dict|None`, `timestamp: float`,
  `extra_info: dict|None`, `message_type: WhatsappMessageType|None`. Class
  factories `TaskStatus.finished(...)`, `TaskStatus.failed(...)`.
- `OTPData` (models/tasks.py) — per-phone OTP record (§7). Fields: `otp_id`
  (uuid7), `hashed_otp` (HMAC-SHA256 of the code), `phone`, `timestamp`,
  `status` (OTPStatus, default PENDING), `attempts`, `retry_count`,
  `ttl`, `expiry` (auto = timestamp+ttl via model_validator), `extra_info`.
  Instance helpers: `remaining_ttl()`, `is_expired()` (note: NOT a classmethod).
- `models/enums.py` — domain enums `OTPStatus` (pending/verified/expired/failed),
  `WhatsappMessageType` (text/otp), `QuotaWindow` (daily/monthly). **Enums live in models, NOT configs** —
  configs must never import from models (layering), so never resurrect
  `configs/enums.py`.
- `User` (models/user.py) — API consumer record (Phase 5 groundwork).
  Fields: `user_id` (uuid7), `full_name`, `email` (regex-validated +
  lowercased/stripped via validator; swap to `EmailStr` if/when
  `email-validator` dep is added), `webhook_url` (Optional[HttpUrl]),
  `api_key_hash` (sha256 of the key — PLAINTEXT KEY IS NEVER STORED,
  shown once at create/rotate), `password_hash` (bcrypt — one-way, NOT
  reversible encryption), `created_at`/`updated_at` (floats),
  `is_active` (soft-disable), `extra_info`.
- `QuotaUsage` (models/quota.py) — per-user usage snapshot per window.
  Fields: `user_id`, `window` (QuotaWindow DAILY|MONTHLY), `messages_sent`,
  `otp_sent` (counted SEPARATELY), `messages_limit`/`otp_limit`
  (caps denormalized INTO the record — one read per window for a check),
  `first_message_sent_at`/`last_message_sent_at`,
  `first_otp_sent_at`/`last_otp_sent_at` (epoch — SEPARATE per type:
  OTP vs message sends happen at different times), `window_start`/`window_end`
  (epoch — window_end is the reset authority), `extra_info`.
  Helpers: `remaining_messages()`, `remaining_otp()`,
  `allows(message_type)`, `usage(message_type)` (API-response view).
  ONE model, TWO keys (DAILY + MONTHLY) — different reset boundaries
  make one-key-per-window the clean design (TTL = lazy reset).
- `WAHASendResult` (models/waha.py) — normalized WAHA send response
  (`resp['_data']`): fields `from_` (alias `"from"` — reserved keyword),
  `to`, `viewed`, `id` (str OR dict-with-`_serialized`). Build via
  `WAHASendResult.from_waha(resp)`; serialize with
  `model_dump(by_alias=True)` so JSON keys stay WAHA-original. Extra payload
  keys ignored; missing required keys → ValidationError (task's except
  treats it as send failure).
- Always re-export from `models/__init__.py`. Task/API import as
  `from models import TaskStatus` — never `from models.tasks import ...`.
- Convention: models are pure data + validation, type-only deps (pydantic);
  NO redis/requests imports.

## §7. Task Tracker & OTP Patterns (Redis)

**Task tracker:**
- Redis key: `HUEY_TASK_TRACKER:<user_id>:<job_id>` (one STRING with JSON
  `TaskStatus` — user-scoped; every send belongs to the user whose API key
  triggered it, and `get_job_status(user_id, job_id)` reflects that).
- Written by the task itself just before returning/re-raising, so it only
  reflects terminal states; pending/queued lookup falls back to huey.
- `get_job_status(user_id, job_id)` (`tasks/tasks.py`) resolution order:
  1. `task_tracker.get(f"{user_id}:{job_id}")` → success/failed + rich data
  2. `huey.result(job_id, preserve=True)` → finished
  3. `huey.storage.peek_data(job_id)` (truthy ⇒ pending/queued/retry
     schedule — **but compare against `EmptyData`**: for unknown ids it
     returns the `EmptyData` CLASS, which is truthy — see §11 #20)
  4. else `not_found`
- Always mutate via `task_tracker.set(... overwrite=True)` — retries re-run the
  same job_id and must overwrite the previous attempt's entry.

**OTP (Phase 2 core, implemented — logic lives in `services/otp.py`):**
- Redis key: `OTP:<user_id>:<country_code><phone>` — OWNERSHIP IS KEY-SCOPED
  (user_id lives in the KEY, not as a model field): different users can hold
  independent active codes for the SAME phone, tracked separately
- Redis key: `OTP:<user_id>:<country_code><phone>` — OWNERSHIP IS KEY-SCOPED
  (user_id lives in the KEY, not as a model field): different users can hold
  independent active codes for the SAME phone, tracked separately
  (`verify_otp(user_id, phone, code)`). One ACTIVE code per (user, phone);
  retries and re-requests overwrite with `overwrite=True` and an explicit
  per-key `ttl=otp_ttl` (never the caller's 12h store default).
- `OTPService.generate(phone, ...)` → `(plaintext, record)`; the task sends
  via WAHA, then `otp_service.save(record, user_id, country_code)` — **the
  record persists ONLY after the send succeeds** (a never-delivered code must
  never verify). Plaintext code: never logged, never stored.
- `verify_otp(user_id, phone, code)` — classmethod on `OTPService`
  (services/otp.py, flat alias re-exported from tasks for compat): resolution
  order not_found → terminal (verified/failed) → expired (deletes record) →
  attempts exhausted (locks, status FAILED) → HMAC-compare via
  `hmac.compare_digest` → success / wrong_code (+attempts). **A wrong code
  that ALSO reaches the cap returns `"failed"` (not `"wrong_code"`) — the
  caller sees the terminal state.** Every mutation rewrite re-claims the
  remaining TTL (floor of 1s — NEVER the 12h store default; a dying record
  must not be resurrected).
- `TaskStatus.extra_info` carries `otp_id` (uuid7) — never the code.
- **Not yet built (next up in Phase 2):** resend cooldown/rate limiting,
  verify endpoint in `api/`.

## §8. Huey Rules

- **Tasks stay THIN**: quota gate → send via WAHA → write TaskStatus →
  raise on failure. ALL domain logic (generation, storage rules, state
  machines, multi-store flows) belongs in `services/` — import as
  `from services.otp import otp_service`. Services must NEVER import tasks
  (import cycles); tasks import services downward.
- **Every task takes `user_id` as its FIRST param** — the triggerer of the
  send (resolved from the API key at the future API layer). It scopes the
  tracker key (`HUEY_TASK_TRACKER:<user_id>:<job_id>`) and the OTP key
  (`OTP:<user_id>:<cc><phone>`). EXCEPTION: `send_fire_and_forget` is the
  untracked dev utility (no user/quota/tracker).
- **Quota gate contract** (for send tasks): `quota_gate(user_id, type,
  job_id, task)` from `services/tracking.py` runs `quota_service.check`
  BEFORE the send — over-limit sends are recorded as terminal
  `quota_blocked` failures and RETURN (no raise — a retry cannot fix a
  quota, raising would just burn retries and re-run the same blocked send).
  `quota_service.increment(user_id, type)` AFTER a successful send ONLY —
  failed attempts never consume quota, so a retry that finally succeeds
  counts exactly once. Wrap the post-success increment in try/except and
  LOG (never re-raise) — the message is already delivered; requeueing
  would double-send.
- ONE Huey instance per process: `huey = RedisHuey("wms-huey", url=..., utc=False)`
  in `tasks/tasks.py`. Import chain: consumer imports `tasks.huey` →
  `tasks/__init__.py` re-exports `huey, send_whatsapp_message, get_job_status`
  from `tasks.tasks`. If you move the instance, update BOTH the
  `compose.yml` command AND `tasks/__init__.py`.

- **Getting the job id inside a running task:** the decorator MUST include
  `context=True` (huey injects the `Task` object into kwargs at runtime), and
  the attribute is `task.id` (NOT `task.task_id`). Stable across retries —
  huey requeues the same message id, so the tracker key stays valid.
- **Capture job id at enqueue time (api layer):**
  `result = send_whatsapp_message(...)` → returns a `Result` wrapper —
  `result.task.id` is the same id.
- Retries: `retries=huey_settings.retry_count, retry_delay=huey_settings.delay`
  (from `TaskSettings`; default 2 / 60s).
- Failure contract: tasks MUST mark the failure in the tracker, then `raise error`
  so huey requeues. Never swallow the exception (silent loss) and never
  re-raise a substitute exception type.
- Errors are non-fatal inside tasks; log via `get_logger` with the payload
  (NOT raw otp/secrets).
- Status helpers (e.g. `get_job_status`) live in the same module as
  the tasks they describe.
- Immediate-mode gotcha: never enqueue tasks before the module-level `huey`
  and Redis are reachable; import-safe singletons only.

## §9. Docker & Compose

- Services: `waha` (devlikeapro/waha, `X-Api-Key` auth, sessions/media bind
  mounts, `/health` healthcheck with `$$WAHA_API_KEY` — `$$` needed so the
  container shell expands it, NOT compose-time interpolation) and `huey`
  (builds the `wms` image, runs `uv run huey_consumer tasks.huey -k thread -w 1 --flush-locks info`).
  A commented `app` FastAPI service exists for future use (Phase 1).
- Volume mount ORDER matters for the huey service: directory mounts first,
  then the single-file `ipython_startup.py` mount LAST so it lands on top
  of the mounted `.ipython` dir (empty host dir would otherwise shadow the
  baked-in startup file).
- Image code comes from compose volume mounts (dev-style). The Dockerfile
  bakes only `pyproject.toml`, `uv.lock`, and `modules/ipython_startup.py`.
- `.dockerignore` reduces build context 191MB → ~750KB (excludes `sessions/`
  — sensitive WhatsApp auth data, `.venv`, `.git`, `.idea`, runtime dirs).
  MUST stay in sync with any new runtime dirs (`.env`, caches, logs).
- `.env` flows into containers via `env_file:`; the container PYTHONPATH is `/app`.
- Healthchecks: waha uses `curl` with API key; huey pings redis via
  `redis.Redis.from_url(os.environ["REDIS_URL"]).ping()`.
- IPython debugging: `docker exec wms-huey-1 bash` → run **`ipython`** (not
  `python`); profile persists via `./.ipython` volume (history survives
  rebuilds).

## §10. Sync vs Async Rule

- Huey workers and all `tasks/` code are **sync** (requests + sync redis). Keep it that way.
- FastAPI (`api/`, when it exists) is async; blocking calls must not run on the
  event loop. Allowed: async redis clients; otherwise wrap syncs in
  `run_in_executor`/`AnyIO.to_thread`.
- `run_in_executor` caution: task singletons are process-wide; don't mutate
  shared dicts from executor threads.

## §11. Learned Pitfalls (READ before changing anything env/docker/redis)

1. `.env` lookup must be ABSOLUTE in pydantic-settings — relative
   `env_file=".env"` breaks when process CWD differs (Docker WORKDIR, tests).
2. Generic env names (`API_KEY`, `HOST`, `DEBUG`, `PORT`) silently miss when a
   settings class uses `env_prefix`. Always name fields so
   `prefix + fieldname` is unambiguous (e.g. `api_port` not `port`+`api_`).
3. `env_prefix="api_"` + field `api_port` ⇒ env key `API_API_PORT`. Rename
   field to avoid doubled prefixes.
4. `python-redis`, `logging`, `uuid` PyPI packages are junk/stdlib-shadowing —
   use `redis`; don't add stdlib-shadowing packages.
5. `WAHA_BASE_URL` is for WAHA itself (public URL); app→waha uses
   `WAHA_API_URL`. Do not "fix" one to equal the other.
6. redis clients with `decode_responses=True` CRASH on binary data (huey
   results are pickle). For huey universe, use huey API (`huey.result`,
   `task_tracker`) or a `decode_responses=False` client.
7. huey's default result serializer is PICKLE (not JSON). Keys live in
   `huey.results.wmshuey` / `huey.tasks.wmshuey` / `huey.schedule.wmshuey`.
   Don't hand-parse; use huey methods.
8. `context=True` is REQUIRED to get the Task instance inside a task
   function; attribute is `task.id`.
9. Redis string util `set()` raises on existing keys (overwrite=False) →
   always `overwrite=True` for self-recording flows like the tracker.
10. Compose: `$$VAR` in healthcheck (escape) vs `$VAR` (host/compose
    interpolation); mount order matters for file-over-dir shadows.
11. Python is pinned to 3.14 in `.python-version` and `pyproject.toml` —
    — verify before bumping.
12. Docker volume bind-mount dirs validated: `./modules`, `./configs`,
    `./models`, `./tasks`, `./services`, `.ipython` — adding an app dir means
    editing compose volumes (no bake) or Dockerfile COPY (bake).
13. `RedisStringUtil` and `RedisHashUtil` are SIBLINGS, not parent/child:
    crypto statics (`generate_random_number`, `generate_uuid7`,
    `hash_sensitive_data`, `hash_sensitive_data_hmac`) exist ONLY on
    `RedisHashUtil`. Calling `otp_store.generate_random_number(...)` on a
    string util raises `AttributeError` at runtime.
14. Pydantic class-body defaults are evaluated ONCE at class definition:
    `expiry: float = timestamp + ttl` silently becomes `0.0 + 3600`. Compute
    cross-field defaults in a `@model_validator(mode="after")`, never in
    field defaults, and never override BaseModel `__init__` for it.
15. Domain enums live in `models/enums.py` (NOT `configs/enums.py`) — configs
    must not import from models. Import as `from models import OTPStatus`.
16. OTP pepper (`RedisKeys.otp` etc.) is a KEY NAME, not a secret — hashing
    with it is false security. Use `OTPSettings.pepper` (OTP_PEPPER env).
17. OTP failure contract subtlety: store the OTP record AFTER a successful
    send only — otherwise failed attempts persist verifiable codes the user
    never received, and uuid7-per-attempt orphans keys on every retry.
18. Hash-algorithm asymmetry is INTENTIONAL: passwords → bcrypt (slow,
    low-entropy human strings); API keys → sha256 (fast hot-path auth,
    384-bit entropy makes brute force infeasible). Do NOT "fix" one to
    match the other — bcrypt on every API request adds ~100ms; sha256 on
    passwords is instant to brute-force.
19. NEVER name a prefix-less settings field after a POSIX env var: the
    `user` field in `RedisKeys` resolved to `$USER` (shell login name —
    `paras` on this box) because real env vars beat defaults, so Redis
    keys silently became `PARAS:…` and would have DIFFERED in containers
    (`USER` unset → default). Renamed to `users` (env `USERS`). The danger
    set: `user`, `path`, `home`, `shell`, `pwd`, `tmpdir`, `lang`, `term`.
20. `huey.storage.peek_data()` returns the `EmptyData` CLASS (not an
    instance, still truthy) for unknown ids — checking `if task_data:`
    marks every unknown job_id as "pending". Always compare
    `task_data is not EmptyData` (import from `huey.constants`), the same
    identity check huey's own storage code uses.
21. CALLING a huey task function directly (`send_x(...)`) ENQUEUES it to the
    REAL Redis queue for the consumer to run — it does not run locally.
    For in-process test execution use `.s(...).execute()` (builds the task
    instance without enqueueing, runs it inline). A direct call with a
    stubbed `get_waha` in YOUR process does not protect the container's
    real consumer, which runs the REAL code with REAL WAHA access.
    (Live example: a direct-call test enqueued a fire-and-forget send; the
    consumer executed it against REAL WAHA — the fake number failed with
    `500 No LID for user` so nothing was delivered, but by pure luck. NEVER
    call task functions directly except in throwaway sanity checks; in
    tests always use `.s(...).execute()`.)

## §12. Testing / sanity commands used in this codebase

```bash
# local sanity (host, repo root):
.venv/bin/python -c "from configs import get_settings; s=get_settings(); print(s.redis.redis_url, s.waha.api_url, s.task)"
.venv/bin/python -c "from tasks import huey, send_whatsapp_message, send_whatsapp_otp, verify_otp; print(type(huey).__name__)"
.venv/bin/python -c "from services import otp_service, OTPService; print(type(otp_service).__name__)"

# container:
docker compose up -d --build huey
docker compose logs -f huey
docker exec wms-huey-1 bash  # then: ipython  (r = redis client injected)

# unit-style checks of tasks without real sends:
# stub get_waha in the module namespace, build task instance with
# send_whatsapp_message.s(...), then task.execute() — see session history
```

Issues found in production-ish testing are tracked via §11; fix the ROOT
CAUSE, never bend downstream behavior.

## §13. Roadmap / Current Phase

Completed so far:
- Config/settings system with per-service env prefixes, absolute `.env` path,
  `.env.sample`, `get_settings()`.
- Client singletons (raw redis, string/hash util wrappers, WAHA).
- General utils (logger w/ redaction, IST helpers, dateparser tz util, WAHA
  client, huge Redis toolkit).
- Huey pipeline: `send_whatsapp_message` (text) and `send_whatsapp_otp` (code
  generation + HMAC hashing + Redis record) with retries + TaskStatus
  tracker + `get_job_status()`.
- **`services/` layer** — `OTPService` (`services/otp.py`) owns the whole OTP
  lifecycle (generate/save/get/delete/verify); tasks are thin wrappers;
  `verify_otp` re-exported from both `services` and `tasks`.
- **`UserService`** (`services/user.py`) — user accounts (uuid7 HASH records
  under `USER:`), bcrypt passwords, `wms_`-prefixed API keys with O(1)
  sha256-index auth lookups, rotation, soft-disable, delete.
- **`QuotaService`** (`services/quota.py`) — per-user daily/monthly caps for
  messages vs OTPs (STRING records under `QUOTA:`, TTL lazy reset at each
  window's IST boundary), `check()` pre-send gate + `increment()` on
  successful send, `set_limits` per user.
- `models/` layer with `TaskStatus`, `OTPData`, and domain enums
  (`OTPStatus`, `WhatsappMessageType`).
- **Phase 2 partially done**: send + store + verify_otp backend flow.
- Docker: WAHA + huey containers, healthchecks (waha /health w/ API key, huey
  redis ping), ipython-in-container dev loop, `.dockerignore`.

Next phases (update README + this section as they land):
1. **Phase 1 — Core send via API**: `api/` FastAPI service; `POST /messages`
   enqueues with idempotency-Key (strings store), `GET /messages/{id}` via
   `get_job_status`; message persistence (SQLite→Postgres decision needed:
   Redis = ephemeral only).
2. **Phase 2 — OTP module**: OTP generation via
   `RedisHashUtil.generate_random_number`, HASH-record or string entry with
   `{code?, hash, attempts, last_sent_at}` + TTL, cooldown, verify endpoint,
   rate limiting; use hashed OTP not plaintext.
3. **Phase 3 — Webhooks IN + delivery tracking**: WAHA webhook receiver
   (HMAC via `wa_util.WAHAWebhook.require_valid_request`), correlate
   `message.ack` to stored WAHA msg ids, `session.status` health monitor +
   auto-restart of dead sessions.
4. **Phase 4 — Scheduling**: `send_at` (ISO or natural language),
   `send_whatsapp_message.schedule(...)` (huey eta), cancel via id, list
   scheduled (`huey.scheduled_count` etc.).
5. **Phase 5 — Users & auth, quotas, API keys** (hashed bcrypt tokens).
6. **Phase 6 — Outbound webhooks (delivery callbacks to customers) + media/broadcasts**.
7. **Phase 7 — Hardening**: metrics (send rate, session health), TLS on the
   Redis endpoint (currently public IP w/ password — CRITICAL before real OTP
   traffic), session failover strategy, DB backups.

## §14. Things to confirm before "I fixed it"

- Grep whole repo for old names (`api_key=` misuse, old env names, old prefix).
- Restart the relevant container (code is volume-mounted; no rebuild needed for
  python code, restart only for env/entrypoint changes).
- Update §5a, §5b, §6, §8, §12, §13 and README per Rule 0.1.
