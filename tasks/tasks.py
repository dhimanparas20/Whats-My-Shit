"""
Huey tasks — THIN orchestration only.

A task does four things: (1) quota-gate the send (via services.tracking —
a block is terminal: recorded, NO raise, no retry), (2) run the send via
WAHA, (3) record TaskStatus to the tracker (via services.tracking), (4)
raise on failure so huey retries. ALL domain/business logic lives in
services/ (AGENTS.md §2 — dependencies point downward only):

    api/  →  tasks/  →  services/  →  configs/ · modules/ · models/

Ownership: every tracked send belongs to a user — ``user_id`` is the first
param of send_whatsapp_message/send_whatsapp_otp and scopes the tracker key
(``HUEY_TASK_TRACKER:<user_id>:<job_id>``). Quota contract:
    - check BEFORE send (a block is terminal: recorded, NO raise, no retry)
    - increment ONLY after a successful send (failed attempts never
      consume quota, so a retry that finally succeeds counts exactly once)
Exceptions: ``send_fire_and_forget`` is UNTRACKED (no user/quota/tracker) —
for UI developers poking the pipeline from the IPython shell.
Conventions (AGENTS.md §8):
- ONE huey instance per process, defined here, re-exported via tasks/__init__.py
- Every task: context=True decorator (except fire-and-forget, which needs no
  job id), `task.id` for the job id
- Every task: write TaskStatus to the tracker (overwrite=True) BEFORE raising
- Failure contract: record failure → `raise error` so huey retries
"""

from huey import RedisHuey
from huey.constants import EmptyData

from configs import get_redis_url, get_settings, get_waha
from models import TaskStatus, WhatsappMessageType, WAHASendResult
from modules import get_logger, get_timestamp
from services.otp import otp_service
from services.quota import quota_service
from services.tracking import quota_gate, system_gate, tracker_key, task_tracker


logger = get_logger(__name__, show_time=True)

huey = RedisHuey("wms-huey", url=get_redis_url(), utc=False)
huey_settings = get_settings().task


def get_job_status(user_id: str, job_id: str) -> dict:
    """
    Look up a job three ways, in priority order (user-scoped — ownership is
    key-level, the API layer passes the authenticated user's id):
        1. Tracker  (HUEY_TASK_TRACKER:<user_id>:<job_id>) — rich payload
           written by the task itself (success/failure + result + timestamp)
        2. Huey result store                               — task finished
        3. Huey pending queue                              — queued/running/retrying
        4. else not_found
    """
    logger.info(f"Checking status for user={user_id}, job ID: {job_id}")

    tracked = task_tracker.get(tracker_key(user_id, job_id))
    if tracked is not None:
        status = "success" if tracked.get("success") else "failed"
        return {"status": status, "data": tracked}

    res = huey.result(job_id, preserve=True)
    if res is not None:
        return {"status": "finished", "result": res}

    # peek_data returns the EmptyData CLASS-sentinel (truthy!) for unknown
    # ids — without this check every wrong job_id would read "pending".
    # Huey compares by IDENTITY (is not EmptyData), matching its own source.
    task_data = huey.storage.peek_data(job_id)
    if task_data is not None and task_data is not EmptyData:
        return {"status": "pending", "result": None}

    return {"status": "not_found", "result": None}


@huey.task(
    retries=huey_settings.retry_count,
    retry_delay=huey_settings.delay,
    # context=True → Huey injects the Task instance into `task` at runtime.
    # `task.id` is the job id. It is stable across retries: Huey requeues
    # the SAME message id, so tracker entries stay consistent.
    context=True,
)
def send_whatsapp_message(
    user_id: str,
    phone: str,
    message: str,
    country_code: str = "91",
    link_preview: bool = True,
    task=None,
) -> dict:
    """
    Send a WhatsApp message for ``user_id`` and record the outcome.

    Quota: gated BEFORE the send — over-limit sends are recorded as
    quota-blocked terminal failures (no WAHA call, no retry); successful
    sends consume the daily AND monthly message quota.
    """
    job_id = task.id
    blocked = quota_gate(user_id, WhatsappMessageType.TEXT, job_id, task)
    if blocked is not None:
        return blocked.model_dump()  # terminal — no raise, no retry
    blocked = system_gate(user_id, WhatsappMessageType.TEXT, job_id, task)
    if blocked is not None:
        return blocked.model_dump()  # system super-cap — terminal, no retry

    try:
        waha = get_waha()
        resp = waha.send_text(
            chat_id=phone,
            text=message.strip(),
            link_preview=link_preview,
            country_code=country_code,
        )
        result = WAHASendResult.from_waha(resp).model_dump(by_alias=True)
        error = None
        logger.info(f"WhatsApp message sent to {phone=}, user={user_id}, job={job_id}")
    except Exception as exc:
        error = exc
        result = {"error": str(exc)}
        logger.error(f"Failed to send WhatsApp message to {phone=}: {exc}")

    data = TaskStatus(
        job_id=job_id,
        success=error is None,
        result=result,
        timestamp=get_timestamp(),
        message_type=WhatsappMessageType.TEXT,
        extra_info={
            "user_id": user_id,
            "phone": phone,
            "retries_left": task.retries,
            "error": str(error) if error else None,
        },
    )

    # overwrite=True is REQUIRED: on a retry this key already exists (the
    # failed attempt wrote it) and set() would raise ValueError otherwise.
    task_tracker.set(tracker_key(user_id, job_id), data.model_dump_json(), overwrite=True)

    if error is not None:
        raise error  # lets Huey apply its retry/requeue logic — NO quota consumed

    # Success → consume quota. Guarded: a failure here must NOT requeue the
    # task (the message is ALREADY sent — a retry would double-send).
    try:
        quota_service.increment(user_id, WhatsappMessageType.TEXT)
        quota_service.system_increment(WhatsappMessageType.TEXT)
    except Exception as inc_err:
        logger.error(
            f"QUOTA increment FAILED after successful send: user={user_id}, "
            f"job={job_id} — {inc_err} (usage may under-count this send)"
        )
    return data.model_dump()


@huey.task(
    retries=huey_settings.retry_count,
    retry_delay=huey_settings.delay,
    context=True,
)
def send_whatsapp_otp(
    user_id: str,
    phone: str,
    country_code: str = "91",
    msg_prefix: str = "Your OTP is: ",
    otp_length: int | None = None,
    otp_ttl: int | None = None,
    task=None,
) -> dict:
    """
    Thin wrapper around ``services.otp.otp_service``:

        quota gate → generate (service) → send via WAHA → track → persist (service)

    Ownership is key-scoped: the record lives at
    ``OTP:<user_id>:<country_code><phone>`` — different users can hold
    independent active codes for the SAME phone, tracked separately.

    Behavior notes:
    - Code length/TTL default from OTPSettings (OTP_LENGTH / OTP_TTL env).
    - Quota: gated BEFORE the send (blocked = terminal, no retry, no code
      persisted); successful sends consume the OTP quota for BOTH windows.
    - Persisted ONLY after the send succeeds: a code the user never
      received must never be verifiable.
    - Plaintext code is never logged or stored — only its HMAC hash.
    """
    job_id = task.id
    blocked = quota_gate(user_id, WhatsappMessageType.OTP, job_id, task)
    if blocked is not None:
        return blocked.model_dump()  # terminal — no raise, no retry
    blocked = system_gate(user_id, WhatsappMessageType.OTP, job_id, task)
    if blocked is not None:
        return blocked.model_dump()  # system super-cap — terminal, no retry

    raw_otp, record = otp_service.generate(phone, otp_length, otp_ttl)

    try:
        waha = get_waha()
        resp = waha.send_text(
            chat_id=phone,
            text=f"{msg_prefix}{raw_otp}",
            country_code=country_code,
        )
        result = WAHASendResult.from_waha(resp).model_dump(by_alias=True)
        error = None
        # Info, but never the code itself.
        logger.info(
            f"WhatsApp OTP sent to {phone=}, user={user_id}, job={job_id}, "
            f"otp_id={record.otp_id}"
        )
    except Exception as exc:
        error = exc
        result = {"error": str(exc)}
        logger.error(f"Failed to send WhatsApp OTP to {phone=}: {exc}")

    data = TaskStatus(
        job_id=job_id,
        success=error is None,
        result=result,
        timestamp=get_timestamp(),
        message_type=WhatsappMessageType.OTP,
        extra_info={
            "user_id": user_id,
            "phone": phone,
            "otp_id": record.otp_id,
            "retries_left": task.retries,
            "error": str(error) if error else None,
        },
    )

    task_tracker.set(tracker_key(user_id, job_id), data.model_dump_json(), overwrite=True)

    if error is not None:
        # Do NOT persist the OTP record — retry regenerates a fresh code.
        # NO quota consumed for failed attempts.
        raise error

    # Success → persist + consume quota. Guarded: a failure here must
    # not requeue (code already delivered — a retry would double-send).
    record.retry_count = huey_settings.retry_count - task.retries
    record.extra_info = {"job_id": job_id, "country_code": country_code}
    try:
        otp_service.save(record, user_id=user_id, country_code=country_code)
        quota_service.increment(user_id, WhatsappMessageType.OTP)
        quota_service.system_increment(WhatsappMessageType.OTP)
    except Exception as post_err:
        logger.error(
            f"POST-SAVE FAILED after successful OTP send: user={user_id}, "
            f"job={job_id} — {post_err} (code delivered but may be "
            f"unverifiable — check OTP store connectivity)"
        )
    return data.model_dump()


@huey.task(retries=huey_settings.retry_count, retry_delay=huey_settings.delay)
def send_fire_and_forget(
    phone: str,
    message: str,
    country_code: str = "91",
    link_preview: bool = True,
) -> dict:
    """
    Fire-and-forget WhatsApp send — NO tracking, NO quota, NO user.

    Utility task for UI developers poking the pipeline from the IPython
    shell: give it a number, a message, a country code — it sends and
    returns the normalized WAHA response (or re-raises on failure so Huey
    retries). Because nothing is tracked, this is NOT for production
    accountings — the regular user-scoped tasks do that.
    """
    try:
        waha = get_waha()
        resp = waha.send_text(
            chat_id=phone,
            text=message.strip(),
            link_preview=link_preview,
            country_code=country_code,
        )
        result = WAHASendResult.from_waha(resp).model_dump(by_alias=True)
        logger.info(f"Fire-and-forget WhatsApp message sent to {phone=}")
        return result
    except Exception as exc:
        logger.error(f"Fire-and-forget send failed to {phone=}: {exc}")
        raise  # re-raise → huey retries (no tracking to lose)
