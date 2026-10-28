"""
Huey tasks — the ONLY place where sending/enqueueing work lives.

Conventions (AGENTS.md §8):
- ONE huey instance per process, defined here, re-exported via tasks/__init__.py
- Every task: context=True decorator, `task.id` for the job id
- Every task: write TaskStatus to the tracker (overwrite=True) BEFORE raising
- Failure contract: record failure → `raise error` so huey retries
"""

import hmac as hmac_lib

from huey import RedisHuey

from configs import get_redis_url, get_settings, get_waha, get_redis_str
from models import TaskStatus, OTPData, OTPStatus, WhatsappMessageType
from modules import get_logger, get_timestamp
from modules.redis_util import RedisHashUtil


logger = get_logger(__name__, show_time=True)

huey = RedisHuey("wms-huey", url=get_redis_url(), utc=False)
huey_settings = get_settings().task
otp_settings = get_settings().otp
huey_task_tracker_key = get_settings().redis_keys.huey_task_tracker
otp_data_key = get_settings().redis_keys.otp

task_tracker = get_redis_str(prefix=huey_task_tracker_key)
# OTP records: one STRING per phone. 12h default TTL is just a backstop —
# records are written with ttl=otp_ttl explicitly.
otp_store = get_redis_str(prefix=otp_data_key, default_ttl=12 * 60 * 60)

# NOTE: the crypto helpers below (OTP generation, uuid7, HMAC hashing) are
# STATIC methods on RedisHashUtil — they need no redis connection.
# RedisStringUtil does NOT have them, so never call them via otp_store.
_CryptoUtil = RedisHashUtil

# Loud-marker fallback used ONLY when OTP_PEPPER is unset (dev convenience).
_DEV_PEPPER = "wms-DEV-pepper-NOT-safe-do-not-use-in-prod"


def _otp_pepper() -> str:
    pepper = otp_settings.pepper
    if not pepper:
        logger.warning(
            "OTP_PEPPER is unset — falling back to an insecure dev pepper. "
            "Set OTP_PEPPER in .env before any real OTP traffic."
        )
        return _DEV_PEPPER
    return pepper


def _otp_key(phone: str, country_code: str = "91") -> str:
    """Bare (pre-prefix) Redis key for a phone's active OTP record."""
    return f"{country_code}{phone}"


def _waha_send_result(resp: dict) -> dict:
    """Normalized view of WAHA's send response (``resp['_data']``)."""
    wa = resp["_data"]
    return {
        "from": wa["from"],
        "to": wa["to"],
        "viewed": wa["viewed"],
        "id": wa["id"],
    }


def get_job_status(job_id: str) -> dict:
    """
    Look up a job three ways, in priority order:
        1. Tracker  (HUEY_TASK_TRACKER:<job_id>)  — rich payload written
           by the task itself (success/failure + result + timestamp)
        2. Huey result store                      — task finished
        3. Huey pending queue                     — queued / running / retrying
    """
    logger.info(f"Checking status for job ID: {job_id}")

    tracked = task_tracker.get(job_id)
    if tracked is not None:
        status = "success" if tracked.get("success") else "failed"
        return {"status": status, "data": tracked}

    res = huey.result(job_id, preserve=True)
    if res is not None:
        return {"status": "finished", "result": res}

    task_data = huey.storage.peek_data(job_id)
    if task_data:
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
    phone: str,
    message: str,
    country_code: str = "91",
    link_preview: bool = True,
    task=None,
) -> dict:
    """
    Send a WhatsApp message and record its outcome in the task tracker.
    """
    job_id = task.id

    try:
        waha = get_waha()
        resp = waha.send_text(
            chat_id=phone,
            text=message.strip(),
            link_preview=link_preview,
            country_code=country_code,
        )
        result = _waha_send_result(resp)
        error = None
        logger.info(f"WhatsApp message sent to {phone=}, job={job_id}")
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
            "phone": phone,
            "retries_left": task.retries,
            "error": str(error) if error else None,
        },
    )

    # overwrite=True is REQUIRED: on a retry this key already exists (the
    # failed attempt wrote it) and set() would raise ValueError otherwise.
    task_tracker.set(job_id, data.model_dump_json(), overwrite=True)

    if error is not None:
        raise error  # lets Huey apply its retry/requeue logic
    return data.model_dump()


@huey.task(
    retries=huey_settings.retry_count,
    retry_delay=huey_settings.delay,
    context=True,
)
def send_whatsapp_otp(
    phone: str,
    country_code: str = "91",
    msg_prefix: str = "Your OTP is: ",
    otp_length: int | None = None,
    otp_ttl: int | None = None,
    task=None,
) -> dict:
    """
    Generate a one-time code, send it via WhatsApp, store only its HMAC hash.

    Behavior notes:
    - Code length/TTL default from OTPSettings (OTP_LENGTH / OTP_TTL env).
    - The record lives at ``OTP:<country_code><phone>``: one ACTIVE code per
      phone. On a huey retry the same key is overwritten with a freshly
      generated code (overwrite=True) — no orphan records, latest code wins.
    - The OTP record is stored ONLY after the send succeeds: a code the
      user never received must never be verifiable.
    - Plaintext code is never logged or stored — only its HMAC hash.
    """
    job_id = task.id
    otp_length = otp_length or otp_settings.length
    otp_ttl = otp_ttl or otp_settings.ttl

    raw_otp = _CryptoUtil.generate_random_number(otp_length)
    otp_id = _CryptoUtil.generate_uuid7()
    hashed_otp = _CryptoUtil.hash_sensitive_data_hmac(
        data=raw_otp, secret=_otp_pepper()
    )

    try:
        waha = get_waha()
        resp = waha.send_text(
            chat_id=phone,
            text=f"{msg_prefix}{raw_otp}",
            country_code=country_code,
        )
        result = _waha_send_result(resp)
        error = None
        # Info, but never the code itself.
        logger.info(
            f"WhatsApp OTP sent to {phone=}, job={job_id}, otp_id={otp_id}"
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
            "phone": phone,
            "otp_id": otp_id,
            "retries_left": task.retries,
            "error": str(error) if error else None,
        },
    )

    task_tracker.set(job_id, data.model_dump_json(), overwrite=True)

    if error is not None:
        # Do NOT persist the OTP record — retry regenerates a fresh code.
        raise error

    record = OTPData(
        otp_id=otp_id,
        hashed_otp=hashed_otp,
        phone=phone,
        timestamp=get_timestamp(),
        retry_count=huey_settings.retry_count - task.retries,
        ttl=otp_ttl,
        extra_info={"job_id": job_id, "country_code": country_code},
    )
    # Per-key TTL (not the store's 12h backstop), overwrite=True: a repeat
    # request for the same phone replaces the previous active code.
    otp_store.set(
        _otp_key(phone, country_code),
        record.model_dump_json(),
        ttl=record.ttl,
        overwrite=True,
    )
    return data.model_dump()


def verify_otp(
    phone: str,
    code: str,
    country_code: str = "91",
) -> dict:
    """
    Check a code against the phone's active OTP record (sync helper, like
    get_job_status — the async API layer must not call this directly on the
    event loop; wrap it in run_in_executor/AnyIO.to_thread per AGENTS §10).

    Resolution order:
        1. No record           → not_found
        2. Terminal state      → verified / failed (already settled)
        3. Expired             → expired (record deleted)
        4. Attempts exhausted  → failed (record locked)
        5. HMAC equality       → success / wrong_code (+attempts)

    Every path rewrites the record with a re-claimed TTL so it can never
    outlive its expiry. Status dict carries "verified"/"expires_in" style
    details, not the hash.
    """
    key = _otp_key(phone, country_code)
    rec_raw = otp_store.get(key)
    if not rec_raw:
        return {"status": "not_found"}

    record = OTPData(**rec_raw)
    stale = record.remaining_ttl()
    base_ttl = stale if stale > 0 else None  # None → let default TTL apply

    def rewrite(record: OTPData, ttl: int | None) -> None:
        otp_store.set(key, record.model_dump_json(), ttl=ttl, overwrite=True)

    if record.status in (OTPStatus.VERIFIED, OTPStatus.FAILED):
        return {"status": record.status.value, "attempts": record.attempts}

    if record.is_expired():
        otp_store.delete(key)
        return {"status": "expired"}

    if record.attempts >= otp_settings.max_verify_attempts:
        record.status = OTPStatus.FAILED
        rewrite(record, base_ttl)
        return {"status": "failed", "attempts": record.attempts}

    computed = _CryptoUtil.hash_sensitive_data_hmac(
        data=code, secret=_otp_pepper()
    )
    if hmac_lib.compare_digest(computed, record.hashed_otp):
        record.status = OTPStatus.VERIFIED
        rewrite(record, base_ttl)
        logger.info(f"OTP verified for phone={phone}, otp_id={record.otp_id}")
        return {"status": "success", "otp_id": record.otp_id}

    record.attempts += 1
    if record.attempts >= otp_settings.max_verify_attempts:
        record.status = OTPStatus.FAILED
    rewrite(record, base_ttl)
    logger.info(
        f"OTP wrong code for phone={phone}, attempts={record.attempts}"
    )
    return {"status": "wrong_code", "attempts": record.attempts}
