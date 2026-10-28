"""
Huey tasks — THIN orchestration only.

A task does three things: (1) run the send via WAHA, (2) record TaskStatus
to the tracker, (3) raise on failure so huey retries. ALL domain/business
logic lives in services/ (AGENTS.md §2 — dependencies point downward only):

    api/  →  tasks/  →  services/  →  configs/ · modules/ · models/

Conventions (AGENTS.md §8):
- ONE huey instance per process, defined here, re-exported via tasks/__init__.py
- Every task: context=True decorator, `task.id` for the job id
- Every task: write TaskStatus to the tracker (overwrite=True) BEFORE raising
- Failure contract: record failure → `raise error` so huey retries
"""

from huey import RedisHuey

from configs import get_redis_url, get_settings, get_waha, get_redis_str
from models import TaskStatus, WhatsappMessageType, WAHASendResult
from modules import get_logger, get_timestamp
from services.otp import otp_service


logger = get_logger(__name__, show_time=True)

huey = RedisHuey("wms-huey", url=get_redis_url(), utc=False)
huey_settings = get_settings().task
huey_task_tracker_key = get_settings().redis_keys.huey_task_tracker

task_tracker = get_redis_str(prefix=huey_task_tracker_key)


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
        result = WAHASendResult.from_waha(resp).model_dump(by_alias=True)
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
    Thin wrapper around ``services.otp.otp_service``:

        generate (service) → send via WAHA → track → persist (service)

    Behavior notes:
    - Code length/TTL default from OTPSettings (OTP_LENGTH / OTP_TTL env).
    - Record lives at ``OTP:<country_code><phone>``: one ACTIVE code per
      phone; retries/re-requests overwrite it — latest code wins.
    - Persisted ONLY after the send succeeds: a code the user never
      received must never be verifiable.
    - Plaintext code is never logged or stored — only its HMAC hash.
    """
    job_id = task.id
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
            f"WhatsApp OTP sent to {phone=}, job={job_id}, otp_id={record.otp_id}"
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
            "otp_id": record.otp_id,
            "retries_left": task.retries,
            "error": str(error) if error else None,
        },
    )

    task_tracker.set(job_id, data.model_dump_json(), overwrite=True)

    if error is not None:
        # Do NOT persist the OTP record — retry regenerates a fresh code.
        raise error

    record.retry_count = huey_settings.retry_count - task.retries
    record.extra_info = {"job_id": job_id, "country_code": country_code}
    otp_service.save(record, phone=phone, country_code=country_code)
    return data.model_dump()
