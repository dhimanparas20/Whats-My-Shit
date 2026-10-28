"""
Tracking service — the ONLY writer of the task tracker, plus the quota gate.

Contains everything a send task needs for its bookkeeping so ``tasks/`` stays
pure orchestration (AGENTS.md §8):

    tracker_key(user_id, job_id)          → scoped key
    write_status(user_id, job_id, status) → persist (overwrite=True)
    quota_gate(user_id, type, job_id, task) → TaskStatus | None
        (None = allowed; a TaskStatus means BLOCKED — terminal, already
         written to the tracker, the task must RETURN it without raising)

Layering: configs/modules/models + sibling services only — never tasks/api.
"""

from __future__ import annotations

from configs import get_redis_str, get_settings
from models import TaskStatus, WhatsappMessageType
from modules import get_logger, get_timestamp
from services.quota import quota_service


logger = get_logger(__name__, show_time=True)

# Shared instance — get_redis_str is lru_cached per prefix, so this is the
# SAME object tasks/ (and any other consumer) would build themselves.
task_tracker = get_redis_str(prefix=get_settings().redis_keys.huey_task_tracker)


def tracker_key(user_id: str, job_id: str) -> str:
    """Bare (pre-prefix) tracker key — user-scoped."""
    return f"{user_id}:{job_id}"


def write_status(user_id: str, job_id: str, status: TaskStatus) -> None:
    """
    Persist a TaskStatus under the user-scoped tracker key.
    ``overwrite=True`` is REQUIRED: on a retry the key already exists (the
    failed attempt wrote it) and ``set()`` raises ``ValueError`` otherwise.
    """
    task_tracker.set(
        tracker_key(user_id, job_id), status.model_dump_json(), overwrite=True
    )


def quota_gate(
    user_id: str,
    message_type: WhatsappMessageType,
    job_id: str,
    task,
) -> TaskStatus | None:
    """
    Pre-send quota gate for a send task.

    Returns ``None`` when the send is allowed (task proceeds), or a BLOCKED
    ``TaskStatus`` (already written to the tracker) when over limit. The
    blocked failure is TERMINAL — the task must ``return`` it WITHOUT
    raising: a retry cannot fix a quota and would re-run the same blocked
    send forever.
    """
    verdict = quota_service.check(user_id, message_type)
    if verdict["allowed"]:
        return None

    logger.warning(
        f"Send BLOCKED by quota: user={user_id}, type={message_type.value}, "
        f"reason={verdict['reason']}"
    )
    blocked = TaskStatus(
        job_id=job_id,
        success=False,
        result={"error": verdict["reason"], "quota_blocked": True},
        timestamp=get_timestamp(),
        message_type=message_type,
        extra_info={
            "user_id": user_id,
            "retries_left": task.retries,
            "quota": verdict,
            "error": verdict["reason"],
        },
    )
    write_status(user_id, job_id, blocked)
    return blocked
