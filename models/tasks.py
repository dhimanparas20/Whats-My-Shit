from pydantic import BaseModel
from typing import Optional


class TaskStatus(BaseModel):
    """
    Delivery-tracking record for a Huey job.

    Stored in Redis under ``HUEY_TASK_TRACKER:<job_id>`` as JSON
    (written by the task itself on success/failure, survives Huey's
    result TTL and is readable without unpickling).
    """

    job_id: str
    success: bool = True
    result: dict | None = None
    timestamp: float = 0.0
    extra_info: Optional[dict] = None

    @classmethod
    def finished(
        cls,
        job_id: str,
        result: dict,
        timestamp: float,
        **extra_info,
    ) -> "TaskStatus":
        """Convenience factory for a successful job."""
        return cls(job_id=job_id, success=True, result=result,
                   timestamp=timestamp, extra_info=extra_info or None)

    @classmethod
    def failed(
        cls,
        job_id: str,
        error: str,
        timestamp: float,
        **extra_info,
    ) -> "TaskStatus":
        """Convenience factory for a failed job."""
        return cls(job_id=job_id, success=False, result={"error": error},
                   timestamp=timestamp, extra_info=extra_info or None)
