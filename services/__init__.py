"""
services/ — business-logic layer (AGENTS.md §2).
Public surface: import from here, never from service module paths directly.
"""

from services.otp import OTPService, otp_service, verify_otp
from services.quota import QuotaService, quota_service
from services.tracking import (
    quota_gate,
    task_tracker,
    tracker_key,
    write_status,
)
from services.user import UserService, user_service

__all__ = [
    "OTPService",
    "otp_service",
    "verify_otp",
    "QuotaService",
    "quota_service",
    "UserService",
    "user_service",
    "quota_gate",
    "task_tracker",
    "tracker_key",
    "write_status",
]
