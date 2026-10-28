"""
services/ — business-logic layer (AGENTS.md §2).
Public surface: import from here, never from service module paths directly.
"""

from services.otp import OTPService, otp_service, verify_otp

__all__ = [
    "OTPService",
    "otp_service",
    "verify_otp",
]
