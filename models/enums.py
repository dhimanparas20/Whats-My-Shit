"""
Domain enums for WMS.

Lives in models/ (NOT configs/) because they are pure domain data used by
models and tasks; the models layer must not depend upward on configs.
Re-exported from models/__init__.py — always
``from models import OTPStatus, WhatsappMessageType``.
"""

from enum import Enum


class OTPStatus(str, Enum):
    """Lifecycle of an OTP record in Redis."""

    PENDING = "pending"  # sent, awaiting verification
    VERIFIED = "verified"
    EXPIRED = "expired"
    FAILED = "failed"  # exhausted verify attempts / abandoned


class WhatsappMessageType(str, Enum):
    """Kind of WhatsApp message a task delivered."""

    TEXT = "text"
    OTP = "otp"
