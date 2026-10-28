"""
User model — an API consumer of WMS (Phase 5 groundwork).

Stored by ``services/user.py`` as a Redis HASH under ``USER:<user_id>``
(user_id = uuid7), plus O(1) index maps (see the service docstring).
Pure data + validation — pydantic only, no redis/requests imports.
"""

from __future__ import annotations

import re
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


_EMAIL_RE = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)


class User(BaseModel):
    """
    A WMS user: owns API credentials and an optional delivery webhook.

    Security fields (never returned to other users, never logged):
    - ``api_key_hash`` — sha256 of the API key. The PLAINTEXT key is shown
      to the owner exactly once (at creation or rotation) and never stored.
    - ``password_hash``— bcrypt hash (one-way). NOT reversible encryption —
      storing passwords reversibly is never acceptable.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    user_id: str
    full_name: str = Field(min_length=1)
    email: str
    # Optional callback URL — Phase 6 delivery webhooks will POST here.
    webhook_url: Optional[HttpUrl] = None
    api_key_hash: str  # sha256(api_key) — index key doubles as this value
    password_hash: str
    created_at: float = 0.0
    updated_at: float = 0.0
    # Soft-disable: auth fails while False, data stays intact.
    is_active: bool = True
    extra_info: Optional[dict] = None  # future: quotas, plan, etc.

    @field_validator("email")
    @classmethod
    def _validate_email(cls, value: str) -> str:
        value = value.strip().lower()
        if not _EMAIL_RE.match(value):
            raise ValueError(f"invalid email address: {value!r}")
        return value

    def is_newer_than(self, other: "User") -> bool:
        """updated_at comparison helper (ties → not newer)."""
        return self.updated_at > other.updated_at
