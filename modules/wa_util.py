"""
WhatsAppUtil — Production-ready client for WAHA (WhatsApp HTTP API).

Connects to a self-hosted WAHA instance (https://waha.devlike.pro/) for session
management, outbound chat messages, chat history, contacts, and webhook helpers.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import mimetypes
import os
import secrets
import time
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import quote, urlparse

try:
    import requests
except ImportError:
    raise ImportError("Install requests: pip install requests")

try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator
except ImportError:
    raise ImportError("Install pydantic: pip install pydantic")

from modules.logger import get_logger

logger = get_logger(__name__)

# Media argument for send_image / send_file / etc. (see _resolve_file).
FileInput = Union[str, Path, Dict[str, Any]]
MediaKind = Literal["image", "video", "voice", "sticker", "file"]

_DEFAULT_MIME: Dict[MediaKind, str] = {
    "image": "image/jpeg",
    "video": "video/mp4",
    "voice": "audio/ogg; codecs=opus",
    "sticker": "image/webp",
    "file": "application/octet-stream",
}

RetryPolicy = Literal["constant", "linear", "exponential"]
SessionStatus = Literal[
    "STOPPED",
    "STARTING",
    "SCAN_QR_CODE",
    "PASSKEY_REQUIRED",
    "PASSKEY_CONFIRMATION_REQUIRED",
    "WORKING",
    "FAILED",
]

# WAHA webhook event names — full list from:
# https://waha.devlike.pro/docs/how-to/events/#events
WahaWebhookEventName = Literal[
    "session.status",
    "engine.event",
    "state.change",
    "message",
    "message.reaction",
    "message.any",
    "message.ack",
    "message.ack.group",
    "message.waiting",
    "message.edited",
    "message.revoked",
    "chat.archive",
    "group.v2.join",
    "group.v2.leave",
    "group.v2.participants",
    "group.v2.participants.join-request",
    "group.v2.update",
    "group.join",
    "group.leave",
    "label.upsert",
    "label.deleted",
    "label.chat.added",
    "label.chat.deleted",
    "presence.update",
    "poll.vote",
    "poll.vote.failed",
    "call.received",
    "call.accepted",
    "call.rejected",
]

WebhookEventInput = Union[WahaWebhookEventName, str]


class WahaWebhookEvents(StrEnum):
    """
    WAHA webhook event names (runtime enum for IDE autocomplete and presets).

    Official catalog and per-engine support:
    https://waha.devlike.pro/docs/how-to/events/#events
    """

    SESSION_STATUS = "session.status"
    ENGINE_EVENT = "engine.event"
    STATE_CHANGE = "state.change"
    MESSAGE = "message"
    MESSAGE_REACTION = "message.reaction"
    MESSAGE_ANY = "message.any"
    MESSAGE_ACK = "message.ack"
    MESSAGE_ACK_GROUP = "message.ack.group"
    MESSAGE_WAITING = "message.waiting"
    MESSAGE_EDITED = "message.edited"
    MESSAGE_REVOKED = "message.revoked"
    CHAT_ARCHIVE = "chat.archive"
    GROUP_V2_JOIN = "group.v2.join"
    GROUP_V2_LEAVE = "group.v2.leave"
    GROUP_V2_PARTICIPANTS = "group.v2.participants"
    GROUP_V2_PARTICIPANTS_JOIN_REQUEST = "group.v2.participants.join-request"
    GROUP_V2_UPDATE = "group.v2.update"
    GROUP_JOIN = "group.join"
    GROUP_LEAVE = "group.leave"
    LABEL_UPSERT = "label.upsert"
    LABEL_DELETED = "label.deleted"
    LABEL_CHAT_ADDED = "label.chat.added"
    LABEL_CHAT_DELETED = "label.chat.deleted"
    PRESENCE_UPDATE = "presence.update"
    POLL_VOTE = "poll.vote"
    POLL_VOTE_FAILED = "poll.vote.failed"
    CALL_RECEIVED = "call.received"
    CALL_ACCEPTED = "call.accepted"
    CALL_REJECTED = "call.rejected"

    @classmethod
    def default_subscription(cls) -> List[str]:
        """Minimal useful set: inbound messages + session health."""
        return [cls.MESSAGE.value, cls.SESSION_STATUS.value]

    @classmethod
    def chat_focused(cls) -> List[str]:
        """Chat automation without groups/calls (same as ``WAHAWebhook.COMMON_EVENTS``)."""
        return [
            cls.MESSAGE.value,
            cls.MESSAGE_ANY.value,
            cls.MESSAGE_ACK.value,
            cls.MESSAGE_REACTION.value,
            cls.MESSAGE_EDITED.value,
            cls.MESSAGE_REVOKED.value,
            cls.SESSION_STATUS.value,
        ]

    @classmethod
    def all_documented(cls) -> List[str]:
        """Every event name defined in this enum (not all engines support all)."""
        return [member.value for member in cls]


def _coerce_webhook_events(events: Optional[Sequence[WebhookEventInput]]) -> List[str]:
    if not events:
        return WahaWebhookEvents.default_subscription()
    result: List[str] = []
    for item in events:
        if isinstance(item, WahaWebhookEvents):
            result.append(item.value)
        else:
            result.append(str(item))
    return result


class WebhookCustomHeader(BaseModel):
    """Custom HTTP header on outbound webhook requests."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    value: str


class WebhookHmacConfig(BaseModel):
    """HMAC signing config for webhook payloads."""

    key: str


class WebhookRetryConfig(BaseModel):
    """WAHA webhook retry policy."""

    model_config = ConfigDict(populate_by_name=True)

    policy: RetryPolicy = "constant"
    delay_seconds: int = Field(2, alias="delaySeconds")
    attempts: int = 15


class WebhookConfig(BaseModel):
    """Single webhook entry for WAHA ``SessionConfig.webhooks``."""

    model_config = ConfigDict(populate_by_name=True)

    url: str
    events: List[str]
    hmac: Optional[WebhookHmacConfig] = None
    retries: Optional[WebhookRetryConfig] = Field(default_factory=WebhookRetryConfig)
    custom_headers: Optional[List[WebhookCustomHeader]] = Field(None, alias="customHeaders")

    def to_api_dict(self) -> Dict[str, Any]:
        """Serialize to WAHA JSON field names (camelCase)."""
        return self.model_dump(by_alias=True, exclude_none=True)


class WahaMessagePayload(BaseModel):
    """Subset of WAHA message fields on webhook / API payloads."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    body: Optional[str] = None
    from_me: Optional[bool] = Field(None, alias="fromMe")
    from_number: Optional[str] = Field(None, alias="from")
    chat_id: Optional[str] = Field(None, alias="chatId")
    message_id: Optional[str] = Field(None, alias="id")
    has_media: Optional[bool] = Field(None, alias="hasMedia")
    timestamp: Optional[int] = None


class WahaWebhookEvent(BaseModel):
    """Normalized WAHA webhook POST body."""

    model_config = ConfigDict(extra="allow")

    event: Optional[str] = None
    session: Optional[str] = None
    engine: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    raw: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_api_payload(cls, data: Dict[str, Any]) -> "WahaWebhookEvent":
        """Build from parsed JSON webhook body."""
        inner = data.get("payload", data)
        if not isinstance(inner, dict):
            inner = {"value": inner}
        return cls(
            event=data.get("event"),
            session=data.get("session"),
            engine=data.get("engine"),
            payload=inner,
            raw=data,
        )

    def to_legacy_dict(self) -> Dict[str, Any]:
        """Dict shape returned by ``WAHAWebhook.parse_event``."""
        return {
            "event": self.event,
            "session": self.session,
            "engine": self.engine,
            "payload": self.payload,
            "raw": self.raw,
        }

    @property
    def message(self) -> WahaMessagePayload:
        """Typed view of ``payload`` when the event carries a message."""
        return WahaMessagePayload.model_validate(self.payload)

    @property
    def is_incoming_message(self) -> bool:
        """True for inbound user messages (not ``fromMe``)."""
        if self.event not in ("message", "message.any"):
            return False
        return self.message.from_me is not True


class PhoneNormalizeParams(BaseModel):
    """Validated inputs for international phone normalization."""

    phone: str
    default_country_code: str = "91"
    country_code: Optional[str] = None

    @field_validator("default_country_code", "country_code", mode="before")
    @classmethod
    def _digits_only_cc(cls, value: Any) -> Any:
        if value is None:
            return value
        text = str(value).lstrip("+")
        return "".join(ch for ch in text if ch.isdigit()) or text

    def to_digits(self) -> str:
        """
        Return E.164 digits without ``+``.

        Ten-digit numbers are prefixed with ``country_code`` or
        ``default_country_code``.
        """
        digits = "".join(ch for ch in self.phone if ch.isdigit())
        if not digits:
            raise ValueError("phone must contain digits")

        cc = self.country_code if self.country_code is not None else self.default_country_code
        cc = "".join(ch for ch in str(cc) if ch.isdigit())
        if not cc:
            raise ValueError("country_code must contain digits")

        if len(digits) == 11 and digits.startswith("0"):
            digits = digits[1:]

        if len(digits) == 10:
            return f"{cc}{digits}"

        return digits


class WhatsAppClientConfig(BaseModel):
    """Client settings (constructor or ``from_env()``)."""

    model_config = ConfigDict(extra="ignore")

    base_url: str = "http://localhost:3000"
    api_key: Optional[str] = None
    session: str = "default"
    default_country_code: str = "91"
    timeout: int = 30
    max_retries: int = 2
    retry_delay: float = 1.0

    @classmethod
    def from_env(cls) -> "WhatsAppClientConfig":
        """Load settings from ``WAHA_*`` environment variables."""
        base = (
            os.getenv("WAHA_API_URL")
            or os.getenv("WAHA_BASE_URL")
            or os.getenv("WAHA_URL")
            or "http://localhost:3000"
        )
        return cls(
            base_url=base.rstrip("/"),
            api_key=os.getenv("WAHA_API_KEY"),
            session=os.getenv("WAHA_SESSION", "default"),
            default_country_code=os.getenv("WAHA_DEFAULT_COUNTRY_CODE", "91"),
        )


class WAHAAPIError(Exception):
    """Raised when the WAHA HTTP API returns an error."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class WAHAWebhook:
    """
    Helpers for receiving and verifying WAHA webhook HTTP callbacks.

    **Events:** use :class:`WahaWebhookEvents` or :data:`WahaWebhookEventName` —
    see https://waha.devlike.pro/docs/how-to/events/#events

    **HMAC:** you pick a secret string and pass it as ``hmac_key`` when configuring
    the session webhook. WAHA signs each POST body with HMAC-SHA512; your HTTP
    handler verifies with :meth:`verify_request` or :meth:`verify_hmac`.
    Use :meth:`generate_hmac_key` to create a random secret (store in env, not in git).
    """

    HEADER_HMAC = "X-Webhook-Hmac"
    HEADER_HMAC_ALGORITHM = "X-Webhook-Hmac-Algorithm"
    HEADER_REQUEST_ID = "X-Webhook-Request-Id"
    HEADER_TIMESTAMP = "X-Webhook-Timestamp"

    COMMON_EVENTS: tuple[str, ...] = tuple(WahaWebhookEvents.chat_focused())

    @staticmethod
    def generate_hmac_key(byte_length: int = 32) -> str:
        """
        Generate a cryptographically random webhook HMAC secret.

        You configure this value on WAHA (``hmac.key``) and store the same value on
        your webhook receiver (e.g. ``WAHA_WEBHOOK_HMAC_KEY``) to verify signatures.

        Args:
            byte_length: Entropy length before URL-safe encoding.

        Returns:
            URL-safe secret string suitable for ``hmac_key`` / ``WebhookHmacConfig.key``.
        """
        return secrets.token_urlsafe(byte_length)

    @staticmethod
    def build_config(
        url: str,
        events: Optional[Sequence[WebhookEventInput]] = None,
        hmac_key: Optional[str] = None,
        retries_policy: str = "constant",
        retries_delay_seconds: int = 2,
        retries_attempts: int = 15,
        custom_headers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Build a single webhook entry for ``SessionConfig.webhooks``.

        Args:
            url: Public HTTPS URL that receives POST callbacks.
            events: Event names — use :class:`WahaWebhookEvents` members or strings.
                Defaults to :meth:`WahaWebhookEvents.default_subscription`.
            hmac_key: Shared secret for HMAC (see :meth:`generate_hmac_key`). WAHA
                signs outbound webhooks; your app verifies with :meth:`verify_request`.
            retries_policy: ``constant``, ``linear``, or ``exponential``.
            retries_delay_seconds: Base delay between webhook retries.
            retries_attempts: Maximum retry attempts.
            custom_headers: Optional list of ``{"name": "...", "value": "..."}``.

        Returns:
            Webhook configuration dict suitable for session create/update.
        """
        headers: Optional[List[WebhookCustomHeader]] = None
        if custom_headers:
            headers = [WebhookCustomHeader.model_validate(h) for h in custom_headers]

        config = WebhookConfig(
            url=url,
            events=_coerce_webhook_events(events),
            hmac=WebhookHmacConfig(key=hmac_key) if hmac_key else None,
            retries=WebhookRetryConfig(
                policy=retries_policy,
                delay_seconds=retries_delay_seconds,
                attempts=retries_attempts,
            ),
            custom_headers=headers,
        )
        return config.to_api_dict()

    @staticmethod
    def compute_hmac(body: bytes, secret: str, algorithm: str = "sha512") -> str:
        """
        Compute the HMAC digest WAHA sends in ``X-Webhook-Hmac``.

        Args:
            body: Raw HTTP request body bytes.
            secret: Webhook ``hmac.key`` from session configuration.
            algorithm: Hash algorithm (WAHA uses ``sha512``).

        Returns:
            Lowercase hex digest string.
        """
        digest = algorithm.lower()
        if digest not in ("sha512", "sha256"):
            raise ValueError(f"Unsupported HMAC algorithm: {algorithm}")
        hash_fn = hashlib.sha512 if digest == "sha512" else hashlib.sha256
        return hmac.new(secret.encode("utf-8"), body, hash_fn).hexdigest()

    @classmethod
    def verify_hmac(
        cls,
        body: bytes,
        signature: str,
        secret: str,
        algorithm: str = "sha512",
    ) -> bool:
        """
        Verify ``X-Webhook-Hmac`` against the raw request body.

        Args:
            body: Raw HTTP request body bytes.
            signature: Value of the ``X-Webhook-Hmac`` header.
            secret: Webhook ``hmac.key``.
            algorithm: Value of ``X-Webhook-Hmac-Algorithm`` (default ``sha512``).

        Returns:
            True if the signature matches.
        """
        expected = cls.compute_hmac(body, secret, algorithm=algorithm)
        return hmac.compare_digest(expected, signature.strip().lower())

    @classmethod
    def verify_request(
        cls,
        body: bytes,
        headers: Mapping[str, str],
        secret: str,
    ) -> bool:
        """
        Verify a WAHA webhook HTTP request (headers + raw body).

        Reads ``X-Webhook-Hmac`` and ``X-Webhook-Hmac-Algorithm`` from headers.
        Header names are matched case-insensitively (works with Flask/FastAPI).

        Args:
            body: Raw request body bytes (before JSON parsing).
            headers: Request headers mapping.
            secret: Same value configured as ``hmac.key`` on the WAHA session.

        Returns:
            True if the signature is valid.
        """
        lowered = {name.lower(): value for name, value in headers.items()}
        signature = lowered.get(cls.HEADER_HMAC.lower())
        if not signature:
            return False
        algorithm = lowered.get(cls.HEADER_HMAC_ALGORITHM.lower(), "sha512")
        return cls.verify_hmac(body, signature, secret, algorithm=algorithm)

    @classmethod
    def require_valid_request(
        cls,
        body: bytes,
        headers: Mapping[str, str],
        secret: str,
    ) -> None:
        """
        Raise ``ValueError`` if the webhook signature is missing or invalid.

        Args:
            body: Raw request body bytes.
            headers: Request headers mapping.
            secret: Webhook HMAC secret.

        Raises:
            ValueError: If verification fails.
        """
        if not cls.verify_request(body, headers, secret):
            raise ValueError("Invalid or missing WAHA webhook HMAC signature")

    @staticmethod
    def parse_event_model(payload: Dict[str, Any]) -> WahaWebhookEvent:
        """
        Parse a webhook JSON body into a :class:`WahaWebhookEvent`.

        Args:
            payload: Parsed JSON from the webhook POST body.

        Returns:
            Validated event model.
        """
        return WahaWebhookEvent.from_api_payload(payload)

    @staticmethod
    def parse_event(payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a WAHA webhook JSON body (dict compatibility).

        Args:
            payload: Parsed JSON from the webhook POST body.

        Returns:
            Dict with keys ``event``, ``session``, ``engine``, ``payload``, and
            the original ``raw`` body.
        """
        return WAHAWebhook.parse_event_model(payload).to_legacy_dict()

    @staticmethod
    def is_incoming_message(event: Union[Dict[str, Any], WahaWebhookEvent]) -> bool:
        """Return True if the event is an inbound chat message."""
        if isinstance(event, WahaWebhookEvent):
            return event.is_incoming_message
        return WahaWebhookEvent.from_api_payload(event).is_incoming_message


class WhatsAppUtil:
    """
    Client for WAHA REST API (chat-focused).

    Attributes:
        base_url: WAHA server URL (e.g. ``http://localhost:3000``).
        session: Default session name (usually ``default``).
        api_key: Value for ``X-Api-Key`` header when WAHA auth is enabled.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        session: str = "default",
        default_country_code: str = "91",
        timeout: int = 30,
        max_retries: int = 2,
        retry_delay: float = 1.0,
        config: Optional[WhatsAppClientConfig] = None,
    ) -> None:
        """
        Initialize the WAHA client.

        Args:
            base_url: API base URL. Falls back to env ``WAHA_API_URL``,
                ``WAHA_BASE_URL``, ``WAHA_URL``, then ``http://localhost:3000``.
            api_key: API key. Falls back to env ``WAHA_API_KEY``.
            session: Default WhatsApp session name.
            default_country_code: Prepended to 10-digit local numbers (default India ``91``).
                Falls back to env ``WAHA_DEFAULT_COUNTRY_CODE``.
            timeout: HTTP timeout in seconds.
            max_retries: Retries on connection errors and HTTP 429.
            retry_delay: Base delay between retries (exponential backoff).
            config: Optional :class:`WhatsAppClientConfig` (overrides other args when set).
        """
        if config is not None:
            settings = config
        else:
            env_cfg = WhatsAppClientConfig.from_env()
            settings = WhatsAppClientConfig(
                base_url=(base_url or env_cfg.base_url).rstrip("/"),
                api_key=api_key if api_key is not None else env_cfg.api_key,
                session=session or env_cfg.session,
                default_country_code=default_country_code or env_cfg.default_country_code,
                timeout=timeout,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

        self.base_url = settings.base_url.rstrip("/")
        self.api_key = settings.api_key
        self.session = settings.session
        self.default_country_code = settings.default_country_code
        self.timeout = settings.timeout
        self.max_retries = settings.max_retries
        self.retry_delay = settings.retry_delay

        self._http = requests.Session()
        self._http.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        if self.api_key:
            self._http.headers["X-Api-Key"] = self.api_key

    @staticmethod
    def normalize_phone_digits(
        phone: str,
        default_country_code: str = "91",
        country_code: Optional[str] = None,
    ) -> str:
        """
        Normalize a phone string to international digits (no ``+``).

        Rules:
            - Strips ``+``, spaces, and other non-digits.
            - 10-digit numbers get ``default_country_code`` (default ``91``).
            - Longer numbers are treated as already including country code.
            - Leading trunk ``0`` is removed (e.g. ``09876543210`` → ``91`` + 10 digits).

        Args:
            phone: User input such as ``+919876543210``, ``919876543210``, or ``9876543210``.
            default_country_code: Used when only 10 local digits are present.
            country_code: Per-call override for ``default_country_code``.

        Returns:
            Digit string suitable for WAHA ``chatId`` local part (before ``@c.us``).
        """
        params = PhoneNormalizeParams(
            phone=phone,
            default_country_code=default_country_code,
            country_code=country_code,
        )
        return params.to_digits()

    @staticmethod
    def phone_to_chat_id(
        phone: str,
        suffix: str = "@c.us",
        default_country_code: str = "91",
        country_code: Optional[str] = None,
    ) -> str:
        """
        Convert a phone number to a WhatsApp ``chatId``.

        Args:
            phone: International number with or without leading ``+``, or 10-digit local.
            suffix: Chat suffix (``@c.us`` for direct chats, ``@g.us`` for groups).
            default_country_code: Prepended to 10-digit numbers (default ``91``).
            country_code: Optional override for ``default_country_code``.

        Returns:
            Chat id string, e.g. ``919876543210@c.us``.
        """
        raw = phone.strip()
        if "@" in raw:
            local, domain = raw.split("@", 1)
            if domain == "g.us":
                gid = "".join(ch for ch in local if ch.isdigit())
                return f"{gid}@g.us" if gid else raw
            local_digits = "".join(ch for ch in local if ch.isdigit())
            if not local_digits:
                return raw.lstrip("+")
            return f"{local_digits}@{domain}"

        digits = WhatsAppUtil.normalize_phone_digits(
            raw, default_country_code=default_country_code, country_code=country_code
        )
        return f"{digits}{suffix}"

    def normalize_chat_id(self, chat_id: str, country_code: Optional[str] = None) -> str:
        """
        Accept a WAHA ``chatId`` or a bare phone number and return ``chatId`` form.

        WAHA expects ids like ``919876543210@c.us``, not ``+919876543210``.
        Ten-digit local numbers use this client's ``default_country_code`` (``91`` by default).

        Args:
            chat_id: ``919876543210@c.us``, ``120363...@g.us``, ``...@lid``, or ``+91...``.
            country_code: Optional per-call country code override (e.g. ``"1"`` for US).

        Returns:
            Normalized chat id for API requests.
        """
        raw = chat_id.strip()
        if "@" in raw:
            local, domain = raw.split("@", 1)
            if domain in ("lid", "g.us") or len(local) > 15:
                digits = "".join(ch for ch in local if ch.isdigit())
                if digits:
                    return f"{digits}@{domain}"
                return raw
            digits = self.normalize_phone_digits(
                local,
                default_country_code=self.default_country_code,
                country_code=country_code,
            )
            return f"{digits}@{domain}"
        return self.phone_to_chat_id(
            raw,
            default_country_code=self.default_country_code,
            country_code=country_code,
        )

    @staticmethod
    def encode_chat_id(chat_id: str) -> str:
        """URL-encode a chat id for path segments."""
        return quote(chat_id, safe="@.")

    @staticmethod
    def file_from_url(
        url: str,
        mimetype: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build a WAHA remote ``file`` object (URL download on WAHA side).

        Args:
            url: Public URL to the media file.
            mimetype: MIME type. Guessed from filename/URL when omitted.
            filename: Optional filename metadata.

        Returns:
            ``file`` dict for sendImage/sendFile/etc.

        Note:
            Extensionless URLs (e.g. picsum.photos) often guess as
            ``application/octet-stream``. Use :meth:`send_image` / :meth:`send_video`
            so the client probes ``Content-Type`` and sets image/video MIME types.
        """
        guessed, _ = mimetypes.guess_type(filename or url)
        payload: Dict[str, Any] = {
            "url": url,
            "mimetype": mimetype or guessed or "application/octet-stream",
        }
        if filename:
            payload["filename"] = filename
        return payload

    def _probe_url_metadata(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        HTTP HEAD/GET probe for ``Content-Type`` and filename from final URL.

        Args:
            url: Remote media URL.

        Returns:
            Tuple of ``(mimetype, filename)`` — either may be ``None``.
        """
        try:
            response = self._http.head(url, allow_redirects=True, timeout=min(15, self.timeout))
            if response.status_code in (405, 501) or response.status_code >= 400:
                response = self._http.get(
                    url,
                    stream=True,
                    timeout=min(15, self.timeout),
                    headers={"Range": "bytes=0-0"},
                )
            content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            mimetype = content_type if content_type else None
            path_name = Path(urlparse(response.url).path).name
            filename = path_name if path_name and "." in path_name else None
            return mimetype, filename
        except requests.RequestException as exc:
            logger.debug("URL probe failed for %s: %s", url, exc)
            return None, None

    def _file_from_url_probed(
        self,
        url: str,
        mimetype: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Like :meth:`file_from_url` but probes HTTP headers when type is unknown."""
        payload = self.file_from_url(url, mimetype=mimetype, filename=filename)
        if payload["mimetype"] == "application/octet-stream" or not payload.get("filename"):
            probed_mime, probed_name = self._probe_url_metadata(url)
            if probed_mime and payload["mimetype"] == "application/octet-stream":
                payload["mimetype"] = probed_mime
            if probed_name and not payload.get("filename"):
                payload["filename"] = probed_name
        return payload

    @staticmethod
    def _apply_media_kind(file_obj: Dict[str, Any], media_kind: MediaKind) -> Dict[str, Any]:
        """
        Ensure ``mimetype`` / ``filename`` match WhatsApp media expectations.

        Wrong MIME (e.g. ``application/octet-stream`` on ``sendImage``) causes WhatsApp
        to show a generic document without image preview.
        """
        if media_kind == "file":
            return file_obj

        mime = (file_obj.get("mimetype") or "application/octet-stream").split(";")[0].strip().lower()
        filename = file_obj.get("filename")

        if media_kind == "image":
            if not mime.startswith("image/"):
                mime = _DEFAULT_MIME["image"]
        elif media_kind == "video":
            if not mime.startswith("video/"):
                mime = _DEFAULT_MIME["video"]
        elif media_kind == "voice":
            if not mime.startswith("audio/"):
                mime = "audio/ogg"
        elif media_kind == "sticker":
            mime = "image/webp"

        file_obj["mimetype"] = mime
        if not filename:
            ext = mimetypes.guess_extension(mime.split(";")[0].strip()) or ".bin"
            if ext == ".jpe":
                ext = ".jpg"
            prefix = {"image": "image", "video": "video", "voice": "audio", "sticker": "sticker"}.get(
                media_kind, "file"
            )
            file_obj["filename"] = f"{prefix}{ext}"
        return file_obj

    @staticmethod
    def file_from_path(path: Union[str, Path], mimetype: Optional[str] = None) -> Dict[str, Any]:
        """
        Build a WAHA base64 ``file`` object from a local path.

        Args:
            path: Path to the file on disk.
            mimetype: MIME type. Guessed from extension when omitted.

        Returns:
            ``file`` dict with ``data`` (base64) and ``mimetype``.
        """
        file_path = Path(path)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        guessed, _ = mimetypes.guess_type(str(file_path))
        data = base64.b64encode(file_path.read_bytes()).decode("ascii")
        return {
            "mimetype": mimetype or guessed or "application/octet-stream",
            "filename": file_path.name,
            "data": data,
        }

    def _chat_path(self, chat_id: str, *parts: str) -> str:
        encoded = self.encode_chat_id(self.normalize_chat_id(chat_id))
        tail = "/".join(parts)
        if tail:
            return f"/api/{self.session}/chats/{encoded}/{tail}"
        return f"/api/{self.session}/chats/{encoded}"

    def _with_session(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = dict(payload)
        body.setdefault("session", self.session)
        return body

    def _chat_body(
        self,
        chat_id: str,
        *,
        country_code: Optional[str] = None,
        **fields: Any,
    ) -> Dict[str, Any]:
        """Build a JSON body with normalized ``chatId`` and default session."""
        return self._with_session(
            {"chatId": self.normalize_chat_id(chat_id, country_code=country_code), **fields}
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        *,
        expect_json: bool = True,
    ) -> Any:
        """
        Send an HTTP request to WAHA with optional retries.

        Args:
            method: HTTP method.
            endpoint: Path beginning with ``/api/...``.
            params: Query parameters.
            json_data: JSON request body.
            expect_json: When False, return raw ``Response`` on success.

        Returns:
            Parsed JSON, ``None`` for empty bodies, or a ``Response`` when
            ``expect_json`` is False.

        Raises:
            WAHAAPIError: On HTTP or connection failure.
        """
        url = f"{self.base_url}{endpoint}"
        attempts = max(1, self.max_retries + 1)
        last_error: Optional[Exception] = None

        for attempt in range(attempts):
            try:
                response = self._http.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                    timeout=self.timeout,
                )
                if response.status_code == 429 and attempt < attempts - 1:
                    retry_after = float(response.headers.get("Retry-After", self.retry_delay))
                    time.sleep(retry_after)
                    continue
                if response.status_code >= 400:
                    detail: Any = response.text
                    try:
                        if response.content:
                            detail = response.json()
                    except Exception:
                        pass
                    raise WAHAAPIError(
                        f"HTTP {response.status_code}: {detail}",
                        status_code=response.status_code,
                        response=response,
                    )
                if not expect_json:
                    return response
                if not response.content:
                    return None
                return response.json()
            except WAHAAPIError:
                raise
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt < attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                raise WAHAAPIError(f"Connection error after {attempts} attempts: {exc}") from exc
            except requests.RequestException as exc:
                raise WAHAAPIError(f"HTTP error: {exc}") from exc

        raise WAHAAPIError(f"Request failed after {attempts} attempts: {last_error}")

    def get_version(self) -> Dict[str, Any]:
        """Return WAHA version information."""
        return self._request("GET", "/api/version")

    def list_sessions(self, all_sessions: bool = False) -> List[Dict[str, Any]]:
        """
        List WhatsApp sessions.

        Args:
            all_sessions: Include sessions in ``STOPPED`` state.

        Returns:
            List of session info dicts.
        """
        params = {"all": all_sessions} if all_sessions else None
        result = self._request("GET", "/api/sessions", params=params)
        return result if isinstance(result, list) else []

    def get_session(self, session: Optional[str] = None) -> Dict[str, Any]:
        """Get information for one session."""
        name = session or self.session
        return self._request("GET", f"/api/sessions/{name}")

    def create_session(
        self,
        name: Optional[str] = None,
        start: bool = True,
        config: Optional[Dict[str, Any]] = None,
        webhooks: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Create (and optionally start) a WAHA session.

        Args:
            name: Session name. Defaults to the client's ``session``.
            start: Start the session immediately after creation.
            config: Full ``SessionConfig`` dict (metadata, debug, etc.).
            webhooks: Shortcut — merged into ``config.webhooks`` if provided.

        Returns:
            Created session DTO.
        """
        session_name = name or self.session
        body: Dict[str, Any] = {"name": session_name, "start": start}
        if config or webhooks:
            cfg = dict(config or {})
            if webhooks:
                cfg["webhooks"] = webhooks
            body["config"] = cfg
        return self._request("POST", "/api/sessions", json_data=body)

    def update_session(
        self,
        config: Optional[Dict[str, Any]] = None,
        webhooks: Optional[List[Dict[str, Any]]] = None,
        session: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update session configuration (including webhooks).

        Args:
            config: ``SessionConfig`` fields to apply.
            webhooks: Replaces or sets ``config.webhooks`` when given.
            session: Session name override.

        Returns:
            Updated session info.
        """
        name = session or self.session
        body: Dict[str, Any] = {}
        if config or webhooks:
            cfg = dict(config or {})
            if webhooks is not None:
                cfg["webhooks"] = webhooks
            body["config"] = cfg
        return self._request("PUT", f"/api/sessions/{name}", json_data=body)

    def configure_webhooks(
        self,
        url: str,
        events: Optional[Sequence[WebhookEventInput]] = None,
        hmac_key: Optional[str] = None,
        *,
        generate_hmac_key: bool = False,
        **webhook_kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Register webhooks on the current session (PUT session config).

        Args:
            url: Webhook receiver URL.
            events: Subscribed events — :class:`WahaWebhookEvents` or strings.
                Presets: ``WahaWebhookEvents.chat_focused()``,
                ``WahaWebhookEvents.default_subscription()``,
                ``WAHAWebhook.COMMON_EVENTS``.
            hmac_key: Shared HMAC secret (must match your receiver's env var).
            generate_hmac_key: If True and ``hmac_key`` is omitted, generate a new
                secret via :meth:`WAHAWebhook.generate_hmac_key` (log/store it).
            **webhook_kwargs: Passed to ``WAHAWebhook.build_config`` (retries, headers).

        Returns:
            Updated session info.
        """
        secret = hmac_key
        if secret is None and generate_hmac_key:
            secret = WAHAWebhook.generate_hmac_key()
            logger.info("Generated WAHA webhook HMAC key (store securely): %s", secret)
        hook = WAHAWebhook.build_config(url, events=events, hmac_key=secret, **webhook_kwargs)
        return self.update_session(webhooks=[hook])

    def delete_session(self, session: Optional[str] = None) -> None:
        """Delete a session."""
        name = session or self.session
        self._request("DELETE", f"/api/sessions/{name}")

    def start_session(self, session: Optional[str] = None) -> Dict[str, Any]:
        """Start a stopped session."""
        name = session or self.session
        return self._request("POST", f"/api/sessions/{name}/start")

    def stop_session(self, session: Optional[str] = None) -> Dict[str, Any]:
        """Stop a running session."""
        name = session or self.session
        return self._request("POST", f"/api/sessions/{name}/stop")

    def restart_session(self, session: Optional[str] = None) -> Dict[str, Any]:
        """Restart a session."""
        name = session or self.session
        return self._request("POST", f"/api/sessions/{name}/restart")

    def logout_session(self, session: Optional[str] = None) -> Dict[str, Any]:
        """Log out WhatsApp on the session (requires re-pairing)."""
        name = session or self.session
        return self._request("POST", f"/api/sessions/{name}/logout")

    def get_qr_code(self, format: str = "raw", session: Optional[str] = None) -> Any:
        """
        Get pairing QR code for the session.

        Args:
            format: ``raw`` (JSON with base64) or ``image`` (PNG bytes).
            session: Session name override.

        Returns:
            JSON dict when ``format=raw``, else PNG bytes.
        """
        name = session or self.session
        if format not in ("raw", "image"):
            raise ValueError("format must be 'raw' or 'image'")
        if format == "image":
            response = self._request(
                "GET",
                f"/api/{name}/auth/qr",
                params={"format": "image"},
                expect_json=False,
            )
            return response.content
        return self._request("GET", f"/api/{name}/auth/qr", params={"format": "raw"})

    def get_screenshot(self, session: Optional[str] = None) -> bytes:
        """
        Capture a JPEG screenshot of the linked WhatsApp session (WEBJS/WPP).

        Args:
            session: Session name override.

        Returns:
            JPEG image bytes.
        """
        name = session or self.session
        response = self._request(
            "GET",
            "/api/screenshot",
            params={"session": name},
            expect_json=False,
        )
        return response.content

    def send_text(
        self,
        chat_id: str,
        text: str,
        *,
        reply_to: Optional[str] = None,
        link_preview: bool = True,
        link_preview_high_quality: bool = False,
        message_id: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a plain text message (supports WhatsApp formatting: *bold*, _italic_, etc.).

        Args:
            chat_id: Target chat id or phone (e.g. ``12132132130@c.us`` or ``+91...``).
            text: Message body. Include URLs to enable link previews when allowed.
            reply_to: Optional message id to reply to.
            link_preview: Enable automatic link preview for URLs in text.
            link_preview_high_quality: Request higher-quality previews when supported.
            message_id: Optional pre-generated message id.
            country_code: Override default country code for 10-digit numbers (e.g. ``"1"``).

        Returns:
            Sent message object from WAHA.
        """
        payload = self._chat_body(
            chat_id,
            country_code=country_code,
            text=text,
            linkPreview=link_preview,
            linkPreviewHighQuality=link_preview_high_quality,
        )
        if reply_to:
            payload["reply_to"] = reply_to
        if message_id:
            payload["id"] = message_id
        return self._request("POST", "/api/sendText", json_data=payload)

    def send_link_preview(
        self,
        chat_id: str,
        text: str,
        preview: Dict[str, Any],
        *,
        reply_to: Optional[str] = None,
        link_preview_high_quality: bool = True,
    ) -> Dict[str, Any]:
        """
        Send text with a custom link preview (title, description, image URL).

        Args:
            chat_id: Target chat.
            text: Must contain the same URL as ``preview["url"]``.
            preview: Link preview fields (``url``, ``title``, ``description``, etc.).
            reply_to: Optional message id to reply to.
            link_preview_high_quality: Use HQ preview when supported.

        Returns:
            API response dict.
        """
        payload = self._chat_body(
            chat_id,
            text=text,
            preview=preview,
            linkPreviewHighQuality=link_preview_high_quality,
        )
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/send/link-custom-preview", json_data=payload)

    def _resolve_file(self, file: FileInput, media_kind: MediaKind = "file") -> Dict[str, Any]:
        """
        Turn user input into WAHA's JSON ``file`` object.

        Accepts:
            - ``pathlib.Path`` or path ``str`` → base64 upload via :meth:`file_from_path`
            - ``http(s)://...`` ``str`` → WAHA downloads via :meth:`_file_from_url_probed`
            - ``dict`` → already ``{"url": ...}`` or ``{"data": ..., "mimetype": ...}``

        Args:
            file: Path, URL, or WAHA file dict.
            media_kind: ``image`` / ``video`` / ``voice`` / ``sticker`` / ``file`` — fixes MIME
                so WhatsApp shows previews (not a bare document).
        """
        resolved: Dict[str, Any]
        if isinstance(file, dict):
            resolved = dict(file)
        elif isinstance(file, Path):
            resolved = self.file_from_path(file)
        elif isinstance(file, str):
            if file.startswith(("http://", "https://")):
                resolved = self._file_from_url_probed(file)
            else:
                path = Path(file)
                if path.is_file():
                    resolved = self.file_from_path(path)
                else:
                    raise ValueError(
                        "file must be a Path, local path str, http(s) URL str, or WAHA file dict"
                    )
        else:
            raise ValueError(
                "file must be a Path, local path str, http(s) URL str, or WAHA file dict"
            )
        return self._apply_media_kind(resolved, media_kind)

    def send_image(
        self,
        chat_id: str,
        file: FileInput,
        *,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send an image (URL, local path, or WAHA file dict)."""
        payload = self._chat_body(chat_id, file=self._resolve_file(file, media_kind="image"))
        if caption:
            payload["caption"] = caption
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendImage", json_data=payload)

    def send_file(
        self,
        chat_id: str,
        file: FileInput,
        *,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a document or generic file."""
        payload = self._chat_body(chat_id, file=self._resolve_file(file, media_kind="file"))
        if caption:
            payload["caption"] = caption
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendFile", json_data=payload)

    def send_voice(
        self,
        chat_id: str,
        file: FileInput,
        *,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a voice note (OGG/Opus recommended)."""
        payload = self._chat_body(chat_id, file=self._resolve_file(file, media_kind="voice"))
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendVoice", json_data=payload)

    def send_video(
        self,
        chat_id: str,
        file: FileInput,
        *,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a video file."""
        payload = self._chat_body(chat_id, file=self._resolve_file(file, media_kind="video"))
        if caption:
            payload["caption"] = caption
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendVideo", json_data=payload)

    def send_sticker(self, chat_id: str, file: FileInput, *, reply_to: Optional[str] = None) -> Dict[str, Any]:
        """Send a WebP sticker (PNG/JPEG are not converted)."""
        payload = self._chat_body(chat_id, file=self._resolve_file(file, media_kind="sticker"))
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendSticker", json_data=payload)

    def send_location(
        self,
        chat_id: str,
        latitude: float,
        longitude: float,
        title: str,
        *,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a map location pin."""
        payload = self._chat_body(chat_id, latitude=latitude, longitude=longitude, title=title)
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendLocation", json_data=payload)

    def send_contact(
        self,
        chat_id: str,
        contacts: List[Dict[str, Any]],
        *,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send one or more contacts (vCard or structured contact dict).

        Args:
            chat_id: Target chat.
            contacts: List of ``{"vcard": "..."}`` or WAHA ``Contact`` objects.
            reply_to: Optional message id to reply to.
        """
        payload = self._chat_body(chat_id, contacts=contacts)
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendContactVcard", json_data=payload)

    def send_poll(
        self,
        chat_id: str,
        question: str,
        options: List[str],
        *,
        multiple_answers: bool = False,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a poll message."""
        payload = self._chat_body(
            chat_id,
            poll={"name": question, "options": options, "multipleAnswers": multiple_answers},
        )
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendPoll", json_data=payload)

    def send_list(
        self,
        chat_id: str,
        message: Dict[str, Any],
        *,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send an interactive list message.

        Args:
            chat_id: Target chat.
            message: List structure (``title``, ``button``, ``sections``, etc.).
            reply_to: Optional message id to reply to.
        """
        payload = self._chat_body(chat_id, message=message)
        if reply_to:
            payload["reply_to"] = reply_to
        return self._request("POST", "/api/sendList", json_data=payload)

    def forward_message(self, chat_id: str, message_id: str) -> Dict[str, Any]:
        """Forward an existing message to another chat."""
        payload = self._chat_body(chat_id, messageId=message_id)
        return self._request("POST", "/api/forwardMessage", json_data=payload)

    def send_seen(
        self,
        chat_id: str,
        message_ids: Optional[List[str]] = None,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Mark messages as read (blue ticks)."""
        payload = self._chat_body(chat_id)
        if message_ids:
            payload["messageIds"] = message_ids
        if participant:
            payload["participant"] = participant
        return self._request("POST", "/api/sendSeen", json_data=payload)

    def start_typing(self, chat_id: str) -> None:
        """Show typing indicator in a chat."""
        self._request("POST", "/api/startTyping", json_data=self._chat_body(chat_id))

    def stop_typing(self, chat_id: str) -> None:
        """Clear typing indicator in a chat."""
        self._request("POST", "/api/stopTyping", json_data=self._chat_body(chat_id))

    def set_reaction(self, message_id: str, reaction: str) -> Dict[str, Any]:
        """
        React to a message with an emoji (empty string removes the reaction).

        Args:
            message_id: Full WAHA message id.
            reaction: Emoji string, e.g. ``👍``.
        """
        payload = self._with_session({"messageId": message_id, "reaction": reaction})
        return self._request("PUT", "/api/reaction", json_data=payload)

    def list_chats(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        merge: bool = True,
    ) -> Any:
        """List chats for the session."""
        params: Dict[str, Any] = {"merge": merge}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if sort_by:
            params["sortBy"] = sort_by
        if sort_order:
            params["sortOrder"] = sort_order
        return self._request("GET", f"/api/{self.session}/chats", params=params)

    def get_chats_overview(
        self,
        limit: int = 20,
        offset: Optional[int] = None,
        chat_ids: Optional[List[str]] = None,
        merge: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return chat summaries for inbox-style UIs."""
        params: Dict[str, Any] = {"limit": limit, "merge": merge}
        if offset is not None:
            params["offset"] = offset
        if chat_ids:
            params["ids"] = chat_ids
        result = self._request("GET", f"/api/{self.session}/chats/overview", params=params)
        return result if isinstance(result, list) else []

    def get_messages(
        self,
        chat_id: str,
        limit: int = 20,
        offset: Optional[int] = None,
        download_media: bool = False,
        sort_order: str = "desc",
    ) -> Any:
        """
        Fetch messages from a chat.

        Args:
            chat_id: Chat id.
            limit: Maximum messages to return.
            offset: Skip count for pagination.
            download_media: Ask WAHA to attach downloaded media metadata.
            sort_order: ``desc`` (newest first) or ``asc``.
        """
        params: Dict[str, Any] = {
            "limit": limit,
            "sortOrder": sort_order,
            "downloadMedia": download_media,
        }
        if offset is not None:
            params["offset"] = offset
        return self._request("GET", self._chat_path(chat_id, "messages"), params=params)

    def get_message(
        self,
        chat_id: str,
        message_id: str,
        download_media: bool = False,
    ) -> Dict[str, Any]:
        """Fetch a single message by id."""
        params = {"downloadMedia": download_media}
        encoded_msg = quote(message_id, safe="")
        return self._request(
            "GET",
            f"{self._chat_path(chat_id, 'messages')}/{encoded_msg}",
            params=params,
        )

    def delete_message(self, chat_id: str, message_id: str) -> None:
        """Delete a message for everyone (when supported by the engine)."""
        encoded_msg = quote(message_id, safe="")
        self._request("DELETE", f"{self._chat_path(chat_id, 'messages')}/{encoded_msg}")

    def edit_message(self, chat_id: str, message_id: str, text: str, link_preview: bool = True) -> Dict[str, Any]:
        """Edit a previously sent text message."""
        encoded_msg = quote(message_id, safe="")
        body = {"text": text, "linkPreview": link_preview}
        return self._request("PUT", f"{self._chat_path(chat_id, 'messages')}/{encoded_msg}", json_data=body)

    def mark_chat_read(
        self,
        chat_id: str,
        messages: Optional[int] = None,
        days: int = 7,
    ) -> Dict[str, Any]:
        """Mark unread messages in a chat as read."""
        params: Dict[str, Any] = {"days": days}
        if messages is not None:
            params["messages"] = messages
        return self._request("POST", self._chat_path(chat_id, "messages/read"), params=params)

    def delete_chat(self, chat_id: str) -> None:
        """Delete a chat thread."""
        self._request("DELETE", self._chat_path(chat_id))

    def check_phone_exists(
        self,
        phone: str,
        session: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Check whether a phone number is registered on WhatsApp."""
        normalized = self.normalize_phone_digits(
            phone,
            default_country_code=self.default_country_code,
            country_code=country_code,
        )
        params = {"phone": normalized, "session": session or self.session}
        return self._request("GET", "/api/contacts/check-exists", params=params)

    def get_contact(self, contact_id: str, session: Optional[str] = None) -> Dict[str, Any]:
        """Get basic contact info by id (``...@c.us``)."""
        params = {"contactId": contact_id, "session": session or self.session}
        return self._request("GET", "/api/contacts", params=params)

    def close(self) -> None:
        """Close the HTTP session."""
        self._http.close()

    def __repr__(self) -> str:
        key_hint = f"{self.api_key[:4]}..." if self.api_key else "none"
        return f"WhatsAppUtil(base_url='{self.base_url}', session='{self.session}', api_key='{key_hint}')"

    def __enter__(self) -> "WhatsAppUtil":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    assert WhatsAppUtil.normalize_phone_digits("+919876543210") == "919876543210"
    assert WhatsAppUtil.normalize_phone_digits("919876543210") == "919876543210"
    assert WhatsAppUtil.normalize_phone_digits("9876543210") == "919876543210"
    assert WhatsAppUtil.normalize_phone_digits("09876543210") == "919876543210"
    assert WhatsAppUtil.normalize_phone_digits("4155551234", country_code="1") == "14155551234"
    wa = WhatsAppUtil()
    assert wa.normalize_chat_id("9876543210") == "919876543210@c.us"
    assert wa.normalize_chat_id("+91 98765 43210") == "919876543210@c.us"
    print("phone normalization self-check OK")

    body = b'{"event":"message","session":"default","engine":"WEBJS"}'
    secret = "my-secret-key"
    expected = (
        "208f8a55dde9e05519e898b10b89bf0d0b3b0fdf11fdbf09b6b90476301b98d"
        "8097c462b2b17a6ce93b6b47a136cf2e78a33a63f6752c2c1631777076153fa89"
    )
    assert WAHAWebhook.verify_hmac(body, expected, secret)
    hook = WAHAWebhook.build_config("https://example.com/hook", hmac_key="secret")
    assert hook["retries"]["delaySeconds"] == 2
    assert hook["hmac"]["key"] == "secret"
    evt = WahaWebhookEvent.from_api_payload(
        {"event": "message", "session": "default", "payload": {"body": "hi", "fromMe": False}}
    )
    assert evt.is_incoming_message and evt.message.body == "hi"
    print("Pydantic models self-check OK")
    print("WAHAWebhook HMAC self-check OK")

    wa = WhatsAppUtil()
    print(f"Client: {wa}")
    try:
        print("Version:", wa.get_version())
    except WAHAAPIError as err:
        print(f"WAHA not reachable (expected if container is down): {err}")