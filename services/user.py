"""
UserService — user accounts, credentials, and their lookup indexes.

Storage layout (the "multiple prefixes" case that belongs in services/):
    USER:<user_id>                    HASH  — the User record
    USER:EMAIL:<sha256(email)>        STRING — email → user_id (O(1) lookup,
                                       hashed so raw PII never appears in
                                       Redis key names)
    USER:APIKEY:<sha256(api_key)>     STRING — api_key → user_id (O(1) auth
                                       lookup for the future X-Api-Key header;
                                       hashed so a Redis leak doesn't hand out
                                       usable keys)

Indexes are kept in sync with the record on create/update/rotate/delete.
Users are persistent — NO TTL anywhere in this service.

Layering: configs/modules/models only — never tasks/api (AGENTS §2).
"""

from configs import get_redis_hash, get_redis_str, get_settings
from models import User
from modules import get_logger, get_timestamp
from modules.redis_util import RedisHashUtil, RedisStringUtil


logger = get_logger(__name__, show_time=True)


class UserService:
    """
    Full user lifecycle: create, read, update (whitelisted fields),
    password/api-key rotation, authentication lookups, delete.

    Use the process-wide singleton ``user_service``. All methods are sync
    (Huey-worker compatible); the async API layer must offload them
    (run_in_executor/AnyIO.to_thread per AGENTS §10).
    """

    # Fields update() may touch. NOT updatable here: user_id, api_key
    # (rotate_api_key), password_hash (update_password), email (re-indexing
    # logic not built yet — add a dedicated method when needed),
    # created_at/updated_at (service-managed).
    _UPDATABLE_FIELDS = frozenset(
        {"full_name", "webhook_url", "is_active", "extra_info"}
    )

    def __init__(self) -> None:
        user_key = get_settings().redis_keys.users
        # Hash store for the records — persistent (no default_ttl).
        self.store = get_redis_hash(prefix=user_key)
        # O(1) lookup maps, also under the USER namespace.
        self.email_index: RedisStringUtil = get_redis_str(
            prefix=f"{user_key}:EMAIL"
        )
        self.apikey_index: RedisStringUtil = get_redis_str(
            prefix=f"{user_key}:APIKEY"
        )

    # ── key derivation ──────────────────────────────────────────────

    @staticmethod
    def _email_index_key(email: str) -> str:
        """Hashed email → index key (PII never lands in key names)."""
        return RedisHashUtil.hash_sensitive_data(data=email.strip().lower())

    @staticmethod
    def _apikey_index_key(api_key: str) -> str:
        return RedisHashUtil.hash_sensitive_data(data=api_key)

    @staticmethod
    def generate_api_key() -> str:
        """New API key, e.g. ``wms_AbC...`` (secret-toolkit generator)."""
        return RedisHashUtil.generate_custom_secret("wms", total_length=64)

    # ── CRUD ────────────────────────────────────────────────────────

    def create(
        self,
        full_name: str,
        email: str,
        password: str,
        webhook_url: str | None = None,
        extra_info: dict | None = None,
    ) -> tuple[User, str]:
        """
        Register a user. Raises ValueError when the email is already
        registered.

        Returns ``(user, api_key)`` — the api_key PLAINTEXT, shown to the
        owner exactly ONCE. Only its sha256 is persisted (record field +
        index key); losing it means rotate_api_key().
        """
        now = get_timestamp()
        api_key = self.generate_api_key()
        user = User(
            user_id=RedisHashUtil.generate_uuid7(),
            full_name=full_name,
            email=email,  # model validator normalizes (lower/strip)
            webhook_url=webhook_url,
            api_key_hash=self._apikey_index_key(api_key),
            password_hash=RedisHashUtil.hash_password(password),
            created_at=now,
            updated_at=now,
            extra_info=extra_info,
        )

        email_idx = self._email_index_key(user.email)
        if self.email_index.exists(email_idx):
            raise ValueError(f"email already registered: {user.email}")

        # Raises ValueError on the (astronomically unlikely) uuid7 collision.
        self.store.create(data=user.model_dump(mode="json"), id=user.user_id)
        try:
            # overwrite=False → a freak hash collision raises loudly instead
            # of silently stealing someone's lookup.
            self.email_index.set(email_idx, user.user_id)
            self.apikey_index.set(user.api_key_hash, user.user_id)
        except ValueError:
            self.store.delete(user.user_id)  # no orphan records
            raise

        logger.info(f"User created: {user.user_id}, email={user.email}")
        return user, api_key

    def get(self, user_id: str) -> User | None:
        """Load by id (raw — inactive users still come back; auth callers
        must check ``is_active`` themselves or use authenticate_*)."""
        data = self.store.read(user_id)
        return User(**data) if data else None

    def find_by_email(self, email: str) -> User | None:
        """O(1) email → user lookup via the index (no SCAN)."""
        user_id = self.email_index.get(self._email_index_key(email))
        return self.get(user_id) if user_id else None

    def authenticate_api_key(self, api_key: str) -> User | None:
        """X-Api-Key header check (future): O(1) index → user; None when
        unknown OR inactive."""
        user_id = self.apikey_index.get(self._apikey_index_key(api_key))
        if not user_id:
            return None
        user = self.get(user_id)
        if user and user.is_active:
            return user
        return None

    def authenticate(self, email: str, password: str) -> User | None:
        """Email + password login check; None on any mismatch/inactive."""
        user = self.find_by_email(email)
        if not user or not user.is_active:
            return None
        if not RedisHashUtil.verify_password(password, user.password_hash):
            return None
        return user

    def update(self, user_id: str, **fields) -> User | None:
        """
        Patch whitelisted fields (``_UPDATABLE_FIELDS``) and stamp
        ``updated_at``. Unknown/disallowed fields raise ValueError (call
        rotate_api_key / update_password for credentials). Returns the
        updated User, or None if the user doesn't exist.
        """
        user = self.get(user_id)
        if user is None:
            return None

        bad = set(fields) - self._UPDATABLE_FIELDS
        if bad:
            raise ValueError(
                f"fields not updatable via update(): {sorted(bad)} — use the "
                "dedicated methods (rotate_api_key / update_password)"
            )

        user = user.model_copy(
            update={**fields, "updated_at": get_timestamp()}
        )
        self.store.update(
            user_id,
            user.model_dump(
                mode="json",
                include=self._UPDATABLE_FIELDS | {"updated_at"},
            ),
        )
        logger.info(f"User updated: {user_id}, fields={sorted(fields)}")
        return user

    def update_password(self, user_id: str, new_password: str) -> User | None:
        """
        Re-hash + persist the password, stamp updated_at.

        Takes NO old password — this is the forgot-password/reset primitive:
        the future API layer verifies identity first (OTP to the account
        email/phone, or a signed reset token), THEN calls this. A separate
        "change password (knows old)" check, if wanted, belongs in the API
        layer: authenticate() → update_password().
        """
        user = self.get(user_id)
        if user is None:
            return None
        user.password_hash = RedisHashUtil.hash_password(new_password)
        user.updated_at = get_timestamp()
        self.store.update(
            user_id,
            {"password_hash": user.password_hash, "updated_at": user.updated_at},
        )
        logger.info(f"Password updated for user {user_id}")
        return user

    def rotate_api_key(self, user_id: str) -> tuple[User, str] | None:
        """
        Issue a new api_key, re-index, return ``(user, plaintext)`` —
        plaintext shown ONCE. The old key stops working immediately
        (its index entry is deleted via the stored hash — no plaintext
        needed).
        """
        user = self.get(user_id)
        if user is None:
            return None
        self.apikey_index.delete(user.api_key_hash)
        api_key = self.generate_api_key()
        user.api_key_hash = self._apikey_index_key(api_key)
        user.updated_at = get_timestamp()
        self.store.update(
            user_id,
            {"api_key_hash": user.api_key_hash, "updated_at": user.updated_at},
        )
        self.apikey_index.set(user.api_key_hash, user.user_id)
        logger.info(f"API key rotated for user {user_id}")
        return user, api_key

    def delete(self, user_id: str) -> bool:
        """Hard delete: record + both indexes. (Soft-disable = update
        is_active=False.) Returns False if the user didn't exist."""
        user = self.get(user_id)
        if user is None:
            return False
        self.email_index.delete(self._email_index_key(user.email))
        self.apikey_index.delete(user.api_key_hash)
        self.store.delete(user_id)
        logger.info(f"User deleted: {user_id}")
        return True


# Process-wide singleton — import-safe (get_redis_* PING eagerly, same
# pattern as tasks/ and services/otp).
user_service = UserService()
