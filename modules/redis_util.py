"""
Redis Utility Classes
A production-ready, all-in-one utility for Redis (Valkey) operations.

Modules:
    RedisHashUtil     — Hash-based persistent storage (database replacement).
    RedisStringUtil   — String-based key-value storage with optional TTL.
    RedisCache        — Caching layer built on top of RedisStringUtil.
"""

from __future__ import annotations

import csv
import functools
import hashlib
import hmac
import io
import json
import secrets
import string
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

try:
    import redis.asyncio as aioredis
    import redis
    from redis.lock import Lock
except ImportError:
    raise ImportError("Install redis: pip install redis")

try:
    import bcrypt
except ImportError:
    raise ImportError("Install bcrypt: pip install bcrypt")


T = TypeVar("T")

_REDIS_URL_SCHEMES: tuple = ("redis", "rediss", "unix")
HAS_NATIVE_UUID7 = hasattr(uuid, "uuid7")


def validate_redis_connection(url: str, timeout: float = 5.0) -> None:
    """
    Validate a Redis/Valkey connection URL and verify connectivity.

    Called automatically at the start of every class constructor in this
    module (``RedisHashUtil``, ``RedisStringUtil``, ``RedisCache``) so that
    invalid URLs and unreachable servers are caught at instantiation time
    instead of failing silently on the first operation.

    The URL is first checked for structure (supported scheme and a host),
    then a lightweight ``PING`` probe is issued against the server.

    Args:
        url: Redis connection URL, e.g. ``"redis://localhost:6379/0"``.
        timeout: Socket timeout in seconds for the connectivity probe.
                 Defaults to 5.0.

    Raises:
        ValueError: If the URL is empty, malformed, or uses a scheme other
            than ``redis``, ``rediss``, or ``unix``.
        ConnectionError: If the Redis server cannot be reached or does not
            respond to ``PING``.

    Example:
        >>> validate_redis_connection("redis://localhost:6379/0")
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("Redis connection URL cannot be empty.")
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError as exc:
        raise ValueError(f"Malformed Redis connection URL: {url!r}") from exc
    if parsed.scheme not in _REDIS_URL_SCHEMES:
        raise ValueError(
            f"Unsupported Redis URL scheme {parsed.scheme!r} in {url!r}. "
            f"Expected one of: {', '.join(_REDIS_URL_SCHEMES)}."
        )
    if parsed.scheme != "unix" and not parsed.hostname:
        raise ValueError(f"Redis connection URL is missing a host: {url!r}")

    probe = redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=timeout,
        socket_timeout=timeout,
    )
    try:
        probe.ping()
    except redis.exceptions.RedisError as exc:
        raise ConnectionError(f"Could not connect to Redis at {url!r}: {exc}") from exc
    finally:
        probe.close()


class RedisHashUtil:
    """
    Production-ready Redis hash utility class for CRUD operations,
    bulk operations, locking, indexing, TTL, import/export, and secure data hashing.

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Hash key prefix for namespacing.
        index_key (str): Key segment used for secondary indexes.
        lock_key (str): Key segment used for distributed locks.
        default_ttl (Optional[int]): Default TTL in seconds for entries.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "DEFAULT",
        index_key: str = "IDX",
        lock_key: str = "LOCK",
        default_ttl: Optional[int] = None,
    ) -> None:
        """
        Initialize RedisHashUtil instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Hash map prefix (e.g., "USERS:WORKERS").
                    All keys will be namespaced under this prefix.
            index_key: Key segment for secondary indexes. Defaults to "IDX".
            lock_key: Key segment for distributed locks. Defaults to "LOCK".
            default_ttl: Default TTL in seconds applied to all new entries
                         when no explicit ttl is passed to methods.

        Example:
            >>> util = RedisHashUtil(
            ...     url="redis://localhost:6379/0",
            ...     prefix="USERS:WORKERS",
            ...     index_key="INDEX",
            ...     default_ttl=3600,
            ... )

        Raises:
            ValueError: If the Redis URL is invalid or has an unsupported scheme.
            ConnectionError: If the Redis server cannot be reached.
        """
        validate_redis_connection(url)
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.index_key: str = index_key.upper()
        self.lock_key: str = lock_key.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._sync_client: redis.Redis = redis.Redis.from_url(
            self.url, decode_responses=True
        )
        self._async_client: aioredis.Redis = aioredis.from_url(
            self.url, decode_responses=True
        )

    def _key(self, id: str) -> str:
        """Build full Redis hash key from prefix and id."""
        return f"{self.prefix}:{id}"

    def _apply_ttl(self, key: str, ttl: Optional[int] = None) -> None:
        """Apply TTL to a key. Uses method ttl if provided, else default_ttl."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None:
            self._sync_client.expire(key, effective_ttl)

    async def _apply_ttl_async(self, key: str, ttl: Optional[int] = None) -> None:
        """Async: Apply TTL to a key."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None:
            await self._async_client.expire(key, effective_ttl)

    @staticmethod
    def _serialize_data(data: Dict[str, Any]) -> Dict[str, str]:
        """JSON-encode each value in the dict so Redis can store it faithfully."""
        return {
            k: json.dumps(v, ensure_ascii=False, default=str)
            for k, v in data.items()
        }

    @staticmethod
    def _deserialize_data(data: Dict[str, str]) -> Dict[str, Any]:
        """JSON-decode each value to restore original Python types."""
        result: Dict[str, Any] = {}
        for k, v in data.items():
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                result[k] = v
        return result

    # ──────────────────────────────────────────────
    # SYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    def create(
        self,
        data: Dict[str, Any],
        id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """
        Create a new hash entry or update if overwrite is True.
        If id is not provided, a UUID4 is auto-generated.

        Args:
            data: Dictionary of field-value pairs to store.
            id: Unique identifier for the hash entry. Auto-generated if None.
            overwrite: If True, delete existing data before writing.
            ttl: TTL in seconds for this entry. Overrides default_ttl if provided.

        Returns:
            The entry id (provided or auto-generated).

        Raises:
            ValueError: If entry exists and overwrite is False.
        """
        if id is None:
            id = self.generate_uuid4()
        key = self._key(id)
        if not overwrite and self._sync_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            self._sync_client.delete(key)
        self._sync_client.hset(key, mapping=self._serialize_data(data))
        self._apply_ttl(key, ttl)
        return id

    def read(self, id: str, field: Optional[str] = None) -> Optional[Union[str, Dict[str, Union[str, bool]]]]:
        """
        Read hash entry or specific field.

        Args:
            id: Unique identifier for the hash entry.
            field: Optional specific field to retrieve.

        Returns:
            Single field value if field specified, full dict otherwise, None if not found.
        """
        key = self._key(id)
        if field is not None:
            raw = self._sync_client.hget(key, field)
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw
        data = self._deserialize_data(self._sync_client.hgetall(key))
        return data if data else None

    def update(self, id: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Update specific fields in an existing hash entry.

        Args:
            id: Unique identifier for the hash entry.
            data: Dictionary of field-value pairs to update.
            ttl: TTL in seconds to refresh. Overrides default_ttl if provided.

        Returns:
            True if entry exists and was updated, False otherwise.
        """
        key = self._key(id)
        if not self._sync_client.exists(key):
            return False
        self._sync_client.hset(key, mapping=self._serialize_data(data))
        self._apply_ttl(key, ttl)
        return True

    def delete(self, id: str) -> bool:
        """
        Delete entire hash entry.

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            True if entry existed and was deleted, False otherwise.
        """
        return bool(self._sync_client.delete(self._key(id)))

    def delete_fields(self, id: str, *fields: str) -> int:
        """
        Delete specific fields from a hash entry.

        Args:
            id: Unique identifier for the hash entry.
            *fields: Field names to delete.

        Returns:
            Number of fields removed.
        """
        if not fields:
            return 0
        return self._sync_client.hdel(self._key(id), *fields)

    def exists(self, id: str) -> bool:
        """Check if hash entry exists."""
        return bool(self._sync_client.exists(self._key(id)))

    def field_exists(self, id: str, field: str) -> bool:
        """Check if a specific field exists in a hash entry."""
        return bool(self._sync_client.hexists(self._key(id), field))

    def keys(self, id: str) -> List[str]:
        """Get all field names in a hash entry."""
        return self._sync_client.hkeys(self._key(id))

    def values(self, id: str) -> List[str]:
        """Get all values in a hash entry."""
        return self._sync_client.hvals(self._key(id))

    def length(self, id: str) -> int:
        """Get number of fields in a hash entry."""
        return self._sync_client.hlen(self._key(id))

    def increment(self, id: str, field: str, amount: int = 1) -> int:
        """Increment a numeric field value. Can be negative."""
        return self._sync_client.hincrby(self._key(id), field, amount)

    def increment_float(self, id: str, field: str, amount: float = 1.0) -> float:
        """Increment a float field value. Can be negative."""
        return self._sync_client.hincrbyfloat(self._key(id), field, amount)

    # ──────────────────────────────────────────────
    # SET IF NOT EXISTS / GET OR CREATE
    # ──────────────────────────────────────────────

    def set_if_not_exists(self, id: str, field: str, value: Any) -> bool:
        """
        Atomically set a hash field only if it does not already exist.
        Uses HSETNX under the hood.

        Args:
            id: Unique identifier for the hash entry.
            field: Field name to set.
            value: Value to set.

        Returns:
            True if field was set (did not exist), False if already present.
        """
        return bool(self._sync_client.hsetnx(self._key(id), field, json.dumps(value, ensure_ascii=False, default=str)))

    def get_or_create(
        self,
        id: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Return existing entry data, or create it and return the new data.

        Args:
            id: Unique identifier for the hash entry.
            data: Dictionary to store if entry does not exist.
            ttl: TTL in seconds. Overrides default_ttl if provided.

        Returns:
            The entry data (existing or newly created).
        """
        key = self._key(id)
        existing = self._deserialize_data(self._sync_client.hgetall(key))
        if existing:
            return existing
        self._sync_client.hset(key, mapping=self._serialize_data(data))
        self._apply_ttl(key, ttl)
        return data

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def expire(self, id: str, seconds: int) -> bool:
        """
        Set TTL on a hash entry.

        Args:
            id: Unique identifier for the hash entry.
            seconds: TTL in seconds.

        Returns:
            True if timeout was set, False if key does not exist.
        """
        return bool(self._sync_client.expire(self._key(id), seconds))

    def bulk_expire(self, ids: List[str], seconds: int) -> int:
        """
        Set TTL on multiple hash entries using pipeline.

        Args:
            ids: List of entry identifiers.
            seconds: TTL in seconds.

        Returns:
            Number of entries updated.
        """
        if not ids:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.expire(self._key(id), seconds)
        results = pipe.execute()
        return sum(results)

    def ttl(self, id: str) -> int:
        """
        Get remaining TTL of a hash entry.

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if key does not exist.
        """
        return self._sync_client.ttl(self._key(id))

    def persist(self, id: str) -> bool:
        """
        Remove TTL from a hash entry (make it permanent).

        Args:
            id: Unique identifier for the hash entry.

        Returns:
            True if TTL was removed, False otherwise.
        """
        return bool(self._sync_client.persist(self._key(id)))

    # ──────────────────────────────────────────────
    # COPY / RENAME OPERATIONS
    # ──────────────────────────────────────────────

    def copy(
        self,
        source_id: str,
        dest_id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """
        Copy a hash entry to a new id.
        If dest_id is not provided, a UUID4 is auto-generated.

        Args:
            source_id: Source entry identifier.
            dest_id: Destination entry identifier. Auto-generated if None.
            overwrite: If True, overwrite existing destination.
            ttl: TTL for the copied entry. Overrides default_ttl if provided.

        Returns:
            The destination id (provided or auto-generated).

        Raises:
            ValueError: If destination exists and overwrite is False.
        """
        data = self.read(source_id)
        if data is None:
            return ""
        return self.create(dict(data), id=dest_id, overwrite=overwrite, ttl=ttl)

    def bulk_copy(
        self,
        copies: Dict[str, str],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Copy multiple hash entries using pipeline.

        Args:
            copies: Dict mapping source_id -> dest_id.
            overwrite: If True, overwrite existing destinations.
            ttl: TTL for copied entries. Overrides default_ttl if provided.

        Returns:
            Dict mapping destination ids to themselves.
        """
        if not copies:
            return {}
        source_ids = list(copies.keys())
        bulk_data = self.bulk_read(source_ids)
        new_entries: Dict[str, Dict[str, Any]] = {}
        for src_id, dest_id in copies.items():
            data = bulk_data.get(src_id)
            if data is not None:
                new_entries[dest_id] = dict(data)
        if not new_entries:
            return {}
        return self.bulk_create(new_entries, overwrite=overwrite, ttl=ttl)

    def rename(self, old_id: str, new_id: str, overwrite: bool = False) -> bool:
        """
        Rename a hash entry key.

        Args:
            old_id: Current entry identifier.
            new_id: New entry identifier.
            overwrite: If True, overwrite if new_id already exists.

        Returns:
            True if rename succeeded.

        Raises:
            ValueError: If new_id exists and overwrite is False.
        """
        old_key = self._key(old_id)
        new_key = self._key(new_id)
        if not self._sync_client.exists(old_key):
            return False
        if not overwrite and self._sync_client.exists(new_key):
            raise ValueError(f"Entry '{new_key}' already exists. Use overwrite=True.")
        if overwrite:
            self._sync_client.delete(new_key)
        ttl_val = self._sync_client.ttl(old_key)
        self._sync_client.renamenx(old_key, new_key) if not overwrite else self._sync_client.rename(old_key, new_key)
        if ttl_val > 0:
            self._sync_client.expire(new_key, ttl_val)
        return True

    # ──────────────────────────────────────────────
    # SEARCH OPERATIONS
    # ──────────────────────────────────────────────

    def search(
        self,
        field: str,
        value: Any,
        exact: bool = True,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        Search entries where a field matches a value (linear scan).
        For production, prefer indexed lookups via create_index + find_by_index.

        Args:
            field: Field name to match against.
            value: Value to search for.
            exact: If True, exact match. If False, substring/contains match.
            batch_size: Scan batch size.

        Returns:
            List of matching entry ids.
        """
        matches: List[str] = []
        cursor = 0
        search = f"{self.prefix}:*"
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hget(key, field)
                results = pipe.execute()
                for key, val in zip(keys, results):
                    if val is None:
                        continue
                    try:
                        decoded = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        decoded = val
                    if exact and decoded == value:
                        matches.append(key.removeprefix(f"{self.prefix}:"))
                    elif not exact and str(value) in str(decoded):
                        matches.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        return matches

    def search_with_data(
        self,
        field: str,
        value: str,
        exact: bool = True,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, Union[str, bool]]]:
        """
        Search entries with full data where a field matches a value.

        Args:
            field: Field name to match against.
            value: Value to search for.
            exact: If True, exact match. If False, substring match.
            batch_size: Scan batch size.

        Returns:
            Dict of matching entries with their data.
        """
        ids = self.search(field, value, exact=exact, batch_size=batch_size)
        return self.bulk_read(ids)

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_create(
        self,
        entries: Union[Dict[str, Dict[str, Any]], List[Dict[str, Any]]],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Union[Dict[str, str], List[str]]:
        """
        Create multiple hash entries using pipeline.

        Accepts either a dict of ``{id: data, ...}`` or a list of data dicts
        (UUIDs auto-generated for each).

        Args:
            entries: Dict mapping ids to data dicts, or a list of data dicts.
            overwrite: If True, overwrite existing entries.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.

        Returns:
            Dict of ``{id: id, ...}`` if a dict was passed,
            list of ids if a list was passed.
        """
        pipe = self._sync_client.pipeline(transaction=False)
        items: List[tuple[str, Dict[str, Any]]] = (
            list(entries.items()) if isinstance(entries, dict)
            else [(self.generate_uuid4(), d) for d in entries]
        )
        created: List[str] = []
        for id, data in items:
            key = self._key(id)
            if not overwrite and self._sync_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=self._serialize_data(data))
            created.append(id)
        pipe.execute()
        if created:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for id in created:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                ttl_pipe.execute()
        return {id: id for id in created} if isinstance(entries, dict) else created

    def bulk_read(self, ids: List[str]) -> Dict[str, Optional[Dict[str, Union[str, bool]]]]:
        """Read multiple hash entries using pipeline."""
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.hgetall(self._key(id))
        results = pipe.execute()
        return {
            id: (self._deserialize_data(data) if data else None)
            for id, data in zip(ids, results)
        }

    def bulk_update(
        self,
        updates: Dict[str, Dict[str, Any]],
        ttl: Optional[int] = None,
    ) -> int:
        """
        Update multiple hash entries using pipeline.

        Args:
            updates: Dict mapping ids to their update data.
            ttl: TTL in seconds to refresh. Overrides default_ttl if provided.

        Returns:
            Number of entries updated.
        """
        count = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id, data in updates.items():
            key = self._key(id)
            if self._sync_client.exists(key):
                pipe.hset(key, mapping=self._serialize_data(data))
                count += 1
        pipe.execute()
        if count > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for id in updates:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                ttl_pipe.execute()
        return count

    def bulk_delete(self, ids: List[str]) -> int:
        """Delete multiple hash entries using pipeline."""
        if not ids:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for id in ids:
            pipe.delete(self._key(id))
        results = pipe.execute()
        return sum(results)

    def get_all(
        self,
        pattern: Optional[str] = None,
        filter_by: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, Union[str, bool]]]:
        """
        Get all hash entries under this prefix using SCAN with filtering,
        sorting, and pagination.

        Note: Redis does not support server-side filtering/sorting.
        All data is collected first, then filtered/sorted/paginated in Python.

        Args:
            pattern: Optional sub-pattern to match (appended to prefix).
            filter_by: Optional dict of field-value pairs to filter by.
                       e.g., {"status": "active", "role": "admin"}
            sort_by: Optional field name to sort entries by.
            sort_order: Sort direction. "asc" or "desc". Default "asc".
            offset: Number of entries to skip (for pagination).
            limit: Maximum entries to return. 0 = no limit.
            batch_size: Number of keys to scan per iteration.

        Returns:
            Dict mapping entry ids to their data.
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        result: Dict[str, Dict[str, str]] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                data_list = pipe.execute()
                for key, data in zip(keys, data_list):
                    id = key.removeprefix(f"{self.prefix}:")
                    if data:
                        data = self._deserialize_data(data)
                        # Apply filter
                        if filter_by:
                            match = all(data.get(k) == v for k, v in filter_by.items())
                            if not match:
                                continue
                        result[id] = data
            if cursor == 0:
                break
        # Sort
        if sort_by:
            result = dict(sorted(
                result.items(),
                key=lambda item: item[1].get(sort_by, ""),
                reverse=(sort_order.lower() == "desc"),
            ))
        # Pagination
        if offset > 0 or limit > 0:
            items = list(result.items())
            end = offset + limit if limit > 0 else None
            result = dict(items[offset:end])
        return result

    def delete_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Delete all hash entries under this prefix using SCAN and pipeline."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    def count_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Count all hash entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            count += len(keys)
            if cursor == 0:
                break
        return count

    def list_ids(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List entry ids under this prefix with optional pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of ids to skip (for pagination).
            limit: Maximum ids to return. 0 = no limit.
            batch_size: Scan batch size.

        Returns:
            List of entry ids (without prefix).
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        ids: List[str] = []
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=batch_size)
            for key in keys:
                ids.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        # Pagination
        if offset > 0 or limit > 0:
            end = offset + limit if limit > 0 else None
            return ids[offset:end]
        return ids

    # ──────────────────────────────────────────────
    # INDEX OPERATIONS
    # ──────────────────────────────────────────────

    def create_index(self, id: str, field: str) -> bool:
        """
        Create a secondary index for a field using a SET.
        Index key format: {prefix}:{index_key}:{field}:{value} -> set of ids

        Args:
            id: Entry identifier.
            field: Field name to index.

        Returns:
            True if index was created.
        """
        data = self.read(id)
        if not data or field not in data:
            return False
        value = data[field]
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        self._sync_client.sadd(idx_key, id)
        return True

    def find_by_index(self, field: str, value: str) -> List[str]:
        """Find entry ids by indexed field value."""
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        return list(self._sync_client.smembers(idx_key))

    def find_by_index_with_data(self, field: str, value: str) -> Dict[str, Dict[str, Union[str, bool]]]:
        """Find entries with data by indexed field value."""
        ids = self.find_by_index(field, value)
        return self.bulk_read(ids)

    def remove_index(self, id: str, field: str) -> bool:
        """Remove index entry for a specific id and field."""
        data = self.read(id)
        if not data or field not in data:
            return False
        value = data[field]
        idx_key = f"{self.prefix}:{self.index_key}:{field}:{value}"
        self._sync_client.srem(idx_key, id)
        return True

    def delete_index_field(self, field: str) -> int:
        """Delete all index keys for a field."""
        search = f"{self.prefix}:{self.index_key}:{field}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search)
            if keys:
                deleted += self._sync_client.delete(*keys)
            if cursor == 0:
                break
        return deleted

    # ──────────────────────────────────────────────
    # LOCK OPERATIONS
    # ──────────────────────────────────────────────

    def acquire_lock(
        self,
        id: str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
    ) -> Optional[Lock]:
        """Acquire a distributed lock for a hash entry."""
        lock_redis_key = f"{self.prefix}:{self.lock_key}:{id}"
        lock = self._sync_client.lock(
            lock_redis_key,
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        )
        if lock.acquire():
            return lock
        return None

    def release_lock(self, lock: Lock) -> bool:
        """Release a distributed lock."""
        try:
            lock.release()
            return True
        except Exception:
            return False

    # ──────────────────────────────────────────────
    # IMPORT / EXPORT — JSON
    # ──────────────────────────────────────────────

    def export_json(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        indent: int = 2,
    ) -> int:
        """
        Export all hash entries under this prefix to a JSON file.

        Args:
            filepath: Path to output JSON file.
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            Number of entries exported.
        """
        data = self.get_all(pattern=pattern)
        export_data = {
            "prefix": self.prefix,
            "index_key": self.index_key,
            "entries": data,
        }
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=indent, ensure_ascii=False)
        return len(data)

    def import_json(
        self,
        filepath: str,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a JSON file.

        Args:
            filepath: Path to input JSON file.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.
        """
        with open(filepath, "r", encoding="utf-8") as f:
            import_data = json.load(f)
        entries = import_data.get("entries", {})
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_json_string(self, pattern: Optional[str] = None, indent: int = 2) -> str:
        """
        Export all hash entries to a JSON string.

        Args:
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            JSON string representation of all entries.
        """
        data = self.get_all(pattern=pattern)
        export_data = {
            "prefix": self.prefix,
            "index_key": self.index_key,
            "entries": data,
        }
        return json.dumps(export_data, indent=indent, ensure_ascii=False)

    def import_json_string(
        self,
        json_str: str,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a JSON string.

        Args:
            json_str: JSON string containing entries.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.
        """
        import_data = json.loads(json_str)
        entries = import_data.get("entries", {})
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_csv(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        id_column: str = "_id",
    ) -> int:
        """
        Export all hash entries to a CSV file.
        Each entry becomes a row. The id is stored in a dedicated column.

        Args:
            filepath: Path to output CSV file.
            pattern: Optional sub-pattern to match.
            id_column: Column name used for the entry id. Defaults to "_id".

        Returns:
            Number of entries exported.
        """
        data = self.get_all(pattern=pattern)
        if not data:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([id_column])
            return 0

        # Collect all unique field names across all entries
        all_fields: set[str] = set()
        for entry in data.values():
            all_fields.update(entry.keys())
        fieldnames = [id_column] + sorted(all_fields)

        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for entry_id, entry_data in data.items():
                row = {id_column: entry_id}
                row.update(entry_data)
                writer.writerow(row)
        return len(data)

    def import_csv(
        self,
        filepath: str,
        id_column: str = "_id",
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import hash entries from a CSV file.

        Args:
            filepath: Path to input CSV file.
            id_column: Column name that contains the entry id. Defaults to "_id".
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides default_ttl if provided.

        Returns:
            Number of entries imported.

        Raises:
            ValueError: If id_column is not found in CSV headers.
        """
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None or id_column not in reader.fieldnames:
                raise ValueError(f"Column '{id_column}' not found in CSV headers: {reader.fieldnames}")
            entries: Dict[str, Dict[str, str]] = {}
            for row in reader:
                entry_id = row.pop(id_column)
                entries[entry_id] = {k: v for k, v in row.items() if v}
        return len(self.bulk_create(entries, overwrite=overwrite, ttl=ttl))

    def export_csv_string(
        self,
        pattern: Optional[str] = None,
        id_column: str = "_id",
    ) -> str:
        """
        Export all hash entries to a CSV string.

        Args:
            pattern: Optional sub-pattern to match.
            id_column: Column name for entry id. Defaults to "_id".

        Returns:
            CSV string of all entries.
        """
        data = self.get_all(pattern=pattern)
        all_fields: set[str] = set()
        for entry in data.values():
            all_fields.update(entry.keys())
        fieldnames = [id_column] + sorted(all_fields)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry_id, entry_data in data.items():
            row = {id_column: entry_id}
            row.update(entry_data)
            writer.writerow(row)
        return output.getvalue()

    # ──────────────────────────────────────────────
    # ASYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    async def async_create(
        self,
        data: Dict[str, Any],
        id: Optional[str] = None,
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> str:
        """Async: Create a new hash entry. Auto-generates UUID4 if id is None."""
        if id is None:
            id = self.generate_uuid4()
        key = self._key(id)
        if not overwrite and await self._async_client.exists(key):
            raise ValueError(f"Entry '{key}' already exists. Use overwrite=True to update.")
        if overwrite:
            await self._async_client.delete(key)
        await self._async_client.hset(key, mapping=self._serialize_data(data))
        await self._apply_ttl_async(key, ttl)
        return id

    async def async_read(self, id: str, field: Optional[str] = None) -> Optional[Union[str, Dict[str, Union[str, bool]]]]:
        """Async: Read hash entry or specific field."""
        key = self._key(id)
        if field is not None:
            raw = await self._async_client.hget(key, field)
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw
        data = self._deserialize_data(await self._async_client.hgetall(key))
        return data if data else None

    async def async_update(self, id: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Async: Update specific fields in an existing hash entry."""
        key = self._key(id)
        if not await self._async_client.exists(key):
            return False
        await self._async_client.hset(key, mapping=self._serialize_data(data))
        await self._apply_ttl_async(key, ttl)
        return True

    async def async_delete(self, id: str) -> bool:
        """Async: Delete entire hash entry."""
        return bool(await self._async_client.delete(self._key(id)))

    async def async_set_if_not_exists(self, id: str, field: str, value: Any) -> bool:
        """Async: Atomically set a hash field only if it does not already exist."""
        return bool(await self._async_client.hsetnx(self._key(id), field, json.dumps(value, ensure_ascii=False, default=str)))

    async def async_get_or_create(
        self,
        id: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> Dict[str, Union[str, bool]]:
        """Async: Return existing entry data, or create it and return new data."""
        key = self._key(id)
        existing = self._deserialize_data(await self._async_client.hgetall(key))
        if existing:
            return existing
        await self._async_client.hset(key, mapping=self._serialize_data(data))
        await self._apply_ttl_async(key, ttl)
        return data

    async def async_expire(self, id: str, seconds: int) -> bool:
        """Async: Set TTL on a hash entry."""
        return bool(await self._async_client.expire(self._key(id), seconds))

    async def async_ttl(self, id: str) -> int:
        """Async: Get remaining TTL of a hash entry."""
        return await self._async_client.ttl(self._key(id))

    async def async_bulk_create(
        self,
        entries: Union[Dict[str, Dict[str, Any]], List[Dict[str, Any]]],
        overwrite: bool = False,
        ttl: Optional[int] = None,
    ) -> Union[Dict[str, str], List[str]]:
        """Async: Create multiple hash entries using pipeline."""
        pipe = self._async_client.pipeline(transaction=False)
        items: List[tuple[str, Dict[str, Any]]] = (
            list(entries.items()) if isinstance(entries, dict)
            else [(self.generate_uuid4(), d) for d in entries]
        )
        created: List[str] = []
        for id, data in items:
            key = self._key(id)
            if not overwrite and await self._async_client.exists(key):
                continue
            if overwrite:
                pipe.delete(key)
            pipe.hset(key, mapping=self._serialize_data(data))
            created.append(id)
        await pipe.execute()
        if created:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None:
                ttl_pipe = self._async_client.pipeline(transaction=False)
                for id in created:
                    ttl_pipe.expire(self._key(id), effective_ttl)
                await ttl_pipe.execute()
        return {id: id for id in created} if isinstance(entries, dict) else created

    async def async_bulk_read(self, ids: List[str]) -> Dict[str, Optional[Dict[str, Union[str, bool]]]]:
        """Async: Read multiple hash entries using pipeline."""
        pipe = self._async_client.pipeline(transaction=False)
        for id in ids:
            pipe.hgetall(self._key(id))
        results = await pipe.execute()
        return {
            id: (self._deserialize_data(data) if data else None)
            for id, data in zip(ids, results)
        }

    async def async_get_all(
        self,
        pattern: Optional[str] = None,
        filter_by: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> Dict[str, Dict[str, Union[str, bool]]]:
        """Async: Get all hash entries under this prefix with filtering, sorting, pagination."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        result: Dict[str, Dict[str, str]] = {}
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                data_list = await pipe.execute()
                for key, data in zip(keys, data_list):
                    id = key.removeprefix(f"{self.prefix}:")
                    if data:
                        data = self._deserialize_data(data)
                        if filter_by:
                            match = all(data.get(k) == v for k, v in filter_by.items())
                            if not match:
                                continue
                        result[id] = data
            if cursor == 0:
                break
        if sort_by:
            result = dict(sorted(
                result.items(),
                key=lambda item: item[1].get(sort_by, ""),
                reverse=(sort_order.lower() == "desc"),
            ))
        if offset > 0 or limit > 0:
            items = list(result.items())
            end = offset + limit if limit > 0 else None
            result = dict(items[offset:end])
        return result

    async def async_delete_all(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """Async: Delete all hash entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(cursor=cursor, match=search, count=batch_size)
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = await pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    async def async_close(self) -> None:
        """Close async Redis connection."""
        await self._async_client.aclose()

    def close(self) -> None:
        """Close sync Redis connection."""
        self._sync_client.close()

    # ──────────────────────────────────────────────
    # STATIC UTILITY METHODS
    # ──────────────────────────────────────────────

    @staticmethod
    def generate_random_string(length: int = 32, charset: str = string.ascii_letters + string.digits) -> str:
        """
        Generate a cryptographically secure random alphanumeric string.

        Args:
            length: Length of the string to generate. Defaults to 32.
            charset: Character set to use. Defaults to alphanumeric.

        Returns:
            Random string of specified length.
        """
        return "".join(secrets.choice(charset) for _ in range(length))

    @staticmethod
    def generate_random_number(length: int = 6) -> str:
        """
        Generate a cryptographically secure random numeric string.
        Useful for OTPs, PINs, and verification codes. Alias for generate_secure_otp.

        Args:
            length: Number of digits. Defaults to 6.

        Returns:
            Random numeric string of specified length.
        """
        # Minimum safety threshold for standard numeric identifiers
        if length < 4 or length > 12:
            raise ValueError("OTP/PIN length must be between 4 and 12 digits for production security.")

        digits = string.digits
        return "".join(secrets.choice(digits) for _ in range(length))

    @staticmethod
    def generate_token(total_character_length: int = 64) -> str:
        """
        Generate a cryptographically secure URL-safe token of an exact character length.

        Args:
            total_character_length: Exact character length of the output string. Defaults to 64.

        Returns:
            URL-safe token string matching the exact length requested.
        """
        # For standard non-prefixed tokens, ensure we keep a strong entropy floor
        import math
        if total_character_length < 32:
            raise ValueError("Token length must be at least 32 characters to maintain enterprise-grade security.")

        estimated_bytes = math.ceil(total_character_length * 3 / 4)
        return secrets.token_urlsafe(estimated_bytes + 2)[:total_character_length]

    @staticmethod
    def generate_custom_secret(prefix: str, total_length: int = 48, delimiter: str = "_") -> str:
        """
        Generates a cryptographically secure, prefixed API key of an exact total character length
        using the native secrets.token_urlsafe() function.

        Args:
            prefix: The hardcoded identifier string (e.g., 'sk', 'acme').
            total_length: The precise total character length of the finished key. Defaults to 48.
            delimiter: The separator between the prefix and the secret string. Defaults to "_".

        Returns:
            A securely generated, prefixed token string matching the total_length.
        """
        import math
        prefix_part = f"{prefix}{delimiter}"
        needed_secret_length = total_length - len(prefix_part)

        # Hard production safety check
        if needed_secret_length < 32:
            raise ValueError(
                f"Total length ({total_length}) is too short. After accounting for the "
                f"prefix '{prefix_part}', the remaining secret portion must be at least "
                f"32 characters long to maintain enterprise-grade security."
            )

        # Calculate bytes needed to yield at least that many characters in Base64
        estimated_bytes = math.ceil(needed_secret_length * 3 / 4)
        random_secret = secrets.token_urlsafe(estimated_bytes + 2)[:needed_secret_length]

        return f"{prefix_part}{random_secret}"

    @staticmethod
    def generate_uuid4() -> str:
        """Generate a random UUID4."""
        return str(uuid.uuid4())

    @staticmethod
    def generate_uuid5(namespace: str, name: str) -> str:
        """
        Generate a deterministic UUID5 from namespace and name.

        Args:
            namespace: Namespace string (e.g., "myapp.users").
            name: Name to hash within the namespace.

        Returns:
            UUID5 string.
        """
        ns_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)
        return str(uuid.uuid5(ns_uuid, name))

    @staticmethod
    def generate_uuid7() -> str:
        """
        Generate a cryptographically secure, time-ordered UUIDv7.
        Highly performant for database primary indexing and ordered keys.
        """
        if HAS_NATIVE_UUID7:
            return str(uuid.uuid7())  # Handles native 3.14+ generation efficiently

        # Pure Python fallback that accurately mimics the RFC 9562 structural specification
        # 1. Capture current Unix epoch timestamp in milliseconds (48-bit value)
        unix_ts_ms = int(time.time() * 1000)

        # 2. Extract structured 10 bytes (80 bits) of system entropy
        rand_bytes = bytearray(secrets.token_bytes(10))

        # 3. Inject Version 7 structure into the top 4 bits of UUID Byte 6 (rand_bytes[0])
        rand_bytes[0] = (rand_bytes[0] & 0x0F) | 0x70

        # 4. Inject RFC Variant 1 layout (0x80) into top 2 bits of UUID Byte 8 (rand_bytes[2])
        rand_bytes[2] = (rand_bytes[2] & 0x3F) | 0x80

        # 5. Piece bits together sequentially
        uuid_bytes = (
                unix_ts_ms.to_bytes(6, byteorder='big') +
                rand_bytes
        )
        return str(uuid.UUID(bytes=uuid_bytes))

    @staticmethod
    def hash_password(password: str) -> str:
        """
        Hash a password using bcrypt (most secure for passwords).

        Args:
            password: Plain text password to hash.

        Returns:
            Bcrypt hashed password string.
        """
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """
        Verify a password against its bcrypt hash.

        Args:
            password: Plain text password to verify.
            hashed: Bcrypt hashed password to verify against.

        Returns:
            True if password matches, False otherwise.
        """
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

    @staticmethod
    def hash_sensitive_data(data: str, pepper: str = "") -> str:
        """
        Hash sensitive data (emails, PII) using SHA-256.
        One-way hash for data that doesn't need to be reversed.
        """
        # Append pepper to the end rather than prepending to mitigate structural hashing flaws
        salted = f"{data}{pepper}".encode("utf-8")
        return hashlib.sha256(salted).hexdigest()

    @staticmethod
    def hash_sensitive_data_hmac(data: str, secret: str) -> str:
        """
        Hash sensitive data securely using actual cryptographic HMAC-SHA256.
        """
        key_bytes = secret.encode("utf-8")
        data_bytes = data.encode("utf-8")
        # FIX: Uses the native hmac module to securely handle key padding and processing
        return hmac.new(key_bytes, data_bytes, digestmod=hashlib.sha256).hexdigest()

    @staticmethod
    def generate_hash_id(data: Dict[str, Any]) -> str:
        """
        Generate a deterministic hash ID from a data dictionary safely.
        Handles nested items and maintains exact formatting for stable deduplication.
        """
        # FIX: standardizes JSON formatting, sorts keys, and safely processes complex datasets
        canonical_json = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()[:16]

    def __repr__(self) -> str:
        """String representation of RedisHashUtil."""
        return (
            f"RedisHashUtil(url='{self.url}', prefix='{self.prefix}', "
            f"index_key='{self.index_key}', lock_key='{self.lock_key}', "
            f"default_ttl={self.default_ttl})"
        )

    def __enter__(self) -> "RedisHashUtil":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close connections."""
        self.close()



# ══════════════════════════════════════════════
# REDIS STRING UTIL — STRING-BASED STORAGE
# ══════════════════════════════════════════════


F = TypeVar("F", bound=Callable[..., Any])


class RedisStringUtil:
    """
    Production-ready Redis/Valkey string utility using the STRING type.

    A general-purpose utility for storing whole values — strings, numbers,
    booleans, lists, and dicts — as single JSON-serialized entries with
    optional TTL. This is the STRING counterpart to ``RedisHashUtil``
    (which operates on HASH entries).

    Key format: ``{prefix}:{key}``

    Differences from RedisHashUtil:
        - Uses Redis STRING (``SET``/``GET``) instead of HASH (``HSET``/``HGET``).
        - Each entry is a single JSON-serialized value, not a set of fields.
        - No secondary indexes, distributed locks, or field-level operations.
        - Method names use string semantics (``set``/``get``).

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Key prefix for namespace isolation.
        default_ttl (Optional[int]): Default TTL in seconds. None = permanent.

    Example:
        >>> store = RedisStringUtil(prefix="API:USERS", default_ttl=600)
        >>> store.set("user:123", {"name": "Alice", "role": "admin"})
        >>> store.get("user:123")
        {"name": "Alice", "role": "admin"}
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "STRING",
        default_ttl: Optional[int] = None,
        _skip_validation: bool = False,
    ) -> None:
        """
        Initialize RedisStringUtil instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Key prefix for namespace isolation (e.g., "API:USERS").
                    All keys will be namespaced as ``{prefix}:{key}``.
            default_ttl: Default TTL in seconds for stored entries.
                         Defaults to None (permanent). Set a value like
                         3600 for automatic expiry, or pass ttl per-call.
            _skip_validation: Internal only. When True, the connection
                              validation is skipped (used by ``RedisCache``,
                              which validates before wrapping this class).

        Example:
            >>> # Ephemeral store — entries auto-expire
            >>> store = RedisStringUtil(prefix="API:USERS", default_ttl=600)
            >>> # Persistent store — entries live forever
            >>> store = RedisStringUtil(prefix="CONFIG", default_ttl=None)

        Raises:
            ValueError: If the Redis URL is invalid or has an unsupported scheme.
            ConnectionError: If the Redis server cannot be reached.
        """
        if not _skip_validation:
            validate_redis_connection(url)
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._sync_client: redis.Redis = redis.Redis.from_url(
            self.url, decode_responses=True
        )
        self._async_client: aioredis.Redis = aioredis.from_url(
            self.url, decode_responses=True
        )

    def _key(self, key: str) -> str:
        """Build full Redis key from prefix and key."""
        return f"{self.prefix}:{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize a value to a JSON string for storage."""
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _deserialize(self, raw: Optional[str]) -> Optional[Any]:
        """Deserialize a JSON string back to a Python object."""
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def _apply_ttl(self, key: str, ttl: Optional[int] = None) -> None:
        """Apply TTL to a key. Uses method ttl if provided, else default_ttl."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            self._sync_client.expire(key, effective_ttl)

    async def _apply_ttl_async(self, key: str, ttl: Optional[int] = None) -> None:
        """Async: Apply TTL to a key."""
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            await self._async_client.expire(key, effective_ttl)

    # ──────────────────────────────────────────────
    # SYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = False,
        with_ttl: bool = False,
    ) -> Union[bool, tuple[bool, int]]:
        """
        Store a value at a key.

        Serializes dicts/lists to JSON strings automatically. Strings are
        stored as-is. If the key already exists and ``overwrite=False``, a
        ``ValueError`` is raised.

        Args:
            key: Key to store (will be prefixed with ``{prefix}:``).
            value: Value to store. Dicts, lists, and primitives are JSON-serialized.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            overwrite: If True, overwrite existing entries silently.
            with_ttl: If True, return ``(bool, seconds_to_live)`` instead of bool.

        Returns:
            True if the value was stored, or ``(True, ttl)`` if ``with_ttl=True``.

        Raises:
            ValueError: If key exists and ``overwrite=False``.

        Example:
            >>> store.set("session:abc", {"user_id": "123", "role": "admin"})
            True
            >>> store.set("config:dark_mode", True, ttl=86400)
            True
        """
        full_key = self._key(key)
        if not overwrite and self._sync_client.exists(full_key):
            raise ValueError(
                f"Key '{full_key}' already exists. Use overwrite=True to replace."
            )
        serialized = self._serialize(value)
        self._sync_client.set(full_key, serialized)
        self._apply_ttl(full_key, ttl)
        if with_ttl:
            return True, self._sync_client.ttl(full_key)
        return True

    def get(
        self, key: str, default: Any = None, with_ttl: bool = False
    ) -> Union[Any, tuple[Any, int]]:
        """
        Retrieve a stored value by key.

        Deserializes JSON strings back to Python objects automatically.
        Returns ``default`` if the key does not exist.

        Args:
            key: Key to look up.
            default: Value to return if the key is missing. Defaults to None.
            with_ttl: If True, return ``(value, seconds_to_live)`` instead of value.

        Returns:
            The stored value (deserialized), or ``default`` if not found.
            If ``with_ttl=True``, returns ``(value, ttl)`` or ``(default, -2)``.

        Example:
            >>> store.get("session:abc")
            {"user_id": "123", "role": "admin"}
            >>> store.get("missing:key", default=[])
            []
        """
        full_key = self._key(key)
        raw = self._sync_client.get(full_key)
        if raw is None:
            return (default, -2) if with_ttl else default
        value = self._deserialize(raw)
        if with_ttl:
            return value, self._sync_client.ttl(full_key)
        return value

    def upsert(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        with_ttl: bool = False,
    ) -> Union[bool, tuple[bool, int]]:
        """
        Store or update a value at a key (create-or-update semantics).

        Unlike ``set()``, this never raises ``ValueError`` — it silently
        overwrites existing entries.

        Args:
            key: Key.
            value: Value to store.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            with_ttl: If True, return ``(bool, seconds_to_live)`` instead of bool.

        Returns:
            True if the value was stored, or ``(True, ttl)`` if ``with_ttl=True``.

        Example:
            >>> store.upsert("config:theme", "dark")
            True
        """
        full_key = self._key(key)
        serialized = self._serialize(value)
        self._sync_client.set(full_key, serialized)
        self._apply_ttl(full_key, ttl)
        if with_ttl:
            return True, self._sync_client.ttl(full_key)
        return True

    def increment(
        self,
        key: str,
        amount: Union[int, float] = 1,
        ttl: Optional[int] = None,
    ) -> Union[int, float]:
        """
        Non-atomic increment of a stored numeric value.

        Fetches the current value (starting from 0 if absent), adds ``amount``,
        and stores it back. Since this is a GET + SET operation it is **not
        atomic** — fine for approximate counters like page views.

        Args:
            key: Key.
            amount: Amount to increment by (int or float). Defaults to 1.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.

        Returns:
            The new value after increment.

        Example:
            >>> store.increment("page_views:homepage")
            1
            >>> store.increment("page_views:homepage", 5)
            6
            >>> store.increment("balance:42", -10)
            -4
        """
        current = self.get(key)
        new_val = (current if current is not None else 0) + amount
        self.upsert(key, new_val, ttl=ttl)
        return new_val

    def decrement(
        self,
        key: str,
        amount: Union[int, float] = 1,
        ttl: Optional[int] = None,
    ) -> Union[int, float]:
        """
        Non-atomic decrement of a stored numeric value.

        Convenience wrapper around ``increment`` with a negated amount.
        Starts from 0 if the key does not exist.

        Args:
            key: Key.
            amount: Amount to decrement by (int or float). Defaults to 1.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.

        Returns:
            The new value after decrement.

        Example:
            >>> store.decrement("rate_limit:user:42")
            -1
        """
        return self.increment(key, -amount, ttl=ttl)

    def delete(self, *keys: str) -> int:
        """
        Delete one or more stored entries.

        Args:
            *keys: One or more keys to delete.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> store.delete("session:abc", "session:def")
            2
        """
        if not keys:
            return 0
        full_keys = [self._key(k) for k in keys]
        return int(self._sync_client.delete(*full_keys))

    def exists(self, key: str, with_ttl: bool = False) -> Union[bool, tuple[bool, int]]:
        """Check if a key exists."""
        full_key = self._key(key)
        exists = self._sync_client.exists(full_key)
        if with_ttl:
            return bool(exists), self._sync_client.ttl(full_key) if exists else -2
        return bool(exists)

    # ──────────────────────────────────────────────
    # ATOMIC OPERATIONS
    # ──────────────────────────────────────────────

    def set_if_not_exists(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        with_ttl: bool = False,
    ) -> Union[bool, tuple[bool, int]]:
        """
        Store a value only if the key does not already exist (atomic).

        Uses Redis ``SET NX`` under the hood. Useful for distributed
        "first-write-wins" claim patterns.

        Args:
            key: Key.
            value: Value to store.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            with_ttl: If True, return ``(bool, seconds_to_live)`` instead of bool.

        Returns:
            True if the value was set (key did not exist), False otherwise.
            If ``with_ttl=True``, returns ``(True, ttl)`` on success,
            ``(False, remaining_ttl)`` if the key already exists.

        Example:
            >>> store.set_if_not_exists("lock:job:123", "worker-1")
            True
            >>> store.set_if_not_exists("lock:job:123", "worker-2")
            False
        """
        full_key = self._key(key)
        serialized = self._serialize(value)
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            result = self._sync_client.set(full_key, serialized, nx=True, ex=effective_ttl)
        else:
            result = self._sync_client.set(full_key, serialized, nx=True)
        if with_ttl:
            return bool(result), self._sync_client.ttl(full_key)
        return bool(result)

    def get_or_set(
        self,
        key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
        with_ttl: bool = False,
    ) -> Union[Any, tuple[Any, int]]:
        """
        Retrieve an existing value, or compute and store a new one.

        If the key exists, its value is returned immediately. If not, ``factory``
        is called (if callable) or ``factory`` is used directly as the value,
        stored, and returned.

        Args:
            key: Key.
            factory: A callable that produces the value on a miss, or a
                     static value to store on a miss.
            ttl: TTL in seconds for the new entry. Overrides ``default_ttl``
                 if provided.
            with_ttl: If True, return ``(value, seconds_to_live)`` instead of value.

        Returns:
            The stored or freshly computed value.
            If ``with_ttl=True``, returns ``(value, ttl)``.

        Example:
            >>> # With a factory function (lazy computation)
            >>> store.get_or_set("user:123", lambda: expensive_query("123"))
            {"name": "Alice", "role": "admin"}

            >>> # With a static default
            >>> store.get_or_set("config:feature_x", {"enabled": True})
            {"enabled": True}
        """
        full_key = self._key(key)
        raw = self._sync_client.get(full_key)
        if raw is not None:
            value = self._deserialize(raw)
            if with_ttl:
                return value, self._sync_client.ttl(full_key)
            return value
        value = factory() if callable(factory) else factory
        self.upsert(key, value, ttl=ttl)
        if with_ttl:
            return value, self._sync_client.ttl(full_key)
        return value

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def expire(
        self, key: str, seconds: int, with_ttl: bool = False
    ) -> Union[bool, tuple[bool, int]]:
        """
        Set TTL on an existing entry.

        Args:
            key: Key.
            seconds: TTL in seconds.
            with_ttl: If True, return ``(bool, seconds_to_live)`` instead of bool.

        Returns:
            True if the timeout was set, False if the key does not exist.

        Example:
            >>> store.expire("session:abc", 7200)
            True
        """
        result = bool(self._sync_client.expire(self._key(key), seconds))
        if with_ttl:
            full_key = self._key(key)
            return result, self._sync_client.ttl(full_key) if result else -2
        return result

    def bulk_expire(self, keys: List[str], seconds: int) -> int:
        """
        Set TTL on multiple entries using a pipeline.

        Args:
            keys: List of keys.
            seconds: TTL in seconds.

        Returns:
            Number of entries updated.

        Example:
            >>> store.bulk_expire(["k1", "k2", "k3"], 1800)
            3
        """
        if not keys:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for k in keys:
            pipe.expire(self._key(k), seconds)
        results = pipe.execute()
        return sum(results)

    def ttl(self, key: str) -> int:
        """
        Get the remaining TTL of an entry.

        Args:
            key: Key.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if the key does not exist.

        Example:
            >>> store.ttl("session:abc")
            5400
        """
        return self._sync_client.ttl(self._key(key))

    def persist(
        self, key: str, with_ttl: bool = False
    ) -> Union[bool, tuple[bool, int]]:
        """
        Remove TTL from an entry (make it permanent).

        Args:
            key: Key.
            with_ttl: If True, return ``(bool, seconds_to_live)`` instead of bool.

        Returns:
            True if TTL was removed, False otherwise.
            If ``with_ttl=True``, returns ``(True, -1)`` on success.

        Example:
            >>> store.persist("config:feature_x")
            True
        """
        result = bool(self._sync_client.persist(self._key(key)))
        if with_ttl:
            return result, self._sync_client.ttl(self._key(key))
        return result

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_set(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> int:
        """
        Store multiple values in a pipeline.

        Args:
            entries: Dict mapping keys to values.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            overwrite: If True, overwrite existing entries.

        Returns:
            Number of entries stored.

        Example:
            >>> store.bulk_set({"k1": [1, 2], "k2": {"a": 1}, "k3": "text"})
            3
        """
        if not entries:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        stored = 0
        for key, value in entries.items():
            full_key = self._key(key)
            if not overwrite and self._sync_client.exists(full_key):
                continue
            serialized = self._serialize(value)
            pipe.set(full_key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for key in entries:
                    ttl_pipe.expire(self._key(key), effective_ttl)
                ttl_pipe.execute()
        return stored

    def bulk_get(
        self, keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """
        Retrieve multiple stored values in a pipeline.

        Args:
            keys: List of keys to look up.
            default: Value to return for missing keys.

        Returns:
            Dict mapping keys to their values (or ``default``).

        Example:
            >>> store.bulk_get(["k1", "k2", "missing"])
            {"k1": [1, 2], "k2": {"a": 1}, "missing": None}
        """
        if not keys:
            return {}
        pipe = self._sync_client.pipeline(transaction=False)
        for key in keys:
            pipe.get(self._key(key))
        results = pipe.execute()
        output: Dict[str, Any] = {}
        for key, raw in zip(keys, results):
            output[key] = self._deserialize(raw) if raw is not None else default
        return output

    def bulk_delete(self, keys: List[str]) -> int:
        """
        Delete multiple entries in a pipeline.

        Args:
            keys: List of keys to delete.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> store.bulk_delete(["k1", "k2", "k3"])
            3
        """
        if not keys:
            return 0
        pipe = self._sync_client.pipeline(transaction=False)
        for key in keys:
            pipe.delete(self._key(key))
        results = pipe.execute()
        return sum(results)

    # ──────────────────────────────────────────────
    # PATTERN / NAMESPACE OPERATIONS
    # ──────────────────────────────────────────────

    def delete_pattern(self, pattern: str, batch_size: int = 1000) -> int:
        """
        Delete all entries matching a glob pattern under this prefix.

        Uses SCAN for non-blocking iteration.

        Args:
            pattern: Glob pattern to match (appended to prefix).
                     Example: ``"user:*"`` matches ``STRING:user:123``.
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> store.delete_pattern("session:*")
            42
        """
        search = f"{self.prefix}:{pattern}"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    def delete_namespace(self, namespace: str, batch_size: int = 1000) -> int:
        """
        Delete all entries under a sub-namespace.

        A convenience wrapper around ``delete_pattern`` that matches
        everything under ``{namespace}:*``.

        Args:
            namespace: Sub-namespace to clear (e.g., "user" clears
                       ``STRING:user:123``, ``STRING:user:456``, etc.).
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> store.delete_namespace("session")
            150
        """
        return self.delete_pattern(f"{namespace}*", batch_size=batch_size)

    def delete_all(self, batch_size: int = 1000) -> int:
        """
        Delete ALL entries under this prefix (dangerous).

        Uses SCAN to iterate — safe for production (non-blocking).

        Args:
            batch_size: SCAN batch size.

        Returns:
            Number of keys deleted.

        Example:
            >>> store.delete_all()
            1024
        """
        return self.delete_pattern("*", batch_size=batch_size)

    # ──────────────────────────────────────────────
    # INSPECTION / STATS
    # ──────────────────────────────────────────────

    def count(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """
        Count entries under this prefix.

        Args:
            pattern: Optional sub-pattern to match.
            batch_size: SCAN batch size.

        Returns:
            Number of matching keys.

        Example:
            >>> store.count()
            256
            >>> store.count(pattern="user:*")
            42
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            count += len(keys)
            if cursor == 0:
                break
        return count

    def list_keys(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List keys under this prefix with optional pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of keys to skip.
            limit: Maximum keys to return. 0 = no limit.
            batch_size: SCAN batch size.

        Returns:
            List of keys (without prefix).

        Example:
            >>> store.list_keys(pattern="user:*", limit=10)
            ["user:123", "user:456", ...]
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        keys_list: List[str] = []
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            for key in keys:
                keys_list.append(key.removeprefix(f"{self.prefix}:"))
            if cursor == 0:
                break
        if offset > 0 or limit > 0:
            end = offset + limit if limit > 0 else None
            return keys_list[offset:end]
        return keys_list

    def stats(self) -> Dict[str, Any]:
        """
        Get Redis server statistics from the ``INFO`` command.

        Returns:
            Dict with ``used_memory``, ``used_memory_human``,
            ``keyspace_hits``, ``keyspace_misses``, and computed ``hit_rate``.

        Example:
            >>> store.stats()
            {"used_memory": 1048576, "hit_rate": 0.95, ...}
        """
        info = self._sync_client.info("stats")
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        hit_rate = hits / total if total > 0 else 0.0
        memory = self._sync_client.info("memory")
        return {
            "used_memory": memory.get("used_memory", 0),
            "used_memory_human": memory.get("used_memory_human", "N/A"),
            "keyspace_hits": hits,
            "keyspace_misses": misses,
            "hit_rate": round(hit_rate, 4),
            "total_keys": self.count(),
        }

    # ──────────────────────────────────────────────
    # IMPORT / EXPORT
    # ──────────────────────────────────────────────

    def export_json(
        self,
        filepath: str,
        pattern: Optional[str] = None,
        indent: int = 2,
    ) -> int:
        """
        Export all entries under this prefix to a JSON file.

        Args:
            filepath: Output file path.
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            Number of entries exported.

        Example:
            >>> store.export_json("/tmp/backup.json")
            256
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        entries: Dict[str, Any] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=1000)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                raw_list = pipe.execute()
                for key, raw in zip(keys, raw_list):
                    if raw is not None:
                        bare_key = key.removeprefix(f"{self.prefix}:")
                        entries[bare_key] = self._deserialize(raw)
            if cursor == 0:
                break
        export_data = {"prefix": self.prefix, "entries": entries}
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=indent, ensure_ascii=False, default=str)
        return len(entries)

    def import_json(
        self,
        filepath: str,
        overwrite: bool = True,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import entries from a JSON file.

        Args:
            filepath: Input file path.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries. Overrides ``default_ttl``.

        Returns:
            Number of entries imported.

        Example:
            >>> store.import_json("/tmp/backup.json")
            256
        """
        with open(filepath, "r", encoding="utf-8") as f:
            import_data = json.load(f)
        entries = import_data.get("entries", {})
        stored = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for key, value in entries.items():
            full_key = self._key(key)
            if not overwrite and self._sync_client.exists(full_key):
                continue
            serialized = self._serialize(value)
            pipe.set(full_key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for key in entries:
                    ttl_pipe.expire(self._key(key), effective_ttl)
                ttl_pipe.execute()
        return stored

    def export_json_string(
        self, pattern: Optional[str] = None, indent: int = 2
    ) -> str:
        """
        Export all entries under this prefix to a JSON string.

        Args:
            pattern: Optional sub-pattern to match.
            indent: JSON indentation level.

        Returns:
            JSON string of all entries.

        Example:
            >>> json_str = store.export_json_string()
        """
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        entries: Dict[str, Any] = {}
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor=cursor, match=search, count=1000)
            if keys:
                pipe = self._sync_client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                raw_list = pipe.execute()
                for key, raw in zip(keys, raw_list):
                    if raw is not None:
                        bare_key = key.removeprefix(f"{self.prefix}:")
                        entries[bare_key] = self._deserialize(raw)
            if cursor == 0:
                break
        export_data = {"prefix": self.prefix, "entries": entries}
        return json.dumps(export_data, indent=indent, ensure_ascii=False, default=str)

    def import_json_string(
        self,
        json_str: str,
        overwrite: bool = True,
        ttl: Optional[int] = None,
    ) -> int:
        """
        Import entries from a JSON string.

        Args:
            json_str: JSON string containing entries.
            overwrite: If True, overwrite existing entries.
            ttl: TTL for imported entries.

        Returns:
            Number of entries imported.

        Example:
            >>> store.import_json_string(json_str)
            128
        """
        import_data = json.loads(json_str)
        entries = import_data.get("entries", {})
        stored = 0
        pipe = self._sync_client.pipeline(transaction=False)
        for key, value in entries.items():
            full_key = self._key(key)
            if not overwrite and self._sync_client.exists(full_key):
                continue
            serialized = self._serialize(value)
            pipe.set(full_key, serialized)
            stored += 1
        pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._sync_client.pipeline(transaction=False)
                for key in entries:
                    ttl_pipe.expire(self._key(key), effective_ttl)
                ttl_pipe.execute()
        return stored

    # ──────────────────────────────────────────────
    # DECORATOR — FUNCTION RESULT MEMOIZATION
    # ──────────────────────────────────────────────

    def memoize(
        self,
        ttl: Optional[int] = None,
        key_prefix: str = "",
        fallback: Optional[Callable] = None,
    ) -> Callable[[F], F]:
        """
        Decorator that stores a function's return value.

        The key is derived from the function's module, name, and arguments.
        On a hit, the stored result is returned without calling the function.
        On a miss, the function is called, its result stored, and returned.

        Args:
            ttl: TTL in seconds for the stored result. Overrides ``default_ttl``.
            key_prefix: Optional prefix added to the key for namespacing.
            fallback: Optional callable invoked on Redis errors. Receives the
                      original function and its arguments. If not provided,
                      the exception propagates.

        Returns:
            A decorator that wraps the function with result-storing logic.

        Example:
            >>> @store.memoize(ttl=300)
            ... def get_user(user_id: str) -> dict:
            ...     return db.query_user(user_id)

            >>> @store.memoize(ttl=60, key_prefix="api")
            ... def fetch_products(category: str) -> list:
            ...     return api.get_products(category)
        """

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                key_parts = [func.__module__, func.__qualname__]
                if key_prefix:
                    key_parts.insert(0, key_prefix)
                key_parts.extend([repr(a) for a in args])
                key_parts.extend([f"{k}={repr(v)}" for k, v in sorted(kwargs.items())])
                raw_key = ":".join(key_parts)
                memo_key = (
                    hashlib.sha256(raw_key.encode()).hexdigest()[:32]
                    if len(raw_key) > 128
                    else raw_key
                )
                try:
                    existing = self.get(memo_key)
                    if existing is not None:
                        return existing
                    result = func(*args, **kwargs)
                    self.upsert(memo_key, result, ttl=ttl)
                    return result
                except Exception as e:
                    if fallback is not None:
                        return fallback(func, *args, **kwargs)
                    raise e

            wrapper.clear = lambda *a, **kw: self.delete_pattern(  # type: ignore
                f"{key_prefix or func.__qualname__}*"
            )
            return wrapper  # type: ignore

        return decorator

    # ──────────────────────────────────────────────
    # ASYNC CRUD OPERATIONS
    # ──────────────────────────────────────────────

    async def async_set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> bool:
        """Async: Store a value at a key."""
        full_key = self._key(key)
        if not overwrite and await self._async_client.exists(full_key):
            raise ValueError(
                f"Key '{full_key}' already exists. Use overwrite=True to replace."
            )
        serialized = self._serialize(value)
        await self._async_client.set(full_key, serialized)
        await self._apply_ttl_async(full_key, ttl)
        return True

    async def async_get(
        self, key: str, default: Any = None
    ) -> Any:
        """Async: Retrieve a stored value."""
        full_key = self._key(key)
        raw = await self._async_client.get(full_key)
        if raw is None:
            return default
        return self._deserialize(raw)

    async def async_upsert(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Async: Store or update a value at a key."""
        full_key = self._key(key)
        serialized = self._serialize(value)
        await self._async_client.set(full_key, serialized)
        await self._apply_ttl_async(full_key, ttl)
        return True

    async def async_increment(
        self,
        key: str,
        amount: Union[int, float] = 1,
        ttl: Optional[int] = None,
    ) -> Union[int, float]:
        """Async: Non-atomic increment of a stored numeric value."""
        current = await self.async_get(key)
        new_val = (current if current is not None else 0) + amount
        await self.async_upsert(key, new_val, ttl=ttl)
        return new_val

    async def async_decrement(
        self,
        key: str,
        amount: Union[int, float] = 1,
        ttl: Optional[int] = None,
    ) -> Union[int, float]:
        """Async: Non-atomic decrement of a stored numeric value."""
        return await self.async_increment(key, -amount, ttl=ttl)

    async def async_delete(self, *keys: str) -> int:
        """Async: Delete one or more stored entries."""
        if not keys:
            return 0
        full_keys = [self._key(k) for k in keys]
        return int(await self._async_client.delete(*full_keys))

    async def async_exists(self, key: str) -> bool:
        """Async: Check if a key exists."""
        return bool(await self._async_client.exists(self._key(key)))

    async def async_set_if_not_exists(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Async: Store a value only if the key does not exist (atomic)."""
        full_key = self._key(key)
        serialized = self._serialize(value)
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl is not None and effective_ttl > 0:
            result = await self._async_client.set(
                full_key, serialized, nx=True, ex=effective_ttl
            )
        else:
            result = await self._async_client.set(full_key, serialized, nx=True)
        return bool(result)

    async def async_get_or_set(
        self,
        key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """Async: Retrieve a value, or compute and store a new one."""
        existing = await self.async_get(key)
        if existing is not None:
            return existing
        value = factory() if callable(factory) else factory
        await self.async_upsert(key, value, ttl=ttl)
        return value

    async def async_bulk_get(
        self, keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """Async: Retrieve multiple stored values in a pipeline."""
        if not keys:
            return {}
        pipe = self._async_client.pipeline(transaction=False)
        for key in keys:
            pipe.get(self._key(key))
        results = await pipe.execute()
        output: Dict[str, Any] = {}
        for key, raw in zip(keys, results):
            output[key] = self._deserialize(raw) if raw is not None else default
        return output

    async def async_bulk_set(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = False,
    ) -> int:
        """Async: Store multiple values in a pipeline."""
        if not entries:
            return 0
        pipe = self._async_client.pipeline(transaction=False)
        stored = 0
        for key, value in entries.items():
            full_key = self._key(key)
            if not overwrite and await self._async_client.exists(full_key):
                continue
            serialized = self._serialize(value)
            pipe.set(full_key, serialized)
            stored += 1
        await pipe.execute()
        if stored > 0:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is not None and effective_ttl > 0:
                ttl_pipe = self._async_client.pipeline(transaction=False)
                for key in entries:
                    ttl_pipe.expire(self._key(key), effective_ttl)
                await ttl_pipe.execute()
        return stored

    async def async_delete_pattern(
        self, pattern: str, batch_size: int = 1000
    ) -> int:
        """Async: Delete all entries matching a pattern."""
        search = f"{self.prefix}:{pattern}"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            if keys:
                pipe = self._async_client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = await pipe.execute()
                deleted += sum(results)
            if cursor == 0:
                break
        return deleted

    async def async_count(
        self, pattern: Optional[str] = None, batch_size: int = 1000
    ) -> int:
        """Async: Count entries under this prefix."""
        search = f"{self.prefix}:{pattern}*" if pattern else f"{self.prefix}:*"
        count = 0
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(
                cursor=cursor, match=search, count=batch_size
            )
            count += len(keys)
            if cursor == 0:
                break
        return count

    async def async_delete_all(self, batch_size: int = 1000) -> int:
        """Async: Delete ALL entries under this prefix."""
        return await self.async_delete_pattern("*", batch_size=batch_size)

    async def async_close(self) -> None:
        """Close the async Redis connection."""
        await self._async_client.aclose()

    def close(self) -> None:
        """Close the sync Redis connection."""
        self._sync_client.close()

    # ──────────────────────────────────────────────
    # DUNDER METHODS
    # ──────────────────────────────────────────────

    def __repr__(self) -> str:
        """String representation of RedisStringUtil."""
        return (
            f"RedisStringUtil(url='{self.url}', prefix='{self.prefix}', "
            f"default_ttl={self.default_ttl})"
        )

    def __enter__(self) -> "RedisStringUtil":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit — close connections."""
        self.close()


# ══════════════════════════════════════════════
# REDIS CACHE — CACHING LAYER ON STRING STORAGE
# ══════════════════════════════════════════════


class RedisCache:
    """
    Production-ready caching layer built on top of ``RedisStringUtil``.

    A thin, focused wrapper that instantiates ``RedisStringUtil`` with a
    dedicated ``CACHE`` namespace so every cached entry lives under
    ``CACHE:<key>``. Provides cache-centric operations: set, get, delete,
    exists, count, TTL management, cache-aside, atomic claims, and bulk ops.

    The namespace is always applied internally — callers may pass a bare
    key (``"user:123"``) or a fully-qualified key (``"CACHE:user:123"``);
    both resolve to the same entry.

    Attributes:
        url (str): Redis connection URL.
        prefix (str): Cache namespace. Defaults to "CACHE".
        default_ttl (Optional[int]): Default TTL in seconds. None = permanent.

    Example:
        >>> cache = RedisCache(default_ttl=600)
        >>> cache.set("user:123", {"name": "Alice"}, ttl=300)
        >>> cache.get("user:123")
        {"name": "Alice"}
        >>> cache.get("CACHE:user:123")
        {"name": "Alice"}
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "CACHE",
        default_ttl: Optional[int] = None,
    ) -> None:
        """
        Initialize RedisCache instance.

        Args:
            url: Redis connection URL. Defaults to localhost:6379.
            prefix: Cache namespace. Defaults to "CACHE". All keys are
                    stored under ``{prefix}:{key}``.
            default_ttl: Default TTL in seconds applied to entries when no
                         explicit ``ttl`` is passed. Defaults to None (permanent).

        Example:
            >>> # Ephemeral cache — entries auto-expire after 10 min
            >>> cache = RedisCache(default_ttl=600)
            >>> # Persistent cache — entries live forever
            >>> cache = RedisCache(prefix="CONFIG", default_ttl=None)

        Raises:
            ValueError: If the Redis URL is invalid or has an unsupported scheme.
            ConnectionError: If the Redis server cannot be reached.
        """
        validate_redis_connection(url)
        self.url: str = url
        self.prefix: str = prefix.upper()
        self.default_ttl: Optional[int] = default_ttl
        self._store: RedisStringUtil = RedisStringUtil(
            url=self.url,
            prefix=self.prefix,
            default_ttl=self.default_ttl,
            _skip_validation=True,
        )

    def _normalize_key(self, key: str) -> str:
        """Strip the namespace prefix from a key if already present."""
        namespace = f"{self.prefix}:"
        return key[len(namespace):] if key.startswith(namespace) else key

    # ──────────────────────────────────────────────
    # CORE OPERATIONS
    # ──────────────────────────────────────────────

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        overwrite: bool = True,
    ) -> bool:
        """
        Store a value in the cache under ``{prefix}:{key}``.

        Args:
            key: Cache key. May be bare (``"user:123"``) or already
                 prefixed (``"CACHE:user:123"``).
            value: Value to store. JSON-serialized automatically.
            ttl: TTL in seconds. Overrides ``default_ttl`` if provided.
            overwrite: If False, a ``ValueError`` is raised when the key
                       already exists. Defaults to True (cache semantics).

        Returns:
            True if the value was stored.

        Raises:
            ValueError: If the key exists and ``overwrite=False``.

        Example:
            >>> cache.set("session:abc", {"token": "xyz"}, ttl=300)
            True
        """
        bare = self._normalize_key(key)
        self._store.set(bare, value, ttl=ttl, overwrite=overwrite)
        return True

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a cached value by key.

        Args:
            key: Cache key. Accepts ``"user:123"`` or ``"CACHE:user:123"``.
            default: Value returned when the key is missing.

        Returns:
            The cached value (deserialized), or ``default``.

        Example:
            >>> cache.get("user:123")
            {"name": "Alice"}
            >>> cache.get("user:999", default=None)
            None
        """
        bare = self._normalize_key(key)
        return self._store.get(bare, default=default)

    def delete(self, *keys: str) -> int:
        """
        Delete one or more cached entries.

        Args:
            *keys: Cache keys. Accepts bare or prefixed forms.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> cache.delete("user:123", "CACHE:user:456")
            2
        """
        bare_keys = [self._normalize_key(k) for k in keys]
        return self._store.delete(*bare_keys)

    def exists(self, key: str) -> bool:
        """
        Check whether a cache entry exists.

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            True if the entry exists, False otherwise.

        Example:
            >>> cache.exists("user:123")
            True
        """
        bare = self._normalize_key(key)
        return self._store.exists(bare)

    # ──────────────────────────────────────────────
    # TTL OPERATIONS
    # ──────────────────────────────────────────────

    def ttl(self, key: str) -> int:
        """
        Get the remaining TTL of a cache entry.

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            Remaining seconds, -1 if no expiry, -2 if the key does not exist.

        Example:
            >>> cache.ttl("session:abc")
            5400
        """
        bare = self._normalize_key(key)
        return self._store.ttl(bare)

    def expire(self, key: str, seconds: int) -> bool:
        """
        Update the TTL of an existing cache entry.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            seconds: New TTL in seconds.

        Returns:
            True if the TTL was set, False if the key does not exist.

        Example:
            >>> cache.expire("user:123", 7200)
            True
        """
        bare = self._normalize_key(key)
        return self._store.expire(bare, seconds)

    def persist(self, key: str) -> bool:
        """
        Remove the TTL from a cache entry (make it permanent).

        Args:
            key: Cache key. Accepts bare or prefixed forms.

        Returns:
            True if the TTL was removed, False otherwise.

        Example:
            >>> cache.persist("config:feature_x")
            True
        """
        bare = self._normalize_key(key)
        return self._store.persist(bare)

    # ──────────────────────────────────────────────
    # CACHE-SPECIFIC PATTERNS
    # ──────────────────────────────────────────────

    def get_or_set(
        self,
        key: str,
        factory: Union[Callable[[], Any], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Cache-aside (lazy-loading) pattern: return the cached value, or
        compute/store and return a new one.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            factory: Callable producing the value on a miss, or a static value.
            ttl: TTL in seconds for the new entry. Overrides ``default_ttl``.

        Returns:
            The cached or freshly computed value.

        Example:
            >>> cache.get_or_set("user:123", lambda: db.query(123), ttl=300)
            {"name": "Alice"}
        """
        existing = self.get(key)
        if existing is not None:
            return existing
        value = factory() if callable(factory) else factory
        self.set(key, value, ttl=ttl)
        return value

    def set_if_not_exists(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """
        Atomically store a value only if the key is absent (SET NX).

        Useful for distributed "first-write-wins" claims.

        Args:
            key: Cache key. Accepts bare or prefixed forms.
            value: Value to store.
            ttl: TTL in seconds. Overrides ``default_ttl``.

        Returns:
            True if the value was stored, False if the key already exists.

        Example:
            >>> cache.set_if_not_exists("lock:job:1", "worker-a")
            True
            >>> cache.set_if_not_exists("lock:job:1", "worker-b")
            False
        """
        bare = self._normalize_key(key)
        return self._store.set_if_not_exists(bare, value, ttl=ttl)

    # ──────────────────────────────────────────────
    # BULK OPERATIONS
    # ──────────────────────────────────────────────

    def bulk_set(
        self,
        entries: Dict[str, Any],
        ttl: Optional[int] = None,
        overwrite: bool = True,
    ) -> int:
        """
        Store multiple cache entries in a pipeline.

        Args:
            entries: Dict mapping keys to values. Keys may be bare or prefixed.
            ttl: TTL in seconds. Overrides ``default_ttl``.
            overwrite: If False, existing keys are skipped.

        Returns:
            Number of entries stored.

        Example:
            >>> cache.bulk_set({"item:1": {"qty": 5}, "item:2": {"qty": 3}}, ttl=120)
            2
        """
        normalized = {self._normalize_key(k): v for k, v in entries.items()}
        return self._store.bulk_set(normalized, ttl=ttl, overwrite=overwrite)

    def bulk_get(
        self, keys: List[str], default: Any = None
    ) -> Dict[str, Any]:
        """
        Retrieve multiple cache entries in a pipeline.

        Args:
            keys: List of cache keys. Accepts bare or prefixed forms.
            default: Value returned for missing keys.

        Returns:
            Dict mapping keys to their values.

        Example:
            >>> cache.bulk_get(["k1", "k2", "missing"])
            {"k1": [1, 2], "k2": {"a": 1}, "missing": None}
        """
        normalized = [self._normalize_key(k) for k in keys]
        return self._store.bulk_get(normalized, default=default)

    def bulk_delete(self, keys: List[str]) -> int:
        """
        Delete multiple cache entries in a pipeline.

        Args:
            keys: List of cache keys. Accepts bare or prefixed forms.

        Returns:
            Number of keys actually deleted.

        Example:
            >>> cache.bulk_delete(["k1", "k2", "k3"])
            3
        """
        normalized = [self._normalize_key(k) for k in keys]
        return self._store.bulk_delete(normalized)

    # ──────────────────────────────────────────────
    # INSPECTION & INVALIDATION
    # ──────────────────────────────────────────────

    def count(self, pattern: Optional[str] = None, batch_size: int = 1000) -> int:
        """
        Count cache entries under the namespace.

        Args:
            pattern: Optional sub-pattern to match.
            batch_size: SCAN batch size.

        Returns:
            Number of matching entries.

        Example:
            >>> cache.count()
            256
            >>> cache.count(pattern="user:*")
            42
        """
        return self._store.count(pattern=pattern, batch_size=batch_size)

    def list_keys(
        self,
        pattern: Optional[str] = None,
        offset: int = 0,
        limit: int = 0,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        List cache keys under the namespace with pagination.

        Args:
            pattern: Optional sub-pattern to match.
            offset: Number of keys to skip.
            limit: Maximum keys to return. 0 = no limit.
            batch_size: SCAN batch size.

        Returns:
            List of bare keys (without the namespace prefix).

        Example:
            >>> cache.list_keys(pattern="user:*", limit=10)
            ["user:123", "user:456", ...]
        """
        return self._store.list_keys(
            pattern=pattern, offset=offset, limit=limit, batch_size=batch_size
        )

    def invalidate(self, pattern: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries matching a glob pattern.

        Args:
            pattern: Glob pattern (e.g., ``"user:*"``).
            batch_size: SCAN batch size.

        Returns:
            Number of entries deleted.

        Example:
            >>> cache.invalidate("session:*")
            42
        """
        return self._store.delete_pattern(pattern, batch_size=batch_size)

    def invalidate_namespace(self, namespace: str, batch_size: int = 1000) -> int:
        """
        Delete all cache entries under a sub-namespace.

        Args:
            namespace: Sub-namespace (e.g., "session" clears ``CACHE:session:*``).
            batch_size: SCAN batch size.

        Returns:
            Number of entries deleted.

        Example:
            >>> cache.invalidate_namespace("session")
            150
        """
        return self._store.delete_namespace(namespace, batch_size=batch_size)

    def flush(self, batch_size: int = 1000) -> int:
        """
        Delete ALL cache entries under this namespace (dangerous).

        Args:
            batch_size: SCAN batch size.

        Returns:
            Number of entries deleted.

        Example:
            >>> cache.flush()
            1024
        """
        return self._store.delete_all(batch_size=batch_size)

    def close(self) -> None:
        """Close the sync Redis connection."""
        self._store.close()

    # ──────────────────────────────────────────────
    # DUNDER METHODS
    # ──────────────────────────────────────────────

    def __repr__(self) -> str:
        """String representation of RedisCache."""
        return (
            f"RedisCache(url='{self.url}', prefix='{self.prefix}', "
            f"default_ttl={self.default_ttl})"
        )

    def __enter__(self) -> "RedisCache":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit — close connections."""
        self.close()


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────


def example_redis_hash_util() -> None:
    """Demonstrate RedisHashUtil — hash-based persistent storage."""

    print("=" * 60)
    print("RedisHashUtil — Usage Examples")
    print("=" * 60)

    users = RedisHashUtil(
        url="redis://localhost:6379/0",
        prefix="USERS:WORKERS",
        index_key="INDEX",
        lock_key="MUTEX",
        default_ttl=3600,
    )

    # ── STATIC UTILITIES ──────────────────────

    print("\n--- Static Utilities ---")
    print(f"Random string (16): {RedisHashUtil.generate_random_string(16)}")
    print(f"OTP (6 digits):     {RedisHashUtil.generate_random_number(6)}")
    print(f"Token:              {RedisHashUtil.generate_token(32)}")
    print(f"UUID4:              {RedisHashUtil.generate_uuid4()}")
    print(f"UUID5:              {RedisHashUtil.generate_uuid5('myapp.users', 'john@x.com')}")

    password = "SuperSecret123!"
    hashed_pw = RedisHashUtil.hash_password(password)
    print(f"Hashed password:    {hashed_pw[:30]}...")
    print(f"Password verified:  {RedisHashUtil.verify_password(password, hashed_pw)}")

    # ── CRUD ─────────────────────────────────

    print("\n--- CRUD Operations ---")
    users.delete_all()

    user_id = users.create({
        "username": "johndoe",
        "email": "john@example.com",
        "role": "admin",
        "status": "active",
    })
    print(f"Created user: {user_id}  TTL: {users.ttl(user_id)}s")

    session_id = users.create({"token": "abc123"}, ttl=300)
    print(f"Session TTL: {users.ttl(session_id)}s (overridden to 300)")

    print(f"Read user: {users.read(user_id)}")

    users.update(user_id, {"role": "superadmin", "last_login": "2026-01-15"})
    print(f"Updated user: {users.read(user_id)}")

    print(f"set_if_not_exists (first):  {users.set_if_not_exists(user_id, 'created_at', '2026-01-01')}")
    print(f"set_if_not_exists (second): {users.set_if_not_exists(user_id, 'created_at', '2026-06-01')}")

    # ── TTL ───────────────────────────────────

    print("\n--- TTL Operations ---")
    users.expire(user_id, 7200)
    print(f"TTL after expire(7200): {users.ttl(user_id)}s")
    users.persist(user_id)
    print(f"TTL after persist:      {users.ttl(user_id)} (permanent)")

    # ── BULK ──────────────────────────────────

    print("\n--- Bulk Operations ---")
    users.delete_all()

    bulk_users = {
        RedisHashUtil.generate_uuid4(): {"username": f"user_{i}", "status": "active"}
        for i in range(5)
    }
    created = users.bulk_create(bulk_users, ttl=600)
    print(f"Bulk created: {len(created)} entries")

    all_ids = list(bulk_users.keys())
    print(f"Bulk read: {len(users.bulk_read(all_ids))} entries")

    updates = {id: {"status": "inactive"} for id in all_ids[:2]}
    print(f"Bulk updated: {users.bulk_update(updates)} entries")
    print(f"Bulk deleted: {users.bulk_delete(all_ids[:2])} entries")

    # ── SEARCH / INDEX ────────────────────────

    print("\n--- Search & Index ---")
    users.delete_all()
    users.create({"username": "alice", "role": "admin"}, id="u1", overwrite=True)
    users.create({"username": "bob", "role": "user"}, id="u2", overwrite=True)
    users.create({"username": "charlie", "role": "admin"}, id="u3", overwrite=True)

    print(f"Admins (search):     {users.search('role', 'admin')}")
    print(f"Contains 'ali':      {users.search('username', 'ali', exact=False)}")

    users.create_index("u1", "role")
    users.create_index("u2", "role")
    users.create_index("u3", "role")
    print(f"Admins (index):      {users.find_by_index('role', 'admin')}")
    print(f"Admins with data:    {list(users.find_by_index_with_data('role', 'admin').keys())}")

    # ── IMPORT / EXPORT ───────────────────────

    print("\n--- Export / Import ---")
    users.export_json("/tmp/users_export.json")
    print(f"Exported JSON: {users.export_json_string()[:60]}...")

    users_csv_import = RedisHashUtil(prefix="USERS:CSV_IMPORTED")
    users_csv_import.delete_all()
    users.export_csv("/tmp/users_export.csv")
    imported_csv = users_csv_import.import_csv("/tmp/users_export.csv", overwrite=True)
    print(f"Imported from CSV: {imported_csv} entries")

    # ── CLEANUP ───────────────────────────────

    print("\n--- Cleanup ---")
    users.delete_all()
    users_csv_import.delete_all()
    users.close()
    users_csv_import.close()
    print("Done!\n")




def example_redis_string_util() -> None:
    """Demonstrate RedisStringUtil — string-based storage."""

    print("=" * 60)
    print("RedisStringUtil — Usage Examples")
    print("=" * 60)

    store = RedisStringUtil(
        url="redis://localhost:6379/0",
        prefix="API:USERS",
        default_ttl=600,  # optional — entries auto-expire after 10 min
    )

    # ── BASIC CRUD ────────────────────────────

    print("\n--- Basic CRUD ---")
    store.delete_all()

    # set (raises on duplicate)
    store.set("user:123", {"name": "Alice", "role": "admin"}, overwrite=True)
    print("Stored user:123")

    # get (deserializes JSON automatically)
    user = store.get("user:123")
    print(f"Retrieved:   {user}")

    # get with default
    missing = store.get("user:999", default={"name": "Nobody"})
    print(f"Missing key: {missing}")

    # upsert (silent overwrite)
    store.upsert("user:123", {"name": "Alice", "role": "superadmin"})
    print(f"Upserted:    {store.get('user:123')}")

    # exists
    print(f"Exists:      {store.exists('user:123')}")
    print(f"Missing:     {store.exists('user:999')}")

    # delete
    store.set("temp:key", "ephemeral", overwrite=True)
    store.delete("temp:key")
    print(f"Deleted temp:key — exists: {store.exists('temp:key')}")

    # ── GET OR SET ────────────────────────────

    print("\n--- get_or_set ---")

    call_count = 0

    def expensive_query(user_id: str) -> dict:
        """Simulate an expensive database call."""
        nonlocal call_count
        call_count += 1
        return {"name": f"User_{user_id}", "computed_at": time.time()}

    # First call — miss, calls factory
    result = store.get_or_set("user:456", lambda: expensive_query("456"), ttl=300)
    print(f"First call:  {result}  (factory calls: {call_count})")

    # Second call — hit, factory NOT called
    result = store.get_or_set("user:456", lambda: expensive_query("456"), ttl=300)
    print(f"Second call: {result}  (factory calls: {call_count})")

    # With static default
    config = store.get_or_set("config:features", {"dark_mode": True, "beta": False})
    print(f"Config:      {config}")

    # ── TTL OPERATIONS ────────────────────────

    print("\n--- TTL Operations ---")
    store.set("ttl:test", "expires_soon", ttl=30)
    print(f"TTL remaining: {store.ttl('ttl:test')}s")

    store.expire("ttl:test", 3600)
    print(f"After expire:  {store.ttl('ttl:test')}s")

    store.persist("ttl:test")
    print(f"After persist: {store.ttl('ttl:test')} (-1 = permanent)")

    # ── BULK OPERATIONS ───────────────────────

    print("\n--- Bulk Operations ---")
    store.bulk_set({
        "bulk:1": {"item": "apple", "qty": 5},
        "bulk:2": {"item": "banana", "qty": 3},
        "bulk:3": {"item": "cherry", "qty": 8},
    }, ttl=120)

    bulk_data = store.bulk_get(["bulk:1", "bulk:2", "bulk:3", "bulk:missing"])
    print(f"Bulk retrieved: {len(bulk_data)} keys")
    for k, v in bulk_data.items():
        print(f"  {k}: {v}")

    print(f"Bulk deleted: {store.bulk_delete(['bulk:1', 'bulk:2', 'bulk:3'])}")

    # ── ATOMIC SET IF NOT EXISTS ──────────────

    print("\n--- set_if_not_exists ---")
    r1 = store.set_if_not_exists("atomic:key", "first_writer")
    r2 = store.set_if_not_exists("atomic:key", "second_writer")
    print(f"First:  {r1}  (value: {store.get('atomic:key')})")
    print(f"Second: {r2}  (value still: {store.get('atomic:key')})")

    # ── NAMESPACE / PATTERN DELETION ──────────

    print("\n--- Namespace Deletion ---")
    store.set("session:abc", "data1", overwrite=True)
    store.set("session:def", "data2", overwrite=True)
    store.set("user:sess1", "data3", overwrite=True)
    print(f"Before: {store.count()} keys")

    store.delete_namespace("session")
    print(f"After deleting 'session': {store.count()} keys")

    # ── DECORATOR ─────────────────────────────

    print("\n--- @memoize Decorator ---")

    @store.memoize(ttl=300, key_prefix="memoized")
    def get_product(product_id: str) -> dict:
        """Simulate fetching a product from DB."""
        return {"id": product_id, "name": f"Product-{product_id}", "price": 29.99}

    p1 = get_product("P001")
    p2 = get_product("P001")  # served from store
    print(f"Product: {p1}")
    print(f"Stored:  {p1 == p2}  (same object from store)")

    # ── STATS ─────────────────────────────────

    print("\n--- Redis Stats ---")
    s = store.stats()
    print(f"Total keys:     {s['total_keys']}")
    print(f"Memory:         {s['used_memory_human']}")
    print(f"Hit rate:       {s['hit_rate']}")

    # ── IMPORT / EXPORT ───────────────────────

    print("\n--- Export / Import ---")
    store.export_json("/tmp/string_backup.json")
    print("Exported to /tmp/string_backup.json")

    imported = store.import_json("/tmp/string_backup.json", overwrite=True)
    print(f"Imported: {imported} entries")

    json_str = store.export_json_string()
    print(f"JSON string length: {len(json_str)} chars")

    # ── CLEANUP ───────────────────────────────

    print("\n--- Cleanup ---")
    store.delete_all()
    print(f"Remaining keys: {store.count()}")
    store.close()
    print("Done!\n")


def example_redis_cache() -> None:
    """Demonstrate RedisCache — caching layer on RedisStringUtil."""

    print("=" * 60)
    print("RedisCache — Usage Examples")
    print("=" * 60)

    cache = RedisCache(
        url="redis://localhost:6379/0",
        prefix="CACHE",
        default_ttl=None,
    )

    # ── CORE ──────────────────────────────────

    print("\n--- Core CRUD ---")
    cache.flush()

    cache.set("user:123", {"name": "Alice", "role": "admin"}, ttl=300)
    print(f"Stored user:123 under CACHE:user:123")

    # get with or without the CACHE prefix
    print(f"get('user:123'):        {cache.get('user:123')}")
    print(f"get('CACHE:user:123'):  {cache.get('CACHE:user:123')}")

    print(f"exists('user:123'):     {cache.exists('user:123')}")
    print(f"ttl('user:123'):        {cache.ttl('user:123')}s")

    cache.expire("user:123", 600)
    print(f"After expire:           {cache.ttl('user:123')}s")

    cache.delete("user:123")
    print(f"After delete:           {cache.exists('user:123')}")

    # ── CACHE PATTERNS ────────────────────────

    print("\n--- Cache Patterns ---")

    calls = 0

    def db_query(uid: str) -> dict:
        nonlocal calls
        calls += 1
        return {"id": uid, "name": f"User-{uid}"}

    first = cache.get_or_set("profile:42", lambda: db_query("42"), ttl=120)
    second = cache.get_or_set("profile:42", lambda: db_query("42"), ttl=120)
    print(f"First:  {first}  (factory calls: {calls})")
    print(f"Second: {second}  (factory calls: {calls})")

    claimed = cache.set_if_not_exists("lock:job:1", "worker-a")
    claimed_again = cache.set_if_not_exists("lock:job:1", "worker-b")
    print(f"First claim: {claimed}   Second claim: {claimed_again}")

    # ── BULK ──────────────────────────────────

    print("\n--- Bulk ---")
    cache.bulk_set({"item:1": {"qty": 5}, "item:2": {"qty": 3}}, ttl=120)
    data = cache.bulk_get(["item:1", "item:2", "item:missing"])
    print(f"Bulk get: {data}")
    print(f"Bulk delete: {cache.bulk_delete(['item:1', 'item:2'])}")

    # ── INVALIDATION ──────────────────────────

    print("\n--- Invalidation ---")
    cache.set("session:abc", "s1", overwrite=True)
    cache.set("session:def", "s2", overwrite=True)
    print(f"Count: {cache.count()}")
    cache.invalidate_namespace("session")
    print(f"After invalidate_namespace('session'): {cache.count()}")

    # ── CLEANUP ───────────────────────────────

    cache.flush()
    cache.close()
    print("Done!\n")


if __name__ == "__main__":
    example_redis_hash_util()
    example_redis_string_util()
    example_redis_cache()