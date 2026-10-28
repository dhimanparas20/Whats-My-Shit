"""
WAHA response models — normalized views of raw WAHA HTTP payloads.

Pure data + validation (pydantic only); the send tasks embed these into
``TaskStatus.result`` via ``model_dump(by_alias=True)`` so the JSON keys
stay WAHA's original names (e.g. ``from``, not ``from_``).
"""

from __future__ import annotations

from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class WAHASendResult(BaseModel):
    """
    Normalized view of WAHA's send response (``resp['_data']``).

    Extra keys in the payload are ignored (pydantic v2 default); missing
    required keys raise ValidationError — callers treat that as a send
    failure, exactly like a KeyError did before.
    """

    model_config = ConfigDict(populate_by_name=True)

    # `from` is a Python keyword → field is `from_`, aliased to `from`.
    from_: str = Field(alias="from")
    to: str
    viewed: Optional[bool] = None
    # WEBJS returns a dict with `_serialized`; some engines a plain string.
    id: Union[str, dict, None] = None

    @classmethod
    def from_waha(cls, resp: dict) -> "WAHASendResult":
        """
        Extract + validate from a raw WAHA send response.

        Raises KeyError when ``resp['_data']`` is missing and
        ValidationError when required fields are absent.
        """
        return cls(**resp["_data"])

