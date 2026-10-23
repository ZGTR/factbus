"""The event a producer stages and a consumer receives: CloudEvents 1.0 attribute names.

An factbus envelope is a CloudEvents 1.0 event in structured JSON form, carried as the
EventBridge `detail`. The kit routes and dedupes on three attributes:

* `id`, the dedupe key. Unique per `source`; the kit treats it as globally unique, so
  producers should use a UUID (the default here).
* `type`, the EventBridge `detail-type` and what rules match on.
* `source`, the EventBridge `source` and the producer's identity.

Older envelopes that name these `event_id`, `type` and `producer` (and set a
`specversion` other than "1.0") are still read; `domain.outbox.envelope_keys` decides
which naming an envelope uses. Nothing here performs I/O.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

__all__ = ["CLOUDEVENTS_SPECVERSION", "Event"]

CLOUDEVENTS_SPECVERSION = "1.0"


def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class Event:
    """One fact. `data` must be JSON-serialisable."""

    type: str
    source: str
    data: Mapping[str, Any] = field(default_factory=dict)
    subject: str | None = None
    id: str = field(default_factory=_new_id)
    time: str = field(default_factory=_now)
    #: Extension attributes (CloudEvents allows lowercase alphanumeric names), e.g.
    #: `{"orgid": "o-1", "authorityepoch": 3}`.
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("type", "source", "id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"Event.{name} must be a non-empty string, got {value!r}")
        reserved = {"specversion", "id", "type", "source", "subject", "time", "data", "datacontenttype"}
        clash = reserved.intersection(self.extensions)
        if clash:
            raise ValueError(f"extension attributes may not reuse core names: {sorted(clash)}")

    def to_envelope(self) -> dict[str, Any]:
        envelope: dict[str, Any] = {
            "specversion": CLOUDEVENTS_SPECVERSION,
            "id": self.id,
            "type": self.type,
            "source": self.source,
            "time": self.time,
            "datacontenttype": "application/json",
            "data": dict(self.data),
        }
        if self.subject is not None:
            envelope["subject"] = self.subject
        envelope.update(self.extensions)
        return envelope

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> Event:
        """Read a CloudEvents 1.0 envelope, or a legacy `event_id`/`producer` one."""
        if envelope.get("specversion") == CLOUDEVENTS_SPECVERSION:
            core = {"specversion", "id", "type", "source", "subject", "time", "data", "datacontenttype"}
            return cls(
                id=envelope["id"],
                type=envelope["type"],
                source=envelope["source"],
                subject=envelope.get("subject"),
                time=envelope.get("time") or _now(),
                data=envelope.get("data") or {},
                extensions={k: v for k, v in envelope.items() if k not in core},
            )
        legacy_core = {"event_id", "type", "producer", "subject", "occurred_at", "data"}
        return cls(
            id=envelope["event_id"],
            type=envelope["type"],
            source=envelope["producer"],
            subject=envelope.get("subject"),
            time=envelope.get("occurred_at") or _now(),
            data=envelope.get("data") or {},
            extensions={k: v for k, v in envelope.items() if k not in legacy_core},
        )
