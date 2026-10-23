"""One event as a consumer holds it after the transport is peeled off, and what the inbox did.

`application.consume` turns an SQS record into a `ConsumedEvent`; nothing below the
application layer sees a raw record (the architecture gate's clause 5). The dedupe key
is `event_id` from the envelope (EventBridge's `detail.event_id`), NEVER the EventBridge
`id`, which is new on every replay and would let a replayed window apply twice.
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass

__all__ = ["ConsumedEvent", "InboxOutcome"]


@dataclass(frozen=True)
class ConsumedEvent:
    event_id: str
    envelope: Mapping[str, object]
    message_id: str
    receive_count: int


class InboxOutcome(enum.Enum):
    #: The effect and the inbox row committed together.
    APPLIED = "applied"
    #: The inbox already held `event_id`, found by a read or by the unique key refusing
    #: the insert (C0b-17). Acknowledged, never a failure.
    DUPLICATE = "duplicate"
    #: The subject's applied version is at or above this event's. The inbox row is
    #: committed with no effect, so a later copy is a plain duplicate.
    STALE = "stale"
