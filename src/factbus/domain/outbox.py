"""The outbox row, its three states, and the rules for turning rows into a `PutEvents` request.

Pure: no I/O, no SDK, no clock. Everything here is a decision about data the caller
already holds.

A row is UNSENT until the bus accepts it, SENT once it has, and FAILED when the bus
refuses its content permanently (R7-26: a permanent reject must
reach a dead end instead of being retried every minute). SENT means "accepted by the
bus", exactly as design §1 defines it; delivery to targets is the consumers' and the
rule-target DLQs' business, not the outbox's.

The kit reads three envelope keys and nothing else: `event_id`, `type` and `producer`.
CloudEvents 1.0 names them `id`, `type` and `source` (`factbus.envelope`); older envelopes
use the names above. The kit reads either without importing any contract (K-1).
"""

from __future__ import annotations

import enum
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

__all__ = [
    "CLOUDEVENTS_KEYS",
    "ENVELOPE_KEYS",
    "MAX_ENTRIES_PER_REQUEST",
    "MAX_ENTRY_BYTES",
    "MAX_REQUEST_BYTES",
    "OVERSIZED_ENTRY",
    "BusEntry",
    "EntryOutcome",
    "EnvelopeKeyMissing",
    "OutboxRow",
    "PutEventsResponse",
    "RowState",
    "canonical_json",
    "classify_error_code",
    "envelope_keys",
    "outcomes_for",
    "request_prefix",
]

#: The three keys the kit routes and dedupes on (design §1).
ENVELOPE_KEYS = ("event_id", "type", "producer")

#: The same three keys under CloudEvents 1.0 names (`factbus.envelope.Event`).
CLOUDEVENTS_KEYS = ("id", "type", "source")

#: `PutEvents` limits, from the EventBridge user guide's PutEvents page (read
#: 2026-09-24): at most 10 entries per request, the request total under 1 MB, and one
#: entry at most 256 KB.
MAX_ENTRIES_PER_REQUEST = 10
MAX_REQUEST_BYTES = 1_048_576
MAX_ENTRY_BYTES = 262_144


class EnvelopeKeyMissing(ValueError):
    """An envelope lacks one of `ENVELOPE_KEYS`, or holds a non-string or empty value."""


class RowState(enum.Enum):
    UNSENT = "unsent"
    SENT = "sent"
    FAILED = "failed"


@dataclass(frozen=True)
class OutboxRow:
    row_id: int
    event_id: str
    event_type: str
    producer: str
    envelope_json: str
    created_at: datetime
    sent_at: datetime | None = None
    failed_at: datetime | None = None
    attempts: int = 0
    last_error: str | None = None

    @property
    def state(self) -> RowState:
        if self.failed_at is not None:
            return RowState.FAILED
        if self.sent_at is not None:
            return RowState.SENT
        return RowState.UNSENT

    def to_entry(self) -> BusEntry:
        return BusEntry(source=self.producer, detail_type=self.event_type, detail=self.envelope_json)


@dataclass(frozen=True)
class BusEntry:
    """One `PutEvents` entry, without the bus name (the adapter owns that)."""

    source: str
    detail_type: str
    detail: str

    @property
    def size_bytes(self) -> int:
        """The documented entry size: Source + DetailType + Detail (+ Resources, + Time).

        The kit sends neither `Resources` nor `Time`, so both contribute zero.
        """
        return sum(len(part.encode("utf-8")) for part in (self.source, self.detail_type, self.detail))


class EntryOutcome(enum.Enum):
    ACCEPTED = "accepted"
    #: Transient: throttling, internal failure, an unknown code, or a response the kit
    #: cannot attribute to one entry. The row stays unsent and the sweeper retries it.
    RETRY = "retry"
    #: The entry's CONTENT is refused (`MalformedDetail`, `InvalidArgument`, or bigger
    #: than one entry may be). Retrying the same bytes cannot succeed: the row is
    #: marked failed, counted, and never retried (R7-26).
    PERMANENT = "permanent"
    #: The DEPLOYMENT is refused (access denied, not authorised for this source or
    #: detail type, bad account). AWS lists these as non-retryable, but they are fixed
    #: by changing IAM or config, not the row. Marking every row failed on an IAM typo
    #: would strand a whole backlog, so the row stays unsent, is counted under its own
    #: metric, and goes out once the deployment is corrected.
    DEPLOYMENT = "deployment"


#: Per-entry error codes from the PutEventsResultEntry API reference (read
#: 2026-09-25). Hand-written, never derived.
_CONTENT_ERROR_CODES = frozenset({"MalformedDetail", "InvalidArgument"})
_DEPLOYMENT_ERROR_CODES = frozenset(
    {
        "AccessDeniedException",
        "InvalidAccountIdException",
        "NotAuthorizedForSourceException",
        "NotAuthorizedForDetailTypeException",
    }
)

#: The code the kit records for an entry it refuses before sending, because it is
#: larger than one entry may be. Not an AWS code, so it cannot collide with one.
OVERSIZED_ENTRY = "factbus.EntryTooLarge"


def classify_error_code(error_code: str | None) -> EntryOutcome:
    if not error_code:
        return EntryOutcome.ACCEPTED
    if error_code in _CONTENT_ERROR_CODES or error_code == OVERSIZED_ENTRY:
        return EntryOutcome.PERMANENT
    if error_code in _DEPLOYMENT_ERROR_CODES:
        return EntryOutcome.DEPLOYMENT
    return EntryOutcome.RETRY


def envelope_keys(envelope: Mapping[str, object]) -> tuple[str, str, str]:
    """Return `(event_id, type, producer)`, read as plain keys.

    A CloudEvents 1.0 envelope (`specversion` "1.0") names them `id`, `type` and
    `source`; any other envelope names them `event_id`, `type` and `producer`.
    """
    keys = CLOUDEVENTS_KEYS if envelope.get("specversion") == "1.0" else ENVELOPE_KEYS
    values = []
    for key in keys:
        value = envelope.get(key)
        if not isinstance(value, str) or not value:
            raise EnvelopeKeyMissing(f"envelope key {key!r} must be a non-empty string, got {value!r}")
        values.append(value)
    return values[0], values[1], values[2]


def canonical_json(envelope: Mapping[str, object]) -> str:
    """The exact bytes stored in the row and sent as `Detail`."""
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class PutEventsResponse:
    """What one `PutEvents` call answered, reduced to what the outbox acts on.

    `entry_error_codes` holds one item per response `Entries[]` item: `None` for an
    accepted entry, the `ErrorCode` otherwise.
    """

    failed_entry_count: int
    entry_error_codes: tuple[str | None, ...]


def outcomes_for(request_count: int, response: PutEventsResponse) -> tuple[tuple[EntryOutcome, ...], bool]:
    """Map a response onto the request's entries. Returns `(outcomes, attributable)`.

    AWS returns one `Entries[]` item per request entry, in order, so the mapping is
    positional. LocalStack 3.8's v2 provider was MEASURED returning fewer items than it
    was sent (P10), so the kit does not assume the contract:

    * same length: positional.
    * different length and `FailedEntryCount` is 0: every entry was accepted, because
      `FailedEntryCount` counts every failed entry.
    * different length and `FailedEntryCount` > 0: the failures cannot be attributed.
      Every entry is RETRY. The accepted ones go out again on the next sweep and the
      consumer inbox absorbs the duplicate; none is marked failed on a guess.
    """
    codes = response.entry_error_codes
    if len(codes) == request_count:
        return tuple(classify_error_code(code) for code in codes), True
    if response.failed_entry_count == 0:
        return tuple(EntryOutcome.ACCEPTED for _ in range(request_count)), False
    return tuple(EntryOutcome.RETRY for _ in range(request_count)), False


def request_prefix(rows: Sequence[OutboxRow]) -> int:
    """How many leading rows fit one `PutEvents` request (10 entries, under 1 MB).

    Oversized single entries are the caller's to refuse before this is asked; one that
    slips through still counts, so a request never exceeds the limit because of it.
    """
    total = 0
    taken = 0
    for row in rows[:MAX_ENTRIES_PER_REQUEST]:
        size = row.to_entry().size_bytes
        if taken and total + size >= MAX_REQUEST_BYTES:
            break
        total += size
        taken += 1
    return taken
