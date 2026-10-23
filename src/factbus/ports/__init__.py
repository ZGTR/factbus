"""The seams the application calls through. Adapters implement them; composition roots
construct the adapters and inject them. Nothing here performs I/O.

A separate `ports` package, rather than a module inside `application`, so the adapters
can name the errors they raise without importing the application layer (the service
kit's layering rule: adapters see ports and domain only).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, Protocol

from factbus.domain import (
    BusEntry,
    ConsumedEvent,
    InboxOutcome,
    OutboxRow,
    PutEventsResponse,
    SubjectVersion,
)
from factbus.domain.claims import Claim, ClaimState

__all__ = [
    "BusUnavailable",
    "ClaimStore",
    "ClaimStoreError",
    "Clock",
    "Effect",
    "EventBus",
    "Inbox",
    "LockedChunk",
    "Metrics",
    "OutboxStore",
    "OutboxStoreError",
    "ProviderError",
    "Sleep",
    "SweepSession",
    "VisibilityChanger",
    "VisibilityUnavailable",
]


class BusUnavailable(Exception):
    """A bus call failed at the request level: no per-entry answer exists.

    Raised for a network error, a throttled or failed request, or an unknown bus.
    `code` carries the AWS error code when there is one, so a row can record it.
    """

    def __init__(self, message: str, *, code: str = "request_failed") -> None:
        super().__init__(message)
        self.code = code


class OutboxStoreError(Exception):
    """The outbox table could not be read or written."""


class Clock(Protocol):
    def __call__(self) -> datetime: ...


class Sleep(Protocol):
    """Blocks the calling thread; `time.sleep` in production, a recorder in tests."""

    def __call__(self, seconds: float) -> None: ...


class Metrics(Protocol):
    def count(self, name: str, value: int = 1) -> None: ...


class EventBus(Protocol):
    def put_events(self, entries: Sequence[BusEntry]) -> PutEventsResponse:
        """One `PutEvents` request. Raises `BusUnavailable` on a request-level failure."""
        ...

    def bus_exists(self) -> bool:
        """A fresh `DescribeEventBus`. True only when the configured bus answers."""
        ...


class LockedChunk(Protocol):
    """Rows selected `FOR UPDATE SKIP LOCKED` inside one open transaction."""

    rows: Sequence[OutboxRow]

    def mark_sent(self, row_ids: Sequence[int], at: datetime) -> None: ...

    def mark_failed(self, row_id: int, at: datetime, error: str) -> None: ...

    def record_retry(self, row_ids: Sequence[int], error: str) -> None: ...


class SweepSession(Protocol):
    def locked_chunk(
        self, *, created_before: datetime, after_row_id: int, limit: int
    ) -> AbstractContextManager[LockedChunk]:
        """Begin a transaction, lock up to `limit` due rows, yield them, commit on exit."""
        ...


class OutboxStore(Protocol):
    def add(self, connection: Any, envelope: Mapping[str, object]) -> OutboxRow:
        """Insert one row on the CALLER's connection, inside the caller's transaction."""
        ...

    def mark_sent(self, row_ids: Sequence[int], at: datetime) -> int: ...

    def mark_failed(self, row_id: int, at: datetime, error: str) -> None: ...

    def record_retry(self, row_ids: Sequence[int], error: str) -> None: ...

    def open_sweep(self) -> AbstractContextManager[SweepSession]: ...

    def sent_since(self, since: datetime, *, after_row_id: int, limit: int) -> list[OutboxRow]: ...

    def purge_sent_before(self, before: datetime, *, limit: int) -> int: ...


class ProviderError(Exception):
    """A retryable error from the provider an effect calls (SES, Twilio, a product API).

    The one error the provider breaker counts. Any other exception from an effect is
    retried with backoff but does not move the breaker: a bug in the effect is not an
    outage of the provider.
    """


class VisibilityUnavailable(Exception):
    """`ChangeMessageVisibility` failed. The item is still reported as failed (C0b-19)."""


#: The service's effect: runs on the inbox's open connection, inside the transaction
#: that also inserts the inbox row. Raising rolls both back.
Effect = Callable[[Any, ConsumedEvent], None]


class Inbox(Protocol):
    def process(
        self, event: ConsumedEvent, *, subject: SubjectVersion | None, effect: Effect
    ) -> InboxOutcome:
        """One transaction: dedupe on `event.event_id`, apply the version guard when
        `subject` is given, run `effect`, commit. A unique-key violation on the inbox
        row is DUPLICATE, never an error. Anything else propagates."""
        ...


class VisibilityChanger(Protocol):
    def change_visibility(self, receipt_handle: str, seconds: int) -> None:
        """One `ChangeMessageVisibility`. Raises `VisibilityUnavailable` on failure."""
        ...


class ClaimStoreError(Exception):
    """The claim table could not be read or written. Nothing about the claim is known."""


class ClaimStore(Protocol):
    """The table behind `factbus.once`. Every write is conditional; a lost condition is a
    `False` return, never an exception, because losing a race is an expected outcome.
    Any other failure raises `ClaimStoreError`."""

    def claim(self, claim: Claim, now: datetime) -> bool:
        """Insert `claim` unless a row for its key exists and has not expired at `now`.
        True when this call wrote the row."""
        ...

    def read(self, key: str) -> Claim | None:
        """A strongly consistent read: a row written by `claim` a moment ago is seen."""
        ...

    def settle(self, key: str, owner: str, state: ClaimState, *, result_json: str | None = None) -> bool:
        """Move the row to `state` only while it is `pending` and held by `owner`."""
        ...

    def release(self, key: str, owner: str) -> bool:
        """Delete the row only while it is `pending` and held by `owner`."""
        ...
