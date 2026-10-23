"""Test doubles for the unit suite: a scripted bus, an in-memory store, a counting sink.

The in-memory store honours the port's semantics that the application relies on (a
chunk's marks land only when its block exits cleanly; rows are selected past a cursor)
and nothing more. Locking is NOT modelled: concurrency is proved on real PostgreSQL and
MySQL in tests/integration, never here.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

from factbus.domain import BusEntry, OutboxRow, PutEventsResponse, canonical_json, envelope_keys
from factbus.ports import BusUnavailable, OutboxStoreError

T0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)


def envelope(n: int, **extra) -> dict:
    """A legacy-shaped fact (`event_id`, `producer`), hand-written (the kit imports no catalogue)."""
    body = {
        "contract_version": "example.event/v1",
        "event_id": f"evt-{n:05d}",
        "occurred_at": "2026-09-25T12:00:00Z",
        "org_id": "org-1",
        "producer": "fake-producer",
        "type": "example.widget.changed",
        "subject": f"wid-{n}",
        "authority_epoch": n,
        "causation_id": None,
        "correlation_id": f"cor-{n}",
    }
    body.update(extra)
    return body


class ManualClock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class CountingMetrics:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()

    def count(self, name: str, value: int = 1) -> None:
        self.counts[name] += value


@dataclass
class ScriptedBus:
    """Answers `put_events` from a script; records every entry it was sent."""

    exists: bool = True
    answer: Callable[[Sequence[BusEntry]], PutEventsResponse] | None = None
    raise_code: str | None = None
    calls: list[list[BusEntry]] = field(default_factory=list)
    describe_calls: int = 0

    def put_events(self, entries: Sequence[BusEntry]) -> PutEventsResponse:
        self.calls.append(list(entries))
        if self.raise_code:
            raise BusUnavailable("scripted request failure", code=self.raise_code)
        if self.answer is not None:
            return self.answer(entries)
        return PutEventsResponse(failed_entry_count=0, entry_error_codes=tuple(None for _ in entries))

    def bus_exists(self) -> bool:
        self.describe_calls += 1
        return self.exists

    def sent_event_ids(self) -> Counter[str]:
        return Counter(json.loads(entry.detail)["event_id"] for call in self.calls for entry in call)


class MemoryStore:
    def __init__(self, clock: ManualClock) -> None:
        self._clock = clock
        self.rows: dict[int, OutboxRow] = {}
        self.fail_marks = False

    def add(self, connection, envelope_dict) -> OutboxRow:
        event_id, event_type, producer = envelope_keys(envelope_dict)
        row = OutboxRow(
            row_id=len(self.rows) + 1,
            event_id=event_id,
            event_type=event_type,
            producer=producer,
            envelope_json=canonical_json(envelope_dict),
            created_at=self._clock(),
        )
        self.rows[row.row_id] = row
        return row

    def _update(self, row_id: int, **changes) -> None:
        self.rows[row_id] = replace(self.rows[row_id], **changes)

    def mark_sent(self, row_ids, at) -> int:
        if self.fail_marks:
            raise OutboxStoreError("scripted store failure")
        n = 0
        for row_id in row_ids:
            if self.rows[row_id].sent_at is None and self.rows[row_id].failed_at is None:
                self._update(row_id, sent_at=at, last_error=None)
                n += 1
        return n

    def mark_failed(self, row_id, at, error) -> None:
        row = self.rows[row_id]
        self._update(row_id, failed_at=at, attempts=row.attempts + 1, last_error=error)

    def record_retry(self, row_ids, error) -> None:
        for row_id in row_ids:
            self._update(row_id, attempts=self.rows[row_id].attempts + 1, last_error=error)

    @contextmanager
    def open_sweep(self):
        yield _MemorySweepSession(self)

    def sent_since(self, since, *, after_row_id, limit):
        rows = [r for r in sorted(self.rows.values(), key=lambda r: r.row_id)
                if r.sent_at is not None and r.sent_at >= since and r.row_id > after_row_id]
        return rows[:limit]

    def purge_sent_before(self, before, *, limit) -> int:
        doomed = [r.row_id for r in sorted(self.rows.values(), key=lambda r: r.row_id)
                  if r.sent_at is not None and r.sent_at < before][:limit]
        for row_id in doomed:
            del self.rows[row_id]
        return len(doomed)


class _MemoryChunk:
    def __init__(self, store: MemoryStore, rows) -> None:
        self._store = store
        self.rows = rows
        self._pending: list[tuple[str, tuple]] = []

    def mark_sent(self, row_ids, at):
        self._pending.append(("mark_sent", (row_ids, at)))

    def mark_failed(self, row_id, at, error):
        self._pending.append(("mark_failed", (row_id, at, error)))

    def record_retry(self, row_ids, error):
        self._pending.append(("record_retry", (row_ids, error)))

    def commit(self) -> None:
        for name, args in self._pending:
            getattr(self._store, name)(*args)


class _MemorySweepSession:
    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    @contextmanager
    def locked_chunk(self, *, created_before, after_row_id, limit):
        rows = [r for r in sorted(self._store.rows.values(), key=lambda r: r.row_id)
                if r.sent_at is None and r.failed_at is None and r.created_at < created_before
                and r.row_id > after_row_id][:limit]
        chunk = _MemoryChunk(self._store, rows)
        yield chunk
        chunk.commit()


def sqs_record(event: dict | None, *, message_id: str, receive_count: int = 1, body: str | None = None,
               eventbridge_id: str = "eb-00000000") -> dict:
    """An ESM-shaped SQS record whose body is an EventBridge event carrying `event`."""
    if body is None:
        body = json.dumps({
            "version": "0",
            "id": eventbridge_id,
            "detail-type": event["type"],
            "source": event.get("source") if event.get("specversion") == "1.0" else event["producer"],
            "detail": event,
        })
    return {
        "messageId": message_id,
        "receiptHandle": f"rh-{message_id}-{receive_count}",
        "body": body,
        "attributes": {"ApproximateReceiveCount": str(receive_count)},
        "eventSource": "aws:sqs",
    }


class MemoryInbox:
    """The Inbox port's semantics without a database: dedupe, version guard, effect,
    all-or-nothing. Concurrency is proved on real engines in tests/integration."""

    def __init__(self) -> None:
        self.seen: set[str] = set()
        self.versions: dict[str, int] = {}
        self.fail_with: Exception | None = None

    def process(self, event, *, subject, effect):
        from factbus.domain import InboxOutcome, is_newer

        if self.fail_with is not None:
            raise self.fail_with
        if event.event_id in self.seen:
            return InboxOutcome.DUPLICATE
        if subject is not None and not is_newer(self.versions.get(subject.subject_key), subject.version):
            self.seen.add(event.event_id)
            return InboxOutcome.STALE
        effect(None, event)  # raises before anything is recorded: all-or-nothing
        self.seen.add(event.event_id)
        if subject is not None:
            self.versions[subject.subject_key] = subject.version
        return InboxOutcome.APPLIED


@dataclass
class ScriptedVisibility:
    fail: bool = False
    calls: list[tuple[str, int]] = field(default_factory=list)

    def change_visibility(self, receipt_handle: str, seconds: int) -> None:
        self.calls.append((receipt_handle, seconds))
        if self.fail:
            from factbus.ports import VisibilityUnavailable

            raise VisibilityUnavailable("scripted visibility failure")
