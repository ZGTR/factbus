"""The application layer against the in-memory doubles: on-commit publish, sweep, R7-26,
the P10 attribution rule, republish and retention. Concurrency is not claimed here."""

from __future__ import annotations

from datetime import timedelta

import pytest

from factbus.application import (
    BusReadiness,
    OnCommitPublisher,
    Sweeper,
    purge_sent_rows,
    republish_since,
)
from factbus.domain import MAX_ENTRY_BYTES, OVERSIZED_ENTRY, PutEventsResponse, RowState
from tests.support import CountingMetrics, ManualClock, MemoryStore, ScriptedBus, envelope


@pytest.fixture
def world():
    clock = ManualClock()
    store = MemoryStore(clock)
    bus = ScriptedBus()
    metrics = CountingMetrics()
    readiness = BusReadiness(bus, clock, metrics)
    publisher = OnCommitPublisher(bus=bus, store=store, readiness=readiness, metrics=metrics, clock=clock)
    sweeper = Sweeper(bus=bus, store=store, readiness=readiness, metrics=metrics, clock=clock)
    return clock, store, bus, metrics, readiness, publisher, sweeper


def _add(store, n):
    return [store.add(None, envelope(i)) for i in range(1, n + 1)]


def test_on_commit_publishes_in_chunks_of_ten_and_marks_sent(world):
    _, store, bus, _, _, publisher, _ = world
    rows = _add(store, 23)
    report = publisher.publish_on_commit(rows)
    assert [len(call) for call in bus.calls] == [10, 10, 3]
    assert len(report.sent) == 23
    assert all(r.state is RowState.SENT for r in store.rows.values())


def test_on_commit_with_unconfirmed_bus_publishes_nothing(world):
    _, store, bus, metrics, _, publisher, _ = world
    bus.exists = False
    report = publisher.publish_on_commit(_add(store, 3))
    assert not report.bus_confirmed and len(report.deferred) == 3
    assert bus.calls == []
    assert metrics.counts["OutboxPublishDeferred"] == 3


def test_on_commit_never_raises_when_the_marks_fail(world):
    _, store, bus, _, _, publisher, _ = world
    store.fail_marks = True
    report = publisher.publish_on_commit(_add(store, 2))
    assert len(bus.calls) == 1 and len(report.deferred) == 2
    assert all(r.state is RowState.UNSENT for r in store.rows.values())


def test_on_commit_request_failure_defers_the_rest(world):
    _, store, bus, _, _, publisher, _ = world
    bus.raise_code = "ThrottlingException"
    report = publisher.publish_on_commit(_add(store, 15))
    assert len(bus.calls) == 1
    assert len(report.deferred) == 15
    assert store.rows[1].last_error == "request:ThrottlingException"


def test_sweep_takes_only_rows_older_than_thirty_seconds(world):
    clock, store, bus, _, _, _, sweeper = world
    _add(store, 3)
    clock.advance(29)
    assert sweeper.run().sent == []
    clock.advance(2)
    assert sweeper.run().sent == [1, 2, 3]
    assert sweeper.run().sent == []


def test_sweep_stops_at_five_hundred_rows(world):
    clock, store, bus, _, _, _, sweeper = world
    _add(store, 523)
    clock.advance(31)
    report = sweeper.run()
    assert report.examined == 500 and report.chunks == 50
    assert sweeper.run().sent == list(range(501, 524))


def test_sweep_refuses_when_the_bus_is_absent(world):
    clock, store, bus, metrics, _, _, sweeper = world
    _add(store, 2)
    clock.advance(31)
    bus.exists = False
    report = sweeper.run()
    assert not report.bus_confirmed and bus.calls == []
    assert metrics.counts["OutboxSweepRefused"] == 1


def test_sweep_stops_on_a_request_level_failure(world):
    clock, store, bus, metrics, _, _, sweeper = world
    _add(store, 25)
    clock.advance(31)
    bus.raise_code = "InternalFailure"
    report = sweeper.run()
    assert report.stopped_on_request_failure and len(bus.calls) == 1
    assert all(r.attempts == 1 for r in list(store.rows.values())[:10])
    assert all(r.attempts == 0 for r in list(store.rows.values())[10:])


def test_r7_26_permanent_reject_is_failed_counted_and_never_retried(world):
    """R7-26 at unit level (P10: LocalStack 3.8 cannot produce a per-entry error)."""
    clock, store, bus, metrics, _, _, sweeper = world
    _add(store, 3)
    clock.advance(31)
    bus.answer = lambda entries: PutEventsResponse(1, (None, "MalformedDetail", None))
    report = sweeper.run()
    assert report.sent == [1, 3] and report.failed == [2]
    assert store.rows[2].state is RowState.FAILED and store.rows[2].last_error == "MalformedDetail"
    assert metrics.counts["OutboxRowsFailed"] == 1
    calls_before = len(bus.calls)
    clock.advance(60)
    assert sweeper.run().examined == 0
    assert len(bus.calls) == calls_before


def test_r7_26_blinded_permanent_code_would_be_retried_every_minute(world, monkeypatch):
    """Blinding arm: with the permanent class emptied, the same reject is retried forever."""
    import factbus.domain.outbox as outbox

    monkeypatch.setattr(outbox, "_CONTENT_ERROR_CODES", frozenset())
    clock, store, bus, _, _, _, sweeper = world
    _add(store, 1)
    clock.advance(31)
    bus.answer = lambda entries: PutEventsResponse(1, ("MalformedDetail",))
    for _ in range(3):
        sweeper.run()
        clock.advance(60)
    assert store.rows[1].state is RowState.UNSENT and len(bus.calls) == 3


def test_deployment_reject_keeps_the_row_unsent(world):
    clock, store, bus, metrics, _, _, sweeper = world
    _add(store, 1)
    clock.advance(31)
    bus.answer = lambda entries: PutEventsResponse(1, ("AccessDeniedException",))
    sweeper.run()
    assert store.rows[1].state is RowState.UNSENT
    assert metrics.counts["OutboxDeploymentRejected"] == 1


def test_unattributable_response_fails_nothing(world):
    clock, store, bus, metrics, _, _, sweeper = world
    _add(store, 3)
    clock.advance(31)
    bus.answer = lambda entries: PutEventsResponse(1, ("MalformedDetail",))
    report = sweeper.run()
    assert report.failed == [] and report.retry == [1, 2, 3]
    assert metrics.counts["OutboxResponseUnattributable"] == 1


def test_oversized_entry_is_failed_before_sending(world):
    clock, store, bus, _, _, publisher, _ = world
    rows = [store.add(None, envelope(1, pad="x" * MAX_ENTRY_BYTES)), store.add(None, envelope(2))]
    report = publisher.publish_on_commit(rows)
    assert report.failed == [1] and report.sent == [2]
    assert store.rows[1].last_error == OVERSIZED_ENTRY
    assert [len(c) for c in bus.calls] == [1]


def test_republish_resends_rows_sent_since(world):
    clock, store, bus, _, readiness, publisher, _ = world
    publisher.publish_on_commit(_add(store, 5))
    since = clock()
    clock.advance(10)
    publisher.publish_on_commit([store.add(None, envelope(99))])
    bus.calls.clear()
    report = republish_since(since, bus=bus, store=store, readiness=readiness, metrics=CountingMetrics(), clock=clock)
    assert report.sent == [1, 2, 3, 4, 5, 6]
    assert sum(bus.sent_event_ids().values()) == 6


def test_retention_purges_only_sent_rows_older_than_seven_days(world):
    clock, store, bus, _, _, publisher, sweeper = world
    publisher.publish_on_commit(_add(store, 2))
    bus.answer = lambda entries: PutEventsResponse(1, ("MalformedDetail",))
    publisher.publish_on_commit([store.add(None, envelope(3))])
    store.add(None, envelope(4))
    clock.advance(timedelta(days=7, seconds=1).total_seconds())
    assert purge_sent_rows(store=store, clock=clock) == 2
    assert sorted(store.rows) == [3, 4]
    with pytest.raises(ValueError):
        purge_sent_rows(store=store, clock=clock, retention=timedelta(days=6))
