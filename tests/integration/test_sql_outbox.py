"""The SQL adapter on PostgreSQL 16 and MySQL 8.0: migration parity, the caller's
transaction, marks, and the sweep's lock under an overlapping second sweep (K5)."""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import inspect, select

import factbus.adapters.sql as sql_adapter
from factbus.application import BusReadiness, Sweeper
from factbus.domain import RowState
from tests.support import CountingMetrics, ScriptedBus, envelope


def test_migration_matches_the_runtime_table(migrated):
    engine, table = migrated
    inspector = inspect(engine)
    reflected = {c["name"]: c for c in inspector.get_columns(table.name)}
    assert set(reflected) == {c.name for c in table.columns}
    for column in table.columns:
        assert reflected[column.name]["nullable"] == column.nullable, column.name
    unique = {tuple(u["column_names"]) for u in inspector.get_unique_constraints(table.name)}
    unique |= {tuple(i["column_names"]) for i in inspector.get_indexes(table.name) if i.get("unique")}
    assert ("event_id",) in unique
    indexes = {tuple(i["column_names"]) for i in inspector.get_indexes(table.name) if not i.get("unique")}
    assert ("sent_at", "failed_at", "id") in indexes


def test_timestamps_keep_microseconds(store):
    outbox, clock = store
    clock.now = clock.now.replace(microsecond=123456)
    with outbox._engine.begin() as connection:
        row = outbox.add(connection, envelope(1))
    assert outbox.row(row.row_id).created_at == clock.now


def test_add_rolls_back_with_the_callers_transaction(store):
    outbox, _ = store
    engine = outbox._engine
    with pytest.raises(RuntimeError), engine.begin() as connection:
        outbox.add(connection, envelope(1))
        raise RuntimeError("the domain write failed")
    with engine.begin() as connection:
        assert connection.execute(select(outbox.table)).all() == []
    with engine.begin() as connection:
        row = outbox.add(connection, envelope(1))
    assert outbox.row(row.row_id).state is RowState.UNSENT


def test_duplicate_event_id_is_refused_by_the_table(store):
    outbox, _ = store
    with outbox._engine.begin() as connection:
        outbox.add(connection, envelope(1))
    with pytest.raises(Exception), outbox._engine.begin() as connection:
        outbox.add(connection, envelope(1))


def test_marks_are_conditional(store):
    outbox, clock = store
    with outbox._engine.begin() as connection:
        a = outbox.add(connection, envelope(1))
        b = outbox.add(connection, envelope(2))
    outbox.mark_failed(b.row_id, clock(), "MalformedDetail")
    assert outbox.mark_sent([a.row_id, b.row_id], clock()) == 1
    assert outbox.row(b.row_id).state is RowState.FAILED
    assert outbox.row(a.row_id).state is RowState.SENT


class BarrierBus(ScriptedBus):
    """Holds the FIRST `put_events` call (sweep A, after it locked chunk one) until released."""

    def __init__(self) -> None:
        super().__init__()
        self.a_selected = threading.Event()
        self.release = threading.Event()
        self._first = True
        self._lock = threading.Lock()

    def put_events(self, entries):
        with self._lock:
            first, self._first = self._first, False
        if first:
            self.a_selected.set()
            assert self.release.wait(30), "sweep A was never released"
        return super().put_events(entries)


def _overlap(store, rows: int):
    outbox, clock = store
    with outbox._engine.begin() as connection:
        for n in range(1, rows + 1):
            outbox.add(connection, envelope(n))
    clock.advance(31)
    bus = BarrierBus()
    metrics = CountingMetrics()

    def sweeper():
        return Sweeper(bus=bus, store=outbox, readiness=BusReadiness(bus, clock, metrics), metrics=metrics, clock=clock)

    reports = {}
    a = threading.Thread(target=lambda: reports.setdefault("a", sweeper().run()))
    a.start()
    assert bus.a_selected.wait(30), "sweep A never reached PutEvents"
    reports["b"] = sweeper().run()
    bus.release.set()
    a.join(60)
    return bus, reports


def test_overlapping_sweeps_publish_each_row_once(store):
    bus, reports = _overlap(store, 25)
    sent = bus.sent_event_ids()
    assert len(sent) == 25 and set(sent.values()) == {1}
    assert sorted(reports["a"].sent + reports["b"].sent) == list(range(1, 26))
    assert reports["b"].sent[0] == 11, "sweep B must have skipped the ten rows A held"


def test_blinded_plain_select_publishes_rows_twice(store, monkeypatch):
    """Blinding arm (P0R-14): the locking read replaced by a plain SELECT goes red."""
    original = sql_adapter.due_rows_query

    def plain(table, **kwargs):
        query = original(table, **kwargs)
        query._for_update_arg = None
        return query

    monkeypatch.setattr(sql_adapter, "due_rows_query", plain)
    bus, _ = _overlap(store, 25)
    assert max(bus.sent_event_ids().values()) == 2
