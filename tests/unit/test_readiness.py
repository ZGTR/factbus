"""C0b-18's cache rule: a positive answer lives at most 60 s; a negative one is never cached."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from factbus.application import BusReadiness
from factbus.ports import BusUnavailable
from tests.support import CountingMetrics, ManualClock, ScriptedBus


def test_positive_answer_is_reused_for_under_sixty_seconds():
    clock, bus = ManualClock(), ScriptedBus()
    readiness = BusReadiness(bus, clock, CountingMetrics())
    assert readiness.confirm_for_publish()
    clock.advance(59)
    assert readiness.confirm_for_publish()
    assert bus.describe_calls == 1
    clock.advance(1)
    assert readiness.confirm_for_publish()
    assert bus.describe_calls == 2


def test_a_deleted_bus_stops_publishing_within_sixty_seconds():
    clock, bus = ManualClock(), ScriptedBus()
    readiness = BusReadiness(bus, clock, CountingMetrics())
    assert readiness.confirm_for_publish()
    bus.exists = False
    clock.advance(30)
    assert readiness.confirm_for_publish()  # still inside the window
    clock.advance(30)
    assert not readiness.confirm_for_publish()
    assert readiness.last_confirmed_at is None


def test_negative_answer_is_never_cached():
    clock, bus = ManualClock(), ScriptedBus(exists=False)
    metrics = CountingMetrics()
    readiness = BusReadiness(bus, clock, metrics)
    assert not readiness.confirm_for_publish()
    bus.exists = True
    assert readiness.confirm_for_publish()
    assert bus.describe_calls == 2
    assert metrics.counts["OutboxBusUnconfirmed"] == 1


def test_confirm_now_is_never_cached():
    clock, bus = ManualClock(), ScriptedBus()
    readiness = BusReadiness(bus, clock, CountingMetrics())
    readiness.confirm_now()
    readiness.confirm_now()
    assert bus.describe_calls == 2


def test_a_cache_longer_than_sixty_seconds_is_refused():
    with pytest.raises(ValueError):
        BusReadiness(ScriptedBus(), ManualClock(), CountingMetrics(), max_age=timedelta(seconds=61))


class _DeniedBus(ScriptedBus):
    def bus_exists(self) -> bool:
        self.describe_calls += 1
        raise BusUnavailable("not authorized to perform: events:DescribeEventBus", code="AccessDeniedException")


def test_an_unconfirmable_bus_logs_why(caplog):
    # A role without events:DescribeEventBus defers every publish. The metric alone
    # cannot tell that from a deleted bus, so the reason must reach the log.
    metrics = CountingMetrics()
    readiness = BusReadiness(_DeniedBus(), ManualClock(), metrics)
    with caplog.at_level(logging.WARNING, logger="factbus.application.readiness"):
        assert not readiness.confirm_now()
    assert metrics.counts["OutboxBusUnconfirmed"] == 1
    assert "AccessDeniedException" in caplog.text
    assert "events:DescribeEventBus" in caplog.text
