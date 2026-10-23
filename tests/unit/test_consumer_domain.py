"""Backoff, version and breaker rules: pure, so every boundary is pinned to the second."""

from __future__ import annotations

from datetime import timedelta

import pytest

from factbus.domain import (
    MAX_VISIBILITY_SECONDS,
    REAL_WAITS_SECONDS,
    BackoffSchedule,
    ProviderBreaker,
    SubjectVersion,
    is_newer,
    status_advances,
)
from tests.support import T0

# --- backoff (design §2) -----------------------------------------------------------


def test_real_schedule_is_the_nine_waits_by_receive_count():
    schedule = BackoffSchedule()
    assert [schedule.wait_for(n) for n in range(1, 11)] == [30, 120, 600, 1800, 3600, 7200, 7200, 7200, 7200, 0]
    assert schedule.is_real


def test_real_schedule_totals_about_nine_point_seven_hours():
    assert BackoffSchedule().total_seconds == 34_950  # 9 h 42.5 m, design §2's budget


def test_f13_receive_times_follow_from_the_schedule():
    """Receives at 0, 0.5, 2.5, 12.5 and 42.5 minutes: a 20-minute outage drains on receive 5."""
    elapsed, times = 0, [0]
    for receive in range(1, 5):
        elapsed += BackoffSchedule().wait_for(receive)
        times.append(elapsed)
    assert [t / 60 for t in times] == [0, 0.5, 2.5, 12.5, 42.5]


def test_past_the_redrive_receive_the_last_wait_applies_instead_of_spinning():
    schedule = BackoffSchedule()
    assert schedule.wait_for(10) == 0
    assert [schedule.wait_for(n) for n in (11, 12, 50)] == [7200, 7200, 7200]


def test_receive_count_below_one_is_the_first_receive():
    assert BackoffSchedule().wait_for(0) == 30


def test_a_wait_is_capped_at_twelve_hours():
    assert BackoffSchedule((90_000,)).wait_for(1) == MAX_VISIBILITY_SECONDS


def test_scaled_schedule_is_marked_and_keeps_order():
    scaled = BackoffSchedule.scaled(1 / 600)
    assert not scaled.is_real
    assert scaled.waits_seconds == (1, 1, 1, 3, 6, 12, 12, 12, 12)
    assert list(scaled.waits_seconds) == sorted(scaled.waits_seconds)


@pytest.mark.parametrize("factor", [0, 1, 2, -0.5])
def test_scaled_refuses_a_factor_that_does_not_shrink(factor):
    with pytest.raises(ValueError):
        BackoffSchedule.scaled(factor)


def test_schedule_refuses_negative_or_fractional_waits():
    with pytest.raises(ValueError):
        BackoffSchedule((30, -1))
    with pytest.raises(ValueError):
        BackoffSchedule((0.5,))
    assert REAL_WAITS_SECONDS == (30, 120, 600, 1800, 3600, 7200, 7200, 7200, 7200)


# --- versions (K3) ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("applied", "incoming", "newer"),
    [(None, 0, True), (None, 5, True), (4, 5, True), (5, 5, False), (6, 5, False)],
)
def test_is_newer_drops_at_or_below_the_applied_version(applied, incoming, newer):
    assert is_newer(applied, incoming) is newer


RANK = {"queued": 0, "accepted": 1, "delivered": 2, "read": 3}


def test_status_only_moves_forward():
    assert status_advances(None, "delivered", RANK)
    assert status_advances("delivered", "read", RANK)
    assert not status_advances("read", "delivered", RANK)
    assert not status_advances("read", "read", RANK)


def test_unranked_status_is_a_catalogue_error():
    with pytest.raises(KeyError):
        status_advances(None, "bounced", RANK)
    with pytest.raises(KeyError):
        status_advances("bounced", "read", RANK)


def test_subject_version_refuses_bad_values():
    with pytest.raises(ValueError):
        SubjectVersion("", 1)
    with pytest.raises(ValueError):
        SubjectVersion("s", -1)
    with pytest.raises(ValueError):
        SubjectVersion("s", True)


# --- breaker (design §2) ---------------------------------------------------------------


def _at(seconds: float):
    return T0 + timedelta(seconds=seconds)


def test_breaker_opens_on_the_fifth_error_inside_a_minute():
    breaker = ProviderBreaker()
    for s in range(4):
        breaker.record_failure(_at(s * 10))
    assert not breaker.is_open
    breaker.record_failure(_at(40))
    assert breaker.is_open
    assert not breaker.admits(_at(41))


def test_errors_spread_over_more_than_a_minute_do_not_open_it():
    breaker = ProviderBreaker()
    for s in (0, 20, 40, 60, 80):  # the first falls out of the window at t=60
        breaker.record_failure(_at(s))
    assert not breaker.is_open


def test_interleaved_successes_do_not_hide_a_flapping_provider():
    breaker = ProviderBreaker()
    for s in range(5):
        breaker.record_success()  # a success between errors, while closed
        breaker.record_failure(_at(s))
    assert breaker.is_open


def test_open_breaker_admits_one_probe_per_interval_and_closes_on_success():
    breaker = ProviderBreaker()
    for s in range(5):
        breaker.record_failure(_at(s))
    assert not breaker.admits(_at(20))
    assert breaker.admits(_at(34))  # opened at 4 s, probe interval 30 s
    breaker.probe_started(_at(34))
    assert not breaker.admits(_at(35))  # one probe in flight per interval
    breaker.record_failure(_at(36))  # the probe failed: still open, next probe at 64
    assert breaker.is_open and not breaker.admits(_at(60)) and breaker.admits(_at(64))
    breaker.probe_started(_at(64))
    breaker.record_success()
    assert not breaker.is_open and breaker.admits(_at(65))
