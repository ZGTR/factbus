"""The provider-rate guard (design §2 token bucket): the pure bucket, the per-instance
split, and `Consumer` blocking on it. Every aggregate claim runs twice: with the guard,
where the provider's limit holds in every one-second window, and blinded, where it must
not, so the measurement is shown able to see a breach."""

from __future__ import annotations

from bisect import bisect_right
from datetime import timedelta

import pytest

from factbus.application import Consumer
from factbus.domain import BackoffSchedule, ProviderBreaker, TokenBucket, instance_rate
from tests.support import T0, CountingMetrics, ManualClock, MemoryInbox, ScriptedVisibility, envelope, sqs_record

LONG = timedelta(hours=1)


def peak_per_second(times: list[float]) -> int:
    """The most calls inside any CLOSED one-second window, the strictest reading."""
    ordered = sorted(times)
    return max((bisect_right(ordered, t + 1.0) - i for i, t in enumerate(ordered)), default=0)


def at(seconds: float):
    return T0 + timedelta(seconds=seconds)


# --- the bucket -------------------------------------------------------------------------


def test_a_full_bucket_admits_its_burst_at_once_then_spaces_calls():
    bucket = TokenBucket(rate_per_second=2.0, burst=2)
    waits = [bucket.reserve(at(0), LONG).total_seconds() for _ in range(4)]
    assert waits == [0.0, 0.0, 0.5, 1.0]


def test_a_wait_longer_than_max_wait_books_nothing():
    bucket = TokenBucket(rate_per_second=1.0)
    assert bucket.reserve(at(0), LONG) == timedelta(0)
    before = bucket.tokens
    assert bucket.reserve(at(0), timedelta(seconds=0.5)) is None
    assert bucket.tokens == before
    assert bucket.reserve(at(0), timedelta(seconds=1)) == timedelta(seconds=1)


def test_refill_is_capped_at_the_burst():
    bucket = TokenBucket(rate_per_second=1.0, burst=2)
    bucket.reserve(at(0), LONG)
    bucket.reserve(at(0), LONG)
    assert [bucket.reserve(at(100), LONG).total_seconds() for _ in range(3)] == [0.0, 0.0, 1.0]


def test_a_clock_that_steps_back_refills_nothing():
    bucket = TokenBucket(rate_per_second=1.0)
    bucket.reserve(at(10), LONG)
    assert bucket.reserve(at(5), LONG) == timedelta(seconds=1)


@pytest.mark.parametrize(("rate", "burst"), [(0, 1), (-1, 1), (1, 0)])
def test_the_bucket_refuses_a_meaningless_shape(rate, burst):
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=rate, burst=burst)


# --- the per-instance split -------------------------------------------------------------


def test_instance_rate_subtracts_the_burst_from_each_share():
    assert instance_rate(10, 3) == pytest.approx(10 / 3 - 1)
    assert instance_rate(10, 1, burst=2) == pytest.approx(8)


@pytest.mark.parametrize(("rate", "concurrency", "burst"), [(3, 3, 1), (10, 5, 2), (0, 1, 1), (10, 0, 1), (10, 1, 0)])
def test_instance_rate_refuses_a_split_that_cannot_hold(rate, concurrency, burst):
    with pytest.raises(ValueError):
        instance_rate(rate, concurrency, burst=burst)


def _saturate(per_instance: float | None, instances: int, seconds: float, burst: int = 1) -> list[float]:
    """Every instance calls as soon as its own bucket allows, for `seconds`. `None` is the
    guard switched off: each instance calls every 10 ms."""
    times: list[float] = []
    for offset in range(instances):
        phase = offset * 0.013  # instances do not start in lockstep
        if per_instance is None:
            times += [phase + n * 0.01 for n in range(int(seconds / 0.01))]
            continue
        bucket, now = TokenBucket(rate_per_second=per_instance, burst=burst), phase
        while now < seconds:
            now += bucket.reserve(at(now), LONG).total_seconds()
            times.append(now)
    return times


@pytest.mark.parametrize(
    ("label", "per_instance", "holds"),
    [
        ("guarded: instance_rate(10, 3)", instance_rate(10, 3), True),
        ("blinded: no guard", None, False),
        ("blinded: the share without the burst term, 10 / 3", 10 / 3, False),
    ],
)
def test_three_instances_hold_the_providers_limit_only_with_the_guard(label, per_instance, holds):
    peak = peak_per_second(_saturate(per_instance, instances=3, seconds=20))
    assert (peak <= 10) is holds, f"{label}: peak {peak}/s against a limit of 10/s"


# --- the consumer blocks on the guard ---------------------------------------------------


class ClockedEffect:
    """Records the clock at each provider call, i.e. each effect."""

    def __init__(self, clock: ManualClock) -> None:
        self.clock, self.calls = clock, []

    def __call__(self, connection, event) -> None:
        self.calls.append((self.clock() - T0).total_seconds())


class AdvancingSleep:
    def __init__(self, clock: ManualClock) -> None:
        self.clock, self.slept = clock, []

    def __call__(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.clock.advance(seconds)


def _consumer(guard: TokenBucket | None, *, max_wait: timedelta = timedelta(seconds=5)):
    clock = ManualClock()
    effect, sleep, visibility, metrics = ClockedEffect(clock), AdvancingSleep(clock), ScriptedVisibility(), CountingMetrics()
    consumer = Consumer(
        inbox=MemoryInbox(), visibility=visibility, effect=effect, schedule=BackoffSchedule(), clock=clock,
        metrics=metrics, breaker=ProviderBreaker(), rate_guard=guard, rate_max_wait=max_wait,
        sleep=sleep if guard is not None else None,
    )
    return consumer, effect, sleep, visibility, metrics


def _batch(count: int):
    return [sqs_record(envelope(n), message_id=f"m{n}") for n in range(1, count + 1)]


@pytest.mark.parametrize(("label", "guarded"), [("guarded", True), ("blinded: guard off", False)])
def test_a_batch_is_paced_at_the_guards_rate_only_when_guarded(label, guarded):
    consumer, effect, sleep, _, _ = _consumer(TokenBucket(rate_per_second=2.0) if guarded else None)
    assert consumer.handle_batch(_batch(10)) == {"batchItemFailures": []}
    assert len(effect.calls) == 10
    peak = peak_per_second(effect.calls)
    assert (peak <= 3) is guarded, f"{label}: peak {peak}/s, guard allows 1 + 2 per closed second"
    if guarded:
        assert sleep.slept == [0.5] * 9


def test_a_wait_beyond_max_wait_defers_with_backoff_and_never_drops():
    consumer, effect, _, visibility, metrics = _consumer(
        TokenBucket(rate_per_second=1.0), max_wait=timedelta(seconds=0.5)
    )
    response = consumer.handle_batch(_batch(3))
    assert effect.calls == [0.0]
    assert response == {"batchItemFailures": [{"itemIdentifier": "m2"}, {"itemIdentifier": "m3"}]}
    assert [seconds for _, seconds in visibility.calls] == [30, 30]
    assert metrics.counts["ConsumerRateDeferred"] == 2


def test_a_rate_guard_without_sleep_is_refused():
    with pytest.raises(ValueError, match="sleep"):
        Consumer(
            inbox=MemoryInbox(), visibility=ScriptedVisibility(), effect=lambda c, e: None,
            schedule=BackoffSchedule(), clock=ManualClock(), metrics=CountingMetrics(),
            rate_guard=TokenBucket(rate_per_second=1.0),
        )


def test_a_breaker_deferral_spends_no_rate_token():
    guard = TokenBucket(rate_per_second=1.0)
    consumer, effect, _, _, _ = _consumer(guard)
    breaker = consumer._breaker
    for _ in range(5):
        breaker.record_failure(T0)
    consumer.handle_batch(_batch(1))
    assert effect.calls == [] and guard.tokens == 1.0


# --- admission at the actual inbox effect boundary --------------------------------------


def test_try_take_waits_without_booking_and_only_zero_spends_a_token():
    bucket = TokenBucket(rate_per_second=2.0)
    assert bucket.try_take(at(0)) == timedelta(0)
    assert bucket.tokens == 0.0
    assert bucket.try_take(at(0)) == timedelta(seconds=0.5)
    assert bucket.try_take(at(0)) == timedelta(seconds=0.5)
    assert bucket.tokens == 0.0  # waiting did not reserve two future calls
    assert bucket.try_take(at(0.5)) == timedelta(0)
    assert bucket.tokens == 0.0


def test_try_take_cannot_round_a_positive_deficit_into_zero_admission():
    bucket = TokenBucket(rate_per_second=3.0)
    assert bucket.try_take(at(0)) == timedelta(0)
    # The missing token needs only 1/3 microsecond: timedelta would round to zero.
    assert bucket.try_take(at(0.333333)) == timedelta(microseconds=1)
    assert 0 < bucket.tokens < 1
    assert bucket.try_take(at(0.333334)) == timedelta(0)
    assert 0 <= bucket.tokens < 0.00001  # a real debit occurred on the zero result


class DelayedInbox(MemoryInbox):
    """Models pre-effect transaction/lock work; it does not model database locking."""

    def __init__(self, clock: ManualClock, delay: float) -> None:
        super().__init__()
        self.clock, self.delay, self.delays = clock, delay, 0

    def process(self, event, *, subject, effect):
        def after_inbox_work(connection, consumed):
            if self.delay and self.delays == 0:
                self.clock.advance(self.delay)
                self.delays += 1
            effect(connection, consumed)

        return super().process(event, subject=subject, effect=after_inbox_work)


class OversleepOnce(AdvancingSleep):
    def __init__(self, clock: ManualClock, extra: float) -> None:
        super().__init__(clock)
        self.extra, self.oversleeps = extra, 0

    def __call__(self, seconds: float) -> None:
        super().__call__(seconds)
        if self.extra and self.oversleeps == 0:
            self.clock.advance(self.extra)
            self.oversleeps += 1


def _boundary_consumer(guard, *, inbox_delay=0.0, sleep_extra=0.0, max_wait=timedelta(seconds=5)):
    clock = ManualClock()
    inbox, effect = DelayedInbox(clock, inbox_delay), ClockedEffect(clock)
    sleep, visibility, metrics = OversleepOnce(clock, sleep_extra), ScriptedVisibility(), CountingMetrics()
    consumer = Consumer(
        inbox=inbox, visibility=visibility, effect=effect, schedule=BackoffSchedule(), clock=clock,
        metrics=metrics, breaker=ProviderBreaker(), rate_guard=guard, rate_max_wait=max_wait,
        sleep=sleep if guard is not None else None,
    )
    return consumer, inbox, effect, sleep, visibility, metrics


@pytest.mark.parametrize("guarded", [True, False], ids=["guarded", "blind-guard-off"])
def test_delayed_inbox_cannot_compress_actual_effects_into_a_burst(guarded):
    consumer, inbox, effect, _, visibility, metrics = _boundary_consumer(
        TokenBucket(rate_per_second=2.0) if guarded else None, inbox_delay=1.0
    )
    assert consumer.handle_batch(_batch(8)) == {"batchItemFailures": []}
    assert inbox.delays == 1  # prove the pre-effect delay seam was exercised
    assert len(effect.calls) == 8
    assert inbox.seen == {f"evt-{n:05d}" for n in range(1, 9)}
    assert (peak_per_second(effect.calls) <= 3) is guarded
    assert visibility.calls == [] and metrics.counts["ConsumerRateDeferred"] == 0


@pytest.mark.parametrize("guarded", [True, False], ids=["guarded", "blind-guard-off"])
def test_oversleep_cannot_compress_actual_effects_into_a_burst(guarded):
    consumer, inbox, effect, sleep, visibility, metrics = _boundary_consumer(
        TokenBucket(rate_per_second=2.0) if guarded else None, sleep_extra=1.0
    )
    assert consumer.handle_batch(_batch(8)) == {"batchItemFailures": []}
    assert sleep.oversleeps == int(guarded)  # the guarded arm really overslept
    assert len(effect.calls) == 8 and len(inbox.seen) == 8
    assert (peak_per_second(effect.calls) <= 3) is guarded
    assert visibility.calls == [] and metrics.counts["ConsumerRateDeferred"] == 0


@pytest.mark.parametrize("extra", [0.0, 0.01], ids=["exact-deadline", "past-deadline"])
def test_actual_wait_deadline_defers_without_committing_and_allows_later_retry(extra):
    guard = TokenBucket(rate_per_second=1.0)
    consumer, inbox, effect, sleep, visibility, metrics = _boundary_consumer(
        guard, sleep_extra=extra, max_wait=timedelta(seconds=1)
    )
    response = consumer.handle_batch(_batch(2))
    assert sleep.slept == [1.0]
    if extra == 0.0:
        assert response == {"batchItemFailures": []}
        assert effect.calls == [0.0, 1.0]
        assert len(inbox.seen) == 2 and visibility.calls == []
        assert metrics.counts["ConsumerRateDeferred"] == 0
    else:
        assert response == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
        assert effect.calls == [0.0] and inbox.seen == {"evt-00001"}
        assert guard.tokens == 0.0  # no admission/debit was attempted after the deadline
        assert visibility.calls == [("rh-m2-1", 30)]
        assert metrics.counts["ConsumerRateDeferred"] == 1
        retry = sqs_record(envelope(2), message_id="m2", receive_count=2)
        assert consumer.handle_batch([retry]) == {"batchItemFailures": []}
        assert effect.calls == [0.0, 1.01] and len(inbox.seen) == 2


def test_zero_max_wait_admits_available_token_but_does_not_wait():
    consumer, inbox, effect, sleep, visibility, metrics = _boundary_consumer(
        TokenBucket(rate_per_second=1.0), max_wait=timedelta(0)
    )
    assert consumer.handle_batch(_batch(2)) == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
    assert effect.calls == [0.0] and inbox.seen == {"evt-00001"}
    assert sleep.slept == [] and visibility.calls == [("rh-m2-1", 30)]
    assert metrics.counts["ConsumerRateDeferred"] == 1


def test_rate_deferred_probe_does_not_spend_the_breakers_probe_interval():
    guard = TokenBucket(rate_per_second=1.0)
    consumer, inbox, effect, _, visibility, metrics = _boundary_consumer(
        guard, max_wait=timedelta(seconds=0.5)
    )
    breaker, clock = consumer._breaker, effect.clock
    for _ in range(5):
        breaker.record_failure(T0)
    clock.advance(30)
    assert guard.reserve(clock(), LONG) == timedelta(0)
    assert breaker.admits(clock())
    assert consumer.handle_batch(_batch(1)) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert breaker.is_open and breaker.admits(clock())  # provider probe has not started
    assert effect.calls == [] and inbox.seen == set()
    assert visibility.calls == [("rh-m1-1", 30)] and metrics.counts["ConsumerRateDeferred"] == 1
    clock.advance(1)
    assert consumer.handle_batch(_batch(1)) == {"batchItemFailures": []}
    assert effect.calls == [31.0] and not breaker.is_open


def test_a_duplicate_needs_no_provider_token_or_wait():
    consumer, inbox, effect, sleep, visibility, metrics = _boundary_consumer(
        TokenBucket(rate_per_second=1.0), max_wait=timedelta(0)
    )
    assert consumer.handle_batch(_batch(1)) == {"batchItemFailures": []}
    replay = sqs_record(envelope(1), message_id="replay")
    assert consumer.handle_batch([replay]) == {"batchItemFailures": []}
    assert effect.calls == [0.0] and len(inbox.seen) == 1
    assert metrics.counts["ConsumerDuplicates"] == 1
    assert metrics.counts["ConsumerRateDeferred"] == 0
    assert sleep.slept == [] and visibility.calls == []
