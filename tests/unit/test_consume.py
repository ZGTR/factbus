"""`Consumer.handle_batch` against in-memory doubles: the ReportBatchItemFailures shape,
dedupe on `detail.event_id`, the version guard, backoff, the breaker and C0b-19.
Concurrency (C0b-17) is proved on real engines in tests/integration."""

from __future__ import annotations

import pytest

from factbus.application import Consumer, TransportFieldsMissing
from factbus.domain import BackoffSchedule, ProviderBreaker, SubjectVersion
from factbus.ports import ProviderError
from tests.support import CountingMetrics, ManualClock, MemoryInbox, ScriptedVisibility, envelope, sqs_record


class Effects:
    def __init__(self) -> None:
        self.applied: list[str] = []
        self.raise_next: list[Exception] = []

    def __call__(self, connection, event) -> None:
        if self.raise_next:
            raise self.raise_next.pop(0)
        self.applied.append(event.event_id)


def by_subject(env):
    return SubjectVersion(env["subject"], env["authority_epoch"])


@pytest.fixture
def world():
    clock = ManualClock()
    inbox, visibility, effects, metrics = MemoryInbox(), ScriptedVisibility(), Effects(), CountingMetrics()
    consumer = Consumer(
        inbox=inbox, visibility=visibility, effect=effects, schedule=BackoffSchedule(), clock=clock,
        metrics=metrics, version_of=by_subject, breaker=ProviderBreaker(),
    )
    return consumer, inbox, visibility, effects, metrics, clock


def test_a_clean_batch_reports_no_failures(world):
    consumer, _, visibility, effects, metrics, _ = world
    records = [sqs_record(envelope(n), message_id=f"m{n}") for n in range(1, 4)]
    assert consumer.handle_batch(records) == {"batchItemFailures": []}
    assert effects.applied == ["evt-00001", "evt-00002", "evt-00003"]
    assert visibility.calls == [] and metrics.counts["ConsumerApplied"] == 3


def test_dedupe_key_is_detail_event_id_not_the_eventbridge_id(world):
    """F7/K1 in miniature: a replay carries a NEW EventBridge id and the same event_id."""
    consumer, _, _, effects, metrics, _ = world
    first = sqs_record(envelope(1), message_id="m1", eventbridge_id="eb-original")
    replay = sqs_record(envelope(1), message_id="m2", eventbridge_id="eb-replayed")
    assert consumer.handle_batch([first, replay]) == {"batchItemFailures": []}
    assert effects.applied == ["evt-00001"]
    assert metrics.counts["ConsumerDuplicates"] == 1


def test_an_older_version_is_dropped_as_a_success(world):
    """F8/K3 in miniature: version 3 then version 2 of one subject leaves version 3."""
    consumer, inbox, _, effects, metrics, _ = world
    newer = sqs_record(envelope(1, subject="s", authority_epoch=3), message_id="m1")
    older = sqs_record(envelope(2, subject="s", authority_epoch=2), message_id="m2")
    assert consumer.handle_batch([newer, older]) == {"batchItemFailures": []}
    assert effects.applied == ["evt-00001"] and inbox.versions["s"] == 3
    assert metrics.counts["ConsumerStale"] == 1


def test_a_failure_reports_only_that_item_with_its_backoff(world):
    consumer, _, visibility, effects, _, _ = world
    effects.raise_next = [RuntimeError("db down")]
    records = [
        sqs_record(envelope(1), message_id="m1", receive_count=3),
        sqs_record(envelope(2), message_id="m2"),
    ]
    assert consumer.handle_batch(records) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert visibility.calls == [("rh-m1-3", 600)]  # receive 3 waits 10 minutes
    assert effects.applied == ["evt-00002"]


def test_a_malformed_record_is_reported_with_backoff(world):
    """F9 in miniature: reported every time, so maxReceiveCount dead-letters it."""
    consumer, _, visibility, _, metrics, _ = world
    records = [
        sqs_record(None, message_id="bad-json", body="{not json"),
        sqs_record(None, message_id="no-detail", body='{"detail-type": "x"}'),
        sqs_record(None, message_id="no-id", body='{"detail": {"type": "x"}}'),
        sqs_record(envelope(1), message_id="ok"),
    ]
    report = consumer.process_batch(records)
    assert report.failed == ["bad-json", "no-detail", "no-id"]
    assert report.applied == ["ok"]
    assert metrics.counts["ConsumerMalformed"] == 3 and len(visibility.calls) == 3


def test_a_version_of_that_cannot_read_the_event_is_malformed(world):
    consumer, _, _, _, metrics, _ = world
    no_subject = envelope(1)
    del no_subject["subject"]
    assert consumer.handle_batch([sqs_record(no_subject, message_id="m1")]) == {
        "batchItemFailures": [{"itemIdentifier": "m1"}]
    }
    assert metrics.counts["ConsumerMalformed"] == 1


def test_the_tenth_receive_waits_zero_so_the_redrive_policy_takes_it(world):
    consumer, _, visibility, effects, _, _ = world
    effects.raise_next = [RuntimeError("still failing")]
    consumer.handle_batch([sqs_record(envelope(1), message_id="m1", receive_count=10)])
    assert visibility.calls == [("rh-m1-10", 0)]


def test_c0b_19_a_failed_visibility_call_still_reports_the_item(world):
    consumer, _, visibility, effects, metrics, _ = world
    visibility.fail = True
    effects.raise_next = [RuntimeError("db down")]
    report = consumer.process_batch([sqs_record(envelope(1), message_id="m1")])
    assert report.response == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert report.visibility_failed == ["m1"]
    assert metrics.counts["ConsumerVisibilityCallFailed"] == 1


def test_an_unexpected_visibility_exception_still_reports_the_item(world):
    consumer, _, _, effects, _, _ = world

    class Exploding:
        def change_visibility(self, receipt_handle, seconds):
            raise AttributeError("a bug in the adapter")

    consumer._visibility = Exploding()
    effects.raise_next = [RuntimeError("db down")]
    assert consumer.handle_batch([sqs_record(envelope(1), message_id="m1")]) == {
        "batchItemFailures": [{"itemIdentifier": "m1"}]
    }


def test_a_record_without_transport_fields_fails_the_whole_batch(world):
    consumer, _, _, effects, _, _ = world
    broken = sqs_record(envelope(2), message_id="m2")
    del broken["receiptHandle"]
    with pytest.raises(TransportFieldsMissing):
        consumer.handle_batch([sqs_record(envelope(1), message_id="m1"), broken])
    assert effects.applied == []  # checked before any effect runs


def test_breaker_opens_after_five_provider_errors_and_defers_without_calling(world):
    consumer, _, visibility, effects, metrics, clock = world
    effects.raise_next = [ProviderError("503")] * 5
    first = [sqs_record(envelope(n), message_id=f"m{n}") for n in range(1, 6)]
    assert len(consumer.handle_batch(first)["batchItemFailures"]) == 5
    assert consumer._breaker.is_open

    clock.advance(5)
    report = consumer.process_batch([sqs_record(envelope(6), message_id="m6")])
    assert report.failed == ["m6"] and effects.applied == []
    assert metrics.counts["ConsumerBreakerDeferred"] == 1

    clock.advance(30)  # the probe is due: one item goes through, succeeds, closes the breaker
    report = consumer.process_batch([sqs_record(envelope(7), message_id="m7"), sqs_record(envelope(8), message_id="m8")])
    assert report.applied == ["m7", "m8"] and not consumer._breaker.is_open


def test_a_non_provider_error_does_not_move_the_breaker(world):
    consumer, _, _, effects, _, _ = world
    effects.raise_next = [RuntimeError("bug")] * 6
    consumer.handle_batch([sqs_record(envelope(n), message_id=f"m{n}") for n in range(1, 7)])
    assert not consumer._breaker.is_open


def test_a_duplicate_probe_does_not_close_the_breaker(world):
    consumer, inbox, _, effects, _, clock = world
    effects.raise_next = [ProviderError("503")] * 5
    consumer.handle_batch([sqs_record(envelope(n), message_id=f"m{n}") for n in range(10, 15)])
    inbox.seen.add("evt-00001")
    clock.advance(31)
    consumer.handle_batch([sqs_record(envelope(1), message_id="dup")])
    assert consumer._breaker.is_open  # the probe never reached the provider


def test_without_version_of_only_the_inbox_guards():
    inbox, effects = MemoryInbox(), Effects()
    consumer = Consumer(inbox=inbox, visibility=ScriptedVisibility(), effect=effects, schedule=BackoffSchedule(),
                        clock=ManualClock(), metrics=CountingMetrics())
    consumer.handle_batch([sqs_record(envelope(1, authority_epoch=9), message_id="a"),
                           sqs_record(envelope(2, authority_epoch=1), message_id="b")])
    assert effects.applied == ["evt-00001", "evt-00002"] and inbox.versions == {}
