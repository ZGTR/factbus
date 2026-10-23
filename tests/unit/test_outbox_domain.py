"""The pure outbox rules: envelope keys, error taxonomy, response attribution, request sizing."""

from __future__ import annotations

import pytest

from factbus.domain import (
    MAX_REQUEST_BYTES,
    OVERSIZED_ENTRY,
    EntryOutcome,
    EnvelopeKeyMissing,
    OutboxRow,
    PutEventsResponse,
    RowState,
    canonical_json,
    classify_error_code,
    envelope_keys,
    outcomes_for,
    request_prefix,
)
from tests.support import T0, envelope


def _row(n: int, detail: str = "{}") -> OutboxRow:
    return OutboxRow(row_id=n, event_id=f"e{n}", event_type="t.t.t", producer="p", envelope_json=detail, created_at=T0)


def test_envelope_keys_read_both_envelopes_as_plain_keys():
    product = envelope(1)
    legacy = {
        "specversion": "example.event/v1",
        "event_id": "c-1",
        "type": "example.delivery.requested",
        "producer": "legacy-producer",
    }
    assert envelope_keys(product) == ("evt-00001", "example.widget.changed", "fake-producer")
    assert envelope_keys(legacy) == ("c-1", "example.delivery.requested", "legacy-producer")


@pytest.mark.parametrize("key", ["event_id", "type", "producer"])
@pytest.mark.parametrize("bad", [None, "", 7])
def test_envelope_keys_refuse_missing_or_non_string(key, bad):
    body = envelope(1)
    body[key] = bad
    with pytest.raises(EnvelopeKeyMissing):
        envelope_keys(body)


def test_canonical_json_is_order_independent():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1}) == '{"a":2,"b":1}'


@pytest.mark.parametrize(
    ("code", "outcome"),
    [
        (None, EntryOutcome.ACCEPTED),
        ("MalformedDetail", EntryOutcome.PERMANENT),
        ("InvalidArgument", EntryOutcome.PERMANENT),
        (OVERSIZED_ENTRY, EntryOutcome.PERMANENT),
        ("AccessDeniedException", EntryOutcome.DEPLOYMENT),
        ("NotAuthorizedForSourceException", EntryOutcome.DEPLOYMENT),
        ("NotAuthorizedForDetailTypeException", EntryOutcome.DEPLOYMENT),
        ("InvalidAccountIdException", EntryOutcome.DEPLOYMENT),
        ("ThrottlingException", EntryOutcome.RETRY),
        ("InternalFailure", EntryOutcome.RETRY),
        ("RedactionFailure", EntryOutcome.RETRY),
        ("SomethingNewFromAws", EntryOutcome.RETRY),
    ],
)
def test_error_code_taxonomy(code, outcome):
    assert classify_error_code(code) is outcome


def test_positional_mapping_when_lengths_agree():
    response = PutEventsResponse(1, (None, "MalformedDetail", None))
    outcomes, attributable = outcomes_for(3, response)
    assert attributable
    assert outcomes == (EntryOutcome.ACCEPTED, EntryOutcome.PERMANENT, EntryOutcome.ACCEPTED)


def test_short_response_with_zero_failures_accepts_every_entry():
    """LocalStack 3.8 answered 1 Entries[] item for 2 entries at FailedEntryCount 0 (P10)."""
    outcomes, attributable = outcomes_for(2, PutEventsResponse(0, (None,)))
    assert not attributable
    assert outcomes == (EntryOutcome.ACCEPTED, EntryOutcome.ACCEPTED)


def test_short_response_with_failures_retries_every_entry_and_fails_none():
    outcomes, attributable = outcomes_for(3, PutEventsResponse(1, ("MalformedDetail",)))
    assert not attributable
    assert outcomes == (EntryOutcome.RETRY,) * 3


def test_row_state_machine():
    row = _row(1)
    assert row.state is RowState.UNSENT
    assert OutboxRow(**{**row.__dict__, "sent_at": T0}).state is RowState.SENT
    assert OutboxRow(**{**row.__dict__, "failed_at": T0}).state is RowState.FAILED


def test_request_prefix_caps_at_ten_entries():
    assert request_prefix([_row(n) for n in range(1, 24)]) == 10
    assert request_prefix([_row(n) for n in range(1, 4)]) == 3


def test_request_prefix_stops_before_one_megabyte():
    big = "x" * 200_000
    rows = [_row(n, big) for n in range(1, 11)]
    taken = request_prefix(rows)
    assert taken == 5
    assert sum(r.to_entry().size_bytes for r in rows[:taken]) < MAX_REQUEST_BYTES
    assert sum(r.to_entry().size_bytes for r in rows[: taken + 1]) >= MAX_REQUEST_BYTES


def test_request_prefix_always_takes_the_first_row():
    assert request_prefix([_row(1, "x" * (MAX_REQUEST_BYTES + 1))]) == 1
