"""The boto3 adapter against botocore's Stubber: request shape, response parsing, the
missing-bus answer, and the written retry policy."""

from __future__ import annotations

import boto3
import pytest
from botocore.stub import Stubber

from factbus.adapters import EVENTBRIDGE_CLIENT_CONFIG, EventBridgeBus, build_eventbridge_bus
from factbus.domain import BusEntry
from factbus.ports import BusUnavailable


def _bus():
    client = boto3.client(
        "events", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x",
        config=EVENTBRIDGE_CLIENT_CONFIG,
    )
    return EventBridgeBus(client, "product-bus"), Stubber(client)


def test_put_events_names_the_bus_and_parses_per_entry_codes():
    bus, stub = _bus()
    stub.add_response(
        "put_events",
        {"FailedEntryCount": 1, "Entries": [{"EventId": "a"}, {"ErrorCode": "MalformedDetail", "ErrorMessage": "x"}]},
        {"Entries": [
            {"EventBusName": "product-bus", "Source": "p", "DetailType": "t", "Detail": "{}"},
            {"EventBusName": "product-bus", "Source": "p", "DetailType": "t", "Detail": "{"},
        ]},
    )
    with stub:
        response = bus.put_events([BusEntry("p", "t", "{}"), BusEntry("p", "t", "{")])
    assert response.failed_entry_count == 1
    assert response.entry_error_codes == (None, "MalformedDetail")


def test_request_level_error_becomes_bus_unavailable_with_its_code():
    bus, stub = _bus()
    stub.add_client_error("put_events", service_error_code="InternalException", http_status_code=500)
    with stub, pytest.raises(BusUnavailable) as caught:
        bus.put_events([BusEntry("p", "t", "{}")])
    assert caught.value.code == "InternalException"


def test_missing_bus_answers_false():
    bus, stub = _bus()
    stub.add_client_error("describe_event_bus", service_error_code="ResourceNotFoundException", http_status_code=400)
    with stub:
        assert bus.bus_exists() is False


def test_present_bus_answers_true():
    bus, stub = _bus()
    stub.add_response("describe_event_bus", {"Name": "product-bus"}, {"Name": "product-bus"})
    with stub:
        assert bus.bus_exists() is True


def test_client_is_built_with_one_attempt():
    bus = build_eventbridge_bus(bus_name="b", region_name="us-east-1", endpoint_url="http://localhost:1")
    retries = bus._client.meta.config.retries
    assert retries["total_max_attempts"] == 1
    assert retries["mode"] == "standard"
