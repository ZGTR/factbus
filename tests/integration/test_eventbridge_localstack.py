"""The EventBridge adapter against LocalStack 3.8 (v2 events provider): bus check and
delivery to a probe queue through a rule. An emulator, so this proves the adapter's
calls, not AWS's behaviour."""

from __future__ import annotations

import json
import uuid

import boto3

from factbus.adapters import build_eventbridge_bus
from factbus.domain import BusEntry


def _clients(endpoint: str):
    kwargs = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    return boto3.client("events", **kwargs), boto3.client("sqs", **kwargs)


def test_bus_check_and_delivery(localstack_endpoint):
    events, sqs = _clients(localstack_endpoint)
    name = f"bus-{uuid.uuid4().hex[:8]}"
    bus = build_eventbridge_bus(bus_name=name, region_name="us-east-1", endpoint_url=localstack_endpoint,
                                session=boto3.session.Session(aws_access_key_id="test", aws_secret_access_key="test"))
    assert bus.bus_exists() is False
    events.create_event_bus(Name=name)
    queue_url = sqs.create_queue(QueueName=name)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    try:
        events.put_rule(Name="all", EventBusName=name, EventPattern=json.dumps({"source": ["fake-producer"]}))
        events.put_targets(Rule="all", EventBusName=name, Targets=[{"Id": "q", "Arn": arn}])
        assert bus.bus_exists() is True
        detail = json.dumps({"event_id": "evt-1", "type": "example.widget.changed", "producer": "fake-producer"})
        response = bus.put_events([BusEntry("fake-producer", "example.widget.changed", detail)])
        assert response.failed_entry_count == 0
        messages = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5).get("Messages", [])
        assert [json.loads(m["Body"])["detail"]["event_id"] for m in messages] == ["evt-1"]
    finally:
        events.remove_targets(Rule="all", EventBusName=name, Ids=["q"])
        events.delete_rule(Name="all", EventBusName=name)
        events.delete_event_bus(Name=name)
        sqs.delete_queue(QueueUrl=queue_url)
