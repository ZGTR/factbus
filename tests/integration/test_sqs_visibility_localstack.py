"""The SQS visibility adapter against LocalStack 3.8: a changed visibility hides the message
for that long, and a stale receipt handle is `VisibilityUnavailable`. An emulator, so
this proves the adapter's calls, not AWS's timing."""

from __future__ import annotations

import time
import uuid

import boto3
import pytest

from factbus.adapters import build_sqs_visibility
from factbus.ports import VisibilityUnavailable


def test_change_visibility_hides_then_returns_the_message(localstack_endpoint):
    kwargs = dict(region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    sqs = boto3.client("sqs", endpoint_url=localstack_endpoint, **kwargs)
    queue_url = sqs.create_queue(QueueName=f"vis-{uuid.uuid4().hex[:8]}", Attributes={"VisibilityTimeout": "30"})["QueueUrl"]
    adapter = build_sqs_visibility(queue_url=queue_url, region_name="us-east-1", endpoint_url=localstack_endpoint,
                                   session=boto3.session.Session(aws_access_key_id="test", aws_secret_access_key="test"))
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody="x")
        first = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)["Messages"][0]
        adapter.change_visibility(first["ReceiptHandle"], 3)  # the queue default would be 30 s
        assert sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1).get("Messages", []) == []
        time.sleep(3.5)
        again = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2,
                                    AttributeNames=["ApproximateReceiveCount"])["Messages"]
        assert again[0]["MessageId"] == first["MessageId"]
        assert again[0]["Attributes"]["ApproximateReceiveCount"] == "2"
        with pytest.raises(VisibilityUnavailable):
            adapter.change_visibility("not-a-receipt-handle", 5)
    finally:
        sqs.delete_queue(QueueUrl=queue_url)
