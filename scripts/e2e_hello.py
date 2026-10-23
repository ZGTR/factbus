#!/usr/bin/env python3
"""End-to-end check of examples/hello against an applied stack (LocalStack by default).

Run after `tofu -chdir=examples/hello apply -var localstack_endpoint=...` (or `make e2e`).
Four arms, each printed as a PASS/FAIL row; any FAIL exits 1:

1. applied:   one `hello.said` event on the bus -> exactly one ledger item for its id.
2. duplicate: the SAME CloudEvents event put again (EventBridge gives it a new id) ->
              still exactly one ledger item. A fresh control event put right after it
              must land, and the queue must drain, so the duplicate was consumed and
              not merely late.
3. poison:    `data.poison = true` -> the message reaches the DLQ after
              max_receive_count receives, and writes no ledger item.
4. evidence:  the Lambda's own EMF lines in CloudWatch Logs count the duplicate
              (`ConsumerDuplicates`) and the poison's retries (`ConsumerRetried`).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from factbus import Event


def tofu_outputs(tf_dir: str) -> dict[str, Any]:
    raw = subprocess.run(
        ["tofu", f"-chdir={tf_dir}", "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {name: value["value"] for name, value in json.loads(raw).items()}


class Stack:
    def __init__(self, outputs: dict[str, Any], endpoint: str | None, region: str) -> None:
        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint:
            kwargs.update(endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test")
        self.events = boto3.client("events", **kwargs)
        self.sqs = boto3.client("sqs", **kwargs)
        self.ddb = boto3.resource("dynamodb", **kwargs)
        self.logs = boto3.client("logs", **kwargs)
        self.out = outputs
        self.ledger = self.ddb.Table(outputs["ledger_table"])
        self.inbox = self.ddb.Table(outputs["inbox_table"])

    def put(self, event: Event) -> str:
        response = self.events.put_events(
            Entries=[
                {
                    "EventBusName": self.out["bus_name"],
                    "Source": event.source,
                    "DetailType": event.type,
                    "Detail": json.dumps(event.to_envelope()),
                }
            ]
        )
        if response.get("FailedEntryCount"):
            raise RuntimeError(f"PutEvents rejected {event.id}: {response['Entries']}")
        return response["Entries"][0]["EventId"]

    def ledger_count(self, event_id: str) -> int:
        response = self.ledger.query(KeyConditionExpression=Key("pk").eq(event_id), ConsistentRead=True)
        return len(response["Items"])

    def in_inbox(self, event_id: str) -> bool:
        response = self.inbox.get_item(Key={"pk": f"inbox#{event_id}", "sk": "inbox"}, ConsistentRead=True)
        return "Item" in response

    def queue_depth(self) -> int:
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.out["queue_url"],
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]) + int(attrs["ApproximateNumberOfMessagesNotVisible"])

    def lambda_metric_lines(self, metric: str, since_ms: int) -> int:
        """EMF lines naming `metric` in the function's log group since `since_ms`."""
        logs = self.logs
        group = f"/aws/lambda/{self.out['function_name']}"
        count, token = 0, None
        while True:
            kwargs: dict[str, Any] = {"logGroupName": group, "startTime": since_ms}
            if token:
                kwargs["nextToken"] = token
            try:
                response = logs.filter_log_events(**kwargs)
            except logs.exceptions.ResourceNotFoundException:
                return 0
            count += sum(1 for e in response["events"] if f'"{metric}"' in e["message"])
            token = response.get("nextToken")
            if not token:
                return count

    def dlq_holds(self, event_id: str) -> bool:
        # VisibilityTimeout=0 peeks without hiding the message from the next poll.
        response = self.sqs.receive_message(
            QueueUrl=self.out["dlq_url"], MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0
        )
        for message in response.get("Messages", []):
            detail = json.loads(message["Body"]).get("detail") or {}
            if detail.get("id") == event_id:
                return True
        return False


def wait_for(predicate, timeout: float, interval: float = 1.0) -> tuple[bool, float]:
    start = time.monotonic()
    while True:
        if predicate():
            return True, time.monotonic() - start
        if time.monotonic() - start > timeout:
            return False, time.monotonic() - start
        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tf-dir", default=str(REPO / "examples" / "hello"))
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
        help="LocalStack edge URL; pass '' for real AWS",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--dlq-timeout", type=float, default=120.0)
    parser.add_argument("--settle", type=float, default=10.0, help="seconds to wait after the duplicate")
    args = parser.parse_args()

    stack = Stack(tofu_outputs(args.tf_dir), args.endpoint or None, args.region)
    mode = f"Lambda {stack.out['function_name']}"
    print(f"consumer: {mode}")

    rows: list[tuple[str, bool, str]] = []
    started_ms = int(time.time() * 1000) - 5000
    source, kind = stack.out["event_source"], stack.out["event_type"]

    # 1. applied once
    hello = Event(type=kind, source=source, data={"text": "hello"})
    eb_first = stack.put(hello)
    ok, took = wait_for(lambda: stack.ledger_count(hello.id) >= 1, args.timeout)
    count = stack.ledger_count(hello.id)
    rows.append(("applied: one ledger item", ok and count == 1, f"id={hello.id} items={count} after {took:.1f}s"))
    rows.append(("applied: inbox recorded", stack.in_inbox(hello.id), f"pk=inbox#{hello.id}"))

    # 2. the same CloudEvents event again, then a fresh control event
    eb_second = stack.put(hello)
    control = Event(type=kind, source=source, data={"text": "control"})
    stack.put(control)
    ok_c, took_c = wait_for(lambda: stack.ledger_count(control.id) >= 1, args.timeout)
    ok_d, _ = wait_for(lambda: stack.queue_depth() == 0, args.timeout)
    time.sleep(args.settle)
    count = stack.ledger_count(hello.id)
    rows.append(("duplicate: new EventBridge id", eb_first != eb_second, f"{eb_first} != {eb_second}"))
    rows.append(
        (
            "duplicate: control applied",
            ok_c and stack.ledger_count(control.id) == 1,
            f"id={control.id} after {took_c:.1f}s",
        )
    )
    rows.append(("duplicate: queue drained", ok_d, f"depth={stack.queue_depth()}"))
    rows.append(
        ("duplicate: still one item", count == 1, f"id={hello.id} items={count} after {args.settle:.0f}s settle")
    )

    # 3. poison -> DLQ
    poison = Event(type=kind, source=source, data={"poison": True})
    stack.put(poison)
    ok, took = wait_for(lambda: stack.dlq_holds(poison.id), args.dlq_timeout, interval=2.0)
    rows.append(("poison: reached DLQ", ok, f"id={poison.id} after {took:.1f}s"))
    count = stack.ledger_count(poison.id)
    rows.append(("poison: no ledger item", count == 0, f"items={count}"))

    # 4. the function's own account of the run, from its EMF lines in CloudWatch Logs
    ok, _ = wait_for(lambda: stack.lambda_metric_lines("ConsumerDuplicates", started_ms) >= 1, 30, 2.0)
    dupes = stack.lambda_metric_lines("ConsumerDuplicates", started_ms)
    rows.append(("evidence: Lambda counted duplicate", ok, f"ConsumerDuplicates lines={dupes}"))
    retried = stack.lambda_metric_lines("ConsumerRetried", started_ms)
    rows.append(
        ("evidence: Lambda retried poison", retried >= 3, f"ConsumerRetried lines={retried} (max_receive_count=3)")
    )

    width = max(len(name) for name, _, _ in rows)
    print(f"\n{'check':<{width}}  result  detail")
    for name, passed, detail in rows:
        print(f"{name:<{width}}  {'PASS' if passed else 'FAIL':<6}  {detail}")
    failed = sum(1 for _, passed, _ in rows if not passed)
    print(f"\n{len(rows) - failed}/{len(rows)} PASS ({mode})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
