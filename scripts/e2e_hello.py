#!/usr/bin/env python3
"""End-to-end check of examples/hello against an applied stack (LocalStack by default).

Run after `tofu -chdir=examples/hello apply -var localstack_endpoint=...` (or `make e2e`).
The example has two consumers of the same event, `ledger` and `audit`, each with its own
queue, DLQs, inbox and Lambda. Each arm prints PASS/FAIL rows; any FAIL exits 1:

1. applied:     one `hello.said` event on the bus -> exactly one item in EACH consumer's
                ledger for its id.
2. duplicate:   the SAME CloudEvents event put again (EventBridge gives it a new id) ->
                still exactly one item per consumer. A fresh control event put right
                after it must land, and the queues must drain, so the duplicate was
                consumed and not merely late.
3. independent: with `ledger`'s event source mapping disabled (the kill switch), a new
                event is applied by `audit` alone; re-enabled, `ledger` applies it once.
4. delivery:    real AWS only. With `audit`'s queue policy removed, EventBridge cannot
                deliver to `audit`'s queue and parks the event on its delivery DLQ, while
                `ledger` applies it. The policy is restored afterwards. LocalStack does
                not enforce queue policies, so there this arm prints SKIP.
5. poison:      `data.poison = true` -> the message reaches each consumer's processing
                DLQ after max_receive_count receives, and writes no ledger item.
6. evidence:    each Lambda's own EMF lines in CloudWatch Logs count the duplicate
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

FIRST, SECOND = "ledger", "audit"


def tofu_outputs(tf_dir: str) -> dict[str, Any]:
    raw = subprocess.run(
        ["tofu", f"-chdir={tf_dir}", "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {name: value["value"] for name, value in json.loads(raw).items()}


def wait_for(predicate, timeout: float, interval: float = 1.0) -> tuple[bool, float]:
    start = time.monotonic()
    while True:
        if predicate():
            return True, time.monotonic() - start
        if time.monotonic() - start > timeout:
            return False, time.monotonic() - start
        time.sleep(interval)


class Consumer:
    """One consumer's resources, from the `consumers` output."""

    def __init__(self, key: str, out: dict[str, Any], clients: dict[str, Any]) -> None:
        self.key = key
        self.out = out
        self.sqs = clients["sqs"]
        self.logs = clients["logs"]
        self.lam = clients["lambda"]
        self.ledger = clients["ddb"].Table(out["ledger_table"])
        self.inbox = clients["ddb"].Table(out["inbox_table"])

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

    def holds(self, queue: str, event_id: str) -> bool:
        """Whether the queue output named `queue` holds the event. Peeks: VisibilityTimeout=0
        leaves the message for the next poll."""
        response = self.sqs.receive_message(
            QueueUrl=self.out[queue], MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0
        )
        for message in response.get("Messages", []):
            detail = json.loads(message["Body"]).get("detail") or {}
            if detail.get("id") == event_id:
                return True
        return False

    def mapping_state(self) -> str | None:
        try:
            return self.lam.get_event_source_mapping(UUID=self.out["mapping_uuid"])["State"]
        except Exception:  # a transient read failure is not a state; poll again
            return None

    def set_mapping(self, enabled: bool, timeout: float = 180.0) -> bool:
        """Ask for the state until the mapping reports it. Lambda refuses an update while a
        previous one is still transitioning (ResourceConflictException), so retry rather
        than give up, which matters most when this restores a mapping after a failure."""
        uuid = self.out["mapping_uuid"]
        want = "Enabled" if enabled else "Disabled"

        def settled() -> bool:
            state = self.mapping_state()
            if state == want:
                return True
            if state in ("Enabled", "Disabled"):
                try:
                    self.lam.update_event_source_mapping(UUID=uuid, Enabled=enabled)
                except Exception:  # conflict or throttle: the next poll retries
                    pass
            return False

        ok, _ = wait_for(settled, timeout, 2.0)
        return ok

    def queue_policy(self) -> str:
        attrs = self.sqs.get_queue_attributes(QueueUrl=self.out["queue_url"], AttributeNames=["Policy"])
        return attrs.get("Attributes", {}).get("Policy", "")

    def set_queue_policy(self, policy: str) -> None:
        self.sqs.set_queue_attributes(QueueUrl=self.out["queue_url"], Attributes={"Policy": policy})

    def metric_lines(self, metric: str, since_ms: int) -> int:
        """EMF lines naming `metric` in the function's log group since `since_ms`."""
        group = f"/aws/lambda/{self.out['function_name']}"
        count, token = 0, None
        while True:
            kwargs: dict[str, Any] = {"logGroupName": group, "startTime": since_ms}
            if token:
                kwargs["nextToken"] = token
            try:
                response = self.logs.filter_log_events(**kwargs)
            except self.logs.exceptions.ResourceNotFoundException:
                return 0
            count += sum(1 for e in response["events"] if f'"{metric}"' in e["message"])
            token = response.get("nextToken")
            if not token:
                return count


class Stack:
    def __init__(self, outputs: dict[str, Any], endpoint: str | None, region: str) -> None:
        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint:
            kwargs.update(endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test")
        self.events = boto3.client("events", **kwargs)
        clients = {
            "sqs": boto3.client("sqs", **kwargs),
            "logs": boto3.client("logs", **kwargs),
            "lambda": boto3.client("lambda", **kwargs),
            "ddb": boto3.resource("dynamodb", **kwargs),
        }
        self.out = outputs
        self.consumers = {key: Consumer(key, value, clients) for key, value in outputs["consumers"].items()}

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
    parser.add_argument(
        "--stop-settle",
        type=float,
        default=None,
        help="seconds to let a disabled mapping's pollers finish in-flight receives before the "
        "independent arm puts its event (default 5 on LocalStack, 90 on AWS)",
    )
    parser.add_argument(
        "--policy-settle",
        type=float,
        default=60.0,
        help="seconds for a queue-policy change to take effect before the delivery arm puts its event",
    )
    args = parser.parse_args()

    local = bool(args.endpoint)
    stop_settle = args.stop_settle if args.stop_settle is not None else (5.0 if local else 90.0)
    stack = Stack(tofu_outputs(args.tf_dir), args.endpoint or None, args.region)
    consumers = stack.consumers
    mode = ", ".join(f"Lambda {c.out['function_name']}" for c in consumers.values())
    print(f"consumers: {mode} ({'LocalStack' if local else 'AWS'})")

    rows: list[tuple[str, str, str]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        rows.append((name, "PASS" if passed else "FAIL", detail))

    started_ms = int(time.time() * 1000) - 5000
    source, kind = stack.out["event_source"], stack.out["event_type"]

    # 1. applied once, by each consumer
    hello = Event(type=kind, source=source, data={"text": "hello"})
    eb_first = stack.put(hello)
    for c in consumers.values():
        ok, took = wait_for(lambda c=c: c.ledger_count(hello.id) >= 1, args.timeout)
        count = c.ledger_count(hello.id)
        check(f"applied[{c.key}]: one ledger item", ok and count == 1, f"id={hello.id} items={count} after {took:.1f}s")
        check(f"applied[{c.key}]: inbox recorded", c.in_inbox(hello.id), f"pk=inbox#{hello.id}")

    # 2. the same CloudEvents event again, then a fresh control event
    eb_second = stack.put(hello)
    control = Event(type=kind, source=source, data={"text": "control"})
    stack.put(control)
    check("duplicate: new EventBridge id", eb_first != eb_second, f"{eb_first} != {eb_second}")
    for c in consumers.values():
        ok_c, took_c = wait_for(lambda c=c: c.ledger_count(control.id) >= 1, args.timeout)
        ok_d, _ = wait_for(lambda c=c: c.queue_depth() == 0, args.timeout)
        check(
            f"duplicate[{c.key}]: control applied",
            ok_c and c.ledger_count(control.id) == 1,
            f"id={control.id} after {took_c:.1f}s",
        )
        check(f"duplicate[{c.key}]: queue drained", ok_d, f"depth={c.queue_depth()}")
    time.sleep(args.settle)
    for c in consumers.values():
        count = c.ledger_count(hello.id)
        check(f"duplicate[{c.key}]: still one item", count == 1, f"id={hello.id} items={count} after settle")

    # 3. one consumer stopped; the other carries on, and the stopped one catches up once
    stopped, running = consumers[FIRST], consumers[SECOND]
    try:
        check(f"independent: {stopped.key} mapping disabled", stopped.set_mapping(False), stopped.out["mapping_uuid"])
        # `Disabled` stops new polls; a receive already in flight still finishes
        # (docs/kill-switch-runbook.md), so let it before putting the event.
        time.sleep(stop_settle)
        alone = Event(type=kind, source=source, data={"text": "while stopped"})
        stack.put(alone)
        ok, took = wait_for(lambda: running.ledger_count(alone.id) >= 1, args.timeout)
        check(f"independent: {running.key} applied while {stopped.key} stopped", ok, f"id={alone.id} after {took:.1f}s")
        time.sleep(args.settle)
        count = stopped.ledger_count(alone.id)
        check(f"independent: {stopped.key} applied nothing while stopped", count == 0, f"items={count}")
    finally:
        # Re-enabled even when an arm above raised, so a failed run leaves no stopped consumer.
        enabled = stopped.set_mapping(True)
    check(f"independent: {stopped.key} mapping re-enabled", enabled, stopped.out["mapping_uuid"])
    ok, took = wait_for(lambda: stopped.ledger_count(alone.id) >= 1, args.timeout)
    count = stopped.ledger_count(alone.id)
    check(f"independent: {stopped.key} caught up once", ok and count == 1, f"items={count} after {took:.1f}s")

    # 4. EventBridge cannot deliver to one consumer's queue -> that consumer's delivery DLQ
    blocked, healthy = consumers[SECOND], consumers[FIRST]
    if local:
        rows.append(("delivery: target failure -> delivery DLQ", "SKIP", "LocalStack does not enforce queue policies"))
    else:
        policy = blocked.queue_policy()
        undeliverable = Event(type=kind, source=source, data={"text": "undeliverable"})
        try:
            blocked.set_queue_policy("")
            time.sleep(args.policy_settle)
            stack.put(undeliverable)
            ok, took = wait_for(
                lambda: blocked.holds("delivery_dlq_url", undeliverable.id), args.dlq_timeout + 180, interval=5.0
            )
            check(f"delivery[{blocked.key}]: reached delivery DLQ", ok, f"id={undeliverable.id} after {took:.1f}s")
            count = blocked.ledger_count(undeliverable.id)
            check(f"delivery[{blocked.key}]: no ledger item", count == 0, f"items={count}")
            ok, took = wait_for(lambda: healthy.ledger_count(undeliverable.id) >= 1, args.timeout)
            check(f"delivery[{healthy.key}]: healthy consumer applied", ok, f"after {took:.1f}s")
        finally:
            blocked.set_queue_policy(policy)
        # A policy change takes up to a minute to read back and to apply; publishing
        # sooner could send the next arm's event to the delivery DLQ.
        ok, took = wait_for(lambda: blocked.queue_policy() == policy, args.policy_settle, 2.0)
        check(f"delivery[{blocked.key}]: queue policy restored", ok, f"after {took:.1f}s")
        time.sleep(args.policy_settle)

    # 5. poison -> each consumer's processing DLQ
    poison = Event(type=kind, source=source, data={"poison": True})
    stack.put(poison)
    for c in consumers.values():
        ok, took = wait_for(lambda c=c: c.holds("dlq_url", poison.id), args.dlq_timeout, interval=2.0)
        check(f"poison[{c.key}]: reached DLQ", ok, f"id={poison.id} after {took:.1f}s")
        count = c.ledger_count(poison.id)
        check(f"poison[{c.key}]: no ledger item", count == 0, f"items={count}")
        check(f"poison[{c.key}]: not on delivery DLQ", not c.holds("delivery_dlq_url", poison.id), "")

    # 6. each function's own account of the run, from its EMF lines in CloudWatch Logs
    for c in consumers.values():
        ok, _ = wait_for(lambda c=c: c.metric_lines("ConsumerDuplicates", started_ms) >= 1, 30, 2.0)
        dupes = c.metric_lines("ConsumerDuplicates", started_ms)
        check(f"evidence[{c.key}]: Lambda counted duplicate", ok, f"ConsumerDuplicates lines={dupes}")
        retried = c.metric_lines("ConsumerRetried", started_ms)
        check(
            f"evidence[{c.key}]: Lambda retried poison",
            retried >= 3,
            f"ConsumerRetried lines={retried} (max_receive_count=3)",
        )

    width = max(len(name) for name, _, _ in rows)
    print(f"\n{'check':<{width}}  result  detail")
    for name, result, detail in rows:
        print(f"{name:<{width}}  {result:<6}  {detail}")
    failed = sum(1 for _, result, _ in rows if result == "FAIL")
    skipped = sum(1 for _, result, _ in rows if result == "SKIP")
    print(f"\n{len(rows) - failed - skipped}/{len(rows)} PASS, {skipped} SKIP ({mode})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
