"""hello-ledger: the example consumer.

Each `hello.said` event writes one ledger item, in the same DynamoDB transaction as the
inbox record, so a redelivered or replayed copy of the event writes nothing. An event
with `data.poison == true` raises; the kit retries it with backoff and the queue's
redrive policy moves it to the DLQ.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from factbus import Event, consumer
from factbus.adapters.dynamodb_inbox import DynamoInbox
from factbus.domain import BackoffSchedule

# Test profile only: the e2e run shrinks the retry waits so the DLQ arm takes seconds.
_FACTOR = os.environ.get("HELLO_TEST_BACKOFF_FACTOR")
_SCHEDULE = BackoffSchedule.scaled(float(_FACTOR)) if _FACTOR else None


def _now() -> datetime:
    return datetime.now(UTC)


@consumer(
    inbox=DynamoInbox.from_env,
    types=["hello.said"],
    schedule=_SCHEDULE,
    service="hello-ledger",
)
def handler(event: Event, tx) -> None:
    if event.data.get("poison") is True:
        raise ValueError(f"poison event {event.id}")
    tx.put(
        os.environ["LEDGER_TABLE"],
        {
            "pk": event.id,
            "sk": str(uuid.uuid4()),
            "type": event.type,
            "source": event.source,
            "text": str(event.data.get("text", "")),
            "applied_at": _now().isoformat(),
        },
    )
