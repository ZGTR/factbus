# factbus

Event-driven services on AWS without building the plumbing. A producer publishes facts
from its own database transaction. Each consumer applies every event exactly once. A
side effect such as an email or a charge runs at most once, even across crashes and
redeliveries.

factbus is a Python library plus three OpenTofu/Terraform modules on EventBridge, SQS and
DynamoDB. It adds no broker, no cluster and no VPC.

```
 producer service                     AWS                                consumer service
┌──────────────────┐                                                   ┌──────────────────┐
│ BEGIN            │  after COMMIT  ┌─────────────┐ rule ┌───────┐ ESM │ @consumer        │
│  write order     │ ─────────────▶ │ EventBridge │ ───▶ │  SQS  │ ──▶ │  dedupe (inbox)  │
│  stage(event) ───┼─ outbox row    │     bus     │      │ + DLQ │     │  version guard   │
│ COMMIT           │                └─────────────┘      └───────┘     │  your effect     │
└──────────────────┘   sweeper (1/min) republishes misses              │  once(email)     │
                                                                       └──────────────────┘
```

## Quickstart

### 1. Infrastructure

```hcl
module "bus" {
  source = "github.com/<org>/factbus//iac/modules/bus"
  name   = "events"
}

module "ledger" {                       # one block per consumer
  source       = "github.com/<org>/factbus//iac/modules/consumer"
  name         = "ledger"
  bus_name     = module.bus.bus_name
  sources      = ["shop.orders"]
  event_types  = ["order.placed"]
  function_arn = aws_lambda_function.ledger.arn
  role_name    = aws_iam_role.ledger.name
}
```

The `producer` module grants `PutEvents` under one `source` and schedules the sweeper.
See [`iac/README.md`](iac/README.md).

### 2. Produce (SQL database, in your transaction)

```python
from sqlalchemy.orm import Session
from factbus import Event, SqlProducer

producer = SqlProducer.connect(engine, bus_name="events")
producer.attach()                                   # once, at startup

with Session(engine) as session, session.begin():
    session.add(order)
    producer.stage(session, Event(type="order.placed", source="shop.orders",
                                  subject=str(order.id), data={"total": order.total}))
# committed -> published now; anything missed goes out on the next sweep

sweep = producer.sweep_handler()                    # Lambda entry point, every minute
```

`data` must be JSON-serialisable: convert a `Decimal` to `str` (exact) or `float` before
staging. Create the outbox table from your own Alembic revision with
`factbus.adapters.sql_migration.upgrade_0001(op)`. PostgreSQL and MySQL are supported, on
SQLAlchemy 1.4.36+ and 2.0.

### 3. Consume (DynamoDB inbox, no VPC)

```python
import os
from factbus import Event, consumer
from factbus.adapters.dynamodb_inbox import DynamoInbox

@consumer(inbox=DynamoInbox.from_env, types=["order.placed"])
def handler(event: Event, tx) -> None:
    tx.put(os.environ["FACTBUS_INBOX_TABLE"],          # JSON floats are stored as exact decimals
           {"pk": f"order#{event.subject}", "sk": "ledger", "total": event.data["total"]})
```

`handler` is the Lambda entry point. Your writes through `tx` commit in one DynamoDB
transaction with the inbox record, so a redelivered or replayed event changes nothing.
Failures are reported per message with backoff, and a poison message reaches the DLQ
after 10 receives.

### 4. A side effect that must not repeat

```python
from factbus import once
from factbus.adapters.dynamodb_claims import DynamoClaimStore

claims = DynamoClaimStore.from_env()                # same table as the inbox

@once(key=lambda event: f"welcome#{event.subject}", store=claims)
def send_welcome(event):
    return mailer.send(to=event.data["email"], template="welcome")
```

If the call raises after the provider may already have accepted the request, the key
becomes `Unknown` and is never retried automatically. A duplicate email is the worse
failure.

## Run it locally

```bash
docker compose up -d --wait        # LocalStack 3.8, PostgreSQL 16, MySQL 8.0
make test                          # unit suite, no Docker needed
make integration                   # real engines + LocalStack
make e2e                           # examples/hello: publish -> exactly once -> replay -> DLQ
```

## What factbus guarantees, and what it does not

| Guarantee | Mechanism |
| --- | --- |
| A fact is published at least once if and only if its transaction committed | outbox row in the same transaction, published after commit, swept every minute |
| A consumer applies each event once | inbox keyed on the event `id`, written in the same transaction as the effect |
| An older version never overwrites a newer one | optional `version_of`, conditional write |
| A side effect runs at most once per key | `once`: claim before the call, unknown outcomes are final |
| One poison message does not block its batch | `ReportBatchItemFailures` plus per-message backoff |

It does **not** promise global ordering, exactly-once delivery on the wire (it dedupes
instead), or that an `Unknown` side effect happened. Each of those is a stated decision;
see [`docs/design.md`](docs/design.md).

## Compared with

| Option | What you still build |
| --- | --- |
| Raw EventBridge + SQS | outbox, sweeper, inbox, partial batch failure, backoff, DLQ alarms, IAM per source |
| AWS Lambda Powertools (batch, idempotency) | outbox and IaC. Its idempotency record expires, so a crash mid-call can repeat the side effect |
| Kafka / MSK, RabbitMQ | a broker to run, plus outbox and inbox |
| Debezium CDC | Kafka Connect and a connector per database |

Envelopes are CloudEvents 1.0 (`id`, `type`, `source`, `subject`, `data`), carried as
the EventBridge `detail`.

## Status

`0.1.0.dev0`, pre-release. Python 3.12+. Licensed under Apache-2.0. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).
