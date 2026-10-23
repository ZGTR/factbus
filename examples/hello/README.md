# hello: one producer, one bus, one consumer Lambda

`hello.said` events go onto an EventBridge bus. The `consumer` module routes them to a
queue, and the `hello-ledger` Lambda (`handler.py`) writes one item per event to the
`hello-ledger` DynamoDB table, in the same transaction as the factbus inbox record. A
second copy of the same CloudEvents `id` writes nothing. An event with
`data.poison = true` raises, is retried with backoff, and lands in the DLQ after three
receives.

The whole consumer is one module block in `main.tf`:

```hcl
module "consumer" {
  source             = "../../iac/modules/consumer"
  name               = "hello-ledger"
  bus_name           = module.bus.bus_name
  sources            = [local.event_source]
  event_types        = [local.event_type]
  function_arn       = aws_lambda_function.hello.arn
  role_name          = aws_iam_role.hello.name
  max_receive_count  = 3
  create_inbox_table = true
}
```

It creates no VPC, subnet, NAT gateway or database instance. The inbox and the ledger
are DynamoDB tables.

## Run it on LocalStack

From the repo root, with `tofu` and `uv` on PATH and Docker running:

```bash
uv venv -q -p 3.12 .venv && uv pip install -q -p .venv -e '.[dev]'
make up       # docker compose up -d --wait (LocalStack 3.8, PostgreSQL, MySQL)
make e2e      # tofu init + apply, scripts/e2e_hello.py, tofu destroy
```

`make e2e` exits non-zero if any check fails, or if the destroy fails. To keep the stack
for poking at it:

```bash
KEEP=1 make e2e
tofu -chdir=examples/hello output
make down     # tofu destroy; compose stays up
```

The same steps by hand:

```bash
tofu -chdir=examples/hello init
tofu -chdir=examples/hello apply \
  -var localstack_endpoint=http://localhost:4566 -var test_backoff_factor=0.01
.venv/bin/python scripts/e2e_hello.py --endpoint http://localhost:4566
tofu -chdir=examples/hello destroy \
  -var localstack_endpoint=http://localhost:4566 -var test_backoff_factor=0.01
```

`test_backoff_factor` shrinks the retry waits (30 s, 2 m, 10 m, ...) so the poison event
reaches the DLQ in seconds. It is for tests only. Leave it unset anywhere real.

LocalStack runs the function in a `public.ecr.aws/lambda/python:3.12` container via the
Docker socket. The first run pulls that image. Inside the container LocalStack sets
`AWS_ENDPOINT_URL`, and `handler.py` copies it to `FACTBUS_ENDPOINT_URL`.

## What the e2e script checks

| Check | Passes when |
| --- | --- |
| applied | one event on the bus gives exactly one ledger item for its id, and an inbox record |
| duplicate | the same event put again (new EventBridge id, same CloudEvents id) leaves one item; a control event put after it is applied and the queue drains |
| poison | the poison event reaches the DLQ and writes no ledger item |
| evidence | the Lambda's own EMF log lines show `ConsumerDuplicates` and at least three `ConsumerRetried` |

## Run it on real AWS

Leave `localstack_endpoint` unset. The provider then uses your credential chain and
region:

```bash
tofu -chdir=examples/hello init
tofu -chdir=examples/hello apply
.venv/bin/python scripts/e2e_hello.py --endpoint ''
tofu -chdir=examples/hello destroy
```

Without `test_backoff_factor`, the poison arm waits out the real schedule (30 s + 2 m +
10 m) before the DLQ check can pass, so raise `--dlq-timeout` to about 900. The
function zip contains `handler.py` and `src/factbus`. boto3 comes from the Lambda runtime.
