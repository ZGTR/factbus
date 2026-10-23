# Contributing to factbus

## Setup

```bash
uv venv --python 3.12 .venv && uv pip install -e '.[dev]'
docker compose up -d --wait          # LocalStack 3.8, PostgreSQL 16, MySQL 8.0
```

## Before a pull request

```bash
.venv/bin/python -m pytest                                   # unit suite
FACTBUS_PG_URL=postgresql+psycopg://factbus:factbus@localhost:5442/factbus \
FACTBUS_MYSQL_URL=mysql+pymysql://factbus:factbus@localhost:3316/factbus \
LOCALSTACK_ENDPOINT=http://localhost:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  .venv/bin/python -m pytest tests/integration               # real engines; fails, never skips, when one is missing
PYTHON=$(uv python find 3.12) bash scripts/test_sqlalchemy_matrix.sh   # SQLAlchemy 1.4.36 and 2.0
python3 scripts/check_eventing_boundaries.py                 # architecture gate
(cd iac/modules/consumer && tofu init -backend=false && tofu test)
```

## Rules the gate enforces

- boto3 only in `factbus.adapters`, and every client built with an explicit `config=`.
- `domain` is pure: no I/O, no SDK, no clock.
- No event-type literal and no product or contract import in `src`. The kit knows no
  event type.
- Nothing is constructed at import time.

## Changing a guarantee

Every rule in [`docs/design.md`](docs/design.md) names its proof. A change that weakens
a rule updates the design doc and the proving test in the same pull request.

Contributions are licensed under Apache-2.0 (see `LICENSE`).
