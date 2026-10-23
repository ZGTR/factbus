"""The outbox table and every statement against it. PostgreSQL 16 and MySQL 8.0.

One implementation for both engines (A14): adopters run PostgreSQL and MySQL alike, and
the kit should not choose for them.

Timestamps are stored as naive UTC with microseconds. MySQL's `DATETIME` defaults to
whole seconds, which would round a row's age across the sweeper's 30-second boundary and
merge the order of rows written in one second, so the MySQL variant declares `fsp=6`.

SQLAlchemy 1.4.36 and 2.0 are both supported, so a service still pinned to 1.4 can adopt
the kit. `Engine` and `Select` are therefore imported from `sqlalchemy.engine` and
`sqlalchemy.sql`, where both versions export them; the top-level names exist on 2.0 only.
`scripts/test_sqlalchemy_matrix.sh` runs the unit and SQL integration suites on each.

The sweep's locking read lives in `due_rows_query`, a module-level function the sweep
session looks up at call time. The sweep-overlap arm replaces it with a plain `SELECT`
to prove the lock is what keeps two overlapping sweeps from publishing one row twice.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select

from factbus.domain import OutboxRow, canonical_json, envelope_keys
from factbus.ports import Clock, OutboxStoreError

__all__ = [
    "DEFAULT_TABLE_NAME",
    "SqlOutboxStore",
    "due_rows_query",
    "outbox_table",
]

DEFAULT_TABLE_NAME = "factbus_outbox"

TIMESTAMP = DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")
ENVELOPE_TEXT = Text().with_variant(mysql.MEDIUMTEXT(), "mysql")
ERROR_LENGTH = 255


def outbox_table(metadata: MetaData, name: str = DEFAULT_TABLE_NAME) -> Table:
    return Table(
        name,
        metadata,
        Column("id", BigInteger().with_variant(Integer(), "sqlite"), primary_key=True, autoincrement=True),
        Column("event_id", String(128), nullable=False),
        Column("event_type", String(255), nullable=False),
        Column("producer", String(255), nullable=False),
        Column("envelope", ENVELOPE_TEXT, nullable=False),
        Column("created_at", TIMESTAMP, nullable=False),
        Column("sent_at", TIMESTAMP, nullable=True),
        Column("failed_at", TIMESTAMP, nullable=True),
        Column("attempts", Integer(), nullable=False, server_default="0"),
        Column("last_error", String(ERROR_LENGTH), nullable=True),
        UniqueConstraint("event_id", name=f"uq_{name}_event_id"),
        Index(f"ix_{name}_due", "sent_at", "failed_at", "id"),
    )


def due_rows_query(table: Table, *, created_before: datetime, after_row_id: int, limit: int) -> Select:
    """Unsent, not failed, older than the cutoff, past the cursor: locked, skipping locked rows."""
    return (
        select(table)
        .where(
            table.c.sent_at.is_(None),
            table.c.failed_at.is_(None),
            table.c.created_at < _to_db(created_before),
            table.c.id > after_row_id,
        )
        .order_by(table.c.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )


def _to_db(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        raise ValueError("outbox timestamps must be timezone-aware")
    return moment.astimezone(UTC).replace(tzinfo=None)


def _from_db(moment: datetime | None) -> datetime | None:
    return None if moment is None else moment.replace(tzinfo=UTC)


def _row(record) -> OutboxRow:
    return OutboxRow(
        row_id=int(record.id),
        event_id=record.event_id,
        event_type=record.event_type,
        producer=record.producer,
        envelope_json=record.envelope,
        created_at=_from_db(record.created_at),
        sent_at=_from_db(record.sent_at),
        failed_at=_from_db(record.failed_at),
        attempts=int(record.attempts),
        last_error=record.last_error,
    )


def _mark_sent(table: Table, row_ids: Sequence[int], at: datetime):
    return (
        update(table)
        .where(table.c.id.in_(list(row_ids)), table.c.sent_at.is_(None), table.c.failed_at.is_(None))
        .values(sent_at=_to_db(at), last_error=None)
    )


def _mark_failed(table: Table, row_id: int, at: datetime, error: str):
    return (
        update(table)
        .where(table.c.id == row_id, table.c.sent_at.is_(None))
        .values(failed_at=_to_db(at), attempts=table.c.attempts + 1, last_error=error[:ERROR_LENGTH])
    )


def _record_retry(table: Table, row_ids: Sequence[int], error: str):
    return (
        update(table)
        .where(table.c.id.in_(list(row_ids)))
        .values(attempts=table.c.attempts + 1, last_error=error[:ERROR_LENGTH])
    )


class SqlLockedChunk:
    """Marks run on the sweep's connection, inside the chunk's open transaction."""

    def __init__(self, connection, table: Table, rows: Sequence[OutboxRow]) -> None:
        self._connection = connection
        self._table = table
        self.rows = rows

    def mark_sent(self, row_ids: Sequence[int], at: datetime) -> int:
        return self._connection.execute(_mark_sent(self._table, row_ids, at)).rowcount

    def mark_failed(self, row_id: int, at: datetime, error: str) -> None:
        self._connection.execute(_mark_failed(self._table, row_id, at, error))

    def record_retry(self, row_ids: Sequence[int], error: str) -> None:
        self._connection.execute(_record_retry(self._table, row_ids, error))


class SqlSweepSession:
    def __init__(self, connection, table: Table) -> None:
        self._connection = connection
        self._table = table

    @contextmanager
    def locked_chunk(self, *, created_before: datetime, after_row_id: int, limit: int) -> Iterator[SqlLockedChunk]:
        try:
            with self._connection.begin():
                query = due_rows_query(
                    self._table, created_before=created_before, after_row_id=after_row_id, limit=limit
                )
                rows = [_row(record) for record in self._connection.execute(query)]
                yield SqlLockedChunk(self._connection, self._table, rows)
        except SQLAlchemyError as error:
            raise OutboxStoreError(str(error)) from error


class SqlOutboxStore:
    def __init__(self, engine: Engine, table: Table, clock: Clock) -> None:
        self._engine = engine
        self._table = table
        self._clock = clock

    @property
    def table(self) -> Table:
        return self._table

    def add(self, connection, envelope: Mapping[str, object]) -> OutboxRow:
        """Insert on the caller's `Connection` or `Session`. Never begins or commits.

        Database errors propagate unwrapped: they belong to the caller's transaction,
        which must see them to roll back its domain write with the row.
        """
        event_id, event_type, producer = envelope_keys(envelope)
        body = canonical_json(envelope)
        created_at = self._clock()
        result = connection.execute(
            insert(self._table).values(
                event_id=event_id,
                event_type=event_type,
                producer=producer,
                envelope=body,
                created_at=_to_db(created_at),
                attempts=0,
            )
        )
        return OutboxRow(
            row_id=int(result.inserted_primary_key[0]),
            event_id=event_id,
            event_type=event_type,
            producer=producer,
            envelope_json=body,
            created_at=created_at,
        )

    def mark_sent(self, row_ids: Sequence[int], at: datetime) -> int:
        with self._short_transaction() as connection:
            return connection.execute(_mark_sent(self._table, row_ids, at)).rowcount

    def mark_failed(self, row_id: int, at: datetime, error: str) -> None:
        with self._short_transaction() as connection:
            connection.execute(_mark_failed(self._table, row_id, at, error))

    def record_retry(self, row_ids: Sequence[int], error: str) -> None:
        with self._short_transaction() as connection:
            connection.execute(_record_retry(self._table, row_ids, error))

    @contextmanager
    def open_sweep(self) -> Iterator[SqlSweepSession]:
        try:
            with self._engine.connect() as connection:
                yield SqlSweepSession(connection, self._table)
        except SQLAlchemyError as error:
            raise OutboxStoreError(str(error)) from error

    def sent_since(self, since: datetime, *, after_row_id: int, limit: int) -> list[OutboxRow]:
        query = (
            select(self._table)
            .where(self._table.c.sent_at >= _to_db(since), self._table.c.id > after_row_id)
            .order_by(self._table.c.id)
            .limit(limit)
        )
        with self._short_transaction() as connection:
            return [_row(record) for record in connection.execute(query)]

    def purge_sent_before(self, before: datetime, *, limit: int) -> int:
        with self._short_transaction() as connection:
            ids = [
                record.id
                for record in connection.execute(
                    select(self._table.c.id)
                    .where(self._table.c.sent_at < _to_db(before))
                    .order_by(self._table.c.id)
                    .limit(limit)
                )
            ]
            if not ids:
                return 0
            return connection.execute(delete(self._table).where(self._table.c.id.in_(ids))).rowcount

    def row(self, row_id: int) -> OutboxRow | None:
        with self._short_transaction() as connection:
            record = connection.execute(select(self._table).where(self._table.c.id == row_id)).first()
        return None if record is None else _row(record)

    @contextmanager
    def _short_transaction(self):
        try:
            with self._engine.begin() as connection:
                yield connection
        except SQLAlchemyError as error:
            raise OutboxStoreError(str(error)) from error
