"""The consumer inbox and applied-version tables, and the one transaction that uses them.

PostgreSQL 16 and MySQL 8.0, one implementation (A14).

`SqlInbox.process` runs, in ONE transaction on one connection:

1. a read of the inbox for `event_id`: present means DUPLICATE;
2. the inbox row insert. A unique-key violation here is DUPLICATE, not a failure
   (C0b-17): a second worker holding a copy of the same event blocks on the first
   worker's uncommitted key, and is refused once the first commits;
3. when the event has an ordered subject, the version guard: `is_newer` against the
   applied version, then a conditional write (`WHERE version < :v`), so a concurrent
   newer write for the same subject still wins (K3). At or below: STALE, and the inbox
   row commits with no effect;
4. the service's effect on the same connection;
5. commit. Anything raised by steps 3-5 rolls back the inbox row with the effect.

The dedupe key is the envelope's `event_id`, never EventBridge's `id` (new on replay).
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, MetaData, String, Table, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from factbus.adapters.sql import TIMESTAMP, _to_db
from factbus.domain import ConsumedEvent, InboxOutcome, SubjectVersion, is_newer
from factbus.ports import Clock, Effect

__all__ = [
    "DEFAULT_INBOX_TABLE_NAME",
    "DEFAULT_VERSIONS_TABLE_NAME",
    "SqlInbox",
    "applied_versions_table",
    "inbox_table",
]

DEFAULT_INBOX_TABLE_NAME = "factbus_inbox"
DEFAULT_VERSIONS_TABLE_NAME = "factbus_applied_version"


def inbox_table(metadata: MetaData, name: str = DEFAULT_INBOX_TABLE_NAME) -> Table:
    return Table(
        name,
        metadata,
        Column("event_id", String(128), primary_key=True),
        Column("received_at", TIMESTAMP, nullable=False),
    )


def applied_versions_table(metadata: MetaData, name: str = DEFAULT_VERSIONS_TABLE_NAME) -> Table:
    return Table(
        name,
        metadata,
        Column("subject_key", String(255), primary_key=True),
        Column("version", BigInteger(), nullable=False),
        Column("updated_at", TIMESTAMP, nullable=False),
    )


class _Duplicate(Exception):
    """Internal: leaves the transaction so it rolls back, then reads as DUPLICATE."""


class SqlInbox:
    def __init__(self, engine: Engine, inbox: Table, versions: Table, clock: Clock) -> None:
        self._engine = engine
        self._inbox = inbox
        self._versions = versions
        self._clock = clock

    def process(self, event: ConsumedEvent, *, subject: SubjectVersion | None, effect: Effect) -> InboxOutcome:
        try:
            with self._engine.begin() as connection:
                if self._seen(connection, event.event_id):
                    raise _Duplicate
                try:
                    connection.execute(
                        insert(self._inbox).values(event_id=event.event_id, received_at=_to_db(self._clock()))
                    )
                except IntegrityError as error:
                    raise _Duplicate from error
                if subject is not None and not self._advance(connection, subject):
                    return InboxOutcome.STALE
                effect(connection, event)
                return InboxOutcome.APPLIED
        except _Duplicate:
            return InboxOutcome.DUPLICATE

    def contains(self, event_id: str) -> bool:
        with self._engine.connect() as connection:
            return self._seen(connection, event_id)

    def applied_version(self, subject_key: str) -> int | None:
        with self._engine.connect() as connection:
            return self._applied(connection, subject_key)

    def _seen(self, connection, event_id: str) -> bool:
        query = select(self._inbox.c.event_id).where(self._inbox.c.event_id == event_id)
        return connection.execute(query).first() is not None

    def _applied(self, connection, subject_key: str) -> int | None:
        query = select(self._versions.c.version).where(self._versions.c.subject_key == subject_key)
        value = connection.execute(query).scalar_one_or_none()
        return None if value is None else int(value)

    def _advance(self, connection, subject: SubjectVersion) -> bool:
        """True when this event's version was written as the subject's applied version."""
        applied = self._applied(connection, subject.subject_key)
        if not is_newer(applied, subject.version):
            return False
        now = _to_db(self._clock())
        if applied is None:
            try:
                with connection.begin_nested():
                    connection.execute(
                        insert(self._versions).values(
                            subject_key=subject.subject_key, version=subject.version, updated_at=now
                        )
                    )
                return True
            except IntegrityError:
                pass  # another worker wrote this subject's first version; compare against it below
        result = connection.execute(
            update(self._versions)
            .where(self._versions.c.subject_key == subject.subject_key, self._versions.c.version < subject.version)
            .values(version=subject.version, updated_at=now)
        )
        return result.rowcount == 1

