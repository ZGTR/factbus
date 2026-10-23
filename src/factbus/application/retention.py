"""Delete sent rows once they are older than the 7-day republish window.

The kit keeps sent rows for 7 days so `republish_since` has something to
re-send. Unsent and failed rows are never purged: an unsent row is work, and a failed
row is the evidence an operator needs (R7-26).
"""

from __future__ import annotations

from datetime import timedelta

from factbus.ports import Clock, OutboxStore

__all__ = ["SENT_RETENTION", "purge_sent_rows"]

SENT_RETENTION = timedelta(days=7)
PURGE_LIMIT = 500


def purge_sent_rows(*, store: OutboxStore, clock: Clock, retention: timedelta = SENT_RETENTION) -> int:
    if retention < SENT_RETENTION:
        raise ValueError(f"retention {retention} is shorter than the {SENT_RETENTION} republish window")
    return store.purge_sent_before(clock() - retention, limit=PURGE_LIMIT)
