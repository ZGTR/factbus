"""The runbook's `republish --since <t>`: send again every row marked sent at or after `t`.

For a bus that was deleted while the service ran. `PutEvents` to a missing bus answers
200, so rows published between the deletion and the next failed `DescribeEventBus`
(at most 60 s, `BusReadiness`) were marked sent and dropped. Once the bus is recreated,
republishing from `BusReadiness.last_confirmed_at` before the outage re-sends that
window; rows that did arrive arrive twice and the consumer inbox absorbs them.

Rows are read in chunks by id, never locked: a republish does not compete with the
sweeper, which only takes unsent rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from factbus.application.delivery import send_chunk
from factbus.application.readiness import BusReadiness
from factbus.domain import MAX_ENTRIES_PER_REQUEST, request_prefix
from factbus.ports import Clock, EventBus, Metrics, OutboxStore

__all__ = ["RepublishReport", "republish_since"]


@dataclass
class RepublishReport:
    bus_confirmed: bool = True
    sent: list[int] = field(default_factory=list)
    retry: list[int] = field(default_factory=list)


def republish_since(
    since: datetime,
    *,
    bus: EventBus,
    store: OutboxStore,
    readiness: BusReadiness,
    metrics: Metrics,
    clock: Clock,
) -> RepublishReport:
    report = RepublishReport()
    if not readiness.confirm_now():
        report.bus_confirmed = False
        return report
    after_row_id = 0
    while True:
        rows = store.sent_since(since, after_row_id=after_row_id, limit=MAX_ENTRIES_PER_REQUEST)
        if not rows:
            return report
        rows = rows[: request_prefix(rows)]
        after_row_id = rows[-1].row_id
        result = send_chunk(rows, bus=bus, marker=store, metrics=metrics, now=clock())
        report.sent.extend(result.sent)
        report.retry.extend(result.retry)
        if result.request_failed:
            return report
