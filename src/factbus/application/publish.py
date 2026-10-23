"""Publish the rows a request just committed, before the response returns.

Design §1 and R6-4: under the Lambda Web Adapter the environment freezes once the
response is returned, so the on-commit publish runs in the request, after `COMMIT` and
before the response. It is the fast path, not the guarantee: whatever it misses (a crash
between commit and publish, a bus error, an unconfirmed bus) the sweeper publishes after
30 s.

This call NEVER raises into the caller for a bus or store problem. The fact is already
committed; failing the response would tell the client its write failed when it did not.
A problem is counted and left to the sweeper.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from factbus.application.delivery import send_chunk
from factbus.application.readiness import BusReadiness
from factbus.domain import OutboxRow, request_prefix
from factbus.ports import Clock, EventBus, Metrics, OutboxStore, OutboxStoreError

__all__ = ["OnCommitPublisher", "PublishReport"]

DEFERRED_METRIC = "OutboxPublishDeferred"


@dataclass
class PublishReport:
    sent: list[int] = field(default_factory=list)
    failed: list[int] = field(default_factory=list)
    deferred: list[int] = field(default_factory=list)
    bus_confirmed: bool = True


class OnCommitPublisher:
    def __init__(
        self,
        *,
        bus: EventBus,
        store: OutboxStore,
        readiness: BusReadiness,
        metrics: Metrics,
        clock: Clock,
    ) -> None:
        self._bus = bus
        self._store = store
        self._readiness = readiness
        self._metrics = metrics
        self._clock = clock

    def publish_on_commit(self, rows: Sequence[OutboxRow]) -> PublishReport:
        """Publish `rows` (just committed by the caller) and mark the accepted ones sent."""
        report = PublishReport()
        pending = list(rows)
        if not pending:
            return report
        if not self._readiness.confirm_for_publish():
            report.bus_confirmed = False
            report.deferred = [row.row_id for row in pending]
            self._metrics.count(DEFERRED_METRIC, len(pending))
            return report
        while pending:
            take = request_prefix(pending)
            chunk, pending = pending[:take], pending[take:]
            try:
                result = send_chunk(
                    chunk, bus=self._bus, marker=self._store, metrics=self._metrics, now=self._clock()
                )
            except OutboxStoreError:
                # The marks did not land. The rows stay unsent and the sweeper publishes
                # them again; a double publish is absorbed by the consumer inbox (K4).
                report.deferred.extend(row.row_id for row in chunk)
                self._metrics.count(DEFERRED_METRIC, len(chunk))
                continue
            report.sent.extend(result.sent)
            report.failed.extend(result.failed)
            report.deferred.extend(result.retry)
            if result.request_failed:
                rest = [row.row_id for row in pending]
                report.deferred.extend(rest)
                if rest:
                    self._metrics.count(DEFERRED_METRIC, len(rest))
                break
        return report
