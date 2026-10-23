"""Publish the rows the on-commit path missed, each once.

Design §1: rows older than 30 s, unsent and not failed,
selected `FOR UPDATE SKIP LOCKED` on ONE connection, ONE chunk at a time. Each iteration
begins a transaction, locks at most 10 rows (and under 1 MB of entries), calls
`PutEvents` once, marks what the bus accepted and commits, then selects the next chunk
afresh, up to 500 rows per run. No row is held after its lock is released and no lock
spans more than one call, so an overlapping sweep (K5) skips the rows this one holds and
never publishes them a second time. The IaC gives the sweeper reserved concurrency 1 as
well; the lock is what makes overlap safe when that fails.

The chunk cursor (`after_row_id`) moves past every row the run has looked at, so a row
the bus refused transiently is not re-selected in the same run. It waits a minute.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from factbus.application.delivery import send_chunk
from factbus.application.readiness import BusReadiness
from factbus.domain import MAX_ENTRIES_PER_REQUEST, request_prefix
from factbus.ports import Clock, EventBus, Metrics, OutboxStore

__all__ = ["MAX_ROWS_PER_RUN", "MIN_ROW_AGE", "SweepReport", "Sweeper"]

MIN_ROW_AGE = timedelta(seconds=30)
MAX_ROWS_PER_RUN = 500

SWEEP_REFUSED_METRIC = "OutboxSweepRefused"


@dataclass
class SweepReport:
    bus_confirmed: bool = True
    chunks: int = 0
    examined: int = 0
    sent: list[int] = field(default_factory=list)
    failed: list[int] = field(default_factory=list)
    retry: list[int] = field(default_factory=list)
    stopped_on_request_failure: bool = False


class Sweeper:
    def __init__(
        self,
        *,
        bus: EventBus,
        store: OutboxStore,
        readiness: BusReadiness,
        metrics: Metrics,
        clock: Clock,
        min_row_age: timedelta = MIN_ROW_AGE,
        max_rows: int = MAX_ROWS_PER_RUN,
    ) -> None:
        self._bus = bus
        self._store = store
        self._readiness = readiness
        self._metrics = metrics
        self._clock = clock
        self._min_row_age = min_row_age
        self._max_rows = max_rows

    def run(self) -> SweepReport:
        report = SweepReport()
        if not self._readiness.confirm_now():
            report.bus_confirmed = False
            self._metrics.count(SWEEP_REFUSED_METRIC)
            return report
        created_before = self._clock() - self._min_row_age
        after_row_id = 0
        with self._store.open_sweep() as session:
            while report.examined < self._max_rows:
                limit = min(MAX_ENTRIES_PER_REQUEST, self._max_rows - report.examined)
                with session.locked_chunk(
                    created_before=created_before, after_row_id=after_row_id, limit=limit
                ) as chunk:
                    if not chunk.rows:
                        break
                    rows = list(chunk.rows)[: request_prefix(chunk.rows)]
                    # Rows beyond the 1 MB prefix are released unsent at commit; the
                    # cursor stops before them so the next chunk selects them afresh.
                    after_row_id = rows[-1].row_id
                    result = send_chunk(
                        rows, bus=self._bus, marker=chunk, metrics=self._metrics, now=self._clock()
                    )
                report.chunks += 1
                report.examined += len(rows)
                report.sent.extend(result.sent)
                report.failed.extend(result.failed)
                report.retry.extend(result.retry)
                if result.request_failed:
                    # The bus is failing whole requests; the rest of the run would fail
                    # the same way. Stop, and let the next minute try again.
                    report.stopped_on_request_failure = True
                    break
        return report
