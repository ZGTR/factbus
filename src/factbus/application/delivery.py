"""Send one chunk of outbox rows as one `PutEvents` request and record what the bus said.

Shared by the on-commit path and the sweeper so both apply one set of rules:

* an entry bigger than one entry may be is refused before sending and marked failed;
* a request-level failure leaves every row unsent with the error recorded;
* an accepted entry is marked sent, a content reject is marked failed and counted
  (R7-26), a deployment reject stays unsent under its own metric, anything else stays
  unsent for the next sweep.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from factbus.domain import (
    MAX_ENTRY_BYTES,
    OVERSIZED_ENTRY,
    EntryOutcome,
    OutboxRow,
    outcomes_for,
)
from factbus.ports import BusUnavailable, EventBus, Metrics

__all__ = ["ChunkResult", "RowMarker", "send_chunk"]

SENT_METRIC = "OutboxRowsSent"
FAILED_METRIC = "OutboxRowsFailed"
RETRY_METRIC = "OutboxRowsRetried"
DEPLOYMENT_REJECT_METRIC = "OutboxDeploymentRejected"
UNATTRIBUTABLE_METRIC = "OutboxResponseUnattributable"
REQUEST_FAILED_METRIC = "OutboxRequestFailed"


class RowMarker(Protocol):
    def mark_sent(self, row_ids: Sequence[int], at: datetime) -> object: ...

    def mark_failed(self, row_id: int, at: datetime, error: str) -> None: ...

    def record_retry(self, row_ids: Sequence[int], error: str) -> None: ...


@dataclass
class ChunkResult:
    sent: list[int] = field(default_factory=list)
    failed: list[int] = field(default_factory=list)
    retry: list[int] = field(default_factory=list)
    request_failed: bool = False
    attributable: bool = True


def send_chunk(
    rows: Sequence[OutboxRow],
    *,
    bus: EventBus,
    marker: RowMarker,
    metrics: Metrics,
    now: datetime,
) -> ChunkResult:
    result = ChunkResult()
    sendable: list[OutboxRow] = []
    for row in rows:
        if row.to_entry().size_bytes > MAX_ENTRY_BYTES:
            marker.mark_failed(row.row_id, now, OVERSIZED_ENTRY)
            result.failed.append(row.row_id)
        else:
            sendable.append(row)
    if not sendable:
        _count(metrics, result)
        return result

    try:
        response = bus.put_events([row.to_entry() for row in sendable])
    except BusUnavailable as error:
        ids = [row.row_id for row in sendable]
        marker.record_retry(ids, f"request:{error.code}")
        result.retry.extend(ids)
        result.request_failed = True
        metrics.count(REQUEST_FAILED_METRIC)
        _count(metrics, result)
        return result

    outcomes, result.attributable = outcomes_for(len(sendable), response)
    if not result.attributable:
        metrics.count(UNATTRIBUTABLE_METRIC)
    codes = response.entry_error_codes if result.attributable else (None,) * len(sendable)
    deployment: list[int] = []
    retry_by_code: dict[str, list[int]] = {}
    for row, outcome, code in zip(sendable, outcomes, codes, strict=True):
        if outcome is EntryOutcome.ACCEPTED:
            result.sent.append(row.row_id)
        elif outcome is EntryOutcome.PERMANENT:
            marker.mark_failed(row.row_id, now, code or "unknown")
            result.failed.append(row.row_id)
        else:
            if outcome is EntryOutcome.DEPLOYMENT:
                deployment.append(row.row_id)
            retry_by_code.setdefault(code or "unattributable", []).append(row.row_id)
            result.retry.append(row.row_id)
    if result.sent:
        marker.mark_sent(result.sent, now)
    for code, ids in retry_by_code.items():
        marker.record_retry(ids, code)
    if deployment:
        metrics.count(DEPLOYMENT_REJECT_METRIC, len(deployment))
    _count(metrics, result)
    return result


def _count(metrics: Metrics, result: ChunkResult) -> None:
    if result.sent:
        metrics.count(SENT_METRIC, len(result.sent))
    if result.failed:
        metrics.count(FAILED_METRIC, len(result.failed))
    if result.retry:
        metrics.count(RETRY_METRIC, len(result.retry))
