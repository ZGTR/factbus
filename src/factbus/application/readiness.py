"""Whether the configured bus exists, fresh for a sweep and at most 60 s old on commit.

`PutEvents` to a bus that does not exist answers HTTP 200 and drops the event (the
EventBridge PutEvents guide; LocalStack 3.8 measured the same, P11). An
accepted answer therefore never proves the bus is there, and a publisher that trusted it
would mark lost events sent. So the bus is checked with `DescribeEventBus`:

* at startup, where an absent bus refuses readiness (C0b-18);
* before every sweep run, uncached;
* on the on-commit path through a positive answer cached for at most 60 s, so a bus
  deleted while the service runs stops being published to within 60 s. The rows sent in
  that window are recovered with `republish_since(last_confirmed_at)` once the bus is
  back, and the consumer inbox absorbs any duplicate.

A negative answer is never cached: the next caller asks again.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from factbus.ports import BusUnavailable, Clock, EventBus, Metrics

__all__ = ["ON_COMMIT_MAX_AGE", "BusReadiness"]

ON_COMMIT_MAX_AGE = timedelta(seconds=60)

BUS_UNCONFIRMED_METRIC = "OutboxBusUnconfirmed"

log = logging.getLogger(__name__)


class BusReadiness:
    def __init__(
        self,
        bus: EventBus,
        clock: Clock,
        metrics: Metrics,
        *,
        max_age: timedelta = ON_COMMIT_MAX_AGE,
    ) -> None:
        if max_age > ON_COMMIT_MAX_AGE:
            raise ValueError(f"max_age {max_age} exceeds the {ON_COMMIT_MAX_AGE} bound (C0b-18)")
        self._bus = bus
        self._clock = clock
        self._metrics = metrics
        self._max_age = max_age
        self._last_confirmed_at: datetime | None = None

    @property
    def last_confirmed_at(self) -> datetime | None:
        """When `DescribeEventBus` last answered for the configured bus."""
        return self._last_confirmed_at

    def confirm_now(self) -> bool:
        """Ask the bus, uncached. Used at startup, by readiness probes and by each sweep."""
        try:
            exists = self._bus.bus_exists()
        except BusUnavailable as error:
            # Say why: a role missing events:DescribeEventBus looks exactly like a
            # deleted bus in the metric, and defers every publish.
            log.warning("event bus not confirmed (%s): %s", error.code, error)
            exists = False
        if exists:
            self._last_confirmed_at = self._clock()
        else:
            self._last_confirmed_at = None
            self._metrics.count(BUS_UNCONFIRMED_METRIC)
        return exists

    def confirm_for_publish(self) -> bool:
        """The on-commit check: reuse a positive answer younger than `max_age`, else ask."""
        confirmed = self._last_confirmed_at
        if confirmed is not None and self._clock() - confirmed < self._max_age:
            return True
        return self.confirm_now()
