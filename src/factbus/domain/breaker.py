"""Whether a provider may be called now (design §2, "Provider breaker").

In-process, one per provider. It opens after 5 retryable provider errors inside one
minute and stays open until a probe succeeds. While open it admits one probe per probe
interval; every other item returns to its queue with backoff, without calling the
provider, so an outage burns neither provider quota nor receives quickly.

There is deliberately no breaker that disables an event source mapping (design §2:
removed in review, because a handler switching off its own trigger needs a second
function to switch it back on).

No clock is read here: every method takes `now`, so the rules are testable to the
microsecond and the domain stays pure.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

__all__ = ["FAILURE_THRESHOLD", "FAILURE_WINDOW", "PROBE_INTERVAL", "ProviderBreaker"]

FAILURE_THRESHOLD = 5
FAILURE_WINDOW = timedelta(minutes=1)
#: One probe per interval while open. Equal to the first backoff wait, so the probe
#: rides on the first message to come back.
PROBE_INTERVAL = timedelta(seconds=30)


class ProviderBreaker:
    def __init__(
        self,
        *,
        threshold: int = FAILURE_THRESHOLD,
        window: timedelta = FAILURE_WINDOW,
        probe_interval: timedelta = PROBE_INTERVAL,
    ) -> None:
        if threshold < 1:
            raise ValueError("threshold must be at least 1")
        self._threshold = threshold
        self._window = window
        self._probe_interval = probe_interval
        self._failures: deque[datetime] = deque()
        self._opened_at: datetime | None = None
        self._next_probe_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        return self._opened_at is not None

    def admits(self, now: datetime) -> bool:
        """A query: may the provider be called at `now`? Closed: yes. Open: only when a
        probe is due. Call `probe_started` when acting on an open-state admission."""
        if self._opened_at is None:
            return True
        return self._next_probe_at is not None and now >= self._next_probe_at

    def probe_started(self, now: datetime) -> None:
        if self._opened_at is not None:
            self._next_probe_at = now + self._probe_interval

    def record_failure(self, now: datetime) -> None:
        if self._opened_at is not None:
            return
        self._failures.append(now)
        while self._failures and now - self._failures[0] >= self._window:
            self._failures.popleft()
        if len(self._failures) >= self._threshold:
            self._opened_at = now
            self._next_probe_at = now + self._probe_interval
            self._failures.clear()

    def record_success(self) -> None:
        """A provider call succeeded. Closes an open breaker (the probe worked).

        A success while closed does NOT clear the failure window: five errors in a
        minute open the breaker even when successes are interleaved, so a flapping
        provider is caught as well as a dead one.
        """
        if self._opened_at is None:
            return
        self._opened_at = None
        self._next_probe_at = None
        self._failures.clear()
