"""How long a failed message waits before its next receive (design §2).

The handler, not the queue, sets the wait: on a retryable failure it changes the
message's visibility to the wait for its `ApproximateReceiveCount`. Nine waits cover
receives 1 to 9 (30 s, 2 m, 10 m, 30 m, 1 h, 2 h, 2 h, 2 h, 2 h; about 9.7 h in total).
Receive 10 has no wait: the queue's `maxReceiveCount` of 10 moves the message to its
DLQ on the next receive, which is the 9.7 h budget design §2 states. Past receive 10
the last wait applies again: a queue whose redrive policy is missing or larger than 10
then retries every 2 h instead of spinning at 0 s. Measured by F9's blinded run (redrive
policy removed): with a 0 s wait past the table the poison was received 41 more times
before the arm stopped. Any single wait is capped at 12 h, the most one
`ChangeMessageVisibility` may set.

A TEST PROFILE may scale the waits (exception X3: a 9.7-hour arm is not a unit test).
A scaled schedule says so in `is_real`, so a deploy check can refuse it. F13 runs the
real one.

Pure: no clock, no SDK.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["MAX_VISIBILITY_SECONDS", "REAL_WAITS_SECONDS", "BackoffSchedule"]

#: Design §2, hand-written. Index 0 is the wait after receive 1.
REAL_WAITS_SECONDS = (30, 120, 600, 1800, 3600, 7200, 7200, 7200, 7200)

#: SQS's ceiling for one visibility timeout (ChangeMessageVisibility API reference).
MAX_VISIBILITY_SECONDS = 43_200


@dataclass(frozen=True)
class BackoffSchedule:
    waits_seconds: tuple[int, ...] = REAL_WAITS_SECONDS

    def __post_init__(self) -> None:
        if not self.waits_seconds:
            raise ValueError("a backoff schedule needs at least one wait")
        if any(not isinstance(w, int) or w < 0 for w in self.waits_seconds):
            raise ValueError(f"waits must be non-negative whole seconds, got {self.waits_seconds!r}")

    @classmethod
    def scaled(cls, factor: float) -> BackoffSchedule:
        """The real waits multiplied by `factor` (0 < factor < 1), each at least 1 s.

        Test profile only. The floor of 1 s keeps the order of receives the real
        schedule produces; a zero wait would let a failed message race its own retry.
        """
        if not 0 < factor < 1:
            raise ValueError(f"a scaled schedule shrinks the real one; factor {factor!r} is not in (0, 1)")
        return cls(tuple(max(1, round(w * factor)) for w in REAL_WAITS_SECONDS))

    @property
    def is_real(self) -> bool:
        return self.waits_seconds == REAL_WAITS_SECONDS

    @property
    def total_seconds(self) -> int:
        return sum(min(w, MAX_VISIBILITY_SECONDS) for w in self.waits_seconds)

    def wait_for(self, receive_count: int) -> int:
        """Seconds to hide a message that has just failed on receive `receive_count`.

        Receive counts start at 1; a count below 1 (a missing attribute read as 0) is
        treated as the first receive. The receive just past the table waits 0, so the
        redrive policy takes the message at once; any later receive (a queue without a
        correct redrive policy) waits the last wait rather than spinning.
        """
        index = max(receive_count, 1) - 1
        if index == len(self.waits_seconds):
            return 0
        wait = self.waits_seconds[min(index, len(self.waits_seconds) - 1)]
        return min(wait, MAX_VISIBILITY_SECONDS)
