"""How long the next provider call must wait (design §2, "a per-sender token bucket").

One bucket per consumer instance per provider sender. Lambda runs up to the mapping's
`MaximumConcurrency` instances at once and they share nothing, so the provider's limit is
split statically: `instance_rate` gives each instance a share small enough that ALL of
them together stay at or under the provider's limit in every one-second window.

The split subtracts the burst, and that term is not optional. A bucket holding `burst`
tokens at rate `r` can emit `burst + r * w` calls in any closed window of `w` seconds: a
full bucket spends its burst at once, then one call every `1 / r` seconds, and a window
that starts on a call includes the call that ends it. So N instances at rate `R / N`
reach `N * burst + R` calls in one second -- 13 for a limit of 10 at N = 3 and burst 1 --
while `R / N - burst` keeps the sum at `R`. The price is throughput: the aggregate
long-run rate is `R - N * burst`, 7 of 10 at N = 3.

`reserve` books a token rather than testing for one. The token count may go negative:
the caller that reserved it waits out the returned delay, and the next caller's delay is
measured from that booking. A caller whose delay would exceed `max_wait` books nothing and
gets `None`; its item goes back to the queue with backoff.

`try_take` instead spends only an available token. A positive delay books nothing;
the caller must wait and try again with a fresh clock before making its provider call.
This keeps oversleep from turning future reservations into a burst of actual calls.

No clock is read here: both methods take `now`, so the rules are testable exactly and
the domain stays pure.
"""

from __future__ import annotations

from datetime import datetime, timedelta

__all__ = ["TokenBucket", "instance_rate"]


def instance_rate(provider_rate_per_second: float, max_concurrency: int, *, burst: int = 1) -> float:
    """Each instance's share of the provider's per-second limit (see the module docstring)."""
    if provider_rate_per_second <= 0:
        raise ValueError("provider_rate_per_second must be positive")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")
    if burst < 1:
        raise ValueError("burst must be at least 1")
    share = provider_rate_per_second / max_concurrency - burst
    if share <= 0:
        raise ValueError(
            f"a provider limit of {provider_rate_per_second}/s cannot be split across "
            f"{max_concurrency} instances with burst {burst}: each share would be {share:.3f}/s. "
            "Lower the mapping's maximum concurrency or the burst."
        )
    return share


class TokenBucket:
    def __init__(self, *, rate_per_second: float, burst: int = 1) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if burst < 1:
            raise ValueError("burst must be at least 1")
        self.rate_per_second = rate_per_second
        self.burst = burst
        self._tokens = float(burst)
        self._updated: datetime | None = None

    @property
    def tokens(self) -> float:
        """Tokens at the last refill/spend; negative while reservations are outstanding."""
        return self._tokens

    def reserve(self, now: datetime, max_wait: timedelta) -> timedelta | None:
        """Book one call. Returns how long to wait before making it, or `None` (and books
        nothing) when that wait would exceed `max_wait`."""
        self._refill(now)
        wait = max(1.0 - self._tokens, 0.0) / self.rate_per_second
        if wait > max_wait.total_seconds():
            return None
        self._tokens -= 1.0
        return timedelta(seconds=wait)

    def try_take(self, now: datetime) -> timedelta:
        """Spend one available token now, or return a delay without booking it.

        Zero means a token was actually spent. A positive deficit must never round
        down to zero at datetime's microsecond precision and masquerade as admission.
        """
        self._refill(now)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return timedelta(0)
        wait = (1.0 - self._tokens) / self.rate_per_second
        return max(timedelta(microseconds=1), timedelta(seconds=wait))

    def _refill(self, now: datetime) -> None:
        if self._updated is not None and now > self._updated:
            elapsed = (now - self._updated).total_seconds()
            self._tokens = min(float(self.burst), self._tokens + elapsed * self.rate_per_second)
        if self._updated is None or now > self._updated:
            self._updated = now
