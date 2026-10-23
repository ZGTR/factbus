"""`once`: run a side effect at most once per key, even across crashes and redeliveries.

WHY NOT AN IDEMPOTENCY CACHE. SQS hands the same message to two invocations, and a
Lambda can die after the provider accepted the request and before anything was
recorded. A cache that records "in progress" and lets the record expire (Powertools
idempotency) runs the function again after a crash, which repeats the side effect.
`once` closes that gap by treating an unknowable outcome as final:

1. Claim BEFORE the call: a conditional insert of a `pending` row with an owner and a
   deadline. Only one attempt can hold a key.
2. Call once. A function that knows nothing happened raises `CertainlyNotSent`; the
   claim is released and the caller retries. Any other exception means the effect MAY
   have happened: the row becomes `unknown` and nothing ever calls again.
3. Settle `done` with the JSON result, which every later call returns without calling.

A crash between the claim and the settle leaves `pending`. A later call that finds it
past its deadline marks it `unknown` and does not call. So the guarantee is narrow and
exact: at most one attempt that might have taken effect, per key, for as long as the
row is kept. It does not promise the effect happens; a crash can lose it, and the
`unknown` row is where an operator sees that.

`settle` and `release` are conditional on the owner still holding a `pending` row, so a
holder that outlived its deadline cannot overwrite the `unknown` another attempt wrote.

RETENTION MUST OUTLIVE EVERY REPLAY. Once a row expires, the key is free again and a
replayed message would run the effect a second time. `retention` must exceed the
source queue's message retention PLUS its DLQ's retention (a redrive replays from the
DLQ), plus any manual replay window. SQS allows up to 14 days per queue, so a queue and
DLQ both at the maximum need more than 28 days, and an archive replay needs the archive's
window; the 35-day default covers both at the IaC modules' defaults and caps. Size `claim_seconds` above the function's worst-case run time and below the
queue's redelivery window, so a redelivery can find a stale claim and resolve it.

The function itself should make exactly one attempt (an SDK client built with
`total_max_attempts=1`): a retry inside the call is a repeat `once` cannot see.
"""

from __future__ import annotations

import functools
import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, ParamSpec, TypeVar

from factbus.domain.claims import Claim, ClaimState
from factbus.ports import ClaimStore, Clock

__all__ = ["CertainlyNotSent", "KeyReused", "OnceError", "StillPending", "Unknown", "once"]

P = ParamSpec("P")
R = TypeVar("R")


class OnceError(Exception):
    """Base for the outcomes `once` raises instead of calling the function."""

    def __init__(self, key: str, message: str) -> None:
        super().__init__(f"{key}: {message}")
        self.key = key


class Unknown(OnceError):
    """The side effect may or may not have happened, and it will not be attempted again.

    Final for the key. Retrying the message changes nothing; an operator decides."""


class StillPending(OnceError):
    """Another attempt holds the key and may still be running. Retry later: the next
    look finds it settled, released, or stale (which then resolves to `Unknown`)."""


class KeyReused(OnceError):
    """The key was claimed for different content. One key, two intents: a caller bug,
    refused so the second intent is not silently swallowed as a duplicate."""


class CertainlyNotSent(Exception):
    """Raised BY the guarded function when it knows the side effect did not happen (a
    connect timeout, a throttle answered before acceptance). `once` releases the claim
    and re-raises, so the caller's retry makes a fresh attempt. Raise it only when sure:
    a wrong `CertainlyNotSent` turns into a duplicate."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def once(
    key: str | Callable[..., str],
    store: ClaimStore,
    *,
    claim_seconds: float = 300,
    retention: timedelta = timedelta(days=35),
    digest: str | Callable[..., str] | None = None,
    clock: Clock | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Guard a side effect. `key` and `digest` are strings or callables that receive the
    guarded function's arguments. The return value must be JSON-serialisable: it is what
    every later call with the same key gets back. A duplicate returns the decoded JSON,
    so a tuple comes back as a list."""
    if claim_seconds <= 0:
        raise ValueError("claim_seconds must be positive")
    if retention <= timedelta(seconds=claim_seconds):
        raise ValueError("retention must exceed claim_seconds (and every replay window)")
    now = clock or _utc_now

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
            k = key(*args, **kwargs) if callable(key) else key
            d = digest(*args, **kwargs) if callable(digest) else digest
            owner = uuid.uuid4().hex
            at = now()
            mine = Claim(
                key=k,
                state=ClaimState.PENDING,
                owner=owner,
                deadline=at + timedelta(seconds=claim_seconds),
                digest=d,
                expires=at + retention,
            )
            if not store.claim(mine, at):
                existing = store.read(k)
                # A conditional put the SDK re-sent after a lost response finds our own row.
                if existing is None or existing.owner != owner:
                    return _resolve_existing(store, k, d, existing, at)
            return _call(store, k, owner, fn, args, kwargs)

        return guarded

    return decorate


def _resolve_existing(store: ClaimStore, key: str, digest: str | None, row: Claim | None, now: datetime) -> Any:
    if row is None:
        raise StillPending(key, "the claim was released meanwhile; retry")
    if row.digest != digest:
        raise KeyReused(key, "claimed earlier for different content")
    if row.state is ClaimState.DONE:
        return None if row.result_json is None else json.loads(row.result_json)
    if row.state is ClaimState.UNKNOWN:
        raise Unknown(key, "an earlier attempt may have taken effect; not repeating it")
    if not row.stale(now):
        raise StillPending(key, f"held by another attempt until {row.deadline.isoformat()}")
    if store.settle(key, row.owner, ClaimState.UNKNOWN):
        raise Unknown(key, "the holder passed its deadline without settling; not repeating it")
    # The holder settled or released between our read and our write: look again later.
    raise StillPending(key, "the stale claim changed while being resolved; retry")


def _call(store: ClaimStore, key: str, owner: str, fn: Callable[..., R], args: tuple, kwargs: dict) -> R:
    try:
        result = fn(*args, **kwargs)
    except CertainlyNotSent:
        store.release(key, owner)
        raise
    except Exception as error:
        store.settle(key, owner, ClaimState.UNKNOWN)
        raise Unknown(key, f"the call raised {type(error).__name__}; it may have taken effect") from error
    try:
        result_json = json.dumps(result)
    except (TypeError, ValueError) as error:
        # The effect happened but cannot be recorded as returned. `unknown` keeps it from
        # repeating; the error names the bug.
        store.settle(key, owner, ClaimState.UNKNOWN)
        raise Unknown(key, "the call returned a value that is not JSON-serialisable") from error
    # A False here means the claim went stale and another attempt marked it `unknown`.
    # The effect still happened, so the caller gets its result; the row stays `unknown`.
    store.settle(key, owner, ClaimState.DONE, result_json=result_json)
    return result
