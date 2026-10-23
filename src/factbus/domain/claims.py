"""The record `factbus.once` writes before a side effect runs, and the states it moves through.

A claim row exists BEFORE the side effect is attempted, so a crash at any point after
the attempt starts leaves evidence behind. The row moves forward only: `pending` to
`done` or `unknown`, or `pending` deleted when the attempt is known not to have
happened. Nothing moves a row out of `done` or `unknown`; that is what makes a second
attempt impossible rather than merely unlikely.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime

__all__ = ["Claim", "ClaimState"]


class ClaimState(enum.Enum):
    #: An attempt holds the key. Until `deadline` it may still be running; after it,
    #: the holder is presumed dead and the outcome is unknowable.
    PENDING = "pending"
    #: The side effect returned. `result_json` holds what it returned.
    DONE = "done"
    #: The side effect may or may not have happened (it raised, or its holder died).
    #: Final: retrying could repeat it, so nobody does.
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Claim:
    key: str
    state: ClaimState
    #: The attempt that wrote the row. `settle` and `release` match on it, so a late
    #: writer cannot overwrite an outcome another attempt recorded.
    owner: str
    #: After this instant a `pending` row is stale: its holder is presumed dead.
    deadline: datetime
    #: A digest of the call's content. The same key coming back with a different digest
    #: is a caller bug (one key, two intents) and is refused rather than deduplicated.
    digest: str | None
    #: After this instant the row no longer guards anything and may be reclaimed. It
    #: must outlive every replay of the triggering message (see `factbus.once`).
    expires: datetime
    result_json: str | None = None

    def expired(self, now: datetime) -> bool:
        return self.expires < now

    def stale(self, now: datetime) -> bool:
        return self.state is ClaimState.PENDING and now >= self.deadline
