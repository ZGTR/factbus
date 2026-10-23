"""Whether an event is newer than what a consumer has already applied (K3, design §2).

Every subject carries a version. A consumer applies an event only when its version is
above the applied one, and a status only ever moves forward by rank, so `read` arriving
before `delivered` leaves `read` in place.

The kit does not know which field is the version. One catalogue versions a
subject by `authority_epoch`, another by `subject.version` (E-3).
Each service builds `version_of` from its own catalogue and injects it; the kit imports
no catalogue (K-1). `version_of` returns None for an event that carries no ordered
subject, and such an event is guarded by the inbox alone.

The status rank is the service's too: a mapping from status to rank, injected.

Pure: no clock, no SDK, no database. `adapters.sql_inbox` applies `is_newer` and then
writes conditionally (`WHERE version < :v`), so a concurrent newer write still wins.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

__all__ = ["SubjectVersion", "VersionOf", "is_newer", "status_advances"]


@dataclass(frozen=True)
class SubjectVersion:
    subject_key: str
    version: int

    def __post_init__(self) -> None:
        if not self.subject_key:
            raise ValueError("subject_key must be non-empty")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 0:
            raise ValueError(f"version must be a non-negative int, got {self.version!r}")


#: Built by each service from its catalogue; `None` means "no ordered subject".
VersionOf = Callable[[Mapping[str, object]], SubjectVersion | None]


def is_newer(applied: int | None, incoming: int) -> bool:
    """True when `incoming` must be applied. At or below the applied version is dropped."""
    return applied is None or incoming > applied


def status_advances(current: str | None, incoming: str, rank: Mapping[str, int]) -> bool:
    """True when moving from `current` to `incoming` goes strictly forward.

    An unranked status is a programming error in the service's catalogue, not a data
    condition, so it raises rather than guessing a direction.
    """
    if incoming not in rank:
        raise KeyError(f"status {incoming!r} has no rank")
    if current is None:
        return True
    if current not in rank:
        raise KeyError(f"status {current!r} has no rank")
    return rank[incoming] > rank[current]
