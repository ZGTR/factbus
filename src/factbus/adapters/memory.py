"""`MemoryClaimStore`: the `ClaimStore` port in process memory.

For tests, local runs, and a single process whose side effects need guarding only
while it lives. It is NOT a guard across Lambda invocations or hosts: each process has
its own dictionary. One lock serialises every operation, which gives the same
conditional-write semantics as `DynamoClaimStore`.
"""

from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime

from factbus.domain.claims import Claim, ClaimState

__all__ = ["MemoryClaimStore"]


class MemoryClaimStore:
    def __init__(self) -> None:
        self._rows: dict[str, Claim] = {}
        self._lock = threading.Lock()

    def claim(self, claim: Claim, now: datetime) -> bool:
        with self._lock:
            existing = self._rows.get(claim.key)
            if existing is not None and not existing.expired(now):
                return False
            self._rows[claim.key] = claim
            return True

    def read(self, key: str) -> Claim | None:
        with self._lock:
            return self._rows.get(key)

    def settle(self, key: str, owner: str, state: ClaimState, *, result_json: str | None = None) -> bool:
        if state is ClaimState.PENDING:
            raise ValueError("settle moves a claim out of pending, not into it")
        with self._lock:
            row = self._held(key, owner)
            if row is None:
                return False
            self._rows[key] = replace(row, state=state, result_json=result_json)
            return True

    def release(self, key: str, owner: str) -> bool:
        with self._lock:
            if self._held(key, owner) is None:
                return False
            del self._rows[key]
            return True

    def _held(self, key: str, owner: str) -> Claim | None:
        row = self._rows.get(key)
        if row is None or row.owner != owner or row.state is not ClaimState.PENDING:
            return None
        return row
