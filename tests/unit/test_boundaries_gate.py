"""The architecture gate runs green on the real tree and every clause is proved by blinding."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_GATE = Path(__file__).resolve().parents[2] / "scripts" / "check_eventing_boundaries.py"
_spec = importlib.util.spec_from_file_location("check_eventing_boundaries", _GATE)
gate = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = gate  # dataclasses resolve their module through sys.modules
_spec.loader.exec_module(gate)


def test_gate_is_green_on_the_real_tree():
    assert gate.main([]) == 0


def test_every_clause_goes_green_when_blinded():
    assert gate.blinding_table() == 0


def test_k1_real_imports_include_no_contract_package():
    _, stats = gate.analyse(gate.SOURCE)
    assert stats.import_roots["factbus"] >= gate.MIN_INTERNAL_IMPORTS
    assert not any(gate._is_contract_or_product(root) for root in stats.import_roots)
