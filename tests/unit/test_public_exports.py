"""The package's lazy public names: each resolves to the object it names, whichever is
asked for first. Each case runs in a fresh interpreter, since import order is the bug."""

from __future__ import annotations

import subprocess
import sys

import pytest

import factbus


def resolve_in_fresh_interpreter(first: str, then: str) -> str:
    script = f"import factbus; factbus.{first}; print(type(factbus.{then}).__name__)"
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    return result.stdout.strip()


@pytest.mark.parametrize("first", ["Unknown", "StillPending", "KeyReused", "CertainlyNotSent", "once"])
def test_once_is_the_function_after_any_sibling_export(first):
    assert resolve_in_fresh_interpreter(first, "once") == "function"


def test_consumer_is_the_decorator_factory():
    assert resolve_in_fresh_interpreter("Event", "consumer") == "function"


def test_every_export_resolves():
    for name in factbus.__all__:
        assert getattr(factbus, name) is not None
