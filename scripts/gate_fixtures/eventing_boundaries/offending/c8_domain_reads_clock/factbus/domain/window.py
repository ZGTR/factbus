import time
from datetime import datetime


def breaker_is_open(opened_at):
    return time.monotonic() - opened_at < 60  # GATE_MUST_FLAG_THIS clause 8


def stamped():
    return datetime.now()  # GATE_MUST_FLAG_THIS clause 8
