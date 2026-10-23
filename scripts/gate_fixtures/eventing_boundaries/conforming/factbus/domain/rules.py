def event_keys(envelope):
    return envelope["event_id"], envelope["type"], envelope["producer"]


SPEC = "example.event/v1"
FIELDS = ("event_id", "type", "producer")


def breaker_is_open(opened_at, now):
    """The quiet side of clause 8: time arrives as a parameter, never read from a clock."""
    return (now - opened_at).total_seconds() < 60
