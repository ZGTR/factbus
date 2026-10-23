from factbus.domain.rules import event_keys


def route(envelope):
    return event_keys(envelope)


def paced(items, wait, sleep):
    """The conforming shape: the wait is computed elsewhere and blocking is injected."""
    for item in items:
        sleep(wait)
        yield item
