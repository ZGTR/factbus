"""Counts written as CloudWatch Embedded Metric Format lines.

In Lambda, a JSON line on stdout in EMF becomes a CloudWatch metric with no API call,
which is what the C0b.3 alarms read (`OutboxRowsFailed` > 0 pages: R7-26). Locally the
same lines go to whatever stream the composition root passes, so a test reads them back.
"""

from __future__ import annotations

import json
import time
from typing import TextIO

__all__ = ["EmfMetrics"]


class EmfMetrics:
    def __init__(self, stream: TextIO, *, namespace: str, service: str) -> None:
        self._stream = stream
        self._namespace = namespace
        self._service = service

    def count(self, name: str, value: int = 1) -> None:
        record = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self._namespace,
                        "Dimensions": [["Service"]],
                        "Metrics": [{"Name": name, "Unit": "Count"}],
                    }
                ],
            },
            "Service": self._service,
            name: value,
        }
        self._stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._stream.flush()
