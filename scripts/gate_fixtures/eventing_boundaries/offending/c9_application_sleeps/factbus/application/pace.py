import asyncio
import time


def paced(items, rate):
    for item in items:
        time.sleep(1 / rate)  # GATE_MUST_FLAG_THIS clause 9
        yield item


async def paced_async(items, rate):
    for item in items:
        await asyncio.sleep(1 / rate)  # GATE_MUST_FLAG_THIS clause 9
        yield item
