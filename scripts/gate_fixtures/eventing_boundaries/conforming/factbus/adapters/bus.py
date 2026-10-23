import boto3
from botocore.config import Config

from factbus.domain.rules import FIELDS

CONFIG = Config(retries={"total_max_attempts": 1})


class Bus:
    def __init__(self, client):
        self.client = client


def build_bus(region):
    return Bus(boto3.client("events", region_name=region, config=CONFIG))
