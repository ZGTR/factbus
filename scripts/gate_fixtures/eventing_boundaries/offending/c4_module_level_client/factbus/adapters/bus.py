import boto3
from botocore.config import Config

client = boto3.client("events", config=Config(retries={"total_max_attempts": 1}))  # GATE_MUST_FLAG_THIS clause 4
