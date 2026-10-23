import boto3


def build(region):
    return boto3.client("events", region_name=region)  # GATE_MUST_FLAG_THIS clause 2
