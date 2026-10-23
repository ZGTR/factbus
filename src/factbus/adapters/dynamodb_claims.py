"""`DynamoClaimStore`: the `ClaimStore` port on one DynamoDB table.

The table is the consumer's inbox table (the `consumer` IaC module creates it): string
`pk` HASH, string `sk` RANGE, TTL on `expires_at` (epoch seconds). A claim is the item
`pk = "claim#<key>"`, `sk = "claim"`, beside the inbox's `inbox#` and `version#` items, so
one table per consumer holds all three. TTL deletion runs up to days late, so `claim`
does not rely on it: its condition treats a row past `expires_at` as absent, and the
store behaves the same whether TTL has run or not.

Every write is a conditional write, which is the whole mechanism: `claim` succeeds only
when no live row exists, and `settle` and `release` only while the row is `pending`
under the caller's owner. A lost condition (`ConditionalCheckFailedException`) is a
`False`; every other SDK error is `ClaimStoreError`. Reads are strongly consistent, so
a claim written a moment ago by another invocation is always seen.

The client is injected. Its retry policy is harmless here: a re-sent conditional put
whose first response was lost finds its own row, and `factbus.once` recognises the owner.
The client the GUARDED FUNCTION uses is a different matter; see `factbus.once`.
"""

from __future__ import annotations

import math
import os
from datetime import UTC, datetime

import botocore.exceptions

from factbus.domain.claims import Claim, ClaimState
from factbus.ports import ClaimStoreError

__all__ = ["DynamoClaimStore"]

_CONDITION_FAILED = "ConditionalCheckFailedException"

#: `state` and `owner` are DynamoDB reserved words, so every expression names attributes
#: through placeholders.
_HELD = "#owner = :owner AND #state = :pending"

CLAIM_PREFIX = "claim#"
CLAIM_SORT_KEY = "claim"


def _key(key: str) -> dict[str, dict[str, str]]:
    return {"pk": {"S": CLAIM_PREFIX + key}, "sk": {"S": CLAIM_SORT_KEY}}


class DynamoClaimStore:
    def __init__(self, client, table_name: str) -> None:
        if not table_name:
            raise ValueError("table_name is required")
        self._client = client
        self._table = table_name

    @classmethod
    def from_env(cls, table_env: str = "FACTBUS_INBOX_TABLE") -> DynamoClaimStore:
        """Build on the inbox table named by `table_env`, in `AWS_REGION`, with
        `FACTBUS_ENDPOINT_URL` or `AWS_ENDPOINT_URL` set only locally (LocalStack)."""
        import boto3

        from factbus.adapters.dynamodb_inbox import DYNAMODB_CLIENT_CONFIG

        table_name = os.environ.get(table_env)
        if not table_name:
            raise ValueError(f"{table_env} is not set")
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region:
            raise ValueError("AWS_REGION is not set")
        client = boto3.session.Session().client(
            "dynamodb",
            region_name=region,
            endpoint_url=os.environ.get("FACTBUS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or None,
            config=DYNAMODB_CLIENT_CONFIG,
        )
        return cls(client, table_name)

    @property
    def table_name(self) -> str:
        return self._table

    def claim(self, claim: Claim, now: datetime) -> bool:
        item = {
            **_key(claim.key),
            "state": {"S": claim.state.value},
            "owner": {"S": claim.owner},
            "deadline": {"S": claim.deadline.astimezone(UTC).isoformat()},
            # Rounded up: a row is never retired before its retention has run.
            "expires_at": {"N": str(math.ceil(claim.expires.timestamp()))},
        }
        if claim.digest is not None:
            item["digest"] = {"S": claim.digest}
        if claim.result_json is not None:
            item["result"] = {"S": claim.result_json}
        return self._conditional(
            self._client.put_item,
            TableName=self._table,
            Item=item,
            ConditionExpression="attribute_not_exists(#pk) OR #expires < :now",
            ExpressionAttributeNames={"#pk": "pk", "#expires": "expires_at"},
            ExpressionAttributeValues={":now": {"N": repr(now.timestamp())}},
        )

    def read(self, key: str) -> Claim | None:
        try:
            response = self._client.get_item(TableName=self._table, Key=_key(key), ConsistentRead=True)
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as error:
            raise ClaimStoreError(str(error)) from error
        item = response.get("Item")
        if item is None:
            return None
        return Claim(
            key=item["pk"]["S"].removeprefix(CLAIM_PREFIX),
            state=ClaimState(item["state"]["S"]),
            owner=item["owner"]["S"],
            deadline=datetime.fromisoformat(item["deadline"]["S"]),
            digest=item["digest"]["S"] if "digest" in item else None,
            expires=datetime.fromtimestamp(int(item["expires_at"]["N"]), UTC),
            result_json=item["result"]["S"] if "result" in item else None,
        )

    def settle(self, key: str, owner: str, state: ClaimState, *, result_json: str | None = None) -> bool:
        if state is ClaimState.PENDING:
            raise ValueError("settle moves a claim out of pending, not into it")
        values = {":owner": {"S": owner}, ":pending": {"S": ClaimState.PENDING.value}, ":state": {"S": state.value}}
        update = "SET #state = :state"
        names = {"#state": "state", "#owner": "owner"}
        if result_json is not None:
            update += ", #result = :result"
            names["#result"] = "result"
            values[":result"] = {"S": result_json}
        return self._conditional(
            self._client.update_item,
            TableName=self._table,
            Key=_key(key),
            UpdateExpression=update,
            ConditionExpression=_HELD,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def release(self, key: str, owner: str) -> bool:
        return self._conditional(
            self._client.delete_item,
            TableName=self._table,
            Key=_key(key),
            ConditionExpression=_HELD,
            ExpressionAttributeNames={"#state": "state", "#owner": "owner"},
            ExpressionAttributeValues={":owner": {"S": owner}, ":pending": {"S": ClaimState.PENDING.value}},
        )

    @staticmethod
    def _conditional(call, **request) -> bool:
        try:
            call(**request)
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == _CONDITION_FAILED:
                return False
            raise ClaimStoreError(str(error)) from error
        except botocore.exceptions.BotoCoreError as error:
            raise ClaimStoreError(str(error)) from error
        return True
