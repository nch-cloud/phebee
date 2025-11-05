import os
import uuid
import time
import random
import logging
from typing import Dict, Iterable, List, Tuple, Optional

import boto3
from botocore.exceptions import ClientError

from phebee.constants import PHEBEE

logger = logging.getLogger(__name__)

# ---------------------------
# Globals / Clients / Config
# ---------------------------

TABLE_NAME = os.environ["PheBeeDynamoTable"]

# Use both resource (nice marshalling) and client (for transact/batch APIs)
_dy_resource = boto3.resource("dynamodb")
_dy_client = boto3.client("dynamodb")
_table = _dy_resource.Table(TABLE_NAME)


# ---------------------------
# Source version utilities
# ---------------------------

def get_source_records(source_name: str, dynamodb=None):
    """
    Returns all DynamoDB items for a given ontology/source name.
    NOTE: Uses the low-level client to keep the { "S": ... } wire format,
    since existing callers expect that structure.
    """
    client = dynamodb or _dy_client
    query_args = {
        "TableName": TABLE_NAME,
        "KeyConditionExpression": "PK = :pk_value",
        "ExpressionAttributeValues": {":pk_value": {"S": f"SOURCE~{source_name}"}},
    }
    resp = client.query(**query_args)
    return resp.get("Items", [])


def get_current_term_source_version(source_name: str, dynamodb=None):
    """
    Returns the most recent installed 'Version' for a given source, based on InstallTimestamp.
    Ignores records without InstallTimestamp.
    """
    source_records = get_source_records(source_name, dynamodb)

    sorted_records = sorted(
        (r for r in source_records if "InstallTimestamp" in r and "S" in r["InstallTimestamp"]),
        key=lambda x: x["InstallTimestamp"]["S"],
        reverse=True,
    )

    if not sorted_records:
        return None

    return sorted_records[0]["Version"]["S"]


def reset_dynamodb_table():
    """
    Deletes ALL items from the table (paged scan + batch_writer).
    For dev/test only. PITR is enabled in template for safety.
    """
    # Paginated scan + batch delete
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            scan = _table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            scan = _table.scan()

        items = scan.get("Items", [])
        if not items:
            break

        with _table.batch_writer() as batch:
            for item in items:
                # Build the key from table key schema (PK/SK)
                key = {k["AttributeName"]: item[k["AttributeName"]] for k in _table.key_schema}
                batch.delete_item(Key=key)

        last_evaluated_key = scan.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


# ---------------------------
# Subject resolution utilities
# ---------------------------

def psid_pk(project_id: str) -> str:
    return f"PSID#{project_id}"

def psid_sk(project_subject_id: str) -> str:
    return f"PSID#{project_subject_id}"

def subj_pk(subject_iri: str) -> str:
    return f"SUBJ#{subject_iri}"

def subj_sk(project_id: str, project_subject_id: str) -> str:
    return f"PSID#{project_id}#{project_subject_id}"

def _chunks(seq: List, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]

def _batch_get_forward(pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    """
    Read existing forward mappings PSID → subject_iri in batches (≤100 keys).
    Returns dict[(project_id, project_subject_id)] = subject_iri
    """
    if not pairs:
        return {}

    keys = [{"PK": {"S": psid_pk(p)}, "SK": {"S": psid_sk(s)}} for p, s in pairs]
    out: Dict[Tuple[str, str], str] = {}

    for batch in _chunks(keys, 100):
        req = {TABLE_NAME: {"Keys": batch}}
        resp = _dy_client.batch_get_item(RequestItems=req)

        items = resp.get("Responses", {}).get(TABLE_NAME, [])
        for it in items:
            pk = it["PK"]["S"]
            sk = it["SK"]["S"]
            proj = pk.split("#", 1)[1]
            psid = sk.split("#", 1)[1]
            subject_iri = it["subject_iri"]["S"]
            out[(proj, psid)] = subject_iri

        # Retry unprocessed keys with simple backoff
        unproc = resp.get("UnprocessedKeys", {}).get(TABLE_NAME, {}).get("Keys", [])
        backoff = 0.05
        while unproc:
            time.sleep(backoff)
            backoff = min(0.5, backoff * 2)
            resp = _dy_client.batch_get_item(RequestItems={TABLE_NAME: {"Keys": unproc}})
            items = resp.get("Responses", {}).get(TABLE_NAME, [])
            for it in items:
                pk = it["PK"]["S"]; sk = it["SK"]["S"]
                proj = pk.split("#", 1)[1]
                psid = sk.split("#", 1)[1]
                subject_iri = it["subject_iri"]["S"]
                out[(proj, psid)] = subject_iri
            unproc = resp.get("UnprocessedKeys", {}).get(TABLE_NAME, {}).get("Keys", [])

    return out

def _get_forward(project_id: str, project_subject_id: str) -> Optional[Dict]:
    """Single GetItem for forward mapping (used after losing a race)."""
    resp = _table.get_item(Key={"PK": psid_pk(project_id), "SK": psid_sk(project_subject_id)})
    return resp.get("Item")

def resolve_subjects(
    pairs: Iterable[Tuple[str, str]],
    *,
    in_memory_cache: Optional[Dict[Tuple[str, str], str]] = None,
) -> Dict[Tuple[str, str], str]:
    """
    Resolve (project_id, project_subject_id) → subject_iri.

    - Looks up existing mappings in DDB.
    - If missing, mints a new subject IRI once using a race-safe transaction:
        * PSID row:  PK=PSID#{project}, SK=PSID#{psid} → subject_iri
        * Reverse row: PK=SUBJ#{subject_iri}, SK=PSID#{project}#{psid}
    - Supports many PSIDs per subject by ensuring we only create a new subject
      when the (project_id, project_subject_id) pair has never been seen before.

    Returns:
        dict[(project_id, project_subject_id)] = subject_iri
    """
    unique_pairs = sorted(set(pairs))  # stable order & de-dup
    if not unique_pairs:
        return {}

    # Seed from in-memory cache
    result: Dict[Tuple[str, str], str] = {}
    if in_memory_cache:
        for k in list(unique_pairs):
            if k in in_memory_cache:
                result[k] = in_memory_cache[k]

    # Read existing rows for the rest
    to_check = [p for p in unique_pairs if p not in result]
    existing = _batch_get_forward(to_check)
    result.update(existing)

    # Mint for misses, race-safe
    misses = [p for p in to_check if p not in existing]
    for proj, psid in misses:
        subject_iri = f"{PHEBEE}/subjects/{uuid.uuid4()}"
        try:
            _dy_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": TABLE_NAME,
                            "Item": {
                                "PK": {"S": psid_pk(proj)},
                                "SK": {"S": psid_sk(psid)},
                                "entity": {"S": "PSID"},
                                "project_id": {"S": proj},
                                "project_subject_id": {"S": psid},
                                "subject_iri": {"S": subject_iri},
                            },
                            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": TABLE_NAME,
                            "Item": {
                                "PK": {"S": subj_pk(subject_iri)},
                                "SK": {"S": subj_sk(proj, psid)},
                                "entity": {"S": "SUBJECT_PSID"},
                                "project_id": {"S": proj},
                                "project_subject_id": {"S": psid},
                            },
                            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                        }
                    },
                ]
            )
            # We won the race
            result[(proj, psid)] = subject_iri
            if in_memory_cache is not None:
                in_memory_cache[(proj, psid)] = subject_iri

        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                # Someone else won, read the winner
                time.sleep(0.02 + random.random() * 0.03)
                winner = _get_forward(proj, psid)
                if not winner:
                    time.sleep(0.05)
                    winner = _get_forward(proj, psid)
                if not winner:
                    raise
                subject_iri = winner["subject_iri"]
                result[(proj, psid)] = subject_iri
                if in_memory_cache is not None:
                    in_memory_cache[(proj, psid)] = subject_iri
            else:
                raise

    return result


__all__ = [
    # source utilities
    "get_source_records",
    "get_current_term_source_version",
    "reset_dynamodb_table",
    # subject resolution
    "resolve_subjects",
]