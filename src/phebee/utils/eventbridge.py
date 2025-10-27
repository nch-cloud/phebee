import boto3
import json
import os

# Initialize the boto3 client for EventBridge
eventbridge_client = boto3.client("events")

phebee_bus = os.environ["PheBeeBus"]

SUBJECT_CREATED = "subject_created"
SUBJECT_LINKED = "subject_linked"
SUBJECT_TERM_LINK_CREATED = "subject_term_link_created"
SUBJECT_TERM_EVIDENCE_ADDED = "subject_term_evidence_added"
BULK_IMPORT_SUCCESS = "bulk_import_success"
BULK_IMPORT_FAILURE = "bulk_import_failure"


def fire_event(detail_type: str, detail: dict):
    if detail_type not in [
        SUBJECT_CREATED,
        SUBJECT_LINKED,
        SUBJECT_TERM_LINK_CREATED,
        SUBJECT_TERM_EVIDENCE_ADDED,
        BULK_IMPORT_SUCCESS,
        BULK_IMPORT_FAILURE,
    ]:
        raise Exception(f"Unknown detail type passed: {detail_type}")

    event = {
        "Source": "PheBee",
        "DetailType": detail_type,
        "Detail": json.dumps(detail),
        "EventBusName": phebee_bus,
    }

    eventbridge_client.put_events(Entries=[event])
