import uuid
import json
import pytest
from phebee.utils.aws import get_client


@pytest.mark.integration
def test_clinical_note(create_test_encounter_iri, physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateClinicalNoteFunction"]
    get_fn = physical_resources["GetClinicalNoteFunction"]
    remove_fn = physical_resources["RemoveClinicalNoteFunction"]

    encounter_iri = create_test_encounter_iri()
    clinical_note_id = f"note-{uuid.uuid4().hex[:8]}"
    payload = {
        "encounter_iri": encounter_iri,
        "clinical_note_id": clinical_note_id,
        "note_timestamp": "2023-01-01T10:00:00Z"
    }

    # --- Create ---
    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])

    print(f"create_body: {create_body}")

    assert create_body["clinical_note_iri"].endswith(clinical_note_id)

    # --- Get ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(f"get_body: {get_body}")
    assert get_body["clinical_note_id"] == clinical_note_id
    assert get_body["encounter_iri"] == encounter_iri

    # --- Delete ---
    delete_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert delete_resp["StatusCode"] == 200
    delete_body = json.loads(json.loads(delete_resp["Payload"].read())["body"])
    assert delete_body["clinical_note_iri"] == get_body["clinical_note_iri"]

    # --- Get after delete → should return not found message ---
    get_again_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert get_again_resp["StatusCode"] == 200
    get_again_body = json.loads(json.loads(get_again_resp["Payload"].read())["body"])
    assert get_again_body.get("message") == "ClinicalNote not found"

    # --- Bad input (missing clinical_note_id) ---
    bad_payload = {"encounter_iri": encounter_iri}
    bad_create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(bad_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert bad_create_resp["StatusCode"] == 200
    bad_create_body = json.loads(json.loads(bad_create_resp["Payload"].read())["body"])
    assert "Missing required fields" in bad_create_body["message"]