import uuid
import json
import pytest
from phebee.utils.aws import get_client


@pytest.mark.integration
def test_encounter(create_test_subject_iri, physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateEncounterFunction"]
    get_fn = physical_resources["GetEncounterFunction"]
    remove_fn = physical_resources["RemoveEncounterFunction"]

    test_subject_iri = create_test_subject_iri()

    print(f"test_subject_iri: {test_subject_iri}")

    encounter_id = f"test-{uuid.uuid4().hex[:8]}"
    payload = {"subject_iri": test_subject_iri, "encounter_id": encounter_id}

    # --- Create Encounter ---
    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])

    print(f"create_body: {create_body}")

    assert create_body["encounter_iri"].endswith(encounter_id)

    # --- Get Encounter ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    assert get_body["encounter_id"] == encounter_id
    assert get_body["subject_iri"] == test_subject_iri
    assert isinstance(get_body["properties"], dict)

    # --- Delete Encounter ---
    remove_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert remove_resp["StatusCode"] == 200
    remove_body = json.loads(json.loads(remove_resp["Payload"].read())["body"])
    assert remove_body["encounter_iri"] == get_body["encounter_iri"]

    # --- Get Encounter after deletion → should 404 ---
    get_after_delete_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert get_after_delete_resp["StatusCode"] == 200  # Still returns 200
    get_after_delete_body = json.loads(json.loads(get_after_delete_resp["Payload"].read())["body"])
    assert get_after_delete_body.get("message").startswith("Encounter not found")

    # --- Delete Encounter again (redundant) → should succeed (idempotent) ---
    remove_again_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert remove_again_resp["StatusCode"] == 200
    remove_again_body = json.loads(json.loads(remove_again_resp["Payload"].read())["body"])
    assert remove_again_body["encounter_iri"] == create_body["encounter_iri"]

    # --- Create with missing field → 400 ---
    bad_payload = {"encounter_id": encounter_id}  # Missing subject_iri
    create_bad_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(bad_payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_bad_resp["StatusCode"] == 200
    create_bad_body = json.loads(json.loads(create_bad_resp["Payload"].read())["body"])
    assert (
        create_bad_body["message"]
        == "Missing required fields: subject_iri and encounter_id"
    )
