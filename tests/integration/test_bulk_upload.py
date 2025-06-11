import json
import uuid
import time
import pytest
import requests
from phebee.utils.aws import get_client

@pytest.fixture
def test_payload(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_iri = f"http://example.org/encounter/{uuid.uuid4()}"
    note_id = "note-001"

    return {
        "project_id": test_project_id,
        "project_subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": "test-creator",
        "evidence": [
            {
                "type": "clinical_note",
                "encounter_iri": encounter_iri,
                "clinical_note_id": note_id,
                "note_timestamp": "2025-06-06T12:00:00Z"
            }
        ]
    }

def invoke_lambda(name, payload):
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=name,
        Payload=json.dumps(payload).encode()
    )
    payload = json.loads(response["Payload"].read())
    return json.loads(payload["body"]) if isinstance(payload, dict) and "body" in payload else payload

def wait_for_loader_success(job_id, physical_resources, timeout_seconds=300):
    start = time.time()
    while True:
        result = invoke_lambda(physical_resources["GetLoadStatusFunction"], { "load_id": job_id })
        status = result.get("status", "").lower()
        print(f"Loader status: {status}")

        if status in {"load_completed", "load_completed_with_errors"}:
            return result

        if status in {"load_failed", "cancelled", "error"}:
            raise RuntimeError(f"Neptune bulk load failed: {status}")

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Neptune bulk loader did not finish in {timeout_seconds} seconds")

        time.sleep(1000)

def test_perform_bulk_upload_flow(test_payload, test_project_id, physical_resources, update_hpo):
    # Step 1: Call prepare_bulk_upload Lambda to get signed URL and S3 key
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {})
    upload_url = prep_response["upload_url"]
    s3_key = prep_response["s3_key"]

    print(f"upload_url: {upload_url}")
    print(f"s3_key: {s3_key}")

    # Step 2: Upload JSON to the signed S3 URL
    headers = {"Content-Type": "application/json"}
    put_resp = requests.put(upload_url, data=json.dumps([test_payload]), headers=headers)
    assert put_resp.status_code == 200

    # Step 3: Call perform_bulk_upload Lambda
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    response = invoke_lambda(perform_fn, {"s3_key": s3_key})
    assert response["message"] == "Bulk load started"
    assert "load_id" in response

    # Step 4: Resolve subject via get_subject Lambda
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_query = {
        "project_id": test_payload["project_id"],
        "project_subject_id": test_payload["project_subject_id"]
    }
    subject_result = invoke_lambda(get_subject_fn, subject_query)
    print(subject_result)
    subject_iri = subject_result["subject_iri"]
    assert subject_iri.startswith("http://ods.nationwidechildrens.org/phebee/subjects/")

    # Step 5: Get TermLinks for subject
    get_termlink_fn = physical_resources["GetTermLinkFunction"]
    termlink_result = invoke_lambda(get_termlink_fn, {
        "source_node_iri": subject_iri
    })
    termlinks = termlink_result.get("term_links", [])
    matching_links = [tl for tl in termlinks if tl["term_iri"] == test_payload["term_iri"]]
    assert matching_links, "Expected TermLink not found"

    # Step 6: Validate ClinicalNote evidence
    evidence = matching_links[0]["evidence"][0]
    clinical_note_iri = evidence["evidence_iri"]
    get_clinical_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_clinical_note_fn, {
        "clinical_note_iri": clinical_note_iri
    })
    assert note_result["clinical_note_id"] == test_payload["evidence"][0]["clinical_note_id"]
    assert note_result["note_timestamp"] == test_payload["evidence"][0]["note_timestamp"]
