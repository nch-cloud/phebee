import json
import uuid
import time
import pytest
import requests
from phebee.utils.aws import get_client
from general_utils import parse_iso8601

@pytest.fixture
def test_payload(test_project_id):
    subject_id_1 = f"subj-{uuid.uuid4()}"
    subject_id_2 = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id_1 = {uuid.uuid4()}
    encounter_id_2 = {uuid.uuid4()}

    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_1,
            "term_iri": term_iri_1,
            "creator_id": "test-creator",
            "creator_type": "human",
            "creator_name": "Test Creator",
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_1,
                    "clinical_note_id": "note-001",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "creator_id": "robot-creator",
                    "creator_type": "automated",
                    "creator_name": "Robot Creator",
                    "creator_version": "1.2"
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_1,
            "term_iri": term_iri_2,
            "creator_id": "test-creator",
            "creator_type": "human",
            "creator_name": "Test Creator",
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_1,
                    "clinical_note_id": "note-002",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "creator_id": "robot-creator",
                    "creator_type": "automated",
                    "creator_name": "Robot Creator",
                    "creator_version": "1.2"
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_2,
            "term_iri": term_iri_2,
            "creator_id": "test-creator",
            "creator_type": "human",
            "creator_name": "Test Creator",
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_2,
                    "clinical_note_id": "note-003",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "creator_id": "robot-creator",
                    "creator_type": "automated",
                    "creator_name": "Robot Creator",
                    "creator_version": "1.2"
                }
            ]
        }        
    ]

def verify_creator_metadata(creator_iri, expected_id, expected_type, expected_name, expected_version, physical_resources):
    get_creator_fn = physical_resources["GetCreatorFunction"]
    response = invoke_lambda(get_creator_fn, {"creator_id": creator_iri.rsplit("/", 1)[-1]})
    print(f"Creator {creator_iri} metadata: {response}")

    assert response["creator_iri"] == creator_iri
    assert response["creator_id"] == expected_id
    assert response.get("has_version") == expected_version
    
    if expected_type == "human":
        expected_type = "HumanCreator"
    else:
        expected_type = "AutomatedCreator"

    assert response["type"] == f"http://ods.nationwidechildrens.org/phebee#{expected_type}"
    assert response["title"] == expected_name

def invoke_lambda(name, payload):
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=name,
        Payload=json.dumps(payload).encode()
    )
    payload = json.loads(response["Payload"].read())
    return json.loads(payload["body"]) if isinstance(payload, dict) and "body" in payload else payload

def wait_for_loader_success(load_id, physical_resources, timeout_seconds=300):
    print(f"Waiting for load success for job {load_id}")
    start = time.time()
    while True:
        result = invoke_lambda(physical_resources["GetLoadStatusFunction"], { "load_job_id": load_id })
        print(result)
        status = result.get("payload").get("overallStatus").get("status").lower()
        print(f"Loader status: {status}")

        if status in {"load_completed", "load_completed_with_errors"}:
            return result

        if status in {"load_failed", "cancelled", "error"}:
            raise RuntimeError(f"Neptune bulk load failed: {status}")

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Neptune bulk loader did not finish in {timeout_seconds} seconds")

        time.sleep(10)

def test_perform_bulk_upload_flow(test_payload, test_project_id, physical_resources):
    # Step 1: Call prepare_bulk_upload Lambda to get signed URL and S3 key
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {})
    upload_url = prep_response["upload_url"]
    s3_key = prep_response["s3_key"]

    print(f"upload_url: {upload_url}")
    print(f"s3_key: {s3_key}")

    # Step 2: Upload JSON to the signed S3 URL
    headers = {"Content-Type": "application/json"}
    put_resp = requests.put(upload_url, data=json.dumps(test_payload), headers=headers)
    assert put_resp.status_code == 200

    # Step 3: Call perform_bulk_upload Lambda
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    response = invoke_lambda(perform_fn, {"s3_key": s3_key})
    print(response)
    assert response["message"] == "Bulk load started"
    assert "load_id" in response

    load_id = response["load_id"]
    print(f"load_id: {load_id}")

    # Step 3.5 Wait for the Neptune loader to finish
    wait_for_loader_success(load_id, physical_resources)

    # Step 4: Resolve subjects and validate TermLinks and Evidence
    get_subject_fn = physical_resources["GetSubjectFunction"]

    for payload in test_payload:
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
        subject_query = { "project_subject_iri": project_subject_iri }

        subject_result = invoke_lambda(get_subject_fn, subject_query)
        print(subject_result)

        subject_iri = subject_result["subject_iri"]
        assert subject_iri.startswith("http://ods.nationwidechildrens.org/phebee/subjects/")
        assert "terms" in subject_result

        # Verify the expected term exists
        matching_terms = [term for term in subject_result["terms"] if term["term_iri"] == payload["term_iri"]]
        assert matching_terms, f"No TermLink found for {payload['term_iri']}"
        assert len(matching_terms) == 1
        termlink = matching_terms[0]

        # TermLink creator check
        creator_iri = termlink["creator"]
        assert creator_iri.endswith(payload["creator_id"])
        verify_creator_metadata(
            creator_iri=creator_iri,
            expected_id=payload["creator_id"],
            expected_type=payload["creator_type"],
            expected_name=payload["creator_name"],
            expected_version=payload.get("creator_version"),
            physical_resources=physical_resources
        )

        # Evidence checks
        expected_evidences = payload["evidence"]
        assert len(termlink["evidence"]) == len(expected_evidences)

        for expected_evidence in expected_evidences:
            ev = next((e for e in termlink["evidence"]
                    if e["properties"]["http://ods.nationwidechildrens.org/phebee#clinicalNoteId"] == expected_evidence["clinical_note_id"]), None)
            assert ev is not None, f"Evidence not found for {expected_evidence['clinical_note_id']}"

            print(f"ev: {ev}")

            props = ev["properties"]
            assert parse_iso8601(props["http://ods.nationwidechildrens.org/phebee#noteTimestamp"]) == parse_iso8601(expected_evidence["note_timestamp"])
            assert props["http://ods.nationwidechildrens.org/phebee#hasEncounter"] == expected_evidence["encounter_iri"]

            # Evidence-level creator
            if "creator_id" in expected_evidence:
                ev_creator_iri = ev["creator"]
                
                assert ev_creator_iri.endswith(expected_evidence["creator_id"])
                verify_creator_metadata(
                    creator_iri=ev_creator_iri,
                    expected_id=expected_evidence["creator_id"],
                    expected_type=expected_evidence["creator_type"],
                    expected_name=expected_evidence["creator_name"],
                    expected_version=expected_evidence.get("creator_version"),
                    physical_resources=physical_resources
                )