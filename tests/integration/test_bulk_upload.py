import json
import uuid
import time
import pytest
import requests
from urllib.parse import quote
from phebee.utils.aws import get_client
from phebee.utils.sparql import generate_termlink_hash
from general_utils import parse_iso8601

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

def perform_bulk_upload(payload, physical_resources):
    """Helper function to perform a bulk upload and return the load_id"""
    # Step 1: Call prepare_bulk_upload Lambda to get signed URL and S3 key
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {})
    upload_url = prep_response["upload_url"]
    s3_key = prep_response["s3_key"]

    print(f"upload_url: {upload_url}")
    print(f"s3_key: {s3_key}")

    # Step 2: Upload JSON to the signed S3 URL
    headers = {"Content-Type": "application/json"}
    put_resp = requests.put(upload_url, data=json.dumps(payload), headers=headers)
    assert put_resp.status_code == 200

    # Step 3: Call perform_bulk_upload Lambda
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    response = invoke_lambda(perform_fn, {"s3_key": s3_key})
    print(response)
    assert response["message"] == "Bulk load started"
    assert "load_id" in response

    load_id = response["load_id"]
    print(f"load_id: {load_id}")
    
    # Wait for the Neptune loader to finish
    wait_for_loader_success(load_id, physical_resources)
    
    return load_id
@pytest.fixture
def test_payload(test_project_id):
    subject_id_1 = f"subj-{uuid.uuid4()}"
    subject_id_2 = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id_1 = str(uuid.uuid4())
    encounter_id_2 = str(uuid.uuid4())

    # Basic test entries
    basic_entries = [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_1,
            "term_iri": term_iri_1,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_1,
                    "clinical_note_id": "note-001",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_1,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_1,
                    "clinical_note_id": "note-002",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id_2,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id_2,
                    "clinical_note_id": "note-003",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        }        
    ]
    
    return basic_entries

@pytest.fixture
def test_payload_with_qualifiers(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    return [
        # Entry with explicit qualifying terms
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-qual-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "qualifying_terms": ["negated", "hypothetical"]
                }
            ]
        },
        # Entry with context flags
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-qual-2",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "contexts": {"negated": 1, "family": 1, "hypothetical": 0}
                }
            ]
        }
    ]

@pytest.fixture
def test_payload_with_spans(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-span-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "span_start": 120,
                    "span_end": 145
                }
            ]
        }
    ]

@pytest.fixture
def test_payload_with_new_fields(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-fields-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "note_type": "Progress Note",
                    "author_prov_type": "Physician",
                    "author_specialty": "Neurology"
                }
            ]
        }
    ]

@pytest.fixture
def test_duplicate_payload(test_project_id):
    """Fixture for testing duplicate detection"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())
    
    # Create identical entries to test deduplication
    entry = {
        "project_id": test_project_id,
        "project_subject_id": subject_id,
        "term_iri": term_iri,
        "evidence": [
            {
                "type": "clinical_note",
                "encounter_id": encounter_id,
                "clinical_note_id": "note-dup-1",
                "note_timestamp": "2025-06-06T12:00:00Z",
                "evidence_creator_id": "robot-creator",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Robot Creator",
                "evidence_creator_version": "1.2"
            }
        ]
    }
    
    return [entry, entry]  # Return the same entry twice

@pytest.fixture
def test_duplicate_encounters_payload(test_project_id):
    """Fixture for testing encounter duplicate detection"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id = str(uuid.uuid4())  # Same encounter ID used for both entries
    
    return [
        # First entry with encounter
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_1,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-enc-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        },
        # Second entry with same encounter
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-enc-2",
                    "note_timestamp": "2025-06-06T14:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        }
    ]

@pytest.fixture
def test_duplicate_encounters_in_batch_payload(test_project_id):
    """Fixture for testing encounter duplicate detection within the same batch"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())  # Same encounter ID used for multiple notes
    
    return [
        # Single entry with multiple notes using the same encounter
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-2",
                    "note_timestamp": "2025-06-06T14:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-3",
                    "note_timestamp": "2025-06-06T16:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        }
    ]

@pytest.fixture
def test_duplicate_notes_in_batch_payload(test_project_id):
    """Fixture for testing clinical note duplicate detection within the same batch"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id = str(uuid.uuid4())
    clinical_note_id = f"note-{uuid.uuid4()}"  # Same clinical note ID used for multiple term links
    
    return [
        # First entry with clinical note
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_1,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,  # Same clinical note ID
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "author_specialty": "Neurology"
                }
            ]
        },
        # Second entry with same clinical note
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,  # Same clinical note ID
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "author_specialty": "Neurology"
                }
            ]
        }
    ]
def test_perform_bulk_upload_flow(test_payload, test_project_id, physical_resources):
    """Test the basic bulk upload flow with standard entries"""
    load_id = perform_bulk_upload(test_payload, physical_resources)

    # Resolve subjects and validate TermLinks and Evidence
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

        # Evidence checks
        expected_evidences = payload["evidence"]
        assert len(termlink["evidence"]) == len(expected_evidences)

        for expected_evidence in expected_evidences:
            # Find the evidence by clinical note ID
            evidence_found = False
            for ev in termlink["evidence"]:
                # Check if this is a TextAnnotation with a textSource that's a ClinicalNote
                if "properties" in ev and "http://ods.nationwidechildrens.org/phebee#textSource" in ev["properties"]:
                    text_source = ev["properties"]["http://ods.nationwidechildrens.org/phebee#textSource"]
                    
                    # Get the clinical note details
                    get_note_fn = physical_resources["GetClinicalNoteFunction"]
                    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": text_source})
                    
                    if note_result["clinical_note_id"] == expected_evidence["clinical_note_id"]:
                        evidence_found = True
                        
                        # Verify note timestamp
                        assert parse_iso8601(note_result["note_timestamp"]) == parse_iso8601(expected_evidence["note_timestamp"])
                        
                        # Verify encounter
                        encounter_iri = note_result["encounter_iri"]
                        assert f"/encounter/{expected_evidence['encounter_id']}" in encounter_iri
                        
                        # Verify creator
                        creator_iri = ev["creator"]
                        assert creator_iri.endswith(expected_evidence["evidence_creator_id"])
                        verify_creator_metadata(
                            creator_iri=creator_iri,
                            expected_id=expected_evidence["evidence_creator_id"],
                            expected_type=expected_evidence["evidence_creator_type"],
                            expected_name=expected_evidence["evidence_creator_name"],
                            expected_version=expected_evidence["evidence_creator_version"],
                            physical_resources=physical_resources
                        )
                        break
            
            assert evidence_found, f"Evidence not found for {expected_evidence['clinical_note_id']}"

def test_bulk_upload_with_qualifiers(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Test bulk upload with qualifying terms and context flags"""
    load_id = perform_bulk_upload(test_payload_with_qualifiers, physical_resources)
    
    # Resolve subjects and validate TermLinks and Evidence
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_termlink_fn = physical_resources["GetTermLinkFunction"]
    
    # Get the subject
    payload = test_payload_with_qualifiers[0]  # Both entries use the same subject
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # There should be two term links (one for each entry with different qualifiers)
    assert len(subject_result["terms"]) == 2
    
    # Check each term link for the correct qualifiers
    for term in subject_result["terms"]:
        termlink_iri = term["termlink_iri"]
        termlink_result = invoke_lambda(get_termlink_fn, {"termlink_iri": termlink_iri})
        
        # Check for qualifying terms
        qualifying_terms = termlink_result.get("has_qualifying_term", [])
        print(f"Term link {termlink_iri} has qualifying terms: {qualifying_terms}")
        
        # Verify that qualifying terms are present
        assert len(qualifying_terms) > 0, "No qualifying terms found"
        
        # Check if this is the first entry with explicit qualifying terms
        if "note-qual-1" in str(termlink_result):
            assert "http://ods.nationwidechildrens.org/phebee/qualifier/negated" in qualifying_terms
            assert "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical" in qualifying_terms
        
        # Check if this is the second entry with context flags
        elif "note-qual-2" in str(termlink_result):
            assert "http://ods.nationwidechildrens.org/phebee/qualifier/negated" in qualifying_terms
            assert "http://ods.nationwidechildrens.org/phebee/qualifier/family" in qualifying_terms
            assert "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical" not in qualifying_terms

def test_bulk_upload_with_spans(test_payload_with_spans, test_project_id, physical_resources):
    """Test bulk upload with span information"""
    load_id = perform_bulk_upload(test_payload_with_spans, physical_resources)
    
    # Resolve subjects and validate TermLinks and Evidence
    get_subject_fn = physical_resources["GetSubjectFunction"]
    
    # Get the subject
    payload = test_payload_with_spans[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    
    # Verify the term link exists
    assert len(subject_result["terms"]) == 1
    termlink = subject_result["terms"][0]
    
    # Verify the evidence has span information
    assert len(termlink["evidence"]) == 1
    evidence = termlink["evidence"][0]
    
    # Check span properties
    props = evidence["properties"]
    assert "http://ods.nationwidechildrens.org/phebee#spanStart" in props
    assert "http://ods.nationwidechildrens.org/phebee#spanEnd" in props
    
    # Verify span values
    assert int(props["http://ods.nationwidechildrens.org/phebee#spanStart"]) == 120
    assert int(props["http://ods.nationwidechildrens.org/phebee#spanEnd"]) == 145

def test_bulk_upload_with_new_fields(test_payload_with_new_fields, test_project_id, physical_resources):
    """Test bulk upload with new fields like note_type, author_prov_type, and author_specialty"""
    load_id = perform_bulk_upload(test_payload_with_new_fields, physical_resources)
    
    # Get the subject
    payload = test_payload_with_new_fields[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    
    # Verify the term link exists
    assert len(subject_result["terms"]) == 1
    termlink = subject_result["terms"][0]
    
    # Get the evidence
    assert len(termlink["evidence"]) == 1
    evidence = termlink["evidence"][0]
    
    # Get the text source (clinical note)
    text_source = evidence["properties"]["http://ods.nationwidechildrens.org/phebee#textSource"]
    
    # Get the clinical note details
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": text_source})
    
    # Verify the new fields
    assert note_result["provider_type"] == "Physician"
    assert note_result["author_specialty"] == "Neurology"
    assert note_result["note_type"] == "Progress Note"

def test_bulk_upload_duplicate_detection(test_duplicate_payload, test_project_id, physical_resources):
    """Test that duplicate entries are correctly detected and handled"""
    load_id = perform_bulk_upload(test_duplicate_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    
    # Verify only one term link was created despite two identical entries
    assert len(subject_result["terms"]) == 1
    
    # Verify the term link has the correct term
    termlink = subject_result["terms"][0]
    assert termlink["term_iri"] == payload["term_iri"]
    
    # Verify only one evidence was created
    assert len(termlink["evidence"]) == 1

def test_bulk_upload_encounter_duplicate_handling(test_duplicate_encounters_payload, test_project_id, physical_resources):
    """Test that duplicate encounters are correctly detected and handled"""
    # First upload to create the encounters
    load_id = perform_bulk_upload(test_duplicate_encounters_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_encounters_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify two term links were created (one for each term)
    assert len(subject_result["terms"]) == 2
    
    # Get the encounter
    encounter_id = payload["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    
    # Get the clinical notes
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    
    # Check first note
    note1_iri = f"{encounter_iri}/note/note-enc-1"
    note1_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note1_iri})
    assert note1_result["clinical_note_id"] == "note-enc-1"
    
    # Check second note
    note2_iri = f"{encounter_iri}/note/note-enc-2"
    note2_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note2_iri})
    assert note2_result["clinical_note_id"] == "note-enc-2"
    
    # Now upload the same payload again - this should reuse the existing encounter
    second_load_id = perform_bulk_upload(test_duplicate_encounters_payload, physical_resources)
    
    # The encounter should still exist and have the same properties
    # We can verify this by checking that both notes are still accessible
    note1_result_again = invoke_lambda(get_note_fn, {"clinical_note_iri": note1_iri})
    assert note1_result_again["clinical_note_id"] == "note-enc-1"
    
    note2_result_again = invoke_lambda(get_note_fn, {"clinical_note_iri": note2_iri})
    assert note2_result_again["clinical_note_id"] == "note-enc-2"
    
    # Check that there's only one created timestamp
    get_encounter_fn = physical_resources["GetEncounterFunction"]
    encounter_result = invoke_lambda(get_encounter_fn, {
        "subject_iri": subject_iri,
        "encounter_id": encounter_id
    })
    
    created_timestamps = encounter_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"

def test_bulk_upload_encounter_in_batch_duplicate_handling(test_duplicate_encounters_in_batch_payload, test_project_id, physical_resources):
    """Test that duplicate encounters within the same batch are correctly detected and handled"""
    # Upload the payload with multiple notes using the same encounter
    load_id = perform_bulk_upload(test_duplicate_encounters_in_batch_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_encounters_in_batch_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify one term link was created
    assert len(subject_result["terms"]) == 1
    
    # Get the encounter
    encounter_id = payload["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    
    # Get the clinical notes
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    
    # Check all three notes
    for i in range(1, 4):
        note_iri = f"{encounter_iri}/note/note-batch-{i}"
        note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
        assert note_result["clinical_note_id"] == f"note-batch-{i}"
    
    # Now let's verify the encounter doesn't have duplicate created timestamps
    # We need to query the encounter directly to check its properties
    get_encounter_fn = physical_resources["GetEncounterFunction"]
    encounter_result = invoke_lambda(get_encounter_fn, {
        "subject_iri": subject_iri,
        "encounter_id": encounter_id
    })
    
    # Check that there's only one created timestamp
    # The created property might be a list or a single value depending on how it's returned
    created_timestamps = encounter_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"

def test_bulk_upload_clinical_note_duplicate_handling(test_duplicate_notes_in_batch_payload, test_project_id, physical_resources):
    """Test that duplicate clinical notes within the same batch are correctly detected and handled"""
    # Upload the payload with multiple term links using the same clinical note
    load_id = perform_bulk_upload(test_duplicate_notes_in_batch_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_notes_in_batch_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify two term links were created (one for each term)
    assert len(subject_result["terms"]) == 2
    
    # Get the encounter and clinical note
    encounter_id = payload["evidence"][0]["encounter_id"]
    clinical_note_id = payload["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    
    # Get the clinical note details
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
    
    # Verify the clinical note exists and has the correct properties
    assert note_result["clinical_note_id"] == clinical_note_id
    assert note_result["author_specialty"] == "Neurology"
    
    # Check that the note has links to both term links
    term_links = note_result.get("has_term_link", [])
    assert len(term_links) == 2, f"Expected 2 term links, found {len(term_links)}"
    
    # Now check that there's only one created timestamp
    created_timestamps = note_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"

def test_term_link_source_node(test_payload, test_project_id, physical_resources):
    """Test that term links use the appropriate source node (clinical note) as their base IRI"""
    load_id = perform_bulk_upload(test_payload, physical_resources)
    
    # Get the subject
    payload = test_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Get the clinical note
    encounter_id = payload["evidence"][0]["encounter_id"]
    clinical_note_id = payload["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
    
    # Verify the note has hasTermLink property
    assert "has_term_link" in note_result, "Clinical note should have hasTermLink property"
    
    # Get the term link
    term_link_iri = note_result["has_term_link"][0]
    
    # Verify the term link IRI is based on the clinical note IRI
    assert note_iri in term_link_iri, f"Term link IRI {term_link_iri} should be based on clinical note IRI {note_iri}"
    
    # Get the term link details
    get_termlink_fn = physical_resources["GetTermLinkFunction"]
    termlink_result = invoke_lambda(get_termlink_fn, {"termlink_iri": term_link_iri})
    
    # Verify the term link has the correct source node
    assert termlink_result["source_node"] == note_iri, f"Term link source node should be {note_iri}, got {termlink_result['source_node']}"

def test_bulk_upload_error_handling(test_project_id, physical_resources):
    """Test error handling for invalid input"""
    # Create invalid payload (missing required field)
    invalid_payload = [
        {
            "project_id": test_project_id,
            # Missing project_subject_id
            "term_iri": "http://purl.obolibrary.org/obo/HP_0004322",
            "evidence": []
        }
    ]
    
    # Step 1: Call prepare_bulk_upload Lambda to get signed URL and S3 key
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {})
    upload_url = prep_response["upload_url"]
    s3_key = prep_response["s3_key"]
    
    # Step 2: Upload JSON to the signed S3 URL
    headers = {"Content-Type": "application/json"}
    put_resp = requests.put(upload_url, data=json.dumps(invalid_payload), headers=headers)
    assert put_resp.status_code == 200
    
    # Step 3: Call perform_bulk_upload Lambda
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    response = invoke_lambda(perform_fn, {"s3_key": s3_key})
    
    # Verify error response
    assert response["statusCode"] == 400
    assert "error" in response["body"]
    assert "Invalid JSON" in response["body"]
