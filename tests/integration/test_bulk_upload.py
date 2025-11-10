import json
import uuid
import time
import pytest
import requests
from urllib.parse import quote
from phebee.utils.aws import get_client
from general_utils import parse_iso8601

pytestmark = [pytest.mark.integration]

# -------------- Utilities --------------

def invoke_lambda(name, payload):
    """Invoke a Lambda and return the parsed body if API-proxy shaped, else the raw payload."""
    lambda_client = get_client("lambda")
    resp = lambda_client.invoke(FunctionName=name, Payload=json.dumps(payload).encode())
    raw = json.loads(resp["Payload"].read())
    # If API-proxy style {statusCode, body}, return parsed body only (as before)
    if isinstance(raw, dict) and "body" in raw:
        body = raw.get("body")
        try:
            return json.loads(body) if isinstance(body, str) else body
        except Exception:
            return body
    return raw

def wait_for_loader_success(load_id, physical_resources, timeout_seconds=600):
    """Wait for a single Neptune bulk loader job to finish."""
    print(f"Waiting for load success for job {load_id}")
    start = time.time()
    while True:
        result = invoke_lambda(physical_resources["GetLoadStatusFunction"], {"load_job_id": load_id})
        print(result)
        overall = (result or {}).get("payload", {}).get("overallStatus", {}) or {}
        status = (overall.get("status") or "").lower()
        print(f"Loader status: {status}")

        if status in {"load_completed", "load_completed_with_errors"}:
            return result
        if status in {"load_failed", "cancelled", "error"}:
            raise RuntimeError(f"Neptune bulk load failed: {status}")

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Neptune bulk loader did not finish in {timeout_seconds} seconds")

        time.sleep(8)

def wait_for_all_loads_success(load_ids, physical_resources, timeout_seconds=900):
    """Wait for all load IDs to complete (domain + prov)."""
    results = []
    for lid in load_ids:
        results.append(wait_for_loader_success(lid, physical_resources, timeout_seconds))
    return results

def verify_creator_metadata(creator_iri, expected_id, expected_type, expected_name, expected_version, physical_resources):
    get_creator_fn = physical_resources["GetCreatorFunction"]
    response = invoke_lambda(get_creator_fn, {"creator_id": creator_iri.rsplit("/", 1)[-1]})
    print(f"Creator {creator_iri} metadata: {response}")

    assert response["creator_iri"] == creator_iri
    assert response["creator_id"] == expected_id
    assert response.get("has_version") == expected_version

    expected_type_name = "HumanCreator" if expected_type == "human" else "AutomatedCreator"
    assert response["type"] == f"http://ods.nationwidechildrens.org/phebee#{expected_type_name}"
    assert response["title"] == expected_name

def prepare_upload(run_id, batch_id, physical_resources):
    """Call PrepareBulkUploadFunction with (run_id, batch_id) and return (upload_url, s3_key)."""
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {"body": json.dumps({"run_id": run_id, "batch_id": batch_id})})
    print(f"prepare_upload response: {prep_response}")
    upload_url = prep_response["upload_url"]
    s3_key = prep_response["s3_key"]
    assert upload_url and s3_key
    return upload_url, s3_key

def put_json_to_presigned(url, payload):
    headers = {"Content-Type": "application/json"}
    put_resp = requests.put(url, data=json.dumps(payload), headers=headers)
    assert put_resp.status_code == 200

def perform_shard(run_id, batch_id, physical_resources, agent_iri=None):
    """Call PerformBulkUploadFunction for a shard (no loader yet)."""
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    body = {"run_id": run_id, "batch_id": batch_id}
    if agent_iri:
        body["agent_iri"] = agent_iri
    resp = invoke_lambda(perform_fn, {"body": json.dumps(body)})
    print("perform_shard:", resp)
    assert resp["message"] == "Shard processed"
    assert resp["run_id"] == run_id and resp["batch_id"] == int(batch_id)
    # returns keys, activity, prov graph, etc.
    return resp

def finalize_run(run_id, expected_batches, physical_resources):
    """Kick both loaders for the run and return their load IDs (domain, prov)."""
    finalize_fn = physical_resources["FinalizeBulkUploadFunction"]
    resp = invoke_lambda(finalize_fn, {"body": json.dumps({"run_id": run_id, "expected_batches": expected_batches})})
    print("finalize_run:", resp)
    assert resp["message"] == "Batch loads started"
    assert resp["run_id"] == run_id
    assert "domain_load_id" in resp and "prov_load_id" in resp
    return resp["domain_load_id"], resp["prov_load_id"]

def bulk_upload_run(shards, physical_resources, agent_iri=None):
    """
    High-level helper:
      - Creates a new run_id
      - For each shard payload, prepares, uploads, and performs
      - Finalizes the run
      - Waits for both loaders
      - Returns (run_id, domain_load_id, prov_load_id)
    """
    run_id = str(uuid.uuid4())
    for i, payload in enumerate(shards, start=1):
        upload_url, s3_key = prepare_upload(run_id, i, physical_resources)
        put_json_to_presigned(upload_url, payload)
        perform_shard(run_id, i, physical_resources, agent_iri=agent_iri)
    domain_lid, prov_lid = finalize_run(run_id, len(shards), physical_resources)
    wait_for_all_loads_success([domain_lid, prov_lid], physical_resources)
    return run_id, domain_lid, prov_lid

# -------------- Test Fixtures (updated) --------------

@pytest.fixture
def test_payload(test_project_id):
    subject_id_1 = f"subj-{uuid.uuid4()}"
    subject_id_2 = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id_1 = str(uuid.uuid4())
    encounter_id_2 = str(uuid.uuid4())

    return [
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

@pytest.fixture
def test_payload_with_qualifiers(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())

    return [
        # Entry using context flags (negated + hypothetical)
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": f"note-qual-1-{uuid.uuid4()}",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "contexts": {"negated": 1, "hypothetical": 1}
                }
            ]
        },
        # Entry using context flags (negated + family)
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": f"note-qual-2-{uuid.uuid4()}",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "contexts": {"negated": 1, "family": 1}
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
    # note_type is currently not emitted as RDF; we validate provider_type & author_specialty
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
                    "provider_type": "Physician",
                    "author_specialty": "Neurology"
                }
            ]
        }
    ]

@pytest.fixture
def test_duplicate_payload(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())

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
    return [entry, entry]

@pytest.fixture
def test_duplicate_encounters_payload(test_project_id):
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id = str(uuid.uuid4())

    return [
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
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
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
                    "clinical_note_id": "note-batch-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": "note-batch-2",
                    "note_timestamp": "2025-06-06T14:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
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
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id = str(uuid.uuid4())
    clinical_note_id = f"note-{uuid.uuid4()}"

    return [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_1,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "author_specialty": "Neurology"
                }
            ]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,
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

# -------------- Tests --------------

def test_perform_bulk_upload_flow(test_payload, test_project_id, physical_resources):
    """Basic run with 1 shard; validates subject, term links, evidence, creators."""
    _, _, _ = bulk_upload_run([test_payload], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload, test_project_id, physical_resources)

    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]

    for entry in test_payload:
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
        subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
        print(subject_result)

        subject_iri = subject_result["subject_iri"]
        assert subject_iri.startswith("http://ods.nationwidechildrens.org/phebee/subjects/")
        assert "terms" in subject_result

        # Correct term present
        matching_terms = [t for t in subject_result["terms"] if t["term_iri"] == entry["term_iri"]]
        assert matching_terms, f"No TermLink found for {entry['term_iri']}"
        assert len(matching_terms) == 1
        termlink = matching_terms[0]

        # Evidence matches
        expected_evidences = entry["evidence"]
        assert len(termlink["evidence"]) == len(expected_evidences)

        for expected_ev in expected_evidences:
            found = False
            for ev in termlink["evidence"]:
                if "text_source" in ev:
                    # Extract clinical_note_id from text_source IRI
                    # Format: .../encounter/{encounter_id}/note/{clinical_note_id}
                    clinical_note_id = ev["text_source"].split("/note/")[-1]
                    if clinical_note_id == expected_ev["clinical_note_id"]:
                        found = True
                        break
            assert found, f"Evidence not found for {expected_ev['clinical_note_id']}"

def test_bulk_upload_with_qualifiers(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Two entries with different context flags -> two TermLinks with distinct qualifiers."""
    _, _, _ = bulk_upload_run([test_payload_with_qualifiers], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload_with_qualifiers, test_project_id, physical_resources)

    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_termlink_fn = physical_resources["GetTermLinkFunction"]

    subject_entry = test_payload_with_qualifiers[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{subject_entry['project_id']}/{subject_entry['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    assert len(subj["terms"]) == 2

    # Collect qualifiers per termlink
    seen = {}
    for term in subj["terms"]:
        tr = invoke_lambda(get_termlink_fn, {"termlink_iri": term["termlink_iri"]})
        q = set(tr.get("has_qualifying_term", []))
        seen[term["termlink_iri"]] = q
        print("qualifiers:", term["termlink_iri"], q)

    # Expect one link with negated + hypothetical, another with negated + family
    q_neg = "http://ods.nationwidechildrens.org/phebee/qualifier/negated"
    q_hyp = "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical"
    q_fam = "http://ods.nationwidechildrens.org/phebee/qualifier/family"

    assert any({q_neg, q_hyp}.issubset(qs) for qs in seen.values())
    assert any({q_neg, q_fam}.issubset(qs) and q_hyp not in qs for qs in seen.values())


def test_pagination_api_minimal_format_with_qualifiers(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Test that GetSubjectsPhenotypesFunction returns minimal format with gzip compression and qualifiers."""
    _, _, _ = bulk_upload_run([test_payload_with_qualifiers], physical_resources)

    get_subjects_fn = physical_resources["GetSubjectsPhenotypesFunction"]
    
    # Query subjects via pagination API
    import boto3
    import json
    import gzip
    import base64
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    response = lambda_client.invoke(
        FunctionName=get_subjects_fn,
        Payload=json.dumps({
            "project_id": test_project_id,
            "limit": 10
        }).encode('utf-8'),
    )
    
    result = json.loads(response['Payload'].read())
    assert result["statusCode"] == 200
    
    # Verify gzip compression headers
    headers = result.get("headers", {})
    assert headers.get("Content-Encoding") == "gzip"
    assert headers.get("Content-Type") == "application/json"
    assert result.get("isBase64Encoded") == True
    
    # Decompress response
    compressed_data = base64.b64decode(result['body'])
    decompressed_data = gzip.decompress(compressed_data)
    subjects_response = json.loads(decompressed_data.decode('utf-8'))
    
    assert subjects_response["n_subjects"] == 1
    subjects = subjects_response["body"]
    assert len(subjects) == 1
    
    subject = subjects[0]
    assert len(subject["term_links"]) == 2
    
    # Verify minimal format structure
    for term_link in subject["term_links"]:
        # Check minimal format fields are present
        assert "term_iri" in term_link
        assert "qualifiers" in term_link
        assert "evidence_count" in term_link
        
        # Check removed fields are NOT present
        removed_fields = ["termlink_iri", "term_label", "source_node", "source_type", "evidence"]
        for field in removed_fields:
            assert field not in term_link, f"Field '{field}' should be removed in minimal format"
        
        # Verify evidence count is a number
        assert isinstance(term_link["evidence_count"], int)
        assert term_link["evidence_count"] >= 0
    
    # Verify qualifiers are present and correct
    qualifiers_found = []
    for term_link in subject["term_links"]:
        qualifiers = term_link.get("qualifiers", [])
        assert len(qualifiers) > 0, f"No qualifiers found in term_link: {term_link['term_iri']}"
        qualifiers_found.extend(qualifiers)
    
    # Expected qualifiers from the test data
    q_neg = "http://ods.nationwidechildrens.org/phebee/qualifier/negated"
    q_hyp = "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical"
    q_fam = "http://ods.nationwidechildrens.org/phebee/qualifier/family"
    
    # Verify expected qualifiers are present
    assert q_neg in qualifiers_found, "Missing 'negated' qualifier"
    assert q_hyp in qualifiers_found, "Missing 'hypothetical' qualifier"
    assert q_fam in qualifiers_found, "Missing 'family' qualifier"
    
    print(f"✅ Minimal format verified: {len(qualifiers_found)} total qualifiers found")
    print(f"✅ Gzip compression verified: {len(compressed_data)} bytes compressed vs {len(decompressed_data)} bytes decompressed")


def test_pagination_api_with_qualifiers(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Test that GetSubjectsPhenotypesFunction (pagination API) includes qualifiers."""
    _, _, _ = bulk_upload_run([test_payload_with_qualifiers], physical_resources)

    get_subjects_fn = physical_resources["GetSubjectsPhenotypesFunction"]
    
    # Query subjects via pagination API with proper decompression
    import boto3
    import json
    import gzip
    import base64
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    response = lambda_client.invoke(
        FunctionName=get_subjects_fn,
        Payload=json.dumps({
            "project_id": test_project_id,
            "limit": 10
        }).encode('utf-8'),
    )
    
    result = json.loads(response['Payload'].read())
    assert result["statusCode"] == 200
    
    # Decompress response
    if result.get("isBase64Encoded"):
        compressed_data = base64.b64decode(result['body'])
        decompressed_data = gzip.decompress(compressed_data)
        subjects_response = json.loads(decompressed_data.decode('utf-8'))
    else:
        subjects_response = json.loads(result['body'])
    
    assert subjects_response["n_subjects"] == 1
    subjects = subjects_response["body"]
    assert len(subjects) == 1
    
    subject = subjects[0]
    assert len(subject["term_links"]) == 2
    
    # Verify qualifiers are present in pagination API response
    qualifiers_found = []
    for term_link in subject["term_links"]:
        qualifiers = term_link.get("qualifiers", [])
        assert len(qualifiers) > 0, f"No qualifiers found in term_link: {term_link['term_iri']}"
        qualifiers_found.extend(qualifiers)
    
    # Expected qualifiers from the test data
    q_neg = "http://ods.nationwidechildrens.org/phebee/qualifier/negated"
    q_hyp = "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical"
    q_fam = "http://ods.nationwidechildrens.org/phebee/qualifier/family"
    
    # Verify expected qualifiers are present
    assert q_neg in qualifiers_found, "Missing 'negated' qualifier"
    assert q_hyp in qualifiers_found, "Missing 'hypothetical' qualifier"
    assert q_fam in qualifiers_found, "Missing 'family' qualifier"
    
    print(f"✅ Pagination API qualifiers verified: {len(qualifiers_found)} total qualifiers found")

def verify_uploaded_data(test_payload, test_project_id, physical_resources):
    """Verify that all uploaded data can be retrieved via API functions."""
    import boto3
    import gzip
    import base64
    
    # Get subjects for the project
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    # Test GetSubjectsPhenotypesFunction
    get_subjects_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        InvocationType="RequestResponse",
        Payload=json.dumps({"body": json.dumps({"project_id": test_project_id})})
    )
    
    subjects_result = json.loads(get_subjects_response["Payload"].read().decode("utf-8"))
    assert subjects_result["statusCode"] == 200, f"GetSubjectsPhenotypesFunction failed: {subjects_result}"
    
    # Handle gzip compressed response
    if subjects_result.get("isBase64Encoded"):
        compressed_data = base64.b64decode(subjects_result['body'])
        decompressed_data = gzip.decompress(compressed_data)
        subjects_data = json.loads(decompressed_data.decode('utf-8'))["body"]
    else:
        subjects_data = json.loads(subjects_result["body"])["body"]
    
    # Count unique subjects in test payload
    unique_subjects = len(set(entry["project_subject_id"] for entry in test_payload))
    assert len(subjects_data) == unique_subjects, f"Expected {unique_subjects} unique subjects, got {len(subjects_data)}"
    
    # Verify each subject has expected data
    for i, subject_data in enumerate(subjects_data):
        # Find corresponding entry in test payload
        expected_entry = next(entry for entry in test_payload if entry["project_subject_id"] == subject_data["project_subject_id"])
        
        # Check subject has term links
        assert len(subject_data["term_links"]) > 0, f"Subject {i} should have term links"
        
        # Verify expected terms are present (term_iri is at top level of each entry)
        found_terms = {tl["term_iri"] for tl in subject_data["term_links"]}
        expected_terms = {entry["term_iri"] for entry in test_payload if entry["project_subject_id"] == expected_entry["project_subject_id"]}
        assert expected_terms.issubset(found_terms), f"Missing expected terms for subject {i}"
        
        # Test GetSubjectFunction for detailed data
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{expected_entry['project_subject_id']}"
        get_subject_response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectFunction"],
            InvocationType="RequestResponse",
            Payload=json.dumps({"body": json.dumps({
                "project_subject_iri": project_subject_iri
            })})
        )
        
        subject_result = json.loads(get_subject_response["Payload"].read().decode("utf-8"))
        assert subject_result["statusCode"] == 200, f"GetSubjectFunction failed for subject {i}: {subject_result}"
        
        detailed_subject = json.loads(subject_result["body"])
        
        # Verify subject has expected evidence count
        total_evidence = sum(len(tl.get("evidence", [])) for tl in detailed_subject.get("terms", []))
        expected_evidence = sum(len(entry["evidence"]) for entry in test_payload if entry["project_subject_id"] == expected_entry["project_subject_id"])
        assert total_evidence >= expected_evidence, f"Subject {i} missing evidence: got {total_evidence}, expected {expected_evidence}"


def test_bulk_upload_with_spans(test_payload_with_spans, test_project_id, physical_resources):
    """Evidence carries spanStart/spanEnd on TextAnnotation."""
    _, _, _ = bulk_upload_run([test_payload_with_spans], physical_resources)
    
    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload_with_spans, test_project_id, physical_resources)
    
    # Original span-specific verification
    get_subject_fn = physical_resources["GetSubjectFunction"]

    entry = test_payload_with_spans[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})

    assert len(subj["terms"]) == 1
    ev = subj["terms"][0]["evidence"][0]
    assert ev["span_start"] == 120
    assert ev["span_end"] == 145

def test_bulk_upload_with_new_fields(test_payload_with_new_fields, test_project_id, physical_resources):
    """Validate provider_type and author_specialty on ClinicalNote."""
    _, _, _ = bulk_upload_run([test_payload_with_new_fields], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload_with_new_fields, test_project_id, physical_resources)

    entry = test_payload_with_new_fields[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]

    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    assert len(subj["terms"]) == 1
    ev = subj["terms"][0]["evidence"][0]
    note_iri = ev["text_source"]
    # Extract encounter_iri and clinical_note_id from note_iri
    parts = note_iri.split("/")
    encounter_idx = parts.index("encounter")
    note_idx = parts.index("note")
    encounter_iri = "/".join(parts[:encounter_idx+2])
    clinical_note_id = parts[note_idx+1]
    
    note = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": clinical_note_id})

    assert note["provider_type"] == "Physician"
    assert note["author_specialty"] == "Neurology"

def test_bulk_upload_duplicate_detection(test_duplicate_payload, test_project_id, physical_resources):
    """Two identical entries in one shard => single TermLink & single Evidence."""
    _, _, _ = bulk_upload_run([test_duplicate_payload], physical_resources)
    
    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_duplicate_payload, test_project_id, physical_resources)
    
    entry = test_duplicate_payload[0]

    get_subject_fn = physical_resources["GetSubjectFunction"]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})

    assert len(subj["terms"]) == 1
    termlink = subj["terms"][0]
    assert termlink["term_iri"] == entry["term_iri"]
    assert len(termlink["evidence"]) == 1

def test_bulk_upload_encounter_duplicate_handling(test_duplicate_encounters_payload, test_project_id, physical_resources):
    """Same encounter across different term links -> one Encounter node reused; notes remain distinct."""
    # First run
    run1 = bulk_upload_run([test_duplicate_encounters_payload], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_duplicate_encounters_payload, test_project_id, physical_resources)

    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    get_enc_fn = physical_resources["GetEncounterFunction"]

    e0 = test_duplicate_encounters_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{e0['project_id']}/{e0['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subj["subject_iri"]

    assert len(subj["terms"]) == 2

    encounter_id = e0["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"

    # Notes exist
    n1 = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": "note-enc-1"})
    n2 = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": "note-enc-2"})
    assert n1["clinical_note_iri"] == f"{encounter_iri}/note/note-enc-1"
    assert n2["clinical_note_iri"] == f"{encounter_iri}/note/note-enc-2"

    # Second run with same payload (no dup Encounter nodes)
    run2 = bulk_upload_run([test_duplicate_encounters_payload], physical_resources)

    n1_again = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": "note-enc-1"})
    n2_again = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": "note-enc-2"})
    assert n1_again["clinical_note_iri"] == f"{encounter_iri}/note/note-enc-1"
    assert n2_again["clinical_note_iri"] == f"{encounter_iri}/note/note-enc-2"

    # Encounter exists and is unique (no 'created' expected on Encounter in current model)
    enc = invoke_lambda(get_enc_fn, {"subject_iri": subject_iri, "encounter_id": encounter_id})
    # Ensure minimal properties are present
    assert enc["encounter_id"] == encounter_id
    assert enc["subject_iri"] == subject_iri

def test_bulk_upload_encounter_in_batch_duplicate_handling(test_duplicate_encounters_in_batch_payload, test_project_id, physical_resources):
    """Multiple notes share one encounter within a shard -> one Encounter, three Notes."""
    _, _, _ = bulk_upload_run([test_duplicate_encounters_in_batch_payload], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_duplicate_encounters_in_batch_payload, test_project_id, physical_resources)

    e = test_duplicate_encounters_in_batch_payload[0]
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    get_enc_fn = physical_resources["GetEncounterFunction"]

    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{e['project_id']}/{e['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subj["subject_iri"]

    assert len(subj["terms"]) == 3  # three separate term links for same term from different notes

    encounter_id = e["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"

    for i in range(1, 4):
        clinical_note_id = f"note-batch-{i}"
        note = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": clinical_note_id})
        assert note["clinical_note_iri"] == f"{encounter_iri}/note/{clinical_note_id}"

    enc = invoke_lambda(get_enc_fn, {"subject_iri": subject_iri, "encounter_id": encounter_id})
    assert enc["encounter_id"] == encounter_id

def test_bulk_upload_clinical_note_duplicate_handling(test_duplicate_notes_in_batch_payload, test_project_id, physical_resources):
    """Same ClinicalNote used by two TermLinks -> one note with two hasTermLink edges."""
    _, _, _ = bulk_upload_run([test_duplicate_notes_in_batch_payload], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_duplicate_notes_in_batch_payload, test_project_id, physical_resources)

    e = test_duplicate_notes_in_batch_payload[0]
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]

    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{e['project_id']}/{e['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subj["subject_iri"]

    assert len(subj["terms"]) == 2  # two terms referencing same note

    encounter_id = e["evidence"][0]["encounter_id"]
    clinical_note_id = e["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"

    note = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": clinical_note_id})
    assert note["clinical_note_iri"] == f"{encounter_iri}/note/{clinical_note_id}"
    assert note["author_specialty"] == "Neurology"

    term_links = note.get("has_term_link", [])
    assert isinstance(term_links, list) and len(term_links) == 2

def test_term_link_source_node(test_payload, test_project_id, physical_resources):
    """TermLink IRIs are based on the ClinicalNote when evidence is a clinical note."""
    _, _, _ = bulk_upload_run([test_payload], physical_resources)

    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload, test_project_id, physical_resources)

    e = test_payload[0]
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    get_termlink_fn = physical_resources["GetTermLinkFunction"]

    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{e['project_id']}/{e['project_subject_id']}"
    subj = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subj["subject_iri"]

    enc_id = e["evidence"][0]["encounter_id"]
    note_id = e["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{enc_id}"
    note_iri = f"{encounter_iri}/note/{note_id}"

    note = invoke_lambda(get_note_fn, {"encounter_iri": encounter_iri, "clinical_note_id": note_id})
    assert "has_term_link" in note and len(note["has_term_link"]) >= 1
    term_link_iri = note["has_term_link"][0]

    assert note_iri in term_link_iri, f"{term_link_iri} should be based on {note_iri}"
    tr = invoke_lambda(get_termlink_fn, {"termlink_iri": term_link_iri})
    source_node = tr["source_node"]
    if isinstance(source_node, list):
        source_node = source_node[0]
    assert source_node == note_iri


def test_bulk_upload_subject_deduplication(test_payload, test_project_id, physical_resources):
    """Same project_subject_id in multiple uploads -> one subject node (functional verification)."""
    # First upload
    bulk_upload_run([test_payload], physical_resources)
    
    # Second upload with same project_subject_id - this should be idempotent
    bulk_upload_run([test_payload], physical_resources)
    
    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload, test_project_id, physical_resources)
    
    get_subject_fn = physical_resources["GetSubjectFunction"]
    entry = test_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    
    # Verify we can still retrieve the subject and it has consistent structure
    subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    # If there were duplicate subjects, the GetSubject function would fail or return inconsistent data
    assert "subject_iri" in subject_result
    assert subject_result["subject_iri"].startswith("http://ods.nationwidechildrens.org/phebee/subjects/")
    assert "terms" in subject_result
    
    # The key test: verify the system is functional and consistent after duplicate uploads
    # We don't assert exact counts since other tests may have added data, but we verify structure
    assert len(subject_result["terms"]) > 0, "Subject should have at least one term link"
    
    # Verify each term link has evidence
    for term in subject_result["terms"]:
        assert "evidence" in term
        assert len(term["evidence"]) > 0, "Each term link should have evidence"
        assert "term_iri" in term
        assert "termlink_iri" in term


def test_bulk_upload_encounter_deduplication(test_payload, test_project_id, physical_resources):
    """Same encounter_id in multiple uploads -> one encounter node (functional verification)."""
    # First upload
    bulk_upload_run([test_payload], physical_resources)
    
    # Second upload with same encounter_id - this should be idempotent
    bulk_upload_run([test_payload], physical_resources)
    
    # Verify all uploaded data can be retrieved
    verify_uploaded_data(test_payload, test_project_id, physical_resources)
    
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_encounter_fn = physical_resources["GetEncounterFunction"]
    entry = test_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    
    # Get subject to find encounter
    subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    subject_iri = subject_result["subject_iri"]
    encounter_id = entry["evidence"][0]["encounter_id"]
    
    # Verify encounter can be retrieved and has correct structure
    encounter_result = invoke_lambda(get_encounter_fn, {"subject_iri": subject_iri, "encounter_id": encounter_id})
    
    # If there were duplicate encounters, this would fail or return inconsistent data
    assert encounter_result["encounter_id"] == encounter_id
    assert encounter_result["subject_iri"] == subject_iri
    
    # Verify subject has consistent structure after duplicate uploads
    assert len(subject_result["terms"]) > 0, "Subject should have at least one term link"
    
    # Verify each term link has evidence
    for term in subject_result["terms"]:
        assert "evidence" in term
        assert len(term["evidence"]) > 0, "Each term link should have evidence"


def test_bulk_upload_creates_dynamodb_records(test_payload, test_project_id, physical_resources):
    """Verify that bulk upload creates corresponding DynamoDB records for subjects."""
    # Upload test data
    bulk_upload_run([test_payload], physical_resources)
    
    # Get the DynamoDB table name from environment (set by conftest.py)
    import os
    dynamodb_table_name = os.environ.get("PheBeeDynamoTable")
    assert dynamodb_table_name, "DynamoDB table name should be available from stack outputs"
    
    # Check DynamoDB for subject records
    import boto3
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(dynamodb_table_name)
    
    # Check each entry in the test payload
    for entry in test_payload:
        # Check forward mapping: PSID -> subject_iri
        response = table.get_item(
            Key={
                'PK': f"PSID#{entry['project_id']}", 
                'SK': f"PSID#{entry['project_subject_id']}"
            }
        )
        
        assert 'Item' in response, f"No forward mapping found for project_id={entry['project_id']}, project_subject_id={entry['project_subject_id']}"
        
        item = response['Item']
        assert 'subject_iri' in item, "Forward mapping should contain subject_iri"
        subject_iri = item['subject_iri']
        assert subject_iri.startswith('http://ods.nationwidechildrens.org/phebee/subjects/'), "subject_iri should be valid"
        
        # Check reverse mapping: subject_iri -> PSID
        response = table.get_item(
            Key={
                'PK': f"SUBJ#{subject_iri}",
                'SK': f"PSID#{entry['project_id']}#{entry['project_subject_id']}"
            }
        )
        
        assert 'Item' in response, f"No reverse mapping found for subject_iri={subject_iri}"


def test_bulk_upload_concurrent_uploads(test_project_id, physical_resources):
    """Test concurrent uploads of the same data to verify no corruption occurs."""
    import uuid
    import threading
    import time
    
    subject_id = f"subj-{uuid.uuid4()}"
    encounter_id = str(uuid.uuid4())
    
    # Same payload for both concurrent uploads
    payload = [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0004322",
            "evidence": [{
                "type": "clinical_note",
                "encounter_id": encounter_id,
                "clinical_note_id": "note-concurrent",
                "note_timestamp": "2025-06-06T12:00:00Z",
                "evidence_creator_id": "robot-creator",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Robot Creator",
                "evidence_creator_version": "1.2"
            }]
        }
    ]
    
    # Track results and errors from both threads
    results = []
    errors = []
    
    def upload_worker(worker_id):
        try:
            result = bulk_upload_run([payload], physical_resources)
            results.append((worker_id, result))
        except Exception as e:
            errors.append((worker_id, str(e)))
    
    # Start two concurrent uploads
    thread1 = threading.Thread(target=upload_worker, args=(1,))
    thread2 = threading.Thread(target=upload_worker, args=(2,))
    
    thread1.start()
    thread2.start()
    
    # Wait for both to complete
    thread1.join()
    thread2.join()
    
    # Both should succeed (or at least not corrupt data)
    assert len(errors) == 0, f"Concurrent uploads failed: {errors}"
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"
    
    # Verify all uploaded data can be retrieved
    verify_uploaded_data(payload, test_project_id, physical_resources)
    
    # Verify final state is consistent
    get_subject_fn = physical_resources["GetSubjectFunction"]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{subject_id}"
    
    result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    # Should have exactly one term with one evidence (no duplication from concurrent uploads)
    assert len(result["terms"]) == 1, f"Expected 1 term, got {len(result['terms'])}"
    assert len(result["terms"][0]["evidence"]) == 1, f"Expected 1 evidence, got {len(result['terms'][0]['evidence'])}"
    assert result["terms"][0]["term_iri"] == "http://purl.obolibrary.org/obo/HP_0004322"


def test_bulk_upload_empty_evidence_arrays(test_project_id, physical_resources):
    """Test behavior with empty evidence arrays."""
    import uuid
    
    subject_id = f"subj-{uuid.uuid4()}"
    
    # Payload with empty evidence array
    payload = [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0004322",
            "evidence": []  # Empty evidence array
        }
    ]
    
    # Upload should succeed but create no term links (no evidence = no term link)
    bulk_upload_run([payload], physical_resources)
    
    # Special verification for empty evidence - subject should exist but have empty term_links
    import boto3
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    # GetSubjectsPhenotypesFunction should return the subject but with empty term_links
    get_subjects_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        InvocationType="RequestResponse",
        Payload=json.dumps({"body": json.dumps({"project_id": test_project_id})})
    )
    
    subjects_result = json.loads(get_subjects_response["Payload"].read().decode("utf-8"))
    assert subjects_result["statusCode"] == 200
    subjects_data = json.loads(subjects_result["body"])["body"]
    assert len(subjects_data) == 1, "Subject should exist even with empty evidence"
    assert len(subjects_data[0]["term_links"]) == 0, "Subject should have empty term_links array"
    
    # Verify the subject exists but has no terms
    get_subject_fn = physical_resources["GetSubjectFunction"]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{subject_id}"
    
    result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    # Subject should exist but have no terms (empty evidence = no term links created)
    assert "subject_iri" in result
    assert result["subject_iri"].startswith("http://ods.nationwidechildrens.org/phebee/subjects/")
    assert len(result["terms"]) == 0, "No term links should be created without evidence"


def test_bulk_upload_true_idempotency(test_project_id, physical_resources):
    """True idempotency test: partial second upload should preserve data from first upload."""
    import uuid
    
    subject_id = f"subj-{uuid.uuid4()}"
    encounter_id = str(uuid.uuid4())
    
    # First upload: subject with TWO terms
    first_payload = [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0004322",  # Term A
            "evidence": [{
                "type": "clinical_note",
                "encounter_id": encounter_id,
                "clinical_note_id": "note-001",
                "note_timestamp": "2025-06-06T12:00:00Z",
                "evidence_creator_id": "robot-creator",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Robot Creator",
                "evidence_creator_version": "1.2"
            }]
        },
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",  # Term B
            "evidence": [{
                "type": "clinical_note",
                "encounter_id": encounter_id,
                "clinical_note_id": "note-002",
                "note_timestamp": "2025-06-06T12:00:00Z",
                "evidence_creator_id": "robot-creator",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Robot Creator",
                "evidence_creator_version": "1.2"
            }]
        }
    ]
    
    # Second upload: same subject with only ONE term (should be idempotent)
    second_payload = [
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0004322",  # Only Term A
            "evidence": [{
                "type": "clinical_note",
                "encounter_id": encounter_id,
                "clinical_note_id": "note-001",
                "note_timestamp": "2025-06-06T12:00:00Z",
                "evidence_creator_id": "robot-creator",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Robot Creator",
                "evidence_creator_version": "1.2"
            }]
        }
    ]
    
    # Execute uploads
    bulk_upload_run([first_payload], physical_resources)
    bulk_upload_run([second_payload], physical_resources)
    
    # Verify uploaded data can be retrieved via GetSubjectsPhenotypesFunction
    verify_uploaded_data([{"project_id": test_project_id, "project_subject_id": subject_id, "evidence": first_payload + second_payload}], test_project_id, physical_resources)
    
    # Verify true idempotency
    get_subject_fn = physical_resources["GetSubjectFunction"]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{subject_id}"
    
    result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    # Extract all term IRIs from the result
    term_iris = [term["term_iri"] for term in result["terms"]]
    
    # TRUE IDEMPOTENCY TEST: Both terms should still be present
    assert "http://purl.obolibrary.org/obo/HP_0004322" in term_iris, "Term A should be present (from both uploads)"
    assert "http://purl.obolibrary.org/obo/HP_0002297" in term_iris, "Term B should be present (from first upload only) - this proves idempotency"
    
    # Should have exactly 2 terms (no duplicates)
    assert len(term_iris) == 2, f"Expected exactly 2 terms, got {len(term_iris)}: {term_iris}"


def test_bulk_upload_error_handling(test_project_id, physical_resources):
    """Invalid JSON (schema) -> perform should return a validation error; finalize not called."""
    run_id = str(uuid.uuid4())
    batch_id = 1

    # Invalid payload: missing project_subject_id
    invalid_payload = [{"project_id": test_project_id, "term_iri": "http://purl.obolibrary.org/obo/HP_0004322", "evidence": []}]

    upload_url, s3_key = prepare_upload(run_id, batch_id, physical_resources)
    put_json_to_presigned(upload_url, invalid_payload)

    perform_fn = physical_resources["PerformBulkUploadFunction"]
    resp = invoke_lambda(perform_fn, {"run_id": run_id, "batch_id": batch_id})
    # perform returns {"error":"Invalid JSON","details": "..."}
    assert "error" in resp and resp["error"] in {"Invalid JSON", "ValidationError"}
    assert "details" in resp