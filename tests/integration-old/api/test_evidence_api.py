import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_evidence(api_base_url, sigv4_auth):
    subject_id = f"test-subject-{uuid.uuid4()}"
    payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "creator_id": "api-test-user",  # Required field
        "creator_name": "API Test User",
        "creator_type": "human",
        "evidence_type": "manual_annotation",
        "span_start": 5,
        "span_end": 20,
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 201, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    evidence_id = create_body["evidence_id"]
    assert evidence_id is not None

    # --- Delete ---
    delete_resp = requests.post(
        f"{api_base_url}/evidence/remove",
        json={"evidence_id": evidence_id},
        auth=sigv4_auth,
    )
    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert "Evidence deleted successfully" in delete_body["message"]


def test_get_evidence(api_base_url, sigv4_auth):
    subject_id = f"test-subject-{uuid.uuid4()}"
    payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "creator_id": "api-test-creator",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "span_start": 3,
        "span_end": 9,
        "qualifiers": ["negated"],
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 201, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    evidence_id = create_resp.json()["evidence_id"]
    print(f"Created evidence: {evidence_id}")

    # --- Get ---
    get_resp = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": evidence_id},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200, (
        f"Unexpected get status: {get_resp.status_code} - {get_resp.text}"
    )

    body = get_resp.json()
    print(body)
    assert body["evidence_id"] == evidence_id
    assert body["subject_id"] == subject_id
    assert body["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001250"
    assert body["evidence_type"] == "clinical_note"
    assert body["creator"]["creator_id"] == "api-test-creator"
    assert body["text_annotation"]["span_start"] == 3
    assert body["text_annotation"]["span_end"] == 9
    assert "negated" in body["qualifiers"]


def test_evidence_validation(api_base_url, sigv4_auth):
    """Test evidence creation with missing required fields"""
    
    # Test missing subject_id
    payload = {
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "creator_id": "test-creator"
    }
    resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert resp.status_code == 400
    assert "Missing required fields" in resp.json()["message"]

    # Test missing term_iri
    payload = {
        "subject_id": "test-subject",
        "creator_id": "test-creator"
    }
    resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert resp.status_code == 400
    assert "Missing required fields" in resp.json()["message"]

    # Test missing creator_id
    payload = {
        "subject_id": "test-subject",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250"
    }
    resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert resp.status_code == 400
    assert "Missing required fields" in resp.json()["message"]


def test_get_nonexistent_evidence(api_base_url, sigv4_auth):
    """Test getting evidence that doesn't exist"""
    resp = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": "nonexistent-evidence-id"},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404
    assert "Evidence not found" in resp.json()["message"]


def test_delete_nonexistent_evidence(api_base_url, sigv4_auth):
    """Test deleting evidence that doesn't exist"""
    resp = requests.post(
        f"{api_base_url}/evidence/remove",
        json={"evidence_id": "nonexistent-evidence-id"},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404
    assert "Evidence not found" in resp.json()["message"]


def test_evidence_with_term_source(api_base_url, sigv4_auth):
    """Test evidence creation and retrieval with term_source field"""
    subject_id = f"test-subject-{uuid.uuid4()}"
    payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "creator_id": "api-test-user",
        "creator_type": "human",
        "evidence_type": "manual_annotation",
        "term_source": {
            "source": "hpo",
            "version": "2024-01-01",
            "iri": "http://purl.obolibrary.org/obo/hp.owl"
        }
    }

    # Create evidence with term_source
    create_resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 201, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    evidence_id = create_resp.json()["evidence_id"]

    # Get evidence and verify term_source is present
    get_resp = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": evidence_id},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200

    body = get_resp.json()
    assert "term_source" in body
    term_source = body["term_source"]
    assert term_source["source"] == "hpo"
    assert term_source["version"] == "2024-01-01"
    assert term_source["iri"] == "http://purl.obolibrary.org/obo/hp.owl"


def test_evidence_without_term_source(api_base_url, sigv4_auth):
    """Test evidence creation without term_source field (optional)"""
    subject_id = f"test-subject-{uuid.uuid4()}"
    payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
        "creator_id": "api-test-user",
        "creator_type": "human",
        "evidence_type": "manual_annotation"
    }

    # Create evidence without term_source
    create_resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 201, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    evidence_id = create_resp.json()["evidence_id"]

    # Get evidence and verify term_source is not present
    get_resp = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": evidence_id},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200

    body = get_resp.json()
    assert "term_source" not in body or body.get("term_source") is None


def test_evidence_partial_term_source(api_base_url, sigv4_auth):
    """Test evidence creation with partial term_source data"""
    subject_id = f"test-subject-{uuid.uuid4()}"
    payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/MONDO_0005148",
        "creator_id": "api-test-user",
        "creator_type": "human",
        "evidence_type": "manual_annotation",
        "term_source": {
            "source": "mondo"
            # version and iri omitted
        }
    }

    # Create evidence with partial term_source
    create_resp = requests.post(
        f"{api_base_url}/evidence/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 201, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    evidence_id = create_resp.json()["evidence_id"]

    # Get evidence and verify partial term_source is present
    get_resp = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": evidence_id},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200

    body = get_resp.json()
    assert "term_source" in body
    term_source = body["term_source"]
    assert term_source["source"] == "mondo"
    # version and iri should be None or empty
    assert term_source.get("version") is None or term_source.get("version") == ""
    assert term_source.get("iri") is None or term_source.get("iri") == ""
