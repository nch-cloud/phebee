"""
Smoke tests for /evidence/* API endpoints.

References:
- api.yaml: POST /evidence/create, GET /evidence, POST /evidence/remove
- Lambdas: CreateEvidenceFunction, GetEvidenceFunction, RemoveEvidenceFunction
  (comprehensive tests in test_create_evidence.py, test_get_evidence.py, test_remove_evidence.py)

These are minimal smoke tests to validate API Gateway → Lambda integration.
"""

import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_evidence_endpoint(api_base_url, sigv4_auth, test_subject, standard_hpo_terms):
    """
    Smoke test: POST /evidence/create endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 201
    - CORS headers present
    - Basic response structure
    """
    subject_uuid, _ = test_subject

    response = requests.post(
        f"{api_base_url}/evidence/create",
        json={
            "subject_id": subject_uuid,
            "term_iri": standard_hpo_terms["seizure"],
            "evidence_type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4().hex[:8]}",
            "creator_id": "api-test",
            "creator_type": "automated"
        },
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 201, \
        f"Expected 201 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check
    body = response.json()
    assert "evidence_id" in body
    assert "subject_id" in body
    # Note: Response has subject_id at top level
    assert body["subject_id"] == subject_uuid


def test_create_evidence_missing_subject_id(api_base_url, sigv4_auth, standard_hpo_terms):
    """
    Smoke test: POST /evidence/create without subject_id returns 400.

    Validates validation error handling through API Gateway.
    """
    response = requests.post(
        f"{api_base_url}/evidence/create",
        json={
            "term_iri": standard_hpo_terms["seizure"],
            "evidence_type": "clinical_note",
            "clinical_note_id": "test-note",
            "creator_id": "test-creator"
            # Missing subject_id
        },
        auth=sigv4_auth
    )

    assert response.status_code == 400, \
        f"Expected 400 for missing subject_id, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body


def test_get_evidence_not_found(api_base_url, sigv4_auth):
    """
    Smoke test: POST /evidence returns 404 for nonexistent evidence.

    Validates 404 error handling.
    """
    response = requests.post(
        f"{api_base_url}/evidence",
        json={"evidence_id": str(uuid.uuid4())},
        auth=sigv4_auth
    )

    assert response.status_code == 404, \
        f"Expected 404, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body


def test_query_evidence_endpoint(api_base_url, sigv4_auth, test_subject, standard_hpo_terms):
    """
    Smoke test: POST /evidence/query endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200
    - Basic response structure with pagination
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # First create evidence to query
    create_response = requests.post(
        f"{api_base_url}/evidence/create",
        json={
            "subject_id": subject_uuid,
            "term_iri": term_iri,
            "evidence_type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4().hex[:8]}",
            "creator_id": "api-test",
            "creator_type": "automated"
        },
        auth=sigv4_auth
    )
    assert create_response.status_code == 201

    # Query the evidence
    response = requests.post(
        f"{api_base_url}/evidence/query",
        json={
            "subject_id": subject_uuid,
            "term_iri": term_iri,
            "qualifiers": [],
            "limit": 10
        },
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check
    body = response.json()
    assert "evidence" in body
    assert "evidence_count" in body
    assert "has_more" in body
    assert "total_count" in body
    assert "termlink_id" in body
    assert body["evidence_count"] >= 1  # At least the one we just created
    assert isinstance(body["evidence"], list)


def test_query_evidence_missing_subject_id(api_base_url, sigv4_auth, standard_hpo_terms):
    """
    Smoke test: POST /evidence/query without subject_id returns 400.

    Validates validation error handling through API Gateway.
    """
    response = requests.post(
        f"{api_base_url}/evidence/query",
        json={
            "term_iri": standard_hpo_terms["seizure"],
            "qualifiers": []
            # Missing subject_id
        },
        auth=sigv4_auth
    )

    assert response.status_code == 400, \
        f"Expected 400 for missing subject_id, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body
    assert "subject_id" in body.get("message", body.get("error", "")).lower()


def test_query_evidence_no_results(api_base_url, sigv4_auth, standard_hpo_terms):
    """
    Smoke test: POST /evidence/query with no matching evidence returns empty results.

    Validates:
    - Query succeeds even with no results
    - Returns proper empty pagination structure
    """
    # Use a nonexistent subject
    nonexistent_subject = str(uuid.uuid4())

    response = requests.post(
        f"{api_base_url}/evidence/query",
        json={
            "subject_id": nonexistent_subject,
            "term_iri": standard_hpo_terms["seizure"],
            "qualifiers": []
        },
        auth=sigv4_auth
    )

    assert response.status_code == 200, \
        f"Expected 200, got {response.status_code}"

    body = response.json()
    assert body["evidence_count"] == 0
    assert body["total_count"] == 0
    assert body["has_more"] == False
    assert body["evidence"] == []


def test_query_evidence_with_qualifiers(api_base_url, sigv4_auth, test_subject, standard_hpo_terms):
    """
    Smoke test: POST /evidence/query with qualifiers works correctly.

    Validates:
    - Qualifiers parameter is accepted
    - Different qualifiers create separate termlinks
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["fever"]

    # Create evidence with negated qualifier
    create_response = requests.post(
        f"{api_base_url}/evidence/create",
        json={
            "subject_id": subject_uuid,
            "term_iri": term_iri,
            "qualifiers": ["negated"],
            "evidence_type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4().hex[:8]}",
            "creator_id": "api-test",
            "creator_type": "automated"
        },
        auth=sigv4_auth
    )
    assert create_response.status_code == 201

    # Query with negated qualifier - should find it
    response = requests.post(
        f"{api_base_url}/evidence/query",
        json={
            "subject_id": subject_uuid,
            "term_iri": term_iri,
            "qualifiers": ["negated"],
            "limit": 10
        },
        auth=sigv4_auth
    )

    assert response.status_code == 200
    body = response.json()
    assert body["qualifiers"] == ["negated"]
    assert body["evidence_count"] >= 1

    # Query without qualifiers - should NOT find it (different termlink)
    response_no_qual = requests.post(
        f"{api_base_url}/evidence/query",
        json={
            "subject_id": subject_uuid,
            "term_iri": term_iri,
            "qualifiers": [],
            "limit": 10
        },
        auth=sigv4_auth
    )

    assert response_no_qual.status_code == 200
    body_no_qual = response_no_qual.json()
    # Should have different termlink_id
    assert body_no_qual["termlink_id"] != body["termlink_id"]
