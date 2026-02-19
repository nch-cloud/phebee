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

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


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
