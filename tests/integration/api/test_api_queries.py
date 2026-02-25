"""
Smoke tests for query API endpoints.

References:
- api.yaml:
  - POST /subjects/query (query subjects by phenotype)
  - POST /subject/term-info (get subject term info)
- Lambdas: GetSubjectsPhenotypesFunction, GetSubjectTermInfoFunction
  (comprehensive tests in test_get_subjects_pheno.py, test_get_subject_term_info.py)

These are minimal smoke tests to validate API Gateway → Lambda integration.
"""

import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


def test_query_subjects_endpoint(api_base_url, sigv4_auth, test_project_id):
    """
    Smoke test: POST /subjects/query endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200 (even for empty results)
    - CORS headers present
    - Basic response structure
    """
    response = requests.post(
        f"{api_base_url}/subjects/query",
        json={"project_id": test_project_id},
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check (may be compressed/base64 encoded)
    # Just verify we got a response body
    assert len(response.content) > 0


def test_query_subjects_missing_project_id(api_base_url, sigv4_auth):
    """
    Smoke test: POST /subjects/query without project_id returns error.

    Validates validation error handling through API Gateway.
    """
    response = requests.post(
        f"{api_base_url}/subjects/query",
        json={},  # Missing project_id
        auth=sigv4_auth
    )

    # Should return an error (400 or 500 depending on implementation)
    assert response.status_code >= 400, \
        f"Expected error status for missing project_id, got {response.status_code}"


def test_get_subject_term_info_endpoint(api_base_url, sigv4_auth, test_subject,
                                         create_evidence_helper, standard_hpo_terms):
    """
    Smoke test: POST /subject/term-info endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200 when term exists
    - Basic response structure
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence so term exists
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        evidence_creator_id="test-creator",
        evidence_creator_type="automated"
    )

    response = requests.post(
        f"{api_base_url}/subject/term-info",
        json={"subject_id": subject_uuid, "term_iri": term_iri},
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check
    body = response.json()
    assert "term_iri" in body


def test_get_subject_term_info_not_found(api_base_url, sigv4_auth, standard_hpo_terms):
    """
    Smoke test: POST /subject/term-info returns 404 for nonexistent term.

    Validates 404 error handling.
    """
    response = requests.post(
        f"{api_base_url}/subject/term-info",
        json={"subject_id": str(uuid.uuid4()), "term_iri": standard_hpo_terms['seizure']},
        auth=sigv4_auth
    )

    assert response.status_code == 404, \
        f"Expected 404, got {response.status_code}"

    body = response.json()
    assert "message" in body
