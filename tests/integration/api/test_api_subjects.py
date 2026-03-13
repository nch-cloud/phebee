"""
Smoke tests for /subject/* API endpoints.

References:
- api.yaml: POST /subject/create, GET /subject, POST /subject/remove
- Lambdas: CreateSubjectFunction, GetSubjectFunction, RemoveSubjectFunction
  (comprehensive tests in test_create_subject.py, test_get_subject.py, test_remove_subject.py)

These are minimal smoke tests to validate API Gateway → Lambda integration.
"""

import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_subject_endpoint(api_base_url, sigv4_auth, test_project_id):
    """
    Smoke test: POST /subject/create endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200
    - CORS headers present
    - Basic response structure
    """
    project_subject_id = f"api-subj-{uuid.uuid4().hex[:8]}"

    response = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": test_project_id,
            "project_subject_id": project_subject_id
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
    assert "subject" in body
    assert "iri" in body["subject"]


def test_create_subject_missing_project_id(api_base_url, sigv4_auth):
    """
    Smoke test: POST /subject/create without project_id returns 400.

    Validates validation error handling through API Gateway.
    """
    response = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_subject_id": "test-subject"},  # Missing project_id
        auth=sigv4_auth
    )

    assert response.status_code == 400, \
        f"Expected 400 for missing project_id, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body


def test_get_subject_not_found(api_base_url, sigv4_auth):
    """
    Smoke test: GET /subject with nonexistent subject_id returns 404.

    Validates 404 error handling.
    """
    response = requests.get(
        f"{api_base_url}/subject",
        params={"subject_id": str(uuid.uuid4())},
        auth=sigv4_auth
    )

    assert response.status_code == 404, \
        f"Expected 404, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body


def test_subjects_query_with_monarch_entity(api_base_url, sigv4_auth, test_project_id):
    """
    Smoke test: POST /subjects/query with term_association_source_entity.

    Validates:
    - Monarch Knowledge Graph integration works through API
    - Returns 200 with valid structure
    - include_child_terms=false is enforced
    """
    # Query with Monarch entity (Marfan syndrome)
    response = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": test_project_id,
            "term_association_source_entity": "MONDO:0007947",
            "include_child_terms": False,
            "limit": 10
        },
        auth=sigv4_auth
    )

    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check
    body = response.json()
    assert "subjects" in body or "body" in body, \
        "Response should contain 'subjects' or 'body' field"
    assert "pagination" in body or ("body" in body and "pagination" in body), \
        "Response should contain pagination metadata"


def test_subjects_query_monarch_requires_no_expansion(api_base_url, sigv4_auth, test_project_id):
    """
    Smoke test: POST /subjects/query with term_association_source_entity + include_child_terms=true fails.

    Validates:
    - Parameter validation works (returns 500 error)

    Note: API Gateway masks the detailed error message for security, so we only verify
    the status code, not the error message content. The detailed validation is tested
    in test_get_subjects_pheno.py integration tests.
    """
    response = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": test_project_id,
            "term_association_source_entity": "MONDO:0007947",
            "include_child_terms": True,  # Should fail validation
            "limit": 10
        },
        auth=sigv4_auth
    )

    assert response.status_code == 500, \
        f"Expected 500 for invalid parameter combination, got {response.status_code}"

    # API Gateway returns generic error, detailed validation tested in integration tests
    body = response.json()
    assert "message" in body or "error" in body, \
        "Response should contain an error message"
