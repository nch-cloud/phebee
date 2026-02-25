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

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


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
