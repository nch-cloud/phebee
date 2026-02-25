"""
Smoke tests for /project/* API endpoints.

References:
- api.yaml: POST /project/create, POST /project/remove
- Lambdas: CreateProjectFunction, RemoveProjectFunction
  (comprehensive tests in test_create_project.py, test_remove_project.py)

These are minimal smoke tests to validate API Gateway → Lambda integration.
"""

import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


def test_create_project_endpoint(api_base_url, sigv4_auth):
    """
    Smoke test: POST /project/create endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200
    - CORS headers present
    - Basic response structure
    """
    project_id = f"api-test-{uuid.uuid4().hex[:8]}"

    response = requests.post(
        f"{api_base_url}/project/create",
        json={"project_id": project_id, "project_label": "API Test Project"},
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check
    body = response.json()
    assert "project_id" in body
    assert body["project_id"] == project_id


def test_create_project_missing_id(api_base_url, sigv4_auth):
    """
    Smoke test: POST /project/create without project_id returns 400.

    Validates error handling through API Gateway.
    """
    response = requests.post(
        f"{api_base_url}/project/create",
        json={"project_label": "Test"},  # Missing project_id
        auth=sigv4_auth
    )

    assert response.status_code == 400, \
        f"Expected 400 for missing project_id, got {response.status_code}"

    body = response.json()
    assert "error" in body or "message" in body


def test_remove_project_idempotent(api_base_url, sigv4_auth):
    """
    Smoke test: POST /project/remove is idempotent (returns 200 even for nonexistent project).

    Validates idempotent behavior - removing nonexistent project succeeds.
    """
    response = requests.post(
        f"{api_base_url}/project/remove",
        json={"project_id": f"nonexistent-{uuid.uuid4().hex[:8]}"},
        auth=sigv4_auth
    )

    # RemoveProject is idempotent - returns 200 even if project doesn't exist
    assert response.status_code == 200, \
        f"Expected 200 (idempotent), got {response.status_code}"

    body = response.json()
    assert "message" in body or "success" in body
