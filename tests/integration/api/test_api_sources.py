"""
Smoke tests for /source API endpoints.

References:
- api.yaml: GET /source/{source_id}
- Lambda: GetSourceInfoFunction (comprehensive tests in test_get_source_info.py)

These are minimal smoke tests to validate API Gateway → Lambda integration,
not comprehensive business logic tests.
"""

import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


def test_get_source_endpoint_success(api_base_url, sigv4_auth, hpo_update_execution):
    """
    Smoke test: GET /source/{source_id} endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works (SigV4)
    - Returns 200 for known source
    - Basic response structure is valid

    Note: Depends on hpo_update_execution fixture to seed HPO source data
    """
    if not hpo_update_execution:
        pytest.skip("HPO update failed or was skipped - cannot test source endpoint")

    response = requests.get(
        f"{api_base_url}/source/hpo",
        auth=sigv4_auth
    )

    # Validate API Gateway layer
    assert response.status_code == 200, \
        f"Expected 200 but got {response.status_code}: {response.text}"

    # Check content type
    assert response.headers["Content-Type"] == "application/json"

    # Basic structure check (not comprehensive validation)
    body = response.json()
    assert "PK" in body, "Response missing PK field"
    assert body["PK"] == "SOURCE~hpo", \
        f"Expected PK=SOURCE~hpo, got {body.get('PK')}"


def test_get_source_endpoint_not_found(api_base_url, sigv4_auth):
    """
    Smoke test: GET /source/{source_id} returns 404 for nonexistent source.

    Validates:
    - Error handling through API Gateway
    - 404 status code is correct
    - Error response structure
    """
    response = requests.get(
        f"{api_base_url}/source/nonexistent-source-12345",
        auth=sigv4_auth
    )

    assert response.status_code == 404, \
        f"Expected 404 but got {response.status_code}"

    # Basic error structure check
    body = response.json()
    assert "error" in body or "message" in body, \
        "Error response should contain 'error' or 'message' field"

    # Verify error message mentions the source
    error_text = str(body).lower()
    assert "nonexistent-source-12345" in error_text or "not found" in error_text, \
        f"Error message should indicate source not found: {body}"


