import requests
import uuid
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_get_existing_source(api_base_url, sigv4_auth, update_hpo):
    """Ensure a known source returns metadata"""
    response = requests.get(f"{api_base_url}/source/hpo", auth=sigv4_auth)

    assert response.status_code == 200, f"Expected 200 but got {response.status_code}"
    body = response.json()
    assert "PK" in body and body["PK"] == "SOURCE~hpo"


def test_get_nonexistent_source_returns_404(api_base_url, sigv4_auth):
    """Expect 404 for a source name that doesn't exist"""
    fake_source = f"nonexistent-{uuid.uuid4().hex[:8]}"

    response = requests.get(f"{api_base_url}/source/{fake_source}", auth=sigv4_auth)

    assert response.status_code == 404, f"Expected 404 but got {response.status_code}"
    body = response.json()
    assert "error" in body
    assert fake_source in body["error"]
