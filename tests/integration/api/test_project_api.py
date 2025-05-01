import uuid
import requests
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_new_project(api_base_url, sigv4_auth):
    project_id = f"test-project-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Integration Test Project"}

    resp = requests.post(f"{api_base_url}/project/create", json=payload, auth=sigv4_auth)

    assert resp.status_code == 200, (
        f"Unexpected status: {resp.status_code} - {resp.text}"
    )
    body = resp.json()
    assert "project_created" in body
    assert (
        body["project_created"] is True or body["project_created"] is False
    )
    assert "already exists" not in body["message"]


def test_create_duplicate_project(api_base_url, sigv4_auth):
    project_id = f"dupe-project-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Dupe Project"}

    # First request
    resp1 = requests.post(f"{api_base_url}/project/create", json=payload, auth=sigv4_auth)
    assert resp1.status_code == 200
    assert resp1.json().get("project_created") is True

    # Second request (should detect duplicate)
    resp2 = requests.post(f"{api_base_url}/project/create", json=payload, auth=sigv4_auth)
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2.get("project_created") is False
    assert "already exists" in body2.get("message", "").lower()


def test_create_and_remove_project(api_base_url, sigv4_auth):
    # Create a unique test project
    project_id = f"remove-project-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Temporary Test Project"}

    # Create the project
    create_resp = requests.post(
        f"{api_base_url}/project/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, f"Create failed: {create_resp.text}"
    assert create_resp.json().get("project_created") is True

    # Remove the project
    remove_resp = requests.post(
        f"{api_base_url}/project/remove",
        json={"project_id": project_id},
        auth=sigv4_auth,
    )

    assert remove_resp.status_code == 200, f"Remove failed: {remove_resp.text}"

    # Make sure the response is a success
    assert "successfully removed" in remove_resp.text
