import uuid
import pytest
import requests

@pytest.fixture
def test_project(api_base_url, sigv4_auth):
    project_id = f"test-proj-{uuid.uuid4().hex[:6]}"

    # Create the project
    create_resp = requests.post(
        f"{api_base_url}/project/create",
        json={"project_id": project_id, "project_label": "Test Project"},
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200

    yield project_id

    # Cleanup (optional if your API supports project deletion)
    requests.post(
        f"{api_base_url}/project/remove",
        json={"project_id": project_id},
        auth=sigv4_auth,
    )