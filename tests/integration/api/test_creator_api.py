import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


@pytest.mark.parametrize("creator_type,extra", [
    ("human", {"name": "Jane Doe"}),
    ("automated", {"name": "PheBeeBot", "version": "1.0.0"})
])
def test_create_and_delete_creator(api_base_url, sigv4_auth, creator_type, extra):
    creator_id = f"test-{creator_type}-{uuid.uuid4().hex[:6]}"
    payload = {
        "creator_id": creator_id,
        "creator_type": creator_type,
        **extra
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/creator/create",
        json=payload,
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    assert "creator_iri" in create_body
    assert create_body["creator_iri"].endswith(creator_id)

    # --- Delete ---
    delete_resp = requests.post(
        f"{api_base_url}/creator/remove",
        json={"creator_id": creator_id},
        auth=sigv4_auth,
    )
    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert delete_body["creator_iri"].endswith(creator_id)


def test_get_creator(api_base_url, sigv4_auth):
    creator_id = f"test-human-{uuid.uuid4().hex[:6]}"
    payload = {
        "creator_id": creator_id,
        "creator_type": "human",
        "name": "Test User"
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/creator/create",
        json=payload,
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    # --- Get ---
    get_resp = requests.post(
        f"{api_base_url}/creator",
        json={"creator_id": creator_id},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200, (
        f"Unexpected get status: {get_resp.status_code} - {get_resp.text}"
    )

    body = get_resp.json()
    assert body["creator_id"] == creator_id
    assert isinstance(body["properties"], dict)
    assert body["properties"].get("title") == "Test User"