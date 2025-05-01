import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_encounter(api_base_url, sigv4_auth, create_test_subject):
    encounter_id = f"test-{uuid.uuid4().hex[:8]}"
    payload = {"subject_iri": create_test_subject()["iri"], "encounter_id": encounter_id}

    # --- Create Encounter ---
    create_resp = requests.post(
        f"{api_base_url}/encounter/create", json=payload, auth=sigv4_auth
    )

    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    assert "encounter_iri" in create_body
    assert create_body["encounter_iri"].endswith(encounter_id)

    # --- Delete Encounter ---
    delete_resp = requests.post(
        f"{api_base_url}/encounter/remove", json=payload, auth=sigv4_auth
    )

    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert delete_body["encounter_iri"] == create_body["encounter_iri"]


def test_get_encounter(api_base_url, sigv4_auth, create_test_subject):
    encounter_id = f"test-{uuid.uuid4().hex[:8]}"
    test_subject_iri = create_test_subject()["iri"]
    payload = {"subject_iri": test_subject_iri, "encounter_id": encounter_id}

    # --- Create Encounter ---
    create_resp = requests.post(
        f"{api_base_url}/encounter/create", json=payload, auth=sigv4_auth
    )

    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    # --- Get Encounter ---
    get_resp = requests.post(f"{api_base_url}/encounter", json=payload, auth=sigv4_auth)

    assert get_resp.status_code == 200, (
        f"Unexpected get status: {get_resp.status_code} - {get_resp.text}"
    )

    body = get_resp.json()
    assert body["encounter_iri"].endswith(encounter_id)
    assert body["subject_iri"] == test_subject_iri
    assert body["encounter_id"] == encounter_id
