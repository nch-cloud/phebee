import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_clinical_note(api_base_url, sigv4_auth, create_test_encounter_iri):
    clinical_note_id = f"note-{uuid.uuid4().hex[:8]}"
    payload = {
        "encounter_iri": create_test_encounter_iri(),
        "clinical_note_id": clinical_note_id,
        "note_timestamp": "2023-01-01T10:00:00Z"
    }

    # --- Create ClinicalNote ---
    create_resp = requests.post(
        f"{api_base_url}/clinical-note/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    assert "clinical_note_iri" in create_body
    assert create_body["clinical_note_iri"].endswith(clinical_note_id)

    # --- Delete ClinicalNote ---
    delete_resp = requests.post(
        f"{api_base_url}/clinical-note/remove", json=payload, auth=sigv4_auth
    )
    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert delete_body["clinical_note_iri"] == create_body["clinical_note_iri"]


def test_get_clinical_note(api_base_url, sigv4_auth, create_test_encounter_iri):
    clinical_note_id = f"note-{uuid.uuid4().hex[:8]}"
    encounter_iri = create_test_encounter_iri()
    payload = {
        "encounter_iri": encounter_iri,
        "clinical_note_id": clinical_note_id,
        "note_timestamp": "2023-01-01T10:00:00Z"
    }

    # --- Create ClinicalNote ---
    create_resp = requests.post(
        f"{api_base_url}/clinical-note/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    # --- Get ClinicalNote ---
    get_resp = requests.post(
        f"{api_base_url}/clinical-note", json=payload, auth=sigv4_auth
    )
    assert get_resp.status_code == 200, (
        f"Unexpected get status: {get_resp.status_code} - {get_resp.text}"
    )

    body = get_resp.json()
    assert body["clinical_note_iri"].endswith(clinical_note_id)
    assert body["encounter_iri"] == encounter_iri
    assert body["clinical_note_id"] == clinical_note_id