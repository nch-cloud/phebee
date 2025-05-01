import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_subject(api_base_url, sigv4_auth):
    project_id = f"test-proj-{uuid.uuid4().hex[:6]}"

    # --- Create the project first ---
    create_project_resp = requests.post(
        f"{api_base_url}/project/create",
        json={"project_id": project_id, "project_label": "Test Project"},
        auth=sigv4_auth,
    )
    assert create_project_resp.status_code == 200

    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create the subject ---
    payload = {"project_id": project_id, "project_subject_id": project_subject_id}

    resp = requests.post(
        f"{api_base_url}/subject/create", json=payload, auth=sigv4_auth
    )

    assert resp.status_code == 200, (
        f"Unexpected status: {resp.status_code} - {resp.text}"
    )

    body = resp.json()
    assert body["subject_created"] is True
    assert "subject" in body
    assert "iri" in body["subject"]
    assert body["subject"]["projects"][project_id] == project_subject_id


def test_link_existing_subject(api_base_url, sigv4_auth):
    project_id = f"test-proj-{uuid.uuid4().hex[:6]}"
    subj_id_1 = f"test-subj-{uuid.uuid4().hex[:6]}"
    subj_id_2 = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create the project first ---
    create_project_resp = requests.post(
        f"{api_base_url}/project/create",
        json={"project_id": project_id, "project_label": "Test Project"},
        auth=sigv4_auth,
    )
    assert create_project_resp.status_code == 200

    # --- First subject creation ---
    create_resp_1 = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id_1},
        auth=sigv4_auth,
    )
    assert create_resp_1.status_code == 200
    first_subject = create_resp_1.json()["subject"]
    subject_iri = first_subject["iri"]

    # --- Link existing subject to a new project_subject_iri ---
    create_resp_2 = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id_2,
            "known_subject_iri": subject_iri,
        },
        auth=sigv4_auth,
    )

    assert create_resp_2.status_code == 200
    body = create_resp_2.json()
    assert body["subject_created"] is False
    assert body["subject"]["iri"] == subject_iri
    assert body["subject"]["projects"][project_id] == subj_id_2
