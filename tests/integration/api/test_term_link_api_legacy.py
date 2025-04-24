import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_subject_term_link(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create subject
    create_resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_id": test_project_id, "project_subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200

    # Submit evidence payload
    payload = {
        "evidence_list": [
            {
                "project_id": test_project_id,
                "project_subject_id": subject_id,
                "term_iri": term_iri,
                "evidence": {
                    "creator": "pytest",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000001",
                    "assertion_method": "http://purl.obolibrary.org/obo/ECO_0000218",
                    "evidence_text": "API test for subject-term link",
                },
            }
        ]
    }

    link_resp = requests.post(
        f"{api_base_url}/subject-term-link", json=payload, auth=sigv4_auth
    )

    assert link_resp.status_code == 200, f"Unexpected response: {link_resp.text}"
    body = link_resp.json()
    assert "subject_iri" in body
    assert body["term_iri"] == term_iri
    assert "link_id" in body
    assert isinstance(body["link_created"], bool)


def test_subject_term_link_missing_subject(api_base_url, sigv4_auth):
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    payload = {
        "evidence_list": [
            {
                "project_id": "nonexistent-project",
                "project_subject_id": f"subject-{uuid.uuid4().hex[:8]}",
                "term_iri": term_iri,
                "evidence": {
                    "creator": "pytest",
                    "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000001",
                    "assertion_method": "http://purl.obolibrary.org/obo/ECO_0000218",
                },
            }
        ]
    }

    resp = requests.post(
        f"{api_base_url}/subject-term-link", json=payload, auth=sigv4_auth
    )

    assert resp.status_code == 400
    body = resp.json()
    assert "error" in body
    assert "Subject does not exist" in body["error"]


def test_subject_term_link_missing_evidence(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create subject
    create_resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_id": test_project_id, "project_subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200

    # Omit 'evidence' block
    payload = {
        "evidence_list": [
            {
                "project_id": test_project_id,
                "project_subject_id": subject_id,
                "term_iri": term_iri,
            }
        ]
    }

    resp = requests.post(
        f"{api_base_url}/subject-term-link", json=payload, auth=sigv4_auth
    )

    assert resp.status_code == 400
    body = resp.json()
    assert "error" in body
    assert "No evidence element" in body["error"]
