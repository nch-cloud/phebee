import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_term_link(api_base_url, sigv4_auth):
    subject_id = f"test-subject-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    creator_id = f"test-creator-{uuid.uuid4()}"

    payload = {
        "subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": creator_id,
        "qualifiers": []
    }

    # --- Create TermLink ---
    create_resp = requests.post(
        f"{api_base_url}/term-link/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    termlink_iri = create_body["termlink_iri"]
    # TermLink IRI should be based on subject_id
    expected_source_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    assert termlink_iri.startswith(expected_source_iri + "/term-link/")

    # --- Delete TermLink ---
    delete_resp = requests.post(
        f"{api_base_url}/term-link/remove",
        json={"termlink_iri": termlink_iri},
        auth=sigv4_auth,
    )
    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert delete_body["termlink_iri"] == termlink_iri


def test_get_term_link(api_base_url, sigv4_auth):
    source_node_iri = "http://example.org/phebee/subject/test-subject"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    subject_id = f"test-subject-{uuid.uuid4()}"
    creator_id = f"test-creator-{uuid.uuid4()}"

    payload = {
        "subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": creator_id,
        "qualifiers": []
    }

    # --- Create TermLink ---
    create_resp = requests.post(
        f"{api_base_url}/term-link/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200
    termlink_iri = create_resp.json()["termlink_iri"]

    # --- Get TermLink ---
    get_resp = requests.post(
        f"{api_base_url}/term-link",
        json={"termlink_iri": termlink_iri},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200

    body = get_resp.json()
    print(f"test_term_link_api body: {body}")
    assert body["termlink_iri"] == termlink_iri
    assert body["term_iri"] == term_iri
