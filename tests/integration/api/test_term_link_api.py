import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_term_link(api_base_url, sigv4_auth):
    source_node_iri = "http://example.org/phebee/subject/test-subject"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    creator_iri = "http://ods.nationwidechildrens.org/phebee/creator/test-creator"
    evidence_iris = ["http://example.org/phebee/annotation/example-evidence"]

    payload = {
        "source_node_iri": source_node_iri,
        "term_iri": term_iri,
        "creator_iri": creator_iri,
        "evidence_iris": evidence_iris,
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
    assert termlink_iri.startswith(source_node_iri + "/term-link/")

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
    creator_iri = "http://ods.nationwidechildrens.org/phebee/creator/test-creator"

    payload = {
        "source_node_iri": source_node_iri,
        "term_iri": term_iri,
        "creator_iri": creator_iri,
        "evidence_iris": [],
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
