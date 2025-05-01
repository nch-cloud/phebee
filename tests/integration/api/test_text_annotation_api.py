import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_and_delete_text_annotation(api_base_url, sigv4_auth):
    text_source_iri = "http://example.org/phebee/text-source/test-source"
    payload = {
        "text_source_iri": text_source_iri,
        "span_start": 5,
        "span_end": 20,
        "metadata": '{"source": "API", "version": "1.0.0"}',
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/text-annotation/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    create_body = create_resp.json()
    annotation_iri = create_body["annotation_iri"]
    assert annotation_iri.startswith(text_source_iri + "/annotation/")

    # --- Delete ---
    delete_resp = requests.post(
        f"{api_base_url}/text-annotation/remove",
        json={"annotation_iri": annotation_iri},
        auth=sigv4_auth,
    )
    assert delete_resp.status_code == 200, (
        f"Unexpected delete status: {delete_resp.status_code} - {delete_resp.text}"
    )

    delete_body = delete_resp.json()
    assert delete_body["annotation_iri"] == annotation_iri


def test_get_text_annotation(api_base_url, sigv4_auth):
    text_source_iri = "http://example.org/phebee/text-source/test-source"
    payload = {
        "text_source_iri": text_source_iri,
        "span_start": 3,
        "span_end": 9,
        "metadata": '{"note": "via API test"}',
    }

    # --- Create ---
    create_resp = requests.post(
        f"{api_base_url}/text-annotation/create", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200, (
        f"Unexpected create status: {create_resp.status_code} - {create_resp.text}"
    )

    annotation_iri = create_resp.json()["annotation_iri"]

    # --- Get ---
    get_resp = requests.post(
        f"{api_base_url}/text-annotation",
        json={"annotation_iri": annotation_iri},
        auth=sigv4_auth,
    )
    assert get_resp.status_code == 200, (
        f"Unexpected get status: {get_resp.status_code} - {get_resp.text}"
    )

    body = get_resp.json()
    assert body["annotation_iri"] == annotation_iri
    assert body.get("span_start") == "3"
    assert body.get("span_end") == "9"
