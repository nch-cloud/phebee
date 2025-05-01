import uuid
import json
import pytest
import requests
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration, pytest.mark.api]

@pytest.mark.usefixtures("update_hpo")
def test_subjects_query_with_term_and_evidence(
    api_base_url,
    sigv4_auth,
    physical_resources,
    create_test_subject
):
    lambda_client = get_client("lambda")

    # --- Create subject ---
    subject = create_test_subject()
    
    subject_iri = subject["iri"]
    project_id = next(iter(subject["projects"].keys()))
    project_subject_id = subject["projects"][project_id]

    print(f"subject: {subject}")
    print(f"project_id: {project_id}")
    print(f"project_subject_id: {project_subject_id}")

    # --- Create creator ---
    creator_id = f"test-creator-{uuid.uuid4().hex[:6]}"
    creator_payload = {
        "creator_id": creator_id,
        "creator_type": "automated",
        "name": "Test NLP Bot",
        "version": "1.0.0"
    }
    lambda_client.invoke(
        FunctionName=physical_resources["CreateCreatorFunction"],
        Payload=json.dumps({"body": json.dumps(creator_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id}"

    # --- Create evidence (TextAnnotation) ---
    text_source_iri = "http://example.org/text-source/test"
    annotation_payload = {
        "text_source_iri": text_source_iri,
        "span_start": 0,
        "span_end": 10,
        "creator_iri": creator_iri,
        "metadata": "{\"source\": \"test\"}"
    }
    create_annotation_resp = lambda_client.invoke(
        FunctionName=physical_resources["CreateTextAnnotationFunction"],
        Payload=json.dumps({"body": json.dumps(annotation_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    annotation_body = json.loads(json.loads(create_annotation_resp["Payload"].read())["body"])
    annotation_iri = annotation_body["annotation_iri"]

    # --- Create TermLink ---
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    termlink_payload = {
        "source_node_iri": subject_iri,
        "term_iri": term_iri,
        "creator_iri": creator_iri,
        "evidence_iris": [annotation_iri]
    }
    lambda_client.invoke(
        FunctionName=physical_resources["CreateTermLinkFunction"],
        Payload=json.dumps({"body": json.dumps(termlink_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    # --- Run API query ---
    resp = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": project_id,
            "term_iri": term_iri,
            "term_source": "hpo",
        },
        auth=sigv4_auth
    )

    assert resp.status_code == 200, f"Unexpected status: {resp.status_code} - {resp.text}"
    results = resp.json()["body"]
    print(f"results: {results}")
    assert isinstance(results, list)

    subject = next((s for s in results if s["project_subject_id"] == project_subject_id), None)
    assert subject is not None
    assert "term_links" in subject

    term_links = subject["term_links"]
    assert any(tl["term_iri"] == term_iri for tl in term_links)