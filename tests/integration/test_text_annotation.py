import json
import pytest
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration]

def test_text_annotation(physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateTextAnnotationFunction"]
    get_fn = physical_resources["GetTextAnnotationFunction"]
    remove_fn = physical_resources["RemoveTextAnnotationFunction"]

    # Setup: dummy text source IRI
    text_source_iri = "http://example.org/phebee/text-source/test-source"

    # --- Create ---
    payload = {
        "text_source_iri": text_source_iri,
        "span_start": 10,
        "span_end": 42,
        "metadata": "{\"tool\": \"StarAnnotator\", \"score\": 0.98}"
    }

    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])
    annotation_iri = create_body["annotation_iri"]
    assert annotation_iri.startswith(text_source_iri + "/annotation/")

    # --- Get ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"annotation_iri": annotation_iri})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(f"get_body: {get_body}")
    assert get_body["annotation_iri"] == annotation_iri
    assert get_body.get("span_start") == "10"
    assert get_body.get("span_end") == "42"

    # --- Remove ---
    delete_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"annotation_iri": annotation_iri})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert delete_resp["StatusCode"] == 200
    delete_body = json.loads(json.loads(delete_resp["Payload"].read())["body"])
    assert delete_body["annotation_iri"] == annotation_iri

    # --- Get after delete (should return not found) ---
    get_again_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"annotation_iri": annotation_iri})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    get_again_body = json.loads(json.loads(get_again_resp["Payload"].read())["body"])
    print(f"get_again_body: {get_again_body}")
    assert get_again_body.get("message") == "TextAnnotation not found"

    # --- Missing required field ---
    bad_create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps({})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    bad_create_body = json.loads(json.loads(bad_create_resp["Payload"].read())["body"])
    assert "Missing required field" in bad_create_body["message"]