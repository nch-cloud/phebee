import uuid
import json
import pytest
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration]


def test_term_link(physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateTermLinkFunction"]
    get_fn = physical_resources["GetTermLinkFunction"]
    remove_fn = physical_resources["RemoveTermLinkFunction"]

    # --- Setup dummy IRIs ---
    source_node_iri = "http://example.org/phebee/subject/test-subject"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    creator_iri = "http://ods.nationwidechildrens.org/phebee/creator/test-creator"
    
    payload = {
        "source_node_iri": source_node_iri,
        "term_iri": term_iri,
        "creator_iri": creator_iri,
        "evidence_iris": []
    }

    # --- Create TermLink ---
    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])
    termlink_iri = create_body["termlink_iri"]
    assert termlink_iri.startswith(source_node_iri + "/term-link/")

    # --- Get TermLink ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(f"test_term_link get_body: {get_body}")
    assert get_body["termlink_iri"] == termlink_iri
    assert term_iri in get_body.get("has_term", [])
    assert creator_iri in get_body.get("creator", [])
    
    # TODO: Test evidence

    # --- Delete TermLink ---
    delete_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert delete_resp["StatusCode"] == 200
    delete_body = json.loads(json.loads(delete_resp["Payload"].read())["body"])
    assert delete_body["termlink_iri"] == termlink_iri

    # --- Get after delete (should return not found) ---
    get_again_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    get_again_body = json.loads(json.loads(get_again_resp["Payload"].read())["body"])
    assert get_again_body.get("message") == "TermLink not found"