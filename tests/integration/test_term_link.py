import uuid
import json
import pytest
from phebee.utils.aws import get_client
from phebee.utils.sparql import generate_termlink_hash

pytestmark = [pytest.mark.integration]


def test_term_link(physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateTermLinkFunction"]
    get_fn = physical_resources["GetTermLinkFunction"]
    remove_fn = physical_resources["RemoveTermLinkFunction"]

    # --- Setup unique IDs for this test run ---
    test_id = str(uuid.uuid4())
    subject_id = f"test-subject-{test_id}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    creator_id = f"test-creator-{test_id}"
    
    payload = {
        "subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": creator_id,
        "qualifiers": []
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
    
    # Build expected source_node_iri from subject_id
    source_node_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    assert termlink_iri.startswith(source_node_iri + "/term-link/")
    assert create_body["created"] is True  # Should be newly created
    
    # Verify the IRI is deterministic
    expected_hash = generate_termlink_hash(source_node_iri, term_iri, [])
    expected_iri = f"{source_node_iri}/term-link/{expected_hash}"
    assert termlink_iri == expected_iri

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
    assert get_body["term_iri"] == term_iri
    assert get_body["source_node"]["iri"] == source_node_iri
    assert get_body["qualifiers"] == []  # No qualifiers in this test
    assert get_body["evidence"] == []  # No evidence in this test
    
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


def test_term_link_with_qualifiers(physical_resources):
    lambda_client = get_client("lambda")

    create_fn = physical_resources["CreateTermLinkFunction"]
    get_fn = physical_resources["GetTermLinkFunction"]
    remove_fn = physical_resources["RemoveTermLinkFunction"]

    # --- Setup dummy IRIs ---
    source_node_iri = "http://example.org/phebee/subject/test-subject-qualifiers"
    term_iri = "http://purl.obolibrary.org/obo/HP_0000118"
    creator_iri = "http://ods.nationwidechildrens.org/phebee/creator/test-creator"
    qualifiers = [
        "http://ods.nationwidechildrens.org/phebee/qualifier/negated",
        "http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical"
    ]
    
    payload = {
        "subject_id": f"test-subject-{test_id}",
        "term_iri": term_iri,
        "creator_id": f"test-creator-{test_id}",
        "qualifiers": qualifiers
    }

    # --- Create TermLink with qualifiers ---
    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])
    termlink_iri = create_body["termlink_iri"]
    assert termlink_iri.startswith(source_node_iri + "/term-link/")
    
    # Verify the IRI is deterministic and includes qualifiers
    expected_hash = generate_termlink_hash(source_node_iri, term_iri, qualifiers)
    expected_iri = f"{source_node_iri}/term-link/{expected_hash}"
    assert termlink_iri == expected_iri
    
    # Create the same term link again - should get the same IRI
    create_resp_2 = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp_2["StatusCode"] == 200
    create_body_2 = json.loads(json.loads(create_resp_2["Payload"].read())["body"])
    termlink_iri_2 = create_body_2["termlink_iri"]
    assert termlink_iri_2 == termlink_iri  # Should be the same IRI
    assert create_body_2["created"] is False  # Should indicate reuse

    # --- Get TermLink ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(f"test_term_link_with_qualifiers get_body: {get_body}")
    assert get_body["termlink_iri"] == termlink_iri
    assert get_body["term_iri"] == term_iri
    assert get_body["source_node"]["iri"] == source_node_iri
    assert set(get_body["qualifiers"]) == set(qualifiers)  # Check qualifiers match
    assert get_body["evidence"] == []  # No evidence in this test
    
    # --- Create a different term link with different qualifiers ---
    different_qualifiers = [
        "http://ods.nationwidechildrens.org/phebee/qualifier/family"
    ]
    
    different_payload = {
        "subject_id": f"test-subject-{test_id}",
        "term_iri": term_iri,
        "creator_id": f"test-creator-{test_id}",
        "qualifiers": different_qualifiers
    }
    
    different_create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(different_payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert different_create_resp["StatusCode"] == 200
    different_create_body = json.loads(json.loads(different_create_resp["Payload"].read())["body"])
    different_termlink_iri = different_create_body["termlink_iri"]
    
    # Verify it's a different IRI
    assert different_termlink_iri != termlink_iri
    
    # Verify the different IRI is deterministic
    different_expected_hash = generate_termlink_hash(source_node_iri, term_iri, different_qualifiers)
    different_expected_iri = f"{source_node_iri}/term-link/{different_expected_hash}"
    assert different_termlink_iri == different_expected_iri

    # --- Clean up ---
    # Delete first term link
    delete_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert delete_resp["StatusCode"] == 200
    
    # Delete second term link
    different_delete_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"termlink_iri": different_termlink_iri})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert different_delete_resp["StatusCode"] == 200