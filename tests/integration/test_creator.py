import uuid
import json
import pytest
from phebee.utils.aws import get_client
from urllib.parse import quote

pytestmark = [pytest.mark.integration]


@pytest.mark.parametrize("creator_type,extra", [
    ("human", {"name": "Jane Doe"}),
    ("automated", {"name": "PheBeeBot", "version": "1.0.0"})
])
def test_creator_lifecycle(physical_resources, creator_type, extra):
    lambda_client = get_client("lambda")

    creator_id = f"test-{creator_type}-{uuid.uuid4().hex[:6]}"

    create_fn = physical_resources["CreateCreatorFunction"]
    get_fn = physical_resources["GetCreatorFunction"]
    remove_fn = physical_resources["RemoveCreatorFunction"]

    payload = {
        "creator_id": creator_id,
        "creator_type": creator_type,
        **extra
    }

    # --- Create ---
    create_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp["StatusCode"] == 200
    create_body = json.loads(json.loads(create_resp["Payload"].read())["body"])
    creator_id_safe = quote(creator_id, safe="")
    
    # Check for the creator ID in the IRI
    # For automated creators, the IRI now includes /version/{version}
    if creator_type == "automated":
        assert creator_id_safe in create_body["creator_iri"], f"Creator ID {creator_id_safe} not found in IRI {create_body['creator_iri']}"
        assert "/version/" in create_body["creator_iri"], f"Version path not found in IRI {create_body['creator_iri']}"
        version_safe = quote(extra["version"], safe="")
        assert f"/version/{version_safe}" in create_body["creator_iri"], f"Version {version_safe} not found in IRI {create_body['creator_iri']}"
    else:
        # For human creators, the IRI still ends with the creator ID
        assert create_body["creator_iri"].endswith(creator_id_safe)

    # --- Get ---
    get_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"creator_id": creator_id})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(get_body)
    assert get_body["creator_id"] == creator_id
    assert "created" in get_body

    # --- Remove ---
    remove_resp = lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"creator_id": creator_id})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert remove_resp["StatusCode"] == 200
    remove_body = json.loads(json.loads(remove_resp["Payload"].read())["body"])
    if creator_type == "automated":
        assert creator_id_safe in remove_body["creator_iri"], f"Creator ID {creator_id_safe} not found in IRI {remove_body['creator_iri']}"
        assert "/version/" in remove_body["creator_iri"], f"Version path not found in IRI {remove_body['creator_iri']}"
        version_safe = quote(extra["version"], safe="")
        assert f"/version/{version_safe}" in remove_body["creator_iri"], f"Version {version_safe} not found in IRI {remove_body['creator_iri']}"
    else:
        # For human creators, the IRI still ends with the creator ID
        assert remove_body["creator_iri"].endswith(creator_id_safe)

    # --- Get after delete → should return 'not found' ---
    get_again_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"creator_id": creator_id})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    get_again_body = json.loads(json.loads(get_again_resp["Payload"].read())["body"])
    assert get_again_body.get("message") == "Creator not found"
def test_versioned_creator_iris(physical_resources):
    """Test that different versions of the same creator get different IRIs"""
    lambda_client = get_client("lambda")
    
    # Use the same creator ID for both creators
    creator_id = f"test-versioned-{uuid.uuid4().hex[:6]}"
    create_fn = physical_resources["CreateCreatorFunction"]
    get_fn = physical_resources["GetCreatorFunction"]
    remove_fn = physical_resources["RemoveCreatorFunction"]
    
    # Create first version
    payload_v1 = {
        "creator_id": creator_id,
        "creator_type": "automated",
        "name": "Versioned Creator",
        "version": "1.0.0"
    }
    
    create_resp_v1 = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload_v1)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp_v1["StatusCode"] == 200
    create_body_v1 = json.loads(json.loads(create_resp_v1["Payload"].read())["body"])
    creator_iri_v1 = create_body_v1["creator_iri"]
    
    # Create second version
    payload_v2 = {
        "creator_id": creator_id,  # Same ID
        "creator_type": "automated",
        "name": "Versioned Creator",
        "version": "2.0.0"  # Different version
    }
    
    create_resp_v2 = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload_v2)}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    assert create_resp_v2["StatusCode"] == 200
    create_body_v2 = json.loads(json.loads(create_resp_v2["Payload"].read())["body"])
    creator_iri_v2 = create_body_v2["creator_iri"]
    
    # Verify the IRIs are different
    assert creator_iri_v1 != creator_iri_v2, f"Expected different IRIs for different versions, but got {creator_iri_v1} and {creator_iri_v2}"
    
    # Verify the IRIs follow the /version/ pattern
    assert f"/creator/{quote(creator_id, safe='')}/version/" in creator_iri_v1
    assert f"/creator/{quote(creator_id, safe='')}/version/" in creator_iri_v2
    
    # Verify the versions are in the IRIs
    assert f"/version/{quote('1.0.0', safe='')}" in creator_iri_v1
    assert f"/version/{quote('2.0.0', safe='')}" in creator_iri_v2
    
    # Clean up
    # Remove first version
    lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"creator_iri": creator_iri_v1})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    
    # Remove second version
    lambda_client.invoke(
        FunctionName=remove_fn,
        Payload=json.dumps({"body": json.dumps({"creator_iri": creator_iri_v2})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
