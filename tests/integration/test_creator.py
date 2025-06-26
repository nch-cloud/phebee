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
    assert remove_body["creator_iri"].endswith(creator_id)

    # --- Get after delete → should return 'not found' ---
    get_again_resp = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps({"body": json.dumps({"creator_id": creator_id})}).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    get_again_body = json.loads(json.loads(get_again_resp["Payload"].read())["body"])
    assert get_again_body.get("message") == "Creator not found"