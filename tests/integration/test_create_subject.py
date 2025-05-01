import uuid
import json
import pytest
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration]


def test_create_new_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]

    project_id = test_project_id
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"

    print(f"project_id: {project_id}")

    payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id
    }

    response = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    assert response["StatusCode"] == 200
    body = json.loads(json.loads(response["Payload"].read())["body"])

    assert "subject" in body
    assert "iri" in body["subject"]
    assert body["subject_created"] is True
    assert body["subject"]["projects"][project_id] == project_subject_id


def test_link_existing_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]

    project_id = test_project_id
    project_subject_id1 = f"subj1-{uuid.uuid4().hex[:6]}"
    project_subject_id2 = f"subj2-{uuid.uuid4().hex[:6]}"

    # First creation: creates subject
    first_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id1
    }

    first_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(first_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    first_body = json.loads(json.loads(first_resp["Payload"].read())["body"])
    print(f"first_body: {first_body}")
    subject_iri = first_body["subject"]["iri"]

    # Second call: link same subject to a new project_subject_iri
    second_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id2,
        "known_subject_iri": subject_iri
    }

    second_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(second_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    second_body = json.loads(json.loads(second_resp["Payload"].read())["body"])

    print(f"second_body: {second_body}")
    assert second_body["subject_created"] is False
    assert second_body["subject"]["iri"] == subject_iri
    assert second_body["subject"]["projects"][project_id] == project_subject_id2