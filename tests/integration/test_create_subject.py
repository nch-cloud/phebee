import uuid
import json
import pytest
from phebee.utils.aws import get_client
from general_utils import parse_lambda_response

pytestmark = [pytest.mark.integration]


def test_create_new_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]
    get_fn = physical_resources["GetSubjectFunction"]

    project_id = test_project_id
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

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

    status_code, body = parse_lambda_response(response)
    assert status_code == 200
    
    assert "subject" in body
    assert "iri" in body["subject"]
    assert body["subject_created"] is True
    assert body["subject"]["projects"][project_id] == project_subject_id

    get_payload = {
        "project_subject_iri": project_subject_iri
    }

    get_response = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps(get_payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    print(get_response)
    get_status_code, get_body = parse_lambda_response(get_response)
    assert get_status_code == 200
    assert "subject_iri" in get_body
    assert "project_subject_iri" in get_body
    assert get_body["project_subject_iri"] == project_subject_iri


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
    first_status_code, first_body = parse_lambda_response(first_resp)
    print(f"first_body: {first_body}")
    subject_iri = first_body["subject"]["iri"]
    assert first_status_code == 200

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
    second_status_code, second_body = parse_lambda_response(second_resp)
    assert second_status_code == 200

    print(f"second_body: {second_body}")
    assert second_body["subject_created"] is False
    assert second_body["subject"]["iri"] == subject_iri
    assert second_body["subject"]["projects"][project_id] == project_subject_id2