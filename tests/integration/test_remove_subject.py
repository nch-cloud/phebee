import json
import pytest
import uuid
from phebee.utils.aws import get_client
from general_utils import parse_lambda_response

pytestmark = [pytest.mark.integration]

def test_remove_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")

    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    create_payload = {
        "project_id": test_project_id,
        "project_subject_id": project_subject_id
    }

    # Step 1: Create the subject to ensure it exists
    create_response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(create_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    create_status_code, create_body = parse_lambda_response(create_response)
    print(f"create_body: {create_body}")
    assert create_status_code == 200
    assert create_body.get("subject_created")

    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"
    print(f"project_subject_iri: {project_subject_iri}")

    # Step 2: Remove the subject
    remove_payload = {
        "project_subject_iri": project_subject_iri
    }

    remove_response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(remove_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    print(f"remove_response: {remove_response}")
    remove_status_code, remove_body = parse_lambda_response(remove_response)
    print(f"remove_body: {remove_body}")
    assert remove_status_code == 200
    assert remove_body.get("message").startswith("Subject removed")

    # Step 3: Confirm subject no longer exists
    get_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(remove_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    print(f"get_response: {get_response}")
    get_status_code, get_body = parse_lambda_response(get_response)
    assert get_status_code == 404

    # Step 4: Attempt to remove the subject again (should handle gracefully)
    remove_again_response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(remove_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    remove_again_status_code, remove_again_body = parse_lambda_response(remove_again_response)
    assert remove_again_status_code == 404
