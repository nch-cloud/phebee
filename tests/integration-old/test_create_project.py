import pytest
from botocore.exceptions import ClientError
import json

from phebee.utils.aws import get_client
from constants import TEST_PROJECT_ID, TEST_PROJECT_LABEL


@pytest.fixture(scope="module")
def prepare_project(physical_resources):
    """Ensure that the project doesn't exist before the test and is removed after the test"""

    lambda_client = get_client("lambda")

    # Remove the project in case it already exists, but catch the error.
    try:
        response = remove_project(TEST_PROJECT_ID, lambda_client, physical_resources)
    except ClientError as e:
        print(f"Failed to remove the {TEST_PROJECT_ID} project before the test: {e}")

    yield

    # Now remove it again during teardown.
    response = remove_project(TEST_PROJECT_ID, lambda_client, physical_resources)

    assert "successfully removed" in response["body"], (
        f"Failed to remove the {TEST_PROJECT_ID} project"
    )


def create_project(project_id, project_label, lambda_client, physical_resources):
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps({"project_id": project_id, "project_label": project_label}),
    )

    return json.loads(response["Payload"].read())


def remove_project(project_id, lambda_client, physical_resources):
    response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveProjectFunction"],
        Payload=json.dumps({"project_id": project_id}),
    )

    return json.loads(response["Payload"].read())


@pytest.mark.integration
def test_create_project(cloudformation_stack, physical_resources, prepare_project):
    """
    This function tests PheBee project creation.

    :param cloudformation_stack: The name of the CloudFormation stack to check.
    """
    lambda_client = get_client("lambda")

    # Test that project creation works
    response = create_project(
        TEST_PROJECT_ID, TEST_PROJECT_LABEL, lambda_client, physical_resources
    )
    body = json.loads(response["body"])
    
    assert body["project_created"]

    # Test that duplicate project creation fails
    response = create_project(
        TEST_PROJECT_ID,
        TEST_PROJECT_LABEL + " duplicate",
        lambda_client,
        physical_resources,
    )

    body = json.loads(response["body"])

    assert not body["project_created"]
    assert "already exists" in body["message"]
