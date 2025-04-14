import pytest
import json

from phebee.utils.aws import get_client
from constants import TEST_PROJECT_ID, TEST_PROJECT2_ID, TEST_PROJECT_LABEL
from test_create_project import create_project, remove_project

SUBJECT_1_ID = "subject_1"
SUBJECT_1A_ID = "subject_1A"
SUBJECT_2_ID = "subject_2"
SUBJECT_2A_ID = "subject_2A"


@pytest.fixture(scope="module")
def prepare_create_subject(physical_resources, aws_session):
    """Ensure that the project doesn't exist before the test and is removed after the test"""

    lambda_client = get_client("lambda")

    create_project(
        TEST_PROJECT_ID, TEST_PROJECT_LABEL, lambda_client, physical_resources
    )
    create_project(
        TEST_PROJECT2_ID, TEST_PROJECT_LABEL, lambda_client, physical_resources
    )

    yield

    response = remove_project(TEST_PROJECT_ID, lambda_client, physical_resources)
    response = remove_project(TEST_PROJECT2_ID, lambda_client, physical_resources)

    assert response["statusCode"] == 200, (
        f"Failed to remove the {TEST_PROJECT_ID} project"
    )


def remove_subject(project_id, project_subject_id, lambda_client, physical_resources):
    response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveSubjectFunction"],
        Payload=json.dumps(
            {"project_id": project_id, "project_subject_id": project_subject_id}
        ),
    )
    result = json.loads(response["Payload"].read())
    print(result)
    return result


def create_subject(
    project_id,
    project_subject_id,
    lambda_client,
    physical_resources,
    known_project_id=None,
    known_project_subject_id=None,
    known_subject_iri=None,
):
    payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id,
    }

    if known_project_id:
        payload["known_project_id"] = known_project_id
    if known_project_subject_id:
        payload["known_project_subject_id"] = known_project_subject_id
    if known_subject_iri:
        payload["known_subject_iri"] = known_subject_iri

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps(payload),
    )
    result = json.loads(response["Payload"].read())
    print(result)
    return result


def get_subject(project_id, project_subject_id, lambda_client, physical_resources):
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectFunction"],
        Payload=json.dumps(
            {"project_id": project_id, "project_subject_id": project_subject_id}
        ),
    )
    result = json.loads(response["Payload"].read())
    subject = json.loads(result["body"])
    return subject


@pytest.mark.integration
def test_create_subject(
    cloudformation_stack, physical_resources, prepare_create_subject
):
    lambda_client = get_client("lambda")

    # Create a test subject
    response = create_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    response_body = json.loads(response["body"])

    # Test that we can retrieve the subject
    assert response_body["subject_created"]
    subject_1 = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    subject_1_original_iri = subject_1["iri"]

    # Test duplicate subject creation
    response = create_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    response_body = json.loads(response["body"])

    # Test that we can retrieve the subject and that it's the same underlying node
    assert ~response_body["subject_created"]
    subject_1 = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    assert subject_1["iri"] == subject_1_original_iri

    # Test that creating another subject returns a different subject
    response = create_subject(
        TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources
    )
    response_body = json.loads(response["body"])

    assert response_body["subject_created"]

    subject_2 = get_subject(
        TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources
    )
    subject_2_original_iri = subject_2["iri"]
    assert subject_2_original_iri != subject_1_original_iri

    # Test that removing and creating subject is yet another different subject
    remove_subject(TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources)
    response = create_subject(
        TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources
    )
    response_body = json.loads(response["body"])

    subject_2 = get_subject(
        TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources
    )
    assert response_body["subject_created"]
    assert subject_2["iri"] != subject_2_original_iri

    # Test that adds existing subject to new project TEST_PROJECT2_ID using known ids
    response = create_subject(
        TEST_PROJECT2_ID,
        SUBJECT_1A_ID,
        lambda_client,
        physical_resources,
        known_project_id=TEST_PROJECT_ID,
        known_project_subject_id=SUBJECT_1_ID,
    )
    response_body = json.loads(response["body"])
    assert ~response_body["subject_created"]
    subject_1_in_project2 = get_subject(
        TEST_PROJECT2_ID, SUBJECT_1A_ID, lambda_client, physical_resources
    )
    assert subject_1_in_project2["iri"] == subject_1_original_iri

    # Test that adds existing subject to new project TEST_PROJECT2_ID using known subject_iri
    response = create_subject(
        TEST_PROJECT2_ID,
        SUBJECT_2A_ID,
        lambda_client,
        physical_resources,
        known_subject_iri=subject_2["iri"],
    )
    response_body = json.loads(response["body"])
    assert ~response_body["subject_created"]
    subject_2_in_project2_via_iri = get_subject(
        TEST_PROJECT2_ID, SUBJECT_2A_ID, lambda_client, physical_resources
    )
    assert subject_2_in_project2_via_iri["iri"] == subject_2["iri"]

    # Clean up
    # TODO Verify expected behavior for removing a subject linked to multiple projects - subject should still exist, other links should still exist
    remove_subject(TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources)
    remove_subject(TEST_PROJECT_ID, SUBJECT_2_ID, lambda_client, physical_resources)
    remove_subject(TEST_PROJECT2_ID, SUBJECT_1A_ID, lambda_client, physical_resources)
    remove_subject(TEST_PROJECT2_ID, SUBJECT_2A_ID, lambda_client, physical_resources)


@pytest.mark.integration
def test_create_subject_invalid_project(cloudformation_stack, physical_resources):
    lambda_client = get_client("lambda")
    # with pytest.raises(ValueError, match=f"No project found with ID: {TEST_PROJECT_ID + '--MISSING'}"):
    response = create_subject(
        TEST_PROJECT_ID + "--MISSING", SUBJECT_1_ID, lambda_client, physical_resources
    )

    assert response["statusCode"] == 400

    response_body = json.loads(response["body"])

    assert ~response_body["subject_created"]
    assert response_body["error"].startswith("No project found with ID:")


# TODO - Test correct firing of SUBJECT_CREATED event
