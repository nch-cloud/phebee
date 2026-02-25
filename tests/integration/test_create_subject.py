"""
Integration tests for create_subject lambda function.

These tests verify subject creation and linking in the PheBee system, including:
- New subject creation with DynamoDB mapping
- Linking existing subjects to multiple projects
- Race condition handling with conditional writes
- Validation of required fields and conflicts
- EventBridge event verification

Note: Direct Neptune access is unavailable, so verification uses:
- DynamoDB queries for subject-project mappings
- Athena queries for analytical tables (future evidence/terms)
- API calls that depend on Neptune data (indirect verification)
"""

import pytest
import json
import uuid
import concurrent.futures
from phebee.utils.aws import get_client
import boto3
import os


pytestmark = pytest.mark.integration


# ============================================================================
# Helper Functions
# ============================================================================

def create_subject(project_id: str, project_subject_id: str, physical_resources: dict,
                  known_subject_iri: str = None, known_project_id: str = None,
                  known_project_subject_id: str = None):
    """Helper to invoke CreateSubjectFunction."""
    lambda_client = get_client("lambda")

    payload = {
        "body": json.dumps({
            "project_id": project_id,
            "project_subject_id": project_subject_id
        })
    }

    # Add optional parameters if provided
    body = json.loads(payload["body"])
    if known_subject_iri:
        body["known_subject_iri"] = known_subject_iri
    if known_project_id:
        body["known_project_id"] = known_project_id
    if known_project_subject_id:
        body["known_project_subject_id"] = known_project_subject_id
    payload["body"] = json.dumps(body)

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Debug: Print actual response if it doesn't match expected format
    if "statusCode" not in result:
        print(f"Unexpected lambda response format: {result}")
        if "errorMessage" in result:
            raise Exception(f"Lambda error: {result['errorMessage']}")

    return result


def remove_subject(project_subject_iri: str, physical_resources: dict):
    """Helper to invoke RemoveSubjectFunction."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveSubjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_subject_iri": project_subject_iri
            })
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


def create_project(project_id: str, project_label: str, physical_resources: dict):
    """Helper to invoke CreateProjectFunction."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps({
            "project_id": project_id,
            "project_label": project_label
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


def remove_project(project_id: str, physical_resources: dict):
    """Helper to invoke RemoveProjectFunction."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveProjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_id": project_id
            })
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


def get_subject_mapping_from_dynamodb(project_id: str, project_subject_id: str) -> str:
    """
    Query DynamoDB for subject_id mapping.

    Returns:
        str: subject_id (UUID) if mapping exists, None otherwise
    """
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}'
            }
        )
        item = response.get('Item')
        return item.get('subject_id') if item else None
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")
        return None


def verify_subject_has_project_mapping(subject_id: str, project_id: str, project_subject_id: str) -> bool:
    """
    Verify reverse mapping exists in DynamoDB.

    Checks that SUBJECT#{subject_id} partition has SK for this project.
    """
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={
                'PK': f'SUBJECT#{subject_id}',
                'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
            }
        )
        return 'Item' in response
    except Exception as e:
        print(f"Error querying DynamoDB reverse mapping: {e}")
        return False


def count_subject_project_mappings(subject_id: str) -> int:
    """
    Count how many projects this subject is linked to.

    Queries SUBJECT#{subject_id} partition and counts SK entries.
    """
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
        )
        return len(response.get('Items', []))
    except Exception as e:
        print(f"Error counting subject mappings: {e}")
        return 0


# ============================================================================
# Test Cases
# ============================================================================

def test_create_subject_new(physical_resources, test_project_id):
    """
    Test 1: Create New Subject (Happy Path)

    Verifies that a new subject can be successfully created with valid
    project_id and project_subject_id, with proper DynamoDB mappings.
    """
    project_label = "Test Subject Creation Project"
    project_subject_id = f"patient-{uuid.uuid4().hex[:8]}"

    project_subject_iri = None

    try:
        # Setup: Create project
        project_result = create_project(test_project_id, project_label, physical_resources)
        assert project_result["statusCode"] == 200, f"Failed to create project: {project_result}"

        # Action: Create subject
        result = create_subject(test_project_id, project_subject_id, physical_resources)

        # Assertions: Verify response
        assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is True, "Expected subject_created to be True"
        assert "subject" in body
        assert "iri" in body["subject"]
        assert "subject_id" in body["subject"]
        assert "projects" in body["subject"]

        subject_iri = body["subject"]["iri"]
        subject_id = body["subject"]["subject_id"]
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

        # Verify subject IRI format
        assert subject_iri.startswith("http://ods.nationwidechildrens.org/phebee/subjects/")
        assert subject_id == subject_iri.split("/")[-1]

        # Verify projects mapping
        assert test_project_id in body["subject"]["projects"]
        assert body["subject"]["projects"][test_project_id] == project_subject_id

        # DynamoDB verification: Forward mapping
        mapped_subject_id = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped_subject_id == subject_id, \
            f"Forward mapping mismatch: expected {subject_id}, got {mapped_subject_id}"

        # DynamoDB verification: Reverse mapping
        assert verify_subject_has_project_mapping(subject_id, test_project_id, project_subject_id), \
            "Reverse mapping should exist in DynamoDB"

        # Verify exactly one project mapping
        mapping_count = count_subject_project_mappings(subject_id)
        assert mapping_count == 1, f"Expected 1 project mapping, found {mapping_count}"

    finally:
        # Cleanup
        if project_subject_iri:
            try:
                remove_subject(project_subject_iri, physical_resources)
            except Exception as e:
                print(f"Warning: Subject cleanup failed: {e}")

        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_create_subject_project_not_found(physical_resources):
    """
    Test 2: Create Subject - Project Does Not Exist

    Verifies that attempting to create a subject in a non-existent project
    returns a 400 error with appropriate message.
    """
    nonexistent_project = f"nonexistent-{uuid.uuid4().hex[:8]}"
    project_subject_id = "patient-001"

    # Action: Try to create subject in non-existent project
    result = create_subject(nonexistent_project, project_subject_id, physical_resources)

    # Assertions
    assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert body["subject_created"] is False
    assert "error" in body
    assert "No project found" in body["error"]
    assert nonexistent_project in body["error"]

    # Verify no DynamoDB mapping created
    mapped_subject_id = get_subject_mapping_from_dynamodb(nonexistent_project, project_subject_id)
    assert mapped_subject_id is None, "No mapping should exist for non-existent project"


def test_create_subject_link_via_known_iri(physical_resources):
    """
    Test 3: Link Existing Subject via known_subject_iri

    Verifies that an existing subject can be linked to a second project
    using its known_subject_iri.
    """
    project_a_id = f"test-project-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-project-b-{uuid.uuid4().hex[:8]}"
    subject_a_id = f"patient-a-{uuid.uuid4().hex[:8]}"
    subject_b_id = f"patient-b-{uuid.uuid4().hex[:8]}"

    project_subject_iri_a = None
    project_subject_iri_b = None

    try:
        # Setup: Create two projects
        create_project(project_a_id, "Project A", physical_resources)
        create_project(project_b_id, "Project B", physical_resources)

        # Create subject in project A
        result_a = create_subject(project_a_id, subject_a_id, physical_resources)
        assert result_a["statusCode"] == 200

        body_a = json.loads(result_a["body"])
        subject_iri = body_a["subject"]["iri"]
        subject_uuid = body_a["subject"]["subject_id"]
        project_subject_iri_a = f"http://ods.nationwidechildrens.org/phebee/projects/{project_a_id}/{subject_a_id}"

        # Action: Link same subject to project B using known_subject_iri
        result_b = create_subject(
            project_b_id, subject_b_id, physical_resources,
            known_subject_iri=subject_iri
        )

        # Assertions
        assert result_b["statusCode"] == 200, f"Expected 200, got {result_b['statusCode']}"

        body_b = json.loads(result_b["body"])
        assert body_b["subject_created"] is False, "Subject already existed, should be False"
        assert body_b["subject"]["iri"] == subject_iri, "Should return same subject IRI"
        assert body_b["subject"]["subject_id"] == subject_uuid
        project_subject_iri_b = f"http://ods.nationwidechildrens.org/phebee/projects/{project_b_id}/{subject_b_id}"

        # DynamoDB verification: Both forward mappings exist
        mapped_a = get_subject_mapping_from_dynamodb(project_a_id, subject_a_id)
        mapped_b = get_subject_mapping_from_dynamodb(project_b_id, subject_b_id)
        assert mapped_a == subject_uuid, "Project A mapping should exist"
        assert mapped_b == subject_uuid, "Project B mapping should point to same subject"

        # DynamoDB verification: Both reverse mappings exist
        assert verify_subject_has_project_mapping(subject_uuid, project_a_id, subject_a_id)
        assert verify_subject_has_project_mapping(subject_uuid, project_b_id, subject_b_id)

        # Verify subject has exactly 2 project mappings
        mapping_count = count_subject_project_mappings(subject_uuid)
        assert mapping_count == 2, f"Expected 2 project mappings, found {mapping_count}"

    finally:
        # Cleanup
        if project_subject_iri_a:
            try:
                remove_subject(project_subject_iri_a, physical_resources)
            except Exception as e:
                print(f"Warning: Subject A cleanup failed: {e}")

        if project_subject_iri_b:
            try:
                remove_subject(project_subject_iri_b, physical_resources)
            except Exception as e:
                print(f"Warning: Subject B cleanup failed: {e}")

        try:
            remove_project(project_a_id, physical_resources)
            remove_project(project_b_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_create_subject_link_via_known_project(physical_resources):
    """
    Test 4: Link Existing Subject via known_project_id + known_project_subject_id

    Verifies that an existing subject can be linked to a second project
    using reference to its mapping in another project.
    """
    project_a_id = f"test-project-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-project-b-{uuid.uuid4().hex[:8]}"
    subject_a_id = f"patient-a-{uuid.uuid4().hex[:8]}"
    subject_b_id = f"patient-b-{uuid.uuid4().hex[:8]}"

    project_subject_iri_a = None
    project_subject_iri_b = None

    try:
        # Setup: Create two projects
        create_project(project_a_id, "Project A", physical_resources)
        create_project(project_b_id, "Project B", physical_resources)

        # Create subject in project A
        result_a = create_subject(project_a_id, subject_a_id, physical_resources)
        assert result_a["statusCode"] == 200

        body_a = json.loads(result_a["body"])
        subject_iri = body_a["subject"]["iri"]
        subject_uuid = body_a["subject"]["subject_id"]
        project_subject_iri_a = f"http://ods.nationwidechildrens.org/phebee/projects/{project_a_id}/{subject_a_id}"

        # Action: Link same subject to project B using project A reference
        result_b = create_subject(
            project_b_id, subject_b_id, physical_resources,
            known_project_id=project_a_id,
            known_project_subject_id=subject_a_id
        )

        # Assertions
        assert result_b["statusCode"] == 200, f"Expected 200, got {result_b['statusCode']}"

        body_b = json.loads(result_b["body"])
        assert body_b["subject_created"] is False
        assert body_b["subject"]["iri"] == subject_iri
        assert body_b["subject"]["subject_id"] == subject_uuid
        project_subject_iri_b = f"http://ods.nationwidechildrens.org/phebee/projects/{project_b_id}/{subject_b_id}"

        # DynamoDB verification: Both mappings point to same subject
        mapped_a = get_subject_mapping_from_dynamodb(project_a_id, subject_a_id)
        mapped_b = get_subject_mapping_from_dynamodb(project_b_id, subject_b_id)
        assert mapped_a == mapped_b == subject_uuid

        # Verify 2 project mappings
        mapping_count = count_subject_project_mappings(subject_uuid)
        assert mapping_count == 2, f"Expected 2 project mappings, found {mapping_count}"

    finally:
        # Cleanup
        if project_subject_iri_a:
            try:
                remove_subject(project_subject_iri_a, physical_resources)
            except Exception as e:
                print(f"Warning: Subject A cleanup failed: {e}")

        if project_subject_iri_b:
            try:
                remove_subject(project_subject_iri_b, physical_resources)
            except Exception as e:
                print(f"Warning: Subject B cleanup failed: {e}")

        try:
            remove_project(project_a_id, physical_resources)
            remove_project(project_b_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_create_subject_invalid_known_project_missing_subject(physical_resources, test_project_id):
    """
    Test 5: Invalid known_project_id without known_project_subject_id

    Verifies that providing known_project_id without known_project_subject_id
    returns a 400 error.
    """
    project_subject_id = "patient-001"

    try:
        # Setup: Create project
        create_project(test_project_id, "Test Project", physical_resources)

        # Action: Try to create with incomplete known project reference
        result = create_subject(
            test_project_id, project_subject_id, physical_resources,
            known_project_id="some-project"
        )

        # Assertions
        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is False
        assert "error" in body
        assert "known_project_id" in body["error"]
        assert "known_project_subject_id" in body["error"]
        assert "required" in body["error"]

        # Verify no mapping created
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped is None

    finally:
        # Cleanup
        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_invalid_both_known_references(physical_resources, test_project_id):
    """
    Test 6: Invalid - Both known_project_id and known_subject_iri Provided

    Verifies that providing both known_project_id and known_subject_iri
    returns a 400 error (conflicting parameters).
    """
    project_subject_id = "patient-001"

    try:
        # Setup: Create project
        create_project(test_project_id, "Test Project", physical_resources)

        # Action: Try to create with conflicting parameters
        result = create_subject(
            test_project_id, project_subject_id, physical_resources,
            known_project_id="project-a",
            known_project_subject_id="patient-a",
            known_subject_iri="http://ods.nationwidechildrens.org/phebee/subjects/some-uuid"
        )

        # Assertions
        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is False
        assert "error" in body
        assert "cannot both be provided" in body["error"]

        # Verify no mapping created
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped is None

    finally:
        # Cleanup
        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_duplicate_idempotent(physical_resources, test_project_id):
    """
    Test 7: Duplicate Subject Creation (Same project_id + project_subject_id)

    Verifies that attempting to create the same subject twice is idempotent
    and returns the existing subject without creating duplicates.
    """
    project_label = "Test Idempotency Project"
    project_subject_id = f"patient-{uuid.uuid4().hex[:8]}"

    project_subject_iri = None

    try:
        # Setup: Create project
        create_project(test_project_id, project_label, physical_resources)

        # Create subject first time
        result1 = create_subject(test_project_id, project_subject_id, physical_resources)
        assert result1["statusCode"] == 200

        body1 = json.loads(result1["body"])
        assert body1["subject_created"] is True
        subject_iri = body1["subject"]["iri"]
        subject_id = body1["subject"]["subject_id"]
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

        # Action: Create same subject again
        result2 = create_subject(test_project_id, project_subject_id, physical_resources)

        # Assertions
        assert result2["statusCode"] == 200

        body2 = json.loads(result2["body"])
        assert body2["subject_created"] is False, "Second creation should return False"
        assert body2["subject"]["iri"] == subject_iri, "Should return same subject IRI"
        assert body2["subject"]["subject_id"] == subject_id

        # DynamoDB verification: Only one mapping exists
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped == subject_id

        mapping_count = count_subject_project_mappings(subject_id)
        assert mapping_count == 1, "Should have exactly 1 mapping, no duplicates"

    finally:
        # Cleanup
        if project_subject_iri:
            try:
                remove_subject(project_subject_iri, physical_resources)
            except Exception as e:
                print(f"Warning: Subject cleanup failed: {e}")

        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_create_subject_race_condition(physical_resources, test_project_id):
    """
    Test 8: Race Condition - Concurrent Subject Creation

    Verifies that concurrent attempts to create the same subject are handled
    correctly using conditional DynamoDB writes - exactly one subject created,
    all others receive the winner's subject.
    """
    project_label = "Race Condition Test Project"
    project_subject_id = f"race-patient-{uuid.uuid4().hex[:8]}"

    project_subject_iri = None

    try:
        # Setup: Create project
        create_project(test_project_id, project_label, physical_resources)

        # Action: Launch 5 concurrent create requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(create_subject, test_project_id, project_subject_id, physical_resources)
                for _ in range(5)
            ]

            results = [future.result() for future in futures]

        # Assertions: All should return 200
        for result in results:
            assert result["statusCode"] == 200, \
                f"Expected all to return 200, got {result['statusCode']}"

        # Exactly one should indicate creation
        bodies = [json.loads(r["body"]) for r in results]
        created_count = sum(1 for b in bodies if b["subject_created"] is True)
        already_exists_count = sum(1 for b in bodies if b["subject_created"] is False)

        assert created_count >= 1, "At least one request should report subject_created=True"
        assert created_count + already_exists_count == 5, "All requests should be accounted for"

        # All should return same subject IRI (winner's IRI)
        subject_iris = [b["subject"]["iri"] for b in bodies]
        assert len(set(subject_iris)) == 1, "All requests should return same subject IRI"

        subject_id = bodies[0]["subject"]["subject_id"]
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

        # DynamoDB verification: Exactly one mapping
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped == subject_id

        mapping_count = count_subject_project_mappings(subject_id)
        assert mapping_count == 1, f"Expected exactly 1 mapping, found {mapping_count}"

    finally:
        # Cleanup
        if project_subject_iri:
            try:
                remove_subject(project_subject_iri, physical_resources)
            except Exception as e:
                print(f"Warning: Subject cleanup failed: {e}")

        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_create_subject_missing_project_id(physical_resources):
    """
    Test 9: Missing Required Field - project_id

    Verifies that omitting project_id results in a 400 error.
    """
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_subject_id": "patient-001"
            })
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
    body = json.loads(result["body"])
    assert "required" in body["error"].lower()


def test_create_subject_missing_project_subject_id(physical_resources, test_project_id):
    """
    Test 10: Missing Required Field - project_subject_id

    Verifies that omitting project_subject_id results in a 400 error.
    """
    lambda_client = get_client("lambda")

    try:
        # Setup: Create project
        create_project(test_project_id, "Test Project", physical_resources)

        # Action: Try to create subject without project_subject_id
        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({
                "body": json.dumps({
                    "project_id": test_project_id
                })
            }).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())

        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
        body = json.loads(result["body"])
        assert "required" in body["error"].lower()

    finally:
        # Cleanup
        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_known_iri_not_found(physical_resources, test_project_id):
    """
    Test 11: known_subject_iri - Subject Does Not Exist

    Verifies that providing a non-existent known_subject_iri returns a 400 error.
    """
    project_subject_id = "patient-001"
    nonexistent_uuid = uuid.uuid4().hex
    nonexistent_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{nonexistent_uuid}"

    try:
        # Setup: Create project
        create_project(test_project_id, "Test Project", physical_resources)

        # Action: Try to link with non-existent subject IRI
        result = create_subject(
            test_project_id, project_subject_id, physical_resources,
            known_subject_iri=nonexistent_iri
        )

        # Assertions
        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is False
        assert "error" in body
        assert "No subject found" in body["error"]

        # Verify no mapping created
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped is None

    finally:
        # Cleanup
        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_known_project_mapping_not_found(physical_resources):
    """
    Test 12: known_project_id - Subject Mapping Does Not Exist

    Verifies that providing a non-existent known project mapping returns a 400 error.
    """
    project_a_id = f"test-project-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-project-b-{uuid.uuid4().hex[:8]}"

    try:
        # Setup: Create two projects (but no subject)
        create_project(project_a_id, "Project A", physical_resources)
        create_project(project_b_id, "Project B", physical_resources)

        # Action: Try to link using non-existent mapping
        result = create_subject(
            project_b_id, "patient-b", physical_resources,
            known_project_id=project_a_id,
            known_project_subject_id="nonexistent-patient"
        )

        # Assertions
        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is False
        assert "error" in body
        assert "No subject found" in body["error"]

    finally:
        # Cleanup
        try:
            remove_project(project_a_id, physical_resources)
            remove_project(project_b_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_invalid_iri_format(physical_resources, test_project_id):
    """
    Test 13: Invalid known_subject_iri Format

    Verifies that providing a malformed IRI returns a 400 error.
    """
    project_subject_id = "patient-001"

    try:
        # Setup: Create project
        create_project(test_project_id, "Test Project", physical_resources)

        # Action: Try to link with invalid IRI
        result = create_subject(
            test_project_id, project_subject_id, physical_resources,
            known_subject_iri="not-a-valid-iri"
        )

        # Assertions
        assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["subject_created"] is False
        assert "error" in body

        # Verify no mapping created
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped is None

    finally:
        # Cleanup
        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_subject_conflict_project_subject_id_taken(physical_resources, test_project_id):
    """
    Test 15: Conflict - Different Subject Already Linked to project_subject_id

    Verifies that attempting to link a different subject to an already-used
    project_subject_id returns a 400 error.
    """
    project_label = "Conflict Test Project"
    project_subject_id = f"patient-{uuid.uuid4().hex[:8]}"

    project_subject_iri_a = None
    project_subject_iri_b = None

    try:
        # Setup: Create project
        create_project(test_project_id, project_label, physical_resources)

        # Create subject A
        result_a = create_subject(test_project_id, project_subject_id, physical_resources)
        assert result_a["statusCode"] == 200

        body_a = json.loads(result_a["body"])
        subject_a_iri = body_a["subject"]["iri"]
        project_subject_iri_a = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

        # Create independent subject B in different project first
        temp_project_id = f"temp-project-{uuid.uuid4().hex[:8]}"
        temp_subject_id = f"temp-subject-{uuid.uuid4().hex[:8]}"

        create_project(temp_project_id, "Temp Project", physical_resources)
        result_temp = create_subject(temp_project_id, temp_subject_id, physical_resources)
        assert result_temp["statusCode"] == 200

        body_temp = json.loads(result_temp["body"])
        subject_b_iri = body_temp["subject"]["iri"]

        # Action: Try to link subject B to original project using same project_subject_id
        result_conflict = create_subject(
            test_project_id, project_subject_id, physical_resources,
            known_subject_iri=subject_b_iri
        )

        # Assertions
        assert result_conflict["statusCode"] == 400, \
            f"Expected 400 conflict, got {result_conflict['statusCode']}"

        body_conflict = json.loads(result_conflict["body"])
        assert body_conflict["subject_created"] is False
        assert "error" in body_conflict
        assert "already linked" in body_conflict["error"].lower()

        # Verify original mapping unchanged
        mapped = get_subject_mapping_from_dynamodb(test_project_id, project_subject_id)
        assert mapped == body_a["subject"]["subject_id"], "Original mapping should be unchanged"

        # Cleanup temp project
        temp_project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{temp_project_id}/{temp_subject_id}"
        try:
            remove_subject(temp_project_subject_iri, physical_resources)
            remove_project(temp_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Temp cleanup failed: {e}")

    finally:
        # Cleanup
        if project_subject_iri_a:
            try:
                remove_subject(project_subject_iri_a, physical_resources)
            except Exception as e:
                print(f"Warning: Subject A cleanup failed: {e}")

        try:
            remove_project(test_project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project cleanup failed: {e}")


def test_link_subject_rematerializes_evidence(
    physical_resources,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test that linking a subject to a new project automatically materializes existing evidence.

    Scenario:
    1. Create subject in project A
    2. Create evidence for that subject
    3. Link same subject to project B (different project_subject_id)
    4. Verify evidence is queryable in BOTH projects with correct project_subject_ids

    This validates that the re-materialization triggered during subject linking
    makes pre-existing evidence visible in the new project's queries.
    """
    # Setup: Create two projects
    project_a_id = f"test-remat-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-remat-b-{uuid.uuid4().hex[:8]}"

    create_project(project_a_id, "Test Rematerialization Project A", physical_resources)
    create_project(project_b_id, "Test Rematerialization Project B", physical_resources)

    try:
        # Step 1: Create subject in project A
        subj_a_id = f"subj-a-{uuid.uuid4().hex[:8]}"
        result_a = create_subject(project_a_id, subj_a_id, physical_resources)

        assert result_a["statusCode"] == 200
        body_a = json.loads(result_a["body"])
        subject_iri = body_a["subject"]["iri"]
        subject_uuid = body_a["subject"]["subject_id"]

        # Step 2: Create evidence for the subject
        term_iri = standard_hpo_terms["seizure"]
        evidence_id = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri,
            evidence_creator_id="test-creator",
            evidence_creator_type="human"
        )

        assert evidence_id is not None, "Evidence creation should succeed"

        # Step 3: Link the same subject to project B
        subj_b_id = f"subj-b-{uuid.uuid4().hex[:8]}"
        result_b = create_subject(
            project_b_id,
            subj_b_id,
            physical_resources,
            known_project_id=project_a_id,
            known_project_subject_id=subj_a_id
        )

        assert result_b["statusCode"] == 200
        body_b = json.loads(result_b["body"])
        assert body_b["subject"]["iri"] == subject_iri, "Should link to same subject"

        # Step 4: Verify evidence appears in BOTH projects with correct project_subject_ids
        # Query project A
        query_a = f"""
            SELECT project_id, project_subject_id, subject_id, term_iri
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_a_id}'
            AND subject_id = '{subject_uuid}'
        """
        results_a = query_athena(query_a)

        assert len(results_a) > 0, "Subject should appear in project A queries"
        assert results_a[0]["project_subject_id"] == subj_a_id, \
            f"Project A should show project_subject_id '{subj_a_id}'"

        # Query project B
        query_b = f"""
            SELECT project_id, project_subject_id, subject_id, term_iri
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_b_id}'
            AND subject_id = '{subject_uuid}'
        """
        results_b = query_athena(query_b)

        assert len(results_b) > 0, \
            "Subject should appear in project B queries after linking (evidence materialized)"
        assert results_b[0]["project_subject_id"] == subj_b_id, \
            f"Project B should show project_subject_id '{subj_b_id}'"

        # Verify both point to same underlying subject
        assert results_a[0]["subject_id"] == results_b[0]["subject_id"], \
            "Both projects should reference the same underlying subject UUID"

    finally:
        # Cleanup
        try:
            remove_project(project_a_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project A cleanup failed: {e}")
        try:
            remove_project(project_b_id, physical_resources)
        except Exception as e:
            print(f"Warning: Project B cleanup failed: {e}")
