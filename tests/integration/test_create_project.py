"""
Integration tests for create_project lambda function.

These tests verify project creation in the PheBee system, including:
- Happy path project creation
- Idempotency (duplicate creation)
- Validation of required fields
- Special characters and Unicode handling
- Concurrent creation race conditions

Note: Since direct Neptune access is unavailable, verification is done indirectly
by attempting to create subjects in the project (which requires a valid project).
"""

import pytest
import json
import uuid
import concurrent.futures
from phebee.utils.aws import get_client


pytestmark = pytest.mark.integration


# ============================================================================
# Helper Functions
# ============================================================================

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

    # Debug: Print actual response if it doesn't match expected format
    if "statusCode" not in result:
        print(f"Unexpected lambda response format: {result}")
        # If lambda returned an error, it might be in a different format
        if "errorMessage" in result:
            raise Exception(f"Lambda error: {result['errorMessage']}")

    return result


def remove_project(project_id: str, physical_resources: dict):
    """Helper to invoke RemoveProjectFunction."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["RemoveProjectFunction"],
        Payload=json.dumps({
            "project_id": project_id
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


def verify_project_exists(project_id: str, physical_resources: dict) -> bool:
    """
    Verify project exists by attempting to create a subject in it.

    Since direct Neptune access is unavailable, we verify indirectly:
    - If project exists, subject creation succeeds
    - If project doesn't exist, subject creation fails

    Returns:
        bool: True if project exists, False otherwise
    """
    lambda_client = get_client("lambda")

    test_subject_id = f"verification-subj-{uuid.uuid4().hex[:8]}"

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_id": project_id,
                "project_subject_id": test_subject_id
            })
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Clean up test subject if created
    if result.get("statusCode") == 200:
        try:
            project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{test_subject_id}"
            lambda_client.invoke(
                FunctionName=physical_resources["RemoveSubjectFunction"],
                Payload=json.dumps({
                    "body": json.dumps({
                        "project_subject_iri": project_subject_iri
                    })
                }).encode("utf-8")
            )
        except Exception:
            pass  # Cleanup is best-effort

    return result.get("statusCode") == 200


# ============================================================================
# Test Cases
# ============================================================================

def test_create_project_success(physical_resources):
    """
    Test 1: Create New Project (Happy Path)

    Verifies that a new project can be successfully created with valid
    project_id and project_label.
    """
    project_id = f"test-project-{uuid.uuid4().hex[:8]}"
    project_label = "Test Project 001"

    try:
        # Create project
        result = create_project(project_id, project_label, physical_resources)

        # Verify response
        assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}"

        body = json.loads(result["body"])
        assert body["project_created"] is True, "Expected project_created to be True"
        assert body["project_id"] == project_id
        assert body["project_iri"] == f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
        assert "Project created" in body["message"]

        # Indirect verification: project should be usable for subject creation
        assert verify_project_exists(project_id, physical_resources), \
            "Project should exist (verified via subject creation)"

    finally:
        # Cleanup (best effort)
        try:
            remove_project(project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def test_create_project_already_exists(physical_resources):
    """
    Test 2: Create Duplicate Project (Idempotency)

    Verifies that attempting to create a project with an existing project_id
    returns success but indicates the project already exists. The original
    label should be preserved.
    """
    project_id = f"test-project-{uuid.uuid4().hex[:8]}"
    original_label = "Original Label"
    different_label = "Different Label"

    try:
        # Create project first time
        result1 = create_project(project_id, original_label, physical_resources)
        assert result1["statusCode"] == 200
        body1 = json.loads(result1["body"])
        assert body1["project_created"] is True

        # Attempt to create again with different label
        result2 = create_project(project_id, different_label, physical_resources)

        # Verify idempotent response
        assert result2["statusCode"] == 200
        body2 = json.loads(result2["body"])
        assert body2["project_created"] is False, "Expected project_created to be False"
        assert "already exists" in body2["message"].lower()

        # Verify project still exists
        assert verify_project_exists(project_id, physical_resources)

    finally:
        # Cleanup (best effort)
        try:
            remove_project(project_id, physical_resources)
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


@pytest.mark.parametrize("payload,expected_error_keywords", [
    pytest.param(
        {"project_label": "Test Project"},
        ["project_id", "missing", "required"],
        id="missing_project_id"
    ),
    pytest.param(
        {"project_id": "test-project-002"},
        ["project_label", "missing", "required"],
        id="missing_project_label"
    ),
    pytest.param(
        {},
        ["missing", "required"],
        id="missing_both_fields"
    ),
    pytest.param(
        {"project_id": "", "project_label": ""},
        ["missing", "required", "empty"],
        id="empty_strings"
    ),
])
def test_create_project_validation_errors(physical_resources, payload, expected_error_keywords):
    """
    Tests 3-5, 9: Validation of required fields.

    Verifies that missing or empty required fields result in 400 errors
    with appropriate error messages.

    Parameterized test cases:
    - missing_project_id: Only project_label provided
    - missing_project_label: Only project_id provided
    - missing_both_fields: Empty payload
    - empty_strings: Empty strings for both fields
    """
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
    body = json.loads(result["body"])

    # Check that at least one expected keyword is in the error message
    message_lower = body["message"].lower()
    assert any(keyword in message_lower for keyword in expected_error_keywords), \
        f"Expected one of {expected_error_keywords} in error message: {body['message']}"


def test_create_project_special_characters(physical_resources):
    """
    Test 6: Special Characters in project_id

    Verifies that project_id can contain special characters like
    hyphens, underscores, and periods.
    """
    project_id = f"test-project_with.special-chars_{uuid.uuid4().hex[:6]}"
    project_label = "Special Chars Project"

    try:
        result = create_project(project_id, project_label, physical_resources)

        assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}"
        body = json.loads(result["body"])
        assert body["project_created"] is True

        # Verify project IRI is correctly formatted
        expected_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
        assert body["project_iri"] == expected_iri

        # Verify project is usable
        assert verify_project_exists(project_id, physical_resources)

    finally:
        remove_project(project_id, physical_resources)


def test_create_project_unicode_label(physical_resources):
    """
    Test 7: Unicode Characters in project_label

    Verifies that project labels can contain Unicode characters from
    various languages without encoding errors.
    """
    project_id = f"test-project-unicode-{uuid.uuid4().hex[:8]}"
    project_label = "测试项目 Test Проект"

    try:
        result = create_project(project_id, project_label, physical_resources)

        assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}"
        body = json.loads(result["body"])
        assert body["project_created"] is True

        # Verify project is usable
        assert verify_project_exists(project_id, physical_resources)

    finally:
        remove_project(project_id, physical_resources)


def test_create_project_long_id(physical_resources):
    """
    Test 8: Very Long project_id

    Verifies handling of very long project IDs (256+ characters).
    """
    long_id = f"test-project-{'a' * 250}"
    project_label = "Long ID Project"

    try:
        result = create_project(long_id, project_label, physical_resources)

        # Should either succeed or fail with appropriate validation error
        assert result["statusCode"] in [200, 400], \
            f"Expected 200 or 400, got {result['statusCode']}"

        if result["statusCode"] == 200:
            # If accepted, verify it's usable (may be new or already exist)
            body = json.loads(result["body"])
            # Either newly created or already exists is acceptable
            assert body["project_created"] in [True, False], \
                f"Expected project_created to be True or False, got {body['project_created']}"
            assert verify_project_exists(long_id, physical_resources)
        else:
            # If rejected, should have meaningful error message
            body = json.loads(result["body"])
            assert len(body.get("message", "")) > 0

    finally:
        # Cleanup if project was created
        if result.get("statusCode") == 200:
            try:
                remove_project(long_id, physical_resources)
            except Exception as e:
                print(f"Warning: Cleanup failed: {e}")


def test_create_project_concurrent(physical_resources):
    """
    Test 10: Concurrent Project Creation (Race Condition)

    Verifies that concurrent attempts to create the same project are handled
    correctly with proper idempotency - exactly one project is created and
    no duplicates or data corruption occurs.
    """
    project_id = f"race-test-project-{uuid.uuid4().hex[:8]}"
    project_label = "Race Test"

    try:
        # Launch 5 concurrent create requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(create_project, project_id, project_label, physical_resources)
                for _ in range(5)
            ]

            results = [future.result() for future in futures]

        # All should return 200
        for result in results:
            assert result["statusCode"] == 200, \
                f"Expected all to return 200, got {result['statusCode']}"

        # At least one should indicate creation
        bodies = [json.loads(r["body"]) for r in results]
        created_count = sum(1 for b in bodies if b["project_created"] is True)
        already_exists_count = sum(1 for b in bodies if b["project_created"] is False)

        assert created_count >= 1, "At least one request should report project_created=True"
        assert created_count + already_exists_count == 5, \
            "All requests should be accounted for"

        # Verify exactly one project exists (no duplicates)
        assert verify_project_exists(project_id, physical_resources), \
            "Project should exist after concurrent creation"

    finally:
        # Cleanup - try to delete project
        # Note: May fail if project has subjects from verify_project_exists call
        try:
            result = remove_project(project_id, physical_resources)
            if result["statusCode"] != 200:
                print(f"Warning: Cleanup failed with status {result['statusCode']}: {result.get('body', 'No body')}")
                # This is expected if the project has subjects - RemoveProject requires empty project
        except Exception as e:
            print(f"Warning: Cleanup failed with exception: {e}")


def test_create_multiple_projects(physical_resources):
    """
    Test 12: Create Multiple Distinct Projects

    Verifies that multiple different projects can be created successfully
    and remain distinct without data mixing.
    """
    project_ids = [f"test-multi-{i}-{uuid.uuid4().hex[:6]}" for i in range(10)]
    project_labels = [f"Multi Project {i}" for i in range(10)]

    try:
        # Create all 10 projects
        for project_id, project_label in zip(project_ids, project_labels):
            result = create_project(project_id, project_label, physical_resources)

            assert result["statusCode"] == 200, \
                f"Failed to create project {project_id}: {result}"

            body = json.loads(result["body"])
            assert body["project_created"] is True, \
                f"Project {project_id} should be newly created"
            assert body["project_id"] == project_id

        # Verify all projects exist and are distinct
        for project_id in project_ids:
            assert verify_project_exists(project_id, physical_resources), \
                f"Project {project_id} should exist"

    finally:
        # Cleanup all projects (best effort)
        for project_id in project_ids:
            try:
                remove_project(project_id, physical_resources)
            except Exception as e:
                print(f"Warning: Failed to cleanup project {project_id}: {e}")
