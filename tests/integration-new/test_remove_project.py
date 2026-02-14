"""
Integration tests for remove_project Lambda function.

Tests project deletion with cascades to subjects, evidence, and analytical tables.
Validates complete multi-system cleanup and handling of shared subjects.

Test coverage:
1. Single subject cascade (subject only in this project)
2. Multiple subjects cascade
3. Shared subjects (partial cleanup for shared subjects)
4. Validation (not found, missing ID, null ID, empty string)
"""

import json
import pytest
import boto3
import time
import uuid
import os


def invoke_create_project_helper(project_id, project_label, physical_resources):
    """Helper to create a project for testing."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["CreateProjectFunction"]

    payload = {
        "project_id": project_id,
        "project_label": project_label
    }

    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload).encode("utf-8")
    )

    return json.loads(response["Payload"].read())


def invoke_create_subject_helper(project_id, project_subject_id, label, physical_resources, known_subject_iri=None):
    """Helper to create a subject."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["CreateSubjectFunction"]

    payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id,
        "label": label
    }

    if known_subject_iri:
        payload["known_subject_iri"] = known_subject_iri

    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    if result.get("statusCode") == 200:
        body = json.loads(result["body"])
        # Extract subject_id and construct project_subject_iri
        subject_id = body["subject"]["subject_id"]
        project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"
        return {
            "subject_id": subject_id,
            "project_subject_iri": project_subject_iri,
            "full_response": body
        }
    else:
        raise Exception(f"CreateSubject failed: {result}")


@pytest.fixture
def invoke_remove_project(physical_resources):
    """Helper to invoke RemoveProject Lambda."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveProjectFunction"]

    def _invoke(project_id):
        payload = {"body": json.dumps({"project_id": project_id})}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )
        return json.loads(response["Payload"].read())

    return _invoke


def get_dynamodb_forward_mapping(table_name, project_id, project_subject_id):
    """Query DynamoDB for forward mapping (PROJECT# -> SUBJECT#)."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}'
            }
        )
        return response.get('Item')
    except Exception as e:
        print(f"Error querying DynamoDB forward mapping: {e}")
        return None


def get_dynamodb_reverse_mappings(table_name, subject_id):
    """Query DynamoDB for all reverse mappings (SUBJECT# -> PROJECT#)."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error querying DynamoDB reverse mappings: {e}")
        return []


def query_subjects_in_project(table_name, project_id):
    """Query DynamoDB to get all subjects in a project using forward mappings."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'PROJECT#{project_id}'}
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error querying subjects in project: {e}")
        return []


def count_evidence_for_subject(query_athena, subject_id):
    """Count evidence records for a subject."""
    results = query_athena(f"""
        SELECT COUNT(*) as count
        FROM phebee.evidence
        WHERE subject_id = '{subject_id}'
    """)
    return int(results[0]["count"]) if results else 0


def count_subject_terms_by_subject(query_athena, subject_id):
    """Count subject_terms_by_subject rows for a subject."""
    results = query_athena(f"""
        SELECT COUNT(*) as count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_id}'
    """)
    return int(results[0]["count"]) if results else 0


def count_subject_terms_by_project(query_athena, project_id, subject_id=None):
    """Count subject_terms_by_project_term rows for a project (optionally filtered by subject)."""
    if subject_id:
        results = query_athena(f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_id}' AND subject_id = '{subject_id}'
        """)
    else:
        results = query_athena(f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_id}'
        """)
    return int(results[0]["count"]) if results else 0


def test_remove_project_single_subject_cascade(
    physical_resources,
    create_evidence_helper,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms,
    invoke_remove_project
):
    """
    Test 1: Remove Project with Single Subject (Subject Cascade)

    Create project with one subject (only linked to this project), remove project.
    Verify complete cascade deletion of subject, evidence, and analytical data.
    """
    # Create project
    project_id = f"test-proj-{uuid.uuid4().hex[:8]}"
    invoke_create_project_helper(project_id, "Test Project Single Subject", physical_resources)

    # Create subject
    project_subject_id = f"patient-{uuid.uuid4().hex[:8]}"
    result = invoke_create_subject_helper(
        project_id,
        project_subject_id,
        "Test Subject",
        physical_resources
    )
    subject_uuid = result["subject_id"]

    # Create evidence
    term_iri = standard_hpo_terms["seizure"]
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    termlink_id = evidence["termlink_id"]

    # Wait for subject_terms to update
    wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_id, project_id=project_id)

    # Get DynamoDB table name
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify initial state
    project_items = query_subjects_in_project(table_name, project_id)
    assert len(project_items) == 1, "query should return 1 subject"

    initial_evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
    assert initial_evidence_count > 0, "Should have evidence"

    initial_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    assert initial_by_subject_count > 0, "Should have by_subject rows"

    initial_by_project_count = count_subject_terms_by_project(query_athena, project_id)
    assert initial_by_project_count > 0, "Should have by_project_term rows"

    # Remove project
    result = invoke_remove_project(project_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "successfully removed" in body["message"].lower()
    assert project_id in body["message"]

    # Wait for deletions to propagate
    time.sleep(5)

    # Verify DynamoDB: query should be empty
    final_project_items = query_subjects_in_project(table_name, project_id)
    assert len(final_project_items) == 0, "query should return no subjects after project deletion"

    # Verify DynamoDB: forward mapping deleted
    forward_mapping = get_dynamodb_forward_mapping(table_name, project_id, project_subject_id)
    assert forward_mapping is None, "Forward mapping should be deleted"

    # Verify DynamoDB: reverse mapping deleted (subject had no other projects)
    reverse_mappings = get_dynamodb_reverse_mappings(table_name, subject_uuid)
    assert len(reverse_mappings) == 0, "Reverse mappings should be deleted (cascade)"

    # Verify evidence deleted (cascade)
    final_evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
    assert final_evidence_count == 0, "Evidence should be deleted (cascade)"

    # Verify analytical tables deleted
    final_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    assert final_by_subject_count == 0, "by_subject rows should be deleted (cascade)"

    final_by_project_count = count_subject_terms_by_project(query_athena, project_id)
    assert final_by_project_count == 0, "by_project_term rows should be deleted"


def test_remove_project_multiple_subjects_cascade(
    physical_resources,
    create_evidence_helper,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms,
    invoke_remove_project
):
    """
    Test 2: Remove Project with Multiple Subjects (Multiple Cascades)

    Create project with 3 subjects (all only linked to this project).
    Remove project and verify all subjects cascade deleted.
    """
    # Create project
    project_id = f"test-proj-{uuid.uuid4().hex[:8]}"
    invoke_create_project_helper(project_id, "Test Project Multiple Subjects", physical_resources)

    # Create 3 subjects
    subjects = []
    for i in range(3):
        project_subject_id = f"patient-{i}-{uuid.uuid4().hex[:8]}"
        result = invoke_create_subject_helper(
            project_id,
            project_subject_id,
            f"Test Subject {i}",
            physical_resources
        )
        subject_uuid = result["subject_id"]
        subjects.append({
            "subject_id": subject_uuid,
            "project_subject_id": project_subject_id
        })

        # Create evidence for each subject
        term_iri = standard_hpo_terms["seizure"]
        evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
        termlink_id = evidence["termlink_id"]
        wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_id, project_id=project_id)

    # Get DynamoDB table name
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify initial state: 3 subjects in query
    project_items = query_subjects_in_project(table_name, project_id)
    assert len(project_items) == 3, "query should return 3 subjects"

    # Remove project
    result = invoke_remove_project(project_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "successfully removed" in body["message"].lower()

    # Wait for deletions to propagate
    time.sleep(5)

    # Verify DynamoDB: query empty
    final_project_items = query_subjects_in_project(table_name, project_id)
    assert len(final_project_items) == 0, "query should be empty after project deletion"

    # Verify all 3 subjects deleted
    for subject_data in subjects:
        subject_id = subject_data["subject_id"]
        project_subject_id = subject_data["project_subject_id"]

        # Forward mapping deleted
        forward_mapping = get_dynamodb_forward_mapping(table_name, project_id, project_subject_id)
        assert forward_mapping is None, f"Forward mapping for subject {subject_id} should be deleted"

        # Reverse mapping deleted
        reverse_mappings = get_dynamodb_reverse_mappings(table_name, subject_id)
        assert len(reverse_mappings) == 0, f"Reverse mappings for subject {subject_id} should be deleted"

        # Evidence deleted
        evidence_count = count_evidence_for_subject(query_athena, subject_id)
        assert evidence_count == 0, f"Evidence for subject {subject_id} should be deleted"

        # Analytical tables deleted
        by_subject_count = count_subject_terms_by_subject(query_athena, subject_id)
        assert by_subject_count == 0, f"by_subject rows for subject {subject_id} should be deleted"

    # Verify by_project_term table empty for project
    final_by_project_count = count_subject_terms_by_project(query_athena, project_id)
    assert final_by_project_count == 0, "by_project_term rows should be deleted"


def test_remove_project_shared_subjects(
    physical_resources,
    create_evidence_helper,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms,
    invoke_remove_project
):
    """
    Test 3: Remove Project with Shared Subjects (Partial Cleanup)

    Create project-A and project-B.
    Create subject-1 linked to BOTH projects.
    Create subject-2 linked to project-A only.
    Remove project-A and verify:
    - subject-1 unlinked but NOT deleted (still in project-B)
    - subject-2 fully deleted (cascade)
    """
    # Create two projects
    project_a_id = f"test-proj-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-proj-b-{uuid.uuid4().hex[:8]}"

    invoke_create_project_helper(project_a_id, "Test Project A", physical_resources)
    invoke_create_project_helper(project_b_id, "Test Project B", physical_resources)

    # Create subject-1 linked to BOTH projects
    project_subject_id_1a = f"patient-1a-{uuid.uuid4().hex[:8]}"
    result_1a = invoke_create_subject_helper(
        project_a_id,
        project_subject_id_1a,
        "Shared Subject 1",
        physical_resources
    )
    subject_1_uuid = result_1a["subject_id"]

    # Link subject-1 to project-B
    project_subject_id_1b = f"patient-1b-{uuid.uuid4().hex[:8]}"
    invoke_create_subject_helper(
        project_b_id,
        project_subject_id_1b,
        "Shared Subject 1",
        physical_resources,
        known_subject_iri=f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_1_uuid}"
    )

    # Create subject-2 linked to project-A only
    project_subject_id_2 = f"patient-2-{uuid.uuid4().hex[:8]}"
    result_2 = invoke_create_subject_helper(
        project_a_id,
        project_subject_id_2,
        "Exclusive Subject 2",
        physical_resources
    )
    subject_2_uuid = result_2["subject_id"]

    # Create evidence for both subjects
    term_iri = standard_hpo_terms["seizure"]

    evidence_1 = create_evidence_helper(subject_id=subject_1_uuid, term_iri=term_iri)
    termlink_id_1 = evidence_1["termlink_id"]
    wait_for_subject_terms(subject_id=subject_1_uuid, termlink_id=termlink_id_1, project_id=project_a_id)
    wait_for_subject_terms(subject_id=subject_1_uuid, termlink_id=termlink_id_1, project_id=project_b_id)

    evidence_2 = create_evidence_helper(subject_id=subject_2_uuid, term_iri=term_iri)
    termlink_id_2 = evidence_2["termlink_id"]
    wait_for_subject_terms(subject_id=subject_2_uuid, termlink_id=termlink_id_2, project_id=project_a_id)

    # Get DynamoDB table name
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify initial state
    project_items_a = query_subjects_in_project(table_name, project_a_id)
    assert len(project_items_a) == 2, "Project A should have 2 subjects"

    # Remove project-A
    result = invoke_remove_project(project_a_id)

    # Assertions
    assert result["statusCode"] == 200

    # Wait for deletions
    time.sleep(5)

    # Verify project-A mappings deleted
    project_items_a_final = query_subjects_in_project(table_name, project_a_id)
    assert len(project_items_a_final) == 0, "Project A query should be empty"

    # Verify subject-1 (shared): unlinked from project-A but still exists
    forward_mapping_1a = get_dynamodb_forward_mapping(table_name, project_a_id, project_subject_id_1a)
    assert forward_mapping_1a is None, "Subject-1 should be unlinked from project-A"

    reverse_mappings_1 = get_dynamodb_reverse_mappings(table_name, subject_1_uuid)
    assert len(reverse_mappings_1) == 1, "Subject-1 should still be linked to project-B"

    # Evidence for subject-1 NOT deleted (still in project-B)
    evidence_count_1 = count_evidence_for_subject(query_athena, subject_1_uuid)
    assert evidence_count_1 > 0, "Evidence for subject-1 should NOT be deleted"

    # by_subject rows for subject-1 NOT deleted
    by_subject_count_1 = count_subject_terms_by_subject(query_athena, subject_1_uuid)
    assert by_subject_count_1 > 0, "by_subject rows for subject-1 should remain"

    # Verify subject-2 (exclusive): fully deleted
    forward_mapping_2 = get_dynamodb_forward_mapping(table_name, project_a_id, project_subject_id_2)
    assert forward_mapping_2 is None, "Subject-2 should be unlinked from project-A"

    reverse_mappings_2 = get_dynamodb_reverse_mappings(table_name, subject_2_uuid)
    assert len(reverse_mappings_2) == 0, "Subject-2 should have no mappings (cascade)"

    # Evidence for subject-2 deleted (cascade)
    evidence_count_2 = count_evidence_for_subject(query_athena, subject_2_uuid)
    assert evidence_count_2 == 0, "Evidence for subject-2 should be deleted (cascade)"

    # by_subject rows for subject-2 deleted
    by_subject_count_2 = count_subject_terms_by_subject(query_athena, subject_2_uuid)
    assert by_subject_count_2 == 0, "by_subject rows for subject-2 should be deleted (cascade)"

    # Verify by_project_term table: all project-A rows deleted
    by_project_count_a = count_subject_terms_by_project(query_athena, project_a_id)
    assert by_project_count_a == 0, "by_project_term rows for project-A should be deleted"

    # Cleanup: Remove project-B
    try:
        invoke_remove_project(project_b_id)
    except Exception as e:
        print(f"Cleanup error: {e}")


def test_remove_project_not_found(invoke_remove_project, query_athena):
    """
    Test 4: Remove Project - Project Not Found (Empty Project)

    Attempt to remove non-existent project.
    Should succeed idempotently (returns 200, no errors).
    """
    # Use non-existent project ID
    fake_project_id = f"nonexistent-{uuid.uuid4().hex[:8]}"

    # Remove non-existent project
    result = invoke_remove_project(fake_project_id)

    # Assertions: operation succeeds (idempotent)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "successfully removed" in body["message"].lower()

    # Verify no data affected
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    project_items = query_subjects_in_project(table_name, fake_project_id)
    assert len(project_items) == 0, "No subjects should exist for non-existent project"


def test_remove_project_missing_project_id(physical_resources):
    """
    Test 5: Missing Required Field - project_id

    Invoke lambda without project_id parameter.
    """
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveProjectFunction"]

    # Invoke with empty payload
    payload = {"body": json.dumps({})}
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "project_id" in body["error"].lower()
    assert "required" in body["error"].lower()


def test_remove_project_null_project_id(physical_resources):
    """
    Test 6: Remove Project with Null project_id

    Invoke lambda with null project_id.
    """
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveProjectFunction"]

    # Invoke with null project_id
    payload = {"body": json.dumps({"project_id": None})}
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "project_id" in body["error"].lower()


def test_remove_project_empty_string_id(invoke_remove_project):
    """
    Test 7: Remove Project with Empty String project_id

    Invoke with empty string project_id.
    Should return 400 (treated as missing) or 200 (treated as valid but empty).
    """
    # Invoke with empty string
    result = invoke_remove_project("")

    # Assertions: either 400 (validation error) or 200 (idempotent on empty)
    assert result["statusCode"] in [400, 200]

    if result["statusCode"] == 400:
        body = json.loads(result["body"])
        assert "project_id" in body["error"].lower()
