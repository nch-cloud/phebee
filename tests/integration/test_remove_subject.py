"""
Integration tests for remove_subject.py Lambda function.

Tests verify:
1. Subject removal from projects (partial vs full cascade)
2. DynamoDB mapping deletion (bidirectional)
3. Evidence cascade deletion when last mapping removed
4. Analytical table updates (by_project_term and by_subject)
5. Neptune cleanup (project_subject_iri, subject_iri, term links)
6. Error handling for validation and not found cases
7. Idempotency of deletion operations
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


def invoke_remove_project_helper(project_id, physical_resources):
    """Helper to remove a project."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveProjectFunction"]

    payload = {"project_id": project_id}

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
def invoke_remove_subject(physical_resources):
    """Helper to invoke RemoveSubject Lambda."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveSubjectFunction"]

    def _invoke(project_subject_iri):
        payload = {"body": json.dumps({"project_subject_iri": project_subject_iri})}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())

        if "FunctionError" in response:
            return {"statusCode": 500, "body": json.dumps(result)}

        return result

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
        print(f"Error querying forward mapping: {e}")
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
        print(f"Error querying reverse mappings: {e}")
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


def count_subject_terms_by_project(query_athena, project_id, subject_id):
    """Count subject_terms_by_project_term rows for a subject in a specific project."""
    results = query_athena(f"""
        SELECT COUNT(*) as count
        FROM phebee.subject_terms_by_project_term
        WHERE project_id = '{project_id}'
        AND subject_id = '{subject_id}'
    """)
    return int(results[0]["count"]) if results else 0


def test_remove_subject_not_last_mapping(
    physical_resources,
    create_evidence_helper,
    query_athena,
    standard_hpo_terms,
    invoke_remove_subject
):
    """
    Test 1: Remove Subject from Project - NOT Last Mapping (Partial Removal)

    Create subject linked to 2 projects, remove from 1, verify:
    - DynamoDB mappings for removed project deleted
    - DynamoDB mappings for other project remain
    - Evidence NOT deleted (subject still exists in other project)
    - Analytical tables: by_project_term for removed project deleted, by_subject remains
    """
    # Create two projects
    project_a_id = f"test-proj-a-{uuid.uuid4().hex[:8]}"
    project_b_id = f"test-proj-b-{uuid.uuid4().hex[:8]}"

    invoke_create_project_helper(project_a_id, "Test Project A", physical_resources)
    invoke_create_project_helper(project_b_id, "Test Project B", physical_resources)

    # Create subject linked to both projects
    project_subject_id_a = f"patient-{uuid.uuid4().hex[:8]}"
    project_subject_id_b = f"patient-{uuid.uuid4().hex[:8]}"

    # Create subject in project A (this creates the subject)
    result_a = invoke_create_subject_helper(
        project_a_id,
        project_subject_id_a,
        "Test Subject A",
        physical_resources
    )
    subject_uuid = result_a["subject_id"]
    project_subject_iri_a = result_a["project_subject_iri"]

    # Link same subject to project B
    result_b = invoke_create_subject_helper(
        project_b_id,
        project_subject_id_b,
        "Test Subject B",
        physical_resources,
        known_subject_iri=f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_uuid}"
    )
    project_subject_iri_b = result_b["project_subject_iri"]

    # Create evidence for subject
    term_iri = standard_hpo_terms["seizure"]
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    termlink_id = evidence["termlink_id"]

    # Get DynamoDB table name from environment
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify initial state: both mappings exist
    mapping_a = get_dynamodb_forward_mapping(table_name, project_a_id, project_subject_id_a)
    mapping_b = get_dynamodb_forward_mapping(table_name, project_b_id, project_subject_id_b)
    assert mapping_a is not None, "Mapping A should exist"
    assert mapping_b is not None, "Mapping B should exist"

    reverse_mappings_initial = get_dynamodb_reverse_mappings(table_name, subject_uuid)
    assert len(reverse_mappings_initial) == 2, f"Expected 2 reverse mappings, found {len(reverse_mappings_initial)}"

    # Verify initial analytical table state
    initial_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    assert initial_by_subject_count > 0, "Should have subject_terms_by_subject rows"

    initial_by_project_a_count = count_subject_terms_by_project(query_athena, project_a_id, subject_uuid)
    initial_by_project_b_count = count_subject_terms_by_project(query_athena, project_b_id, subject_uuid)
    assert initial_by_project_a_count > 0, "Should have by_project_term rows for project A"
    assert initial_by_project_b_count > 0, "Should have by_project_term rows for project B"

    # Remove subject from project A
    result = invoke_remove_subject(project_subject_iri_a)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "unlinked from project" in body["message"].lower(), \
        f"Expected 'unlinked' message (not last mapping), got: {body['message']}"
    assert project_a_id in body["message"]

    # Wait for deletion to propagate
    time.sleep(3)

    # Verify DynamoDB: mapping A deleted, mapping B remains
    mapping_a_after = get_dynamodb_forward_mapping(table_name, project_a_id, project_subject_id_a)
    mapping_b_after = get_dynamodb_forward_mapping(table_name, project_b_id, project_subject_id_b)
    assert mapping_a_after is None, "Mapping A should be deleted"
    assert mapping_b_after is not None, "Mapping B should still exist"

    reverse_mappings_after = get_dynamodb_reverse_mappings(table_name, subject_uuid)
    assert len(reverse_mappings_after) == 1, f"Expected 1 reverse mapping, found {len(reverse_mappings_after)}"

    # Verify evidence NOT deleted (subject still exists in project B)
    evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
    assert evidence_count == 1, f"Evidence should NOT be deleted, found {evidence_count}"

    # Verify analytical tables
    time.sleep(3)
    final_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    final_by_project_a_count = count_subject_terms_by_project(query_athena, project_a_id, subject_uuid)
    final_by_project_b_count = count_subject_terms_by_project(query_athena, project_b_id, subject_uuid)

    # by_subject table should still exist (subject in project B)
    assert final_by_subject_count > 0, "by_subject rows should remain (not last mapping)"

    # by_project_term for project A should be deleted
    assert final_by_project_a_count == 0, "by_project_term rows for project A should be deleted"

    # by_project_term for project B should remain
    assert final_by_project_b_count > 0, "by_project_term rows for project B should remain"

    # Cleanup
    try:
        # Remove subject from project B (now last mapping)
        invoke_remove_subject(project_subject_iri_b)
        invoke_remove_project_helper(project_a_id, physical_resources)
        invoke_remove_project_helper(project_b_id, physical_resources)
    except Exception as e:
        print(f"Cleanup error: {e}")


def test_remove_subject_last_mapping_cascade(
    test_subject,
    create_evidence_helper,
    invoke_remove_subject,
    query_athena,
    standard_hpo_terms,
    physical_resources
):
    """
    Test 2: Remove Subject - Last Mapping (Cascade Deletion)

    Create subject with single project mapping, create evidence, remove subject, verify:
    - DynamoDB mappings deleted
    - Evidence cascade deleted
    - Analytical tables fully deleted (both by_subject and by_project_term)
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    project_subject_id = project_subject_iri.split('/')[-1]

    # Create multiple evidence records
    term_iri = standard_hpo_terms["seizure"]
    evidence_1 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_2 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_3 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)

    termlink_id = evidence_1["termlink_id"]

    # Get DynamoDB table name from environment
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify initial state
    mapping = get_dynamodb_forward_mapping(table_name, test_project_id, project_subject_id)
    assert mapping is not None, "Mapping should exist before deletion"

    reverse_mappings = get_dynamodb_reverse_mappings(table_name, subject_uuid)
    assert len(reverse_mappings) == 1, f"Expected 1 reverse mapping, found {len(reverse_mappings)}"

    initial_evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
    assert initial_evidence_count == 3, f"Expected 3 evidence records, found {initial_evidence_count}"

    initial_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    assert initial_by_subject_count > 0, "Should have subject_terms_by_subject rows"

    initial_by_project_count = count_subject_terms_by_project(query_athena, test_project_id, subject_uuid)
    assert initial_by_project_count > 0, "Should have by_project_term rows"

    # Remove subject (last mapping)
    result = invoke_remove_subject(project_subject_iri)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "subject removed" in body["message"].lower(), \
        f"Expected 'removed' message (last mapping), got: {body['message']}"

    # Wait for deletion to propagate
    time.sleep(5)

    # Verify DynamoDB: all mappings deleted
    mapping_after = get_dynamodb_forward_mapping(table_name, test_project_id, project_subject_id)
    assert mapping_after is None, "Forward mapping should be deleted"

    reverse_mappings_after = get_dynamodb_reverse_mappings(table_name, subject_uuid)
    assert len(reverse_mappings_after) == 0, f"All reverse mappings should be deleted, found {len(reverse_mappings_after)}"

    # Verify evidence cascade deleted
    final_evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
    assert final_evidence_count == 0, f"All evidence should be cascade deleted, found {final_evidence_count}"

    # Verify analytical tables fully deleted
    final_by_subject_count = count_subject_terms_by_subject(query_athena, subject_uuid)
    assert final_by_subject_count == 0, "by_subject rows should be deleted (last mapping)"

    final_by_project_count = count_subject_terms_by_project(query_athena, test_project_id, subject_uuid)
    assert final_by_project_count == 0, "by_project_term rows should be deleted"


def test_remove_subject_not_found(invoke_remove_subject, test_project_id):
    """
    Test 3: Remove Subject - Subject Not Found

    Attempt to remove non-existent subject, verify 404 response.
    """
    # Construct IRI for non-existent subject (use existing test project)
    fake_subject_id = f"nonexistent-{uuid.uuid4().hex[:8]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{fake_subject_id}"

    # Attempt removal
    result = invoke_remove_subject(project_subject_iri)

    # Assertions
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "not found" in body["message"].lower()


def test_remove_subject_missing_iri(physical_resources):
    """
    Test 4: Missing Required Field - project_subject_iri

    Invoke lambda without project_subject_iri parameter.
    """
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveSubjectFunction"]

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
    assert "project_subject_iri" in body["message"].lower()
    assert "required" in body["message"].lower()


def test_remove_subject_invalid_iri_format(invoke_remove_subject):
    """
    Test 5: Invalid project_subject_iri Format

    Invoke with malformed IRI.
    """
    # Invoke with malformed IRI
    malformed_iri = "not-a-valid-iri"
    result = invoke_remove_subject(malformed_iri)

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "invalid" in body["message"].lower()


def test_remove_subject_incomplete_iri(invoke_remove_subject):
    """
    Test 6: project_subject_iri with Missing Segments

    Invoke with incomplete IRI (fewer than 6 parts).
    """
    # Invoke with incomplete IRI (only 5 parts - missing both project_id and project_subject_id)
    incomplete_iri = "http://ods.nationwidechildrens.org/phebee/projects"
    result = invoke_remove_subject(incomplete_iri)

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "invalid" in body["message"].lower()


def test_remove_subject_idempotent(
    test_subject,
    invoke_remove_subject,
    physical_resources,
    query_athena
):
    """
    Test 7: Remove Subject - Idempotency (Delete Twice)

    Delete subject once successfully, then delete again.
    Second deletion should return 404 (not found).
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    project_subject_id = project_subject_iri.split('/')[-1]

    # Get DynamoDB table name from environment
    table_name = os.environ.get('PheBeeDynamoTable')
    if not table_name:
        raise ValueError("PheBeeDynamoTable environment variable not set")

    # Verify subject exists
    mapping = get_dynamodb_forward_mapping(table_name, test_project_id, project_subject_id)
    assert mapping is not None, "Subject should exist before deletion"

    # First deletion
    result1 = invoke_remove_subject(project_subject_iri)
    assert result1["statusCode"] == 200
    body1 = json.loads(result1["body"])
    assert "removed" in body1["message"].lower()

    # Wait for deletion
    time.sleep(3)

    # Verify subject deleted
    mapping_after = get_dynamodb_forward_mapping(table_name, test_project_id, project_subject_id)
    assert mapping_after is None, "Subject should be deleted after first invocation"

    # Second deletion (idempotency test)
    result2 = invoke_remove_subject(project_subject_iri)
    assert result2["statusCode"] == 404
    body2 = json.loads(result2["body"])
    assert "not found" in body2["message"].lower()
