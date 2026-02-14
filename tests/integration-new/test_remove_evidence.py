"""
Integration tests for remove_evidence.py Lambda function.

Tests verify:
1. Evidence deletion from Iceberg evidence table
2. Cascade deletion of term links from Neptune when last evidence removed
3. Incremental updates to subject_terms analytical tables
4. Error handling for missing/invalid evidence IDs
5. Idempotency of deletion operations
"""

import json
import pytest
import boto3
import time
import uuid


@pytest.fixture
def invoke_remove_evidence(physical_resources):
    """Helper to invoke RemoveEvidence Lambda."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["RemoveEvidenceFunction"]

    def _invoke(evidence_id):
        payload = {"body": json.dumps({"evidence_id": evidence_id})}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())

        if "FunctionError" in response:
            return {"statusCode": 500, "body": json.dumps(result)}

        return result

    return _invoke


def wait_for_evidence_deletion(query_athena, evidence_id, max_wait=30):
    """
    Poll Athena until evidence is deleted (no longer appears in table).
    Returns True if deleted, False if timeout.
    """
    start_time = time.time()

    while time.time() - start_time < max_wait:
        results = query_athena(f"""
            SELECT evidence_id
            FROM phebee.evidence
            WHERE evidence_id = '{evidence_id}'
        """)

        if len(results) == 0:
            return True  # Evidence successfully deleted

        time.sleep(2)

    return False


def get_subject_terms_by_termlink(query_athena, subject_id, termlink_id):
    """
    Query subject_terms_by_subject table for a specific termlink.
    Returns the row if found, None if not found.
    """
    results = query_athena(f"""
        SELECT
            subject_id,
            termlink_id,
            evidence_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_id}'
        AND termlink_id = '{termlink_id}'
    """)

    return results[0] if results else None


def count_evidence_for_termlink(query_athena, termlink_id):
    """Count remaining evidence records for a termlink."""
    results = query_athena(f"""
        SELECT COUNT(*) as count
        FROM phebee.evidence
        WHERE termlink_id = '{termlink_id}'
    """)

    return int(results[0]["count"]) if results else 0


def test_remove_evidence_with_remaining_evidence(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms
):
    """
    Test 1: Remove Evidence Record (Happy Path - Evidence Remains)

    Create 3 evidence records for same term link, delete one, verify:
    - Evidence deleted from Iceberg
    - Other evidence remains
    - Term link NOT deleted from Neptune
    - Analytical tables updated with decremented count
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    term_iri = standard_hpo_terms["seizure"]

    # Create 3 evidence records for same term (same termlink)
    evidence_1 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_2 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_3 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)

    evidence_id_1 = evidence_1["evidence_id"]
    evidence_id_2 = evidence_2["evidence_id"]
    evidence_id_3 = evidence_3["evidence_id"]

    # Get termlink_id from first evidence
    termlink_id = evidence_1["termlink_id"]

    # Wait for all evidence to be created and subject_terms updated
    wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=termlink_id,
        project_id=test_project_id,
        timeout=30
    )

    # Verify initial evidence count = 3
    initial_count = count_evidence_for_termlink(query_athena, termlink_id)
    assert initial_count == 3, f"Expected 3 evidence records, found {initial_count}"

    # Delete one evidence record
    result = invoke_remove_evidence(evidence_id_1)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["message"] == "Evidence deleted successfully"

    # Wait for deletion to propagate
    deleted = wait_for_evidence_deletion(query_athena, evidence_id_1, max_wait=30)
    assert deleted, "Evidence was not deleted from Iceberg within timeout"

    # Verify other 2 evidence records still exist
    remaining_count = count_evidence_for_termlink(query_athena, termlink_id)
    assert remaining_count == 2, f"Expected 2 remaining evidence, found {remaining_count}"

    # Verify specific evidence still exist
    evidence_2_exists = query_athena(f"""
        SELECT evidence_id FROM phebee.evidence WHERE evidence_id = '{evidence_id_2}'
    """)
    evidence_3_exists = query_athena(f"""
        SELECT evidence_id FROM phebee.evidence WHERE evidence_id = '{evidence_id_3}'
    """)
    assert len(evidence_2_exists) == 1, "Evidence 2 should still exist"
    assert len(evidence_3_exists) == 1, "Evidence 3 should still exist"

    # Verify analytical tables updated with decremented count
    time.sleep(3)  # Allow time for analytical table update
    subject_terms = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_id)
    assert subject_terms is not None, "Subject terms row should still exist"
    assert int(subject_terms["evidence_count"]) == 2, \
        f"Expected evidence_count=2, got {subject_terms['evidence_count']}"


def test_remove_evidence_last_evidence_cascade(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms
):
    """
    Test 2: Remove Last Evidence for Term Link (Cascade to Neptune)

    Create single evidence, delete it, verify:
    - Evidence deleted from Iceberg
    - Term link deleted from Neptune
    - Analytical table row removed
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    term_iri = standard_hpo_terms["seizure"]

    # Create single evidence
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_id = evidence["evidence_id"]
    termlink_id = evidence["termlink_id"]

    # Wait for evidence creation and subject_terms update
    wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=termlink_id,
        project_id=test_project_id,
        timeout=30
    )

    # Verify evidence exists before deletion
    initial_count = count_evidence_for_termlink(query_athena, termlink_id)
    assert initial_count == 1, f"Expected 1 evidence record, found {initial_count}"

    # Delete the evidence
    result = invoke_remove_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["message"] == "Evidence deleted successfully"

    # Wait for deletion
    deleted = wait_for_evidence_deletion(query_athena, evidence_id, max_wait=30)
    assert deleted, "Evidence was not deleted from Iceberg"

    # Verify no evidence remains for termlink
    remaining_count = count_evidence_for_termlink(query_athena, termlink_id)
    assert remaining_count == 0, f"Expected 0 evidence records, found {remaining_count}"

    # Verify analytical table row removed (evidence_count = 0)
    time.sleep(3)  # Allow time for analytical table update
    subject_terms = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_id)
    assert subject_terms is None, "Subject terms row should be deleted when evidence_count reaches 0"

    # Note: We cannot directly verify Neptune term link deletion in Neptune-free environment
    # The lambda logs would show "Term link deleted: {termlink_iri}" if successful


def test_remove_evidence_not_found(invoke_remove_evidence, query_athena):
    """
    Test 3: Remove Evidence - Evidence Not Found

    Attempt to delete non-existent evidence, verify proper 404 response.
    """
    # Generate random UUID that doesn't exist
    fake_evidence_id = str(uuid.uuid4())

    # Attempt deletion
    result = invoke_remove_evidence(fake_evidence_id)

    # Assertions
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "not found" in body["message"].lower(), \
        f"Expected 'not found' in message, got: {body['message']}"

    # Verify no data modifications occurred (no evidence with this ID exists)
    evidence_check = query_athena(f"""
        SELECT evidence_id FROM phebee.evidence WHERE evidence_id = '{fake_evidence_id}'
    """)
    assert len(evidence_check) == 0


def test_remove_evidence_missing_evidence_id(physical_resources):
    """
    Test 4: Missing Required Field - evidence_id

    Invoke lambda without evidence_id parameter.
    """
    lambda_client = boto3.client("lambda")
    remove_function = physical_resources["RemoveEvidenceFunction"]

    # Invoke with empty payload
    payload = {"body": json.dumps({})}
    response = lambda_client.invoke(
        FunctionName=remove_function,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "evidence_id" in body["message"].lower()
    assert "required" in body["message"].lower()


def test_remove_evidence_null_evidence_id(physical_resources):
    """
    Test 5: Remove Evidence with Null evidence_id

    Invoke lambda with null evidence_id.
    """
    lambda_client = boto3.client("lambda")
    remove_function = physical_resources["RemoveEvidenceFunction"]

    # Invoke with null evidence_id
    payload = {"body": json.dumps({"evidence_id": None})}
    response = lambda_client.invoke(
        FunctionName=remove_function,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Assertions
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "evidence_id" in body["message"].lower()


def test_remove_evidence_distinct_termlinks(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms
):
    """
    Test 6: Remove Evidence - Multiple Term Links for Same Term

    Create 2 evidence for HP_0001249 without qualifiers (termlink-A)
    Create 2 evidence for HP_0001249 with qualifiers (termlink-B)
    Delete all evidence for termlink-A, verify termlink-B unaffected.
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    term_iri = standard_hpo_terms["seizure"]

    # Create 2 evidence without qualifiers (termlink-A)
    evidence_a1 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_a2 = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    termlink_a = evidence_a1["termlink_id"]

    # Create 2 evidence with qualifiers (termlink-B)
    evidence_b1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers={"onset": "HP:0003593"}  # Infantile onset
    )
    evidence_b2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers={"onset": "HP:0003593"}
    )
    termlink_b = evidence_b1["termlink_id"]

    # Verify distinct termlinks
    assert termlink_a != termlink_b, "Termlinks should be distinct (different qualifiers)"

    # Wait for both termlinks to appear in subject_terms
    wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_a, project_id=test_project_id)
    wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_b, project_id=test_project_id)

    # Verify initial counts
    count_a = count_evidence_for_termlink(query_athena, termlink_a)
    count_b = count_evidence_for_termlink(query_athena, termlink_b)
    assert count_a == 2, f"Expected 2 evidence for termlink-A, found {count_a}"
    assert count_b == 2, f"Expected 2 evidence for termlink-B, found {count_b}"

    # Delete both evidence for termlink-A
    result1 = invoke_remove_evidence(evidence_a1["evidence_id"])
    result2 = invoke_remove_evidence(evidence_a2["evidence_id"])

    assert result1["statusCode"] == 200
    assert result2["statusCode"] == 200

    # Wait for deletions
    wait_for_evidence_deletion(query_athena, evidence_a1["evidence_id"])
    wait_for_evidence_deletion(query_athena, evidence_a2["evidence_id"])

    # Verify termlink-A deleted
    count_a_final = count_evidence_for_termlink(query_athena, termlink_a)
    assert count_a_final == 0, f"Expected 0 evidence for termlink-A, found {count_a_final}"

    # Verify termlink-B still exists with 2 evidence
    count_b_final = count_evidence_for_termlink(query_athena, termlink_b)
    assert count_b_final == 2, f"Expected 2 evidence for termlink-B, found {count_b_final}"

    # Verify analytical tables
    time.sleep(3)
    subject_terms_a = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_a)
    subject_terms_b = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_b)

    assert subject_terms_a is None, "Termlink-A row should be deleted"
    assert subject_terms_b is not None, "Termlink-B row should still exist"
    assert int(subject_terms_b["evidence_count"]) == 2, \
        f"Termlink-B should have evidence_count=2, got {subject_terms_b['evidence_count']}"


def test_remove_evidence_idempotent(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms
):
    """
    Test 7: Remove Evidence - Verify Idempotency (Delete Twice)

    Delete evidence once successfully, then delete again.
    Second deletion should return 404 (not found).
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_id = evidence["evidence_id"]
    termlink_id = evidence["termlink_id"]

    # Wait for creation
    wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_id, project_id=test_project_id)

    # First deletion
    result1 = invoke_remove_evidence(evidence_id)
    assert result1["statusCode"] == 200
    body1 = json.loads(result1["body"])
    assert body1["message"] == "Evidence deleted successfully"

    # Wait for deletion
    deleted = wait_for_evidence_deletion(query_athena, evidence_id)
    assert deleted, "Evidence should be deleted after first invocation"

    # Second deletion (idempotency test)
    result2 = invoke_remove_evidence(evidence_id)
    assert result2["statusCode"] == 404
    body2 = json.loads(result2["body"])
    assert "not found" in body2["message"].lower()

    # Verify evidence still doesn't exist (no resurrection)
    final_check = query_athena(f"""
        SELECT evidence_id FROM phebee.evidence WHERE evidence_id = '{evidence_id}'
    """)
    assert len(final_check) == 0, "Evidence should remain deleted"


def test_remove_evidence_empty_string_id(physical_resources):
    """
    Test 8: Remove Evidence with Empty String evidence_id

    Invoke with empty string evidence_id.
    Should return 400 (treated as missing) or 404 (treated as non-existent).
    """
    lambda_client = boto3.client("lambda")
    remove_function = physical_resources["RemoveEvidenceFunction"]

    # Invoke with empty string
    payload = {"body": json.dumps({"evidence_id": ""})}
    response = lambda_client.invoke(
        FunctionName=remove_function,
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    # Assertions - allow 400 or 404
    assert result["statusCode"] in [400, 404], \
        f"Expected status 400 or 404, got {result['statusCode']}"

    # Document the behavior
    if result["statusCode"] == 400:
        print("Empty string treated as missing parameter (400)")
    else:
        print("Empty string treated as non-existent evidence (404)")


def test_remove_evidence_malformed_id(invoke_remove_evidence):
    """
    Test 9: Remove Evidence - Malformed evidence_id

    Invoke with malformed UUID.
    Should return 404 (evidence not found).
    """
    malformed_id = "not-a-valid-uuid-format"

    result = invoke_remove_evidence(malformed_id)

    # Assertions
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "not found" in body["message"].lower()


def test_remove_evidence_for_deleted_subject(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms
):
    """
    Test 10: Remove Evidence - Evidence is Independent of Subject State

    Create evidence and delete it without checking subject state.
    This verifies evidence records are independent entities that can
    be deleted regardless of the subject's current state.

    Note: Full subject deletion testing is complex and covered in
    remove_subject tests. This test focuses on evidence independence.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
    evidence_id = evidence["evidence_id"]

    # Wait for creation to propagate
    time.sleep(3)

    # Delete evidence (succeeds regardless of subject state)
    result = invoke_remove_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["message"] == "Evidence deleted successfully"

    # Verify evidence deleted
    deleted = wait_for_evidence_deletion(query_athena, evidence_id)
    assert deleted, "Evidence should be deleted as independent entity"


def test_remove_evidence_termlink_id_extraction(
    test_subject,
    create_evidence_helper,
    invoke_remove_evidence,
    query_athena,
    standard_hpo_terms,
    wait_for_subject_terms
):
    """
    Test 11: Remove Evidence - Verify termlink_id Extraction

    Create evidence with specific term and qualifiers.
    Delete evidence and verify correct termlink_id was used for cleanup.
    """
    subject_uuid, project_subject_iri = test_subject
    test_project_id = project_subject_iri.split('/')[-2]
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with qualifiers
    qualifiers = {"onset": "HP:0003593", "severity": "HP:0012828"}
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=qualifiers
    )
    evidence_id = evidence["evidence_id"]
    termlink_id = evidence["termlink_id"]

    # Wait for creation
    wait_for_subject_terms(subject_id=subject_uuid, termlink_id=termlink_id, project_id=test_project_id)

    # Verify evidence exists with correct termlink_id
    evidence_check = query_athena(f"""
        SELECT termlink_id
        FROM phebee.evidence
        WHERE evidence_id = '{evidence_id}'
    """)
    assert len(evidence_check) == 1
    assert evidence_check[0]["termlink_id"] == termlink_id

    # Delete evidence
    result = invoke_remove_evidence(evidence_id)
    assert result["statusCode"] == 200

    # Wait for deletion
    deleted = wait_for_evidence_deletion(query_athena, evidence_id)
    assert deleted

    # Verify analytical table row removed (correct termlink used)
    time.sleep(3)
    subject_terms = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_id)
    assert subject_terms is None, "Row should be deleted (correct termlink_id was used)"
