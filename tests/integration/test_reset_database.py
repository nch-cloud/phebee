"""
Integration tests for reset_database Lambda function.

This function wipes all data from Neptune and DynamoDB tables.
"""
import json
import pytest
from phebee.utils.aws import get_client

# Mark all tests in this module to run last since they reset the database
pytestmark = pytest.mark.run_last


def test_reset_database_basic_success(cloudformation_stack):
    """Test that reset_database returns success response."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True
    assert "message" not in result  # No error message on success


def test_reset_database_response_structure(cloudformation_stack):
    """Test that reset_database returns expected response structure."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    # Verify required fields
    assert "statusCode" in result
    assert "success" in result
    assert isinstance(result["statusCode"], int)
    assert isinstance(result["success"], bool)


def test_reset_database_idempotency(cloudformation_stack):
    """Test that reset_database can be called multiple times without error."""
    lambda_client = get_client("lambda")

    # Call reset multiple times
    for _ in range(3):
        response = lambda_client.invoke(
            FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
            InvocationType="RequestResponse",
            Payload=json.dumps({}).encode("utf-8")
        )

        result = json.loads(response["Payload"].read().decode("utf-8"))

        assert result["statusCode"] == 200
        assert result["success"] is True


def test_reset_database_with_empty_payload(cloudformation_stack):
    """Test that reset_database works with empty payload."""
    lambda_client = get_client("lambda")

    # Call reset with empty dict payload
    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True


def test_reset_database_with_no_payload(cloudformation_stack):
    """Test that reset_database works with no payload at all."""
    lambda_client = get_client("lambda")

    # Call reset with no payload (empty string)
    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=b""
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True


def test_reset_database_clears_iceberg_tables(cloudformation_stack, test_project_id, query_athena, standard_hpo_terms):
    """Test that reset_database actually deletes all data from Iceberg tables.

    Verifies that reset_database clears all three Iceberg tables:
    - phebee.evidence
    - phebee.subject_terms_by_subject
    - phebee.subject_terms_by_project_term
    """
    lambda_client = get_client("lambda")

    # 1. Create a subject with evidence to populate all Iceberg tables
    create_subject_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-CreateSubjectFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "body": json.dumps({
                "project_id": test_project_id,
                "project_subject_id": "test_reset_subject"
            })
        }).encode("utf-8")
    )
    create_subject_result = json.loads(create_subject_response["Payload"].read().decode("utf-8"))
    assert create_subject_result["statusCode"] in [200, 201]

    subject_body = json.loads(create_subject_result["body"])
    subject_id = subject_body["subject"]["subject_id"]

    # 2. Create evidence (this populates evidence table and triggers subject_terms updates)
    term_iri = standard_hpo_terms["seizure"]
    create_evidence_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-CreateEvidenceFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "body": json.dumps({
                "subject_id": subject_id,
                "term_iri": term_iri,
                "evidence_type": "manual_annotation",
                "creator_id": "test-creator",
                "creator_type": "human"
            })
        }).encode("utf-8")
    )
    create_evidence_result = json.loads(create_evidence_response["Payload"].read().decode("utf-8"))
    assert create_evidence_result["statusCode"] == 201

    # Extract termlink_id from the evidence creation response
    evidence_body = json.loads(create_evidence_result["body"])
    termlink_id = evidence_body["termlink_id"]

    # 3. Verify data exists in all three Iceberg tables before reset
    evidence_count_before = int(query_athena("SELECT COUNT(*) as count FROM phebee.evidence")[0]["count"])
    by_subject_count_before = int(query_athena("SELECT COUNT(*) as count FROM phebee.subject_terms_by_subject")[0]["count"])
    by_project_term_count_before = int(query_athena("SELECT COUNT(*) as count FROM phebee.subject_terms_by_project_term")[0]["count"])

    assert evidence_count_before > 0, f"Expected evidence table to have data, got {evidence_count_before}"
    assert by_subject_count_before > 0, f"Expected subject_terms_by_subject table to have data, got {by_subject_count_before}"
    assert by_project_term_count_before > 0, f"Expected subject_terms_by_project_term table to have data, got {by_project_term_count_before}"

    # 4. Reset the database
    reset_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )
    reset_result = json.loads(reset_response["Payload"].read().decode("utf-8"))
    assert reset_result["statusCode"] == 200
    assert reset_result["success"] is True

    # 5. Verify all Iceberg tables are empty after reset
    evidence_count_after = int(query_athena("SELECT COUNT(*) as count FROM phebee.evidence")[0]["count"])
    by_subject_count_after = int(query_athena("SELECT COUNT(*) as count FROM phebee.subject_terms_by_subject")[0]["count"])
    by_project_term_count_after = int(query_athena("SELECT COUNT(*) as count FROM phebee.subject_terms_by_project_term")[0]["count"])

    assert evidence_count_after == 0, f"Expected evidence table to be empty after reset, got {evidence_count_after} rows"
    assert by_subject_count_after == 0, f"Expected subject_terms_by_subject table to be empty after reset, got {by_subject_count_after} rows"
    assert by_project_term_count_after == 0, f"Expected subject_terms_by_project_term table to be empty after reset, got {by_project_term_count_after} rows"
