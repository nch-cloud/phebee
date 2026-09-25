"""
Integration tests for reset_database Lambda function.

This function wipes all data from the DynamoDB table, the Neptune database and the
Iceberg tables of the stack under test.

Because of that, these tests only run against a stack the test session deployed for
itself. When the suite is pointed at an existing stack (--existing-stack or a
.phebee-test-stack file), they are skipped unless PHEBEE_ALLOW_DATABASE_RESET=1 is set,
so a run against a shared or production stack cannot erase its data by accident.
"""
import json
import os
import pytest
from phebee.utils.aws import get_client


def _targets_existing_stack(config) -> bool:
    """Mirror the stack resolution in conftest.cloudformation_stack: flag first, then file."""
    if config.getoption("--existing-stack"):
        return True
    config_file_path = os.path.join(os.getcwd(), ".phebee-test-stack")
    if os.path.exists(config_file_path):
        with open(config_file_path, "r") as f:
            return bool(f.read().strip())
    return False


# Mark all tests in this module to run last since they reset the database.
# The skip condition is a string so pytest evaluates it before any fixture resolves the stack.
pytestmark = [
    pytest.mark.run_last,
    pytest.mark.skipif(
        "_targets_existing_stack(config) and os.environ.get('PHEBEE_ALLOW_DATABASE_RESET') != '1'",
        reason="reset_database erases all data in the target stack; set PHEBEE_ALLOW_DATABASE_RESET=1 "
               "to run it against an existing stack",
    ),
]


def test_reset_database_basic_success(app_name):
    """Test that reset_database returns success response."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{app_name}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True
    assert "message" not in result  # No error message on success


def test_reset_database_response_structure(app_name):
    """Test that reset_database returns expected response structure."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{app_name}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    # Verify required fields
    assert "statusCode" in result
    assert "success" in result
    assert isinstance(result["statusCode"], int)
    assert isinstance(result["success"], bool)


def test_reset_database_idempotency(app_name):
    """Test that reset_database can be called multiple times without error."""
    lambda_client = get_client("lambda")

    # Call reset multiple times
    for _ in range(3):
        response = lambda_client.invoke(
            FunctionName=f"{app_name}-ResetDatabaseFunction",
            InvocationType="RequestResponse",
            Payload=json.dumps({}).encode("utf-8")
        )

        result = json.loads(response["Payload"].read().decode("utf-8"))

        assert result["statusCode"] == 200
        assert result["success"] is True


def test_reset_database_with_empty_payload(app_name):
    """Test that reset_database works with empty payload."""
    lambda_client = get_client("lambda")

    # Call reset with empty dict payload
    response = lambda_client.invoke(
        FunctionName=f"{app_name}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True


def test_reset_database_with_no_payload(app_name):
    """Test that reset_database works with no payload at all."""
    lambda_client = get_client("lambda")

    # Call reset with no payload (empty string)
    response = lambda_client.invoke(
        FunctionName=f"{app_name}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=b""
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 200
    assert result["success"] is True


def test_reset_database_clears_iceberg_tables(app_name, test_project_id, query_athena, standard_hpo_terms):
    """Test that reset_database actually deletes all data from Iceberg tables.

    Verifies that reset_database clears all four Iceberg tables:
    - phebee.evidence
    - phebee.subject_terms_by_subject
    - phebee.subject_terms_by_project_term
    - phebee.ontology_hierarchy

    The ontology hierarchy assertions require HPO to be installed in the
    target stack before this test runs, which the standard_hpo_terms fixture
    already assumes.

    This test leaves the stack with no ontology: the hierarchy table is empty
    and the Neptune reset removed the hpo~<version> graph. Reinstall via
    UpdateHPOSFN before running anything that expands child terms or resolves
    term labels.
    """
    lambda_client = get_client("lambda")

    # 1. Create a subject with evidence to populate all Iceberg tables
    create_subject_response = lambda_client.invoke(
        FunctionName=f"{app_name}-CreateSubjectFunction",
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
        FunctionName=f"{app_name}-CreateEvidenceFunction",
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

    # 3. Verify data exists in all four Iceberg tables before reset
    evidence_count_before = int(query_athena("SELECT COUNT(*) as count FROM evidence")[0]["count"])
    by_subject_count_before = int(query_athena("SELECT COUNT(*) as count FROM subject_terms_by_subject")[0]["count"])
    by_project_term_count_before = int(query_athena("SELECT COUNT(*) as count FROM subject_terms_by_project_term")[0]["count"])
    hierarchy_count_before = int(query_athena("SELECT COUNT(*) as count FROM ontology_hierarchy")[0]["count"])

    assert evidence_count_before > 0, f"Expected evidence table to have data, got {evidence_count_before}"
    assert by_subject_count_before > 0, f"Expected subject_terms_by_subject table to have data, got {by_subject_count_before}"
    assert by_project_term_count_before > 0, f"Expected subject_terms_by_project_term table to have data, got {by_project_term_count_before}"
    assert hierarchy_count_before > 0, (
        f"Expected ontology_hierarchy table to have data, got {hierarchy_count_before}. "
        "Install an ontology (UpdateHPOSFN) before running this test."
    )

    # 4. Reset the database
    reset_response = lambda_client.invoke(
        FunctionName=f"{app_name}-ResetDatabaseFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )
    reset_result = json.loads(reset_response["Payload"].read().decode("utf-8"))
    assert reset_result["statusCode"] == 200
    assert reset_result["success"] is True

    # 5. Verify all Iceberg tables are empty after reset
    evidence_count_after = int(query_athena("SELECT COUNT(*) as count FROM evidence")[0]["count"])
    by_subject_count_after = int(query_athena("SELECT COUNT(*) as count FROM subject_terms_by_subject")[0]["count"])
    by_project_term_count_after = int(query_athena("SELECT COUNT(*) as count FROM subject_terms_by_project_term")[0]["count"])
    hierarchy_count_after = int(query_athena("SELECT COUNT(*) as count FROM ontology_hierarchy")[0]["count"])

    assert evidence_count_after == 0, f"Expected evidence table to be empty after reset, got {evidence_count_after} rows"
    assert by_subject_count_after == 0, f"Expected subject_terms_by_subject table to be empty after reset, got {by_subject_count_after} rows"
    assert by_project_term_count_after == 0, f"Expected subject_terms_by_project_term table to be empty after reset, got {by_project_term_count_after} rows"
    assert hierarchy_count_after == 0, f"Expected ontology_hierarchy table to be empty after reset, got {hierarchy_count_after} rows"
