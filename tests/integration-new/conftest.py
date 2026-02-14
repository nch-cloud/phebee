"""
Pytest configuration and fixtures for integration-new test suite.

This test suite focuses on comprehensive integration testing with Neptune-free
verification using Athena/Iceberg analytical tables.
"""

import pytest
import json
import boto3
import uuid
import time
import os
from typing import Dict, Callable, List


# Import existing fixtures from parent conftest
pytest_plugins = ['tests.integration.conftest']


# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

@pytest.fixture(scope="session", autouse=True)
def setup_environment(physical_resources):
    """Setup environment variables needed for test helpers.

    This autouse fixture runs once per test session and makes CloudFormation
    outputs available as environment variables for helper functions.
    """
    # Set DynamoDB table name for test helpers
    if "DynamoDBTable" in physical_resources:
        os.environ["PheBeeDynamoTable"] = physical_resources["DynamoDBTable"]

    yield

    # Cleanup environment (optional)
    if "PheBeeDynamoTable" in os.environ:
        del os.environ["PheBeeDynamoTable"]


# ============================================================================
# CORE FIXTURES
# ============================================================================

@pytest.fixture
def test_subject(physical_resources, test_project_id):
    """Create a test subject and return (subject_uuid, project_subject_iri).

    This fixture creates a subject in the test project and automatically
    cleans it up on test teardown (via project cleanup).

    Returns:
        tuple: (subject_uuid: str, project_subject_iri: str)
        - subject_uuid: The UUID extracted from the subject IRI (used for evidence creation)
        - project_subject_iri: The full project-scoped IRI (used for queries)

    Example:
        def test_something(test_subject):
            subject_uuid, project_subject_iri = test_subject
            # Use subject_uuid for CreateEvidence
            # Use project_subject_iri for GetSubject queries
    """
    lambda_client = boto3.client("lambda")
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:8]}"

    # Create subject
    payload = {
        "project_id": test_project_id,
        "project_subject_id": project_subject_id
    }

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    assert result["statusCode"] == 200, f"Failed to create test subject: {result}"

    body = json.loads(result["body"])
    subject_iri = body["subject"]["iri"]
    subject_uuid = subject_iri.split("/")[-1]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/{project_subject_id}"

    yield (subject_uuid, project_subject_iri)

    # Cleanup happens automatically via project cleanup
    # (when test_project_id fixture cleans up the entire project)


@pytest.fixture
def create_evidence_helper(physical_resources):
    """Return a function to create evidence with sensible defaults.

    This helper standardizes evidence creation across tests, providing
    defaults for required fields while allowing customization.

    Returns:
        Callable that creates evidence and returns parsed response body

    Example:
        def test_something(test_subject, create_evidence_helper):
            subject_uuid, _ = test_subject

            # Simple usage with defaults
            evidence = create_evidence_helper(
                subject_id=subject_uuid,
                term_iri="http://purl.obolibrary.org/obo/HP_0001250"
            )

            # With qualifiers
            evidence = create_evidence_helper(
                subject_id=subject_uuid,
                term_iri="http://purl.obolibrary.org/obo/HP_0001250",
                qualifiers=["negated", "family"]
            )

            # With custom fields
            evidence = create_evidence_helper(
                subject_id=subject_uuid,
                term_iri="http://purl.obolibrary.org/obo/HP_0001250",
                creator_id="my-custom-creator",
                span_start=10,
                span_end=20
            )
    """
    def _create_evidence(
        subject_id: str,
        term_iri: str,
        qualifiers: list = None,
        creator_id: str = "test-creator",
        evidence_type: str = "clinical_note",
        **kwargs
    ):
        lambda_client = boto3.client("lambda")

        payload = {
            "subject_id": subject_id,
            "term_iri": term_iri,
            "creator_id": creator_id,
            "evidence_type": evidence_type,
            "clinical_note_id": kwargs.get("clinical_note_id", f"note-{uuid.uuid4()}"),
            "encounter_id": kwargs.get("encounter_id", f"encounter-{uuid.uuid4()}"),
        }

        if qualifiers is not None:
            payload["qualifiers"] = qualifiers

        # Add any other kwargs (span_start, span_end, etc.)
        payload.update({k: v for k, v in kwargs.items()
                       if k not in ["clinical_note_id", "encounter_id"]})

        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateEvidenceFunction"],
            Payload=json.dumps(payload).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 201, f"Evidence creation failed: {result}"

        return json.loads(result["body"])

    return _create_evidence


@pytest.fixture(scope="session")
def standard_hpo_terms():
    """HPO terms guaranteed to exist after update_hpo fixture runs.

    Use these instead of hardcoding term IRIs in tests. This provides:
    1. Documentation of which terms are standard test data
    2. Prevention of typos in term IRIs
    3. Easy updates if test terms need to change

    Returns:
        dict: Mapping of term names to HPO term IRIs

    Example:
        def test_something(standard_hpo_terms):
            term_iri = standard_hpo_terms["seizure"]
            # Use term_iri in test...
    """
    return {
        "root": "http://purl.obolibrary.org/obo/HP_0000001",
        "phenotypic_abnormality": "http://purl.obolibrary.org/obo/HP_0000118",
        "abnormality_of_head": "http://purl.obolibrary.org/obo/HP_0000152",
        "abnormal_heart_morphology": "http://purl.obolibrary.org/obo/HP_0001627",
        "seizure": "http://purl.obolibrary.org/obo/HP_0001250",
        "neoplasm": "http://purl.obolibrary.org/obo/HP_0002664",
        "disturbance_in_speech": "http://purl.obolibrary.org/obo/HP_0001332",
    }


@pytest.fixture
def query_athena(physical_resources):
    """Execute Athena query and return results.

    This is the PRIMARY verification method for Neptune-free testing.
    Since direct Neptune access is not available, we verify data consistency
    by querying the Iceberg analytical tables via Athena.

    Returns:
        Callable that executes query and returns list of result dicts

    Example:
        def test_something(query_athena):
            # Query evidence table
            results = query_athena('''
                SELECT subject_id, term_iri, COUNT(*) as count
                FROM phebee.evidence
                WHERE run_id = 'test-run-123'
                GROUP BY subject_id, term_iri
            ''')

            assert len(results) > 0
            assert results[0]["count"] == "5"
    """
    def _query(sql: str, database: str = "phebee", timeout: int = 60) -> List[Dict[str, str]]:
        athena_client = boto3.client("athena")
        s3_bucket = physical_resources["PheBeeBucket"]

        response = athena_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": f"s3://{s3_bucket}/athena-results/"}
        )

        query_execution_id = response["QueryExecutionId"]

        # Poll for completion
        start_time = time.time()
        while time.time() - start_time < timeout:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]

            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "CANCELLED"]:
                reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query failed: {reason}\nSQL: {sql}")

            time.sleep(2)
        else:
            raise TimeoutError(f"Athena query did not complete in {timeout}s")

        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        # Parse results into list of dicts
        if len(results["ResultSet"]["Rows"]) <= 1:
            return []  # Only header row, no data

        header = [col["VarCharValue"] for col in results["ResultSet"]["Rows"][0]["Data"]]
        rows = []

        for row in results["ResultSet"]["Rows"][1:]:  # Skip header
            row_data = {}
            for i, col in enumerate(row["Data"]):
                row_data[header[i]] = col.get("VarCharValue", "")
            rows.append(row_data)

        return rows

    return _query


@pytest.fixture
def wait_for_subject_terms(stack_outputs, query_athena):
    """Helper to poll subject_terms tables until termlink appears.

    Returns a function that waits for a termlink to appear in both subject_terms tables
    and returns the records from both tables.

    Example:
        def test_something(wait_for_subject_terms):
            by_subject, by_project = wait_for_subject_terms(
                subject_id="abc-123",
                termlink_id="xyz-456",
                project_id="test-project"
            )
            assert by_subject["evidence_count"] == "1"
    """
    def _wait(subject_id: str, termlink_id: str, project_id: str, timeout: int = 60):
        """Wait for termlink to appear in both subject_terms tables."""
        by_subject_table = stack_outputs["AthenaSubjectTermsBySubjectTable"]
        by_project_term_table = stack_outputs["AthenaSubjectTermsByProjectTermTable"]
        database = stack_outputs.get("AthenaDatabase", "phebee")

        start_time = time.time()

        while time.time() - start_time < timeout:
            # Query by_subject table
            by_subject_results = query_athena(f"""
                SELECT
                    subject_id,
                    term_iri,
                    term_id,
                    qualifiers,
                    evidence_count,
                    termlink_id,
                    first_evidence_date,
                    last_evidence_date
                FROM {database}.{by_subject_table}
                WHERE subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
            """)

            # Query by_project_term table
            by_project_results = query_athena(f"""
                SELECT
                    project_id,
                    subject_id,
                    term_iri,
                    term_id,
                    qualifiers,
                    evidence_count,
                    termlink_id,
                    first_evidence_date,
                    last_evidence_date
                FROM {database}.{by_project_term_table}
                WHERE project_id = '{project_id}' AND subject_id = '{subject_id}' AND termlink_id = '{termlink_id}'
            """)

            # Both tables must have the record
            if by_subject_results and by_project_results:
                return by_subject_results[0], by_project_results[0]

            time.sleep(2)

        raise TimeoutError(
            f"Termlink {termlink_id} for subject {subject_id} not found in subject_terms tables after {timeout}s"
        )

    return _wait


# ============================================================================
# SPECIALIZED FIXTURES
# ============================================================================
# Future fixtures will be added here as patterns emerge during implementation
# following the "three uses rule" - if a pattern appears 3+ times, extract it.
