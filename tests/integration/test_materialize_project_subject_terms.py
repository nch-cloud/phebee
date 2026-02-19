"""
Integration tests for MaterializeProjectSubjectTerms Lambda.

Tests the materialization of subject-terms analytical tables from evidence data.

NOTE: The MaterializeProjectSubjectTermsFunction was missing the 'PheBeeDynamoTable'
environment variable and DynamoDB permissions. This has been fixed in template.yaml.

REDEPLOY REQUIRED: Run `sam build && sam deploy` to apply the template changes
before running these tests.
"""
import json
import pytest
import time
from phebee.utils.aws import get_client


def invoke_materialize(project_id, physical_resources, batch_size=None):
    """Helper to invoke MaterializeProjectSubjectTerms lambda."""
    lambda_client = get_client("lambda")

    payload = {"project_id": project_id}
    if batch_size is not None:
        payload["batch_size"] = batch_size

    response = lambda_client.invoke(
        FunctionName=physical_resources["MaterializeProjectSubjectTermsFunction"],
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    return result


def test_materialize_project_with_evidence(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 1: Materialize project with subjects and evidence.

    Verifies that materialization aggregates evidence into both subject_terms tables.
    """
    subject_uuid, _ = test_subject

    # Create evidence for multiple terms
    evidence_ids = []
    for term_key in ["seizure", "neoplasm"]:
        evidence_id = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=standard_hpo_terms[term_key],
            run_id=f"test-run-{int(time.time())}"
        )
        evidence_ids.append(evidence_id)

    # Materialize the project
    result = invoke_materialize(test_project_id, physical_resources)

    assert result["statusCode"] == 200
    assert result["status"] == "success"
    assert result["project_id"] == test_project_id
    assert result["subjects_processed"] >= 1
    assert result["terms_materialized"] >= 2

    # Verify data in by_subject table
    by_subject_query = f"""
        SELECT subject_id, term_iri, evidence_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_uuid}'
    """
    by_subject_results = query_athena(by_subject_query)
    assert len(by_subject_results) == 2  # Two terms

    for row in by_subject_results:
        assert int(row["evidence_count"]) >= 1

    # Verify data in by_project_term table
    by_project_query = f"""
        SELECT project_id, subject_id, term_iri, evidence_count
        FROM phebee.subject_terms_by_project_term
        WHERE project_id = '{test_project_id}'
        AND subject_id = '{subject_uuid}'
    """
    by_project_results = query_athena(by_project_query)
    assert len(by_project_results) == 2  # Two terms


def test_materialize_empty_project(physical_resources, test_project_id):
    """
    Test 2: Materialize project with no subjects.

    Verifies that empty projects are handled gracefully.
    """
    result = invoke_materialize(test_project_id, physical_resources)

    assert result["statusCode"] == 200
    assert result["status"] == "success"
    assert result["subjects_processed"] == 0
    assert result["terms_materialized"] == 0


def test_materialize_missing_project_id(physical_resources):
    """
    Test 3: Missing project_id parameter.

    Verifies error handling for missing required field.
    """
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["MaterializeProjectSubjectTermsFunction"],
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 500
    assert result["status"] == "failed"
    assert "Missing or invalid project_id" in result["error"]


def test_materialize_null_project_id(physical_resources):
    """
    Test 4: Null project_id parameter.

    Verifies error handling for null project ID.
    """
    result = invoke_materialize(None, physical_resources)

    assert result["statusCode"] == 500
    assert result["status"] == "failed"
    assert "Missing or invalid project_id" in result["error"]


def test_materialize_string_null_project_id(physical_resources):
    """
    Test 5: String "null" as project_id.

    Verifies that string "null" is rejected.
    """
    result = invoke_materialize("null", physical_resources)

    assert result["statusCode"] == 500
    assert result["status"] == "failed"
    assert "Missing or invalid project_id" in result["error"]


def test_materialize_nonexistent_project(physical_resources):
    """
    Test 6: Materialize nonexistent project.

    Verifies that nonexistent projects succeed with 0 subjects.
    """
    result = invoke_materialize("nonexistent-project-12345", physical_resources)

    assert result["statusCode"] == 200
    assert result["status"] == "success"
    assert result["subjects_processed"] == 0
    assert result["terms_materialized"] == 0


def test_materialize_custom_batch_size(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms
):
    """
    Test 7: Materialize with custom batch size.

    Verifies that batch_size parameter is accepted.
    """
    subject_uuid, _ = test_subject

    # Create some evidence
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["seizure"]
    )

    # Materialize with custom batch size
    result = invoke_materialize(test_project_id, physical_resources, batch_size=50)

    assert result["statusCode"] == 200
    assert result["status"] == "success"
    assert result["subjects_processed"] >= 1


def test_materialize_replaces_existing_data(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 13: Re-materialization replaces existing data.

    Verifies that re-materializing updates analytical tables correctly.
    """
    subject_uuid, _ = test_subject

    # Create initial evidence
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["seizure"]
    )

    # First materialization
    result1 = invoke_materialize(test_project_id, physical_resources)
    assert result1["statusCode"] == 200
    initial_terms = result1["terms_materialized"]

    # Create additional evidence
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["neoplasm"]
    )

    # Second materialization
    result2 = invoke_materialize(test_project_id, physical_resources)
    assert result2["statusCode"] == 200

    # Should have more terms now
    assert result2["terms_materialized"] >= initial_terms

    # Verify new data in tables
    by_subject_query = f"""
        SELECT COUNT(*) as term_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_uuid}'
    """
    results = query_athena(by_subject_query)
    assert int(results[0]["term_count"]) >= 2


def test_materialize_qualifiers_preserved(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 18: Qualifiers are preserved in materialized tables.

    Verifies that negated and other qualifiers are correctly aggregated.
    """
    subject_uuid, _ = test_subject

    # Create evidence with negated qualifier
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["seizure"],
        qualifiers=["negated"]
    )

    # Materialize
    result = invoke_materialize(test_project_id, physical_resources)
    assert result["statusCode"] == 200

    # Query for qualifiers in by_subject table
    query = f"""
        SELECT qualifiers
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_uuid}'
        AND term_iri = '{standard_hpo_terms["seizure"]}'
    """
    results = query_athena(query)
    assert len(results) == 1

    # Verify negated qualifier is present
    qualifiers = results[0]["qualifiers"]
    assert "negated" in qualifiers.lower()


def test_materialize_evidence_dates_aggregated(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 19: Evidence dates are correctly aggregated.

    Verifies that first_evidence_date and last_evidence_date are computed.
    """
    subject_uuid, _ = test_subject

    # Create multiple evidence records for same term
    for i in range(3):
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=standard_hpo_terms["seizure"],
            run_id=f"test-run-{i}"
        )
        time.sleep(0.1)  # Small delay between creates

    # Materialize
    result = invoke_materialize(test_project_id, physical_resources)
    assert result["statusCode"] == 200

    # Query for dates
    query = f"""
        SELECT
            first_evidence_date,
            last_evidence_date,
            evidence_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_uuid}'
        AND term_iri = '{standard_hpo_terms["seizure"]}'
    """
    results = query_athena(query)
    assert len(results) == 1

    row = results[0]
    assert row["first_evidence_date"] is not None
    assert row["last_evidence_date"] is not None
    assert int(row["evidence_count"]) == 3

    # first_evidence_date should be <= last_evidence_date
    # (They're in ISO format strings, so lexicographic comparison works)
    assert row["first_evidence_date"] <= row["last_evidence_date"]


def test_materialize_idempotency(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 20: Materialization is idempotent.

    Verifies that re-materializing with same data produces consistent results.
    """
    subject_uuid, _ = test_subject

    # Create evidence
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["seizure"]
    )

    # First materialization
    result1 = invoke_materialize(test_project_id, physical_resources)
    assert result1["statusCode"] == 200

    # Second materialization (no new data)
    result2 = invoke_materialize(test_project_id, physical_resources)
    assert result2["statusCode"] == 200

    # Results should be identical
    assert result1["subjects_processed"] == result2["subjects_processed"]
    assert result1["terms_materialized"] == result2["terms_materialized"]

    # Verify no duplication in tables
    by_subject_query = f"""
        SELECT COUNT(*) as term_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id = '{subject_uuid}'
    """
    results = query_athena(by_subject_query)
    assert int(results[0]["term_count"]) == 1  # Still just 1 term


def test_materialize_multiple_subjects(
    physical_resources,
    test_project_id,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test: Materialize project with multiple subjects.

    Verifies that materialization handles multiple subjects correctly.
    """
    import boto3
    import uuid

    lambda_client = boto3.client("lambda")

    # Create 3 subjects in the project
    subject_uuids = []
    for i in range(3):
        project_subject_id = f"test-subj-{uuid.uuid4().hex[:8]}"

        payload = {
            "project_id": test_project_id,
            "project_subject_id": project_subject_id
        }

        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
        )

        result = json.loads(response["Payload"].read().decode("utf-8"))
        body = json.loads(result["body"])
        subject_iri = body["subject"]["iri"]
        subject_uuid = subject_iri.split("/")[-1]
        subject_uuids.append(subject_uuid)

        # Create evidence for each subject
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=standard_hpo_terms["seizure"]
        )

    # Materialize the project
    result = invoke_materialize(test_project_id, physical_resources)

    assert result["statusCode"] == 200
    assert result["subjects_processed"] == 3
    assert result["terms_materialized"] >= 3

    # Verify all subjects in by_subject table
    for subject_uuid in subject_uuids:
        query = f"""
            SELECT subject_id, evidence_count
            FROM phebee.subject_terms_by_subject
            WHERE subject_id = '{subject_uuid}'
        """
        results = query_athena(query)
        assert len(results) == 1
        assert int(results[0]["evidence_count"]) >= 1


def test_materialize_project_term_partitioning(
    physical_resources,
    test_project_id,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms,
    query_athena
):
    """
    Test 12: by_project_term table is correctly partitioned.

    Verifies that the by_project_term table allows efficient term-based queries.
    """
    subject_uuid, _ = test_subject

    # Create evidence for multiple terms
    terms = [
        standard_hpo_terms["seizure"],
        standard_hpo_terms["neoplasm"]
    ]

    for term_iri in terms:
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )

    # Materialize
    result = invoke_materialize(test_project_id, physical_resources)
    assert result["statusCode"] == 200

    # Query by project and term
    query = f"""
        SELECT project_id, term_iri, COUNT(*) as subject_count
        FROM phebee.subject_terms_by_project_term
        WHERE project_id = '{test_project_id}'
        AND term_iri = '{standard_hpo_terms["seizure"]}'
        GROUP BY project_id, term_iri
    """
    results = query_athena(query)

    assert len(results) == 1
    assert results[0]["project_id"] == test_project_id
    assert int(results[0]["subject_count"]) >= 1
