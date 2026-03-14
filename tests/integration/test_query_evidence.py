"""
Integration tests for QueryEvidence Lambda.

Tests the Athena-based evidence query by termlink with pagination,
qualifier handling, and run_id filtering.
"""
import json
import pytest
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration]


def invoke_query_evidence(subject_id, term_iri, qualifiers, app_name, run_id=None, limit=None, next_token=None):
    """Helper to invoke QueryEvidence lambda."""
    lambda_client = get_client("lambda")

    payload = {
        "subject_id": subject_id,
        "term_iri": term_iri,
        "qualifiers": qualifiers
    }
    if run_id:
        payload["run_id"] = run_id
    if limit is not None:
        payload["limit"] = limit
    if next_token:
        payload["next_token"] = next_token

    response = lambda_client.invoke(
        FunctionName=f"{app_name}-QueryEvidenceFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8")
    )

    return json.loads(response["Payload"].read().decode("utf-8"))


def test_query_evidence_single_record(test_subject, create_evidence_helper, app_name):
    """Test querying evidence for termlink with single record."""
    subject_uuid, _ = test_subject
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create evidence
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[]
    )

    # Query
    result = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name
    )

    # Validate
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["evidence_count"] == 1
    assert body["has_more"] == False
    assert body["total_count"] == 1
    assert len(body["evidence"]) == 1
    assert body["evidence"][0]["evidence_id"] == evidence["evidence_id"]


def test_query_evidence_with_qualifiers(test_subject, create_evidence_helper, app_name):
    """Test querying evidence with qualifiers."""
    subject_uuid, _ = test_subject
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create evidence with negated qualifier
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"]
    )

    # Query with qualifiers
    result = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"],
        app_name=app_name
    )

    body = json.loads(result["body"])
    assert body["evidence_count"] == 1
    assert body["qualifiers"] == ["negated"]


def test_query_evidence_pagination(test_subject, create_evidence_helper, app_name):
    """Test pagination with limit."""
    subject_uuid, _ = test_subject
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create 5 evidence records for same termlink
    # Note: Keep sequential to avoid race conditions in termlink creation
    for _ in range(5):
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri,
            qualifiers=[]
        )

    # Query first page (limit=2)
    page1 = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name,
        limit=2
    )

    body1 = json.loads(page1["body"])
    assert body1["evidence_count"] == 2
    assert body1["has_more"] == True
    assert body1["total_count"] == 5
    assert "next_token" in body1

    # Query second page
    page2 = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name,
        limit=2,
        next_token=body1["next_token"]
    )

    body2 = json.loads(page2["body"])
    assert body2["evidence_count"] == 2
    assert body2["has_more"] == True
    assert "total_count" not in body2  # Only on first page

    # Query third page (final)
    page3 = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name,
        limit=2,
        next_token=body2["next_token"]
    )

    body3 = json.loads(page3["body"])
    assert body3["evidence_count"] == 1  # Last record
    assert body3["has_more"] == False


def test_query_evidence_filter_by_run_id(test_subject, create_evidence_helper, app_name):
    """Test filtering by run_id."""
    subject_uuid, _ = test_subject
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create evidence with different run_ids
    # Note: Keep sequential to avoid race conditions
    run_id_1 = "test-run-1"
    run_id_2 = "test-run-2"

    for _ in range(2):
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri,
            qualifiers=[],
            run_id=run_id_1
        )

    for _ in range(3):
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri,
            qualifiers=[],
            run_id=run_id_2
        )

    # Query for run_id_1 only
    result = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name,
        run_id=run_id_1
    )

    body = json.loads(result["body"])
    assert body["evidence_count"] == 2
    assert body["total_count"] == 2
    assert all(e["run_id"] == run_id_1 for e in body["evidence"])


def test_query_evidence_no_results(app_name):
    """Test query with no matching evidence."""
    result = invoke_query_evidence(
        subject_id="nonexistent-subject",
        term_iri="http://purl.obolibrary.org/obo/HP_0001250",
        qualifiers=[],
        app_name=app_name
    )

    body = json.loads(result["body"])
    assert body["evidence_count"] == 0
    assert body["total_count"] == 0
    assert body["has_more"] == False
    assert body["evidence"] == []


def test_query_evidence_missing_parameters(app_name):
    """Test validation of required parameters."""
    lambda_client = get_client("lambda")

    # Missing subject_id
    response = lambda_client.invoke(
        FunctionName=f"{app_name}-QueryEvidenceFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
            "qualifiers": []
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    assert result["statusCode"] == 400
    assert "subject_id" in json.loads(result["body"])["message"].lower()

    # Missing term_iri
    response = lambda_client.invoke(
        FunctionName=f"{app_name}-QueryEvidenceFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "subject_id": "test-subject",
            "qualifiers": []
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    assert result["statusCode"] == 400
    assert "term_iri" in json.loads(result["body"])["message"].lower()


def test_query_evidence_different_qualifiers_separate_termlinks(test_subject, create_evidence_helper, app_name):
    """Test that different qualifiers create separate termlinks."""
    subject_uuid, _ = test_subject
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"

    # Create evidence without qualifiers
    evidence_no_qual = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[]
    )

    # Create evidence with negated qualifier
    evidence_negated = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"]
    )

    # Query without qualifiers - should only get 1 record
    result_no_qual = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=[],
        app_name=app_name
    )

    body_no_qual = json.loads(result_no_qual["body"])
    assert body_no_qual["evidence_count"] == 1
    assert body_no_qual["total_count"] == 1
    assert body_no_qual["evidence"][0]["evidence_id"] == evidence_no_qual["evidence_id"]

    # Query with negated qualifier - should only get 1 record
    result_negated = invoke_query_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"],
        app_name=app_name
    )

    body_negated = json.loads(result_negated["body"])
    assert body_negated["evidence_count"] == 1
    assert body_negated["total_count"] == 1
    assert body_negated["evidence"][0]["evidence_id"] == evidence_negated["evidence_id"]

    # Verify termlink_ids are different
    assert body_no_qual["termlink_id"] != body_negated["termlink_id"]
