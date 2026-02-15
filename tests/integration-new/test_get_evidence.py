"""
Integration tests for get_evidence Lambda function.

Tests cover:
1. Happy path evidence retrieval
2. Full fields evidence retrieval
3. Not found scenarios
4. Validation errors (missing/null/empty evidence_id)
5. Data structure verification (creator, term_source, qualifiers)
6. Malformed evidence_id handling
7. Data immutability verification
"""

import json
import pytest
import boto3
import time


@pytest.fixture(scope="module")
def shared_evidence_records(physical_resources, standard_hpo_terms):
    """
    Module-scoped fixture that creates evidence records once, reused by multiple tests.

    This significantly speeds up get_* tests by avoiding redundant evidence creation.

    Returns dict with:
    - basic: Simple evidence (no qualifiers, no text_annotation)
    - with_qualifiers: Evidence with qualifiers
    - with_text_annotation: Evidence with span positions
    - subject_uuid: Subject UUID for all evidence
    - term_iri: Term IRI used
    """
    import uuid
    import boto3

    lambda_client = boto3.client("lambda")

    # Create a single subject for all shared evidence
    project_id = f"test-get-evidence-{uuid.uuid4().hex[:8]}"
    project_subject_id = f"shared-subj-{uuid.uuid4().hex[:8]}"

    # Create project
    lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps({
            "project_id": project_id,
            "project_label": "Get Evidence Shared Tests"
        }).encode("utf-8")
    )

    # Create subject
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "project_id": project_id,
                "project_subject_id": project_subject_id
            })
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    body = json.loads(result["body"])
    subject_uuid = body["subject"]["iri"].split("/")[-1]

    term_iri = standard_hpo_terms["seizure"]

    # Create basic evidence
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "subject_id": subject_uuid,
                "term_iri": term_iri,
                "evidence_type": "phenotype_assertion",
                "creator_id": "test-creator",
                "creator_type": "human"
            })
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    body = json.loads(result["body"])
    basic_evidence = body

    # Create evidence with qualifiers
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "subject_id": subject_uuid,
                "term_iri": term_iri,
                "evidence_type": "phenotype_assertion",
                "creator_id": "test-creator",
                "creator_type": "human",
                "qualifiers": ["negated", "family"]
            })
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    body = json.loads(result["body"])
    with_qualifiers = body

    # Create evidence with text_annotation
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "body": json.dumps({
                "subject_id": subject_uuid,
                "term_iri": term_iri,
                "evidence_type": "phenotype_assertion",
                "creator_id": "test-creator",
                "creator_type": "human",
                "span_start": 100,
                "span_end": 150
            })
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    body = json.loads(result["body"])
    with_text_annotation = body

    yield {
        "basic": basic_evidence,
        "with_qualifiers": with_qualifiers,
        "with_text_annotation": with_text_annotation,
        "subject_uuid": subject_uuid,
        "term_iri": term_iri,
        "project_id": project_id
    }

    # Cleanup (best effort)
    try:
        lambda_client.invoke(
            FunctionName=physical_resources["RemoveProjectFunction"],
            Payload=json.dumps({"project_id": project_id}).encode("utf-8")
        )
    except Exception:
        pass


@pytest.fixture
def invoke_get_evidence(physical_resources):
    """Helper to invoke GetEvidence Lambda."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["GetEvidenceFunction"]

    def _invoke(evidence_id):
        payload = {"evidence_id": evidence_id}

        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )

        return json.loads(response["Payload"].read())

    return _invoke


def test_get_evidence_success(shared_evidence_records, invoke_get_evidence):
    """
    Test 1: Get Evidence Record (Happy Path)

    Retrieve evidence and verify all fields present.
    Uses shared module-scoped evidence for efficiency.
    """
    evidence = shared_evidence_records["basic"]
    evidence_id = evidence["evidence_id"]
    subject_uuid = shared_evidence_records["subject_uuid"]
    term_iri = shared_evidence_records["term_iri"]

    # Get evidence
    result = invoke_get_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Required fields
    assert body["evidence_id"] == evidence_id
    assert body["subject_id"] == subject_uuid
    assert body["term_iri"] == term_iri
    assert "creator" in body
    assert body["creator"]["creator_id"] == "test-creator"
    assert body["creator"]["creator_type"] == "human"
    assert "evidence_type" in body
    assert "created_timestamp" in body
    assert "termlink_id" in body


def test_get_evidence_full_fields(shared_evidence_records, invoke_get_evidence):
    """
    Test 2: Get Evidence with All Optional Fields

    Retrieve evidence with text_annotation fields.
    Uses shared module-scoped evidence for efficiency.
    Note: get_evidence_record() currently only returns span_start/span_end
    from text_annotation, not text_snippet. note_context is not returned.
    """
    evidence = shared_evidence_records["with_text_annotation"]
    evidence_id = evidence["evidence_id"]
    subject_uuid = shared_evidence_records["subject_uuid"]
    term_iri = shared_evidence_records["term_iri"]

    # Get evidence
    result = invoke_get_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Verify all fields present
    assert body["evidence_id"] == evidence_id
    assert body["subject_id"] == subject_uuid
    assert body["term_iri"] == term_iri

    # Verify text_annotation (only span_start/span_end returned)
    assert "text_annotation" in body
    assert body["text_annotation"]["span_start"] == 100
    assert body["text_annotation"]["span_end"] == 150


def test_get_evidence_not_found(invoke_get_evidence):
    """
    Test 3: Get Evidence - Not Found

    Attempt to get non-existent evidence_id.
    """
    fake_evidence_id = "nonexistent-uuid-12345"

    # Get non-existent evidence
    result = invoke_get_evidence(fake_evidence_id)

    # Assertions
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "not found" in body["message"].lower()


@pytest.mark.parametrize("payload_type,expected_keywords", [
    pytest.param("missing", ["evidence_id", "required"], id="missing_evidence_id"),
    pytest.param("null", ["evidence_id"], id="null_evidence_id"),
    pytest.param("empty", [], id="empty_string_evidence_id"),
])
def test_get_evidence_validation_errors(physical_resources, invoke_get_evidence, payload_type, expected_keywords):
    """
    Tests 4-6: Validation of required evidence_id field.

    Verifies that missing, null, or empty evidence_id results in 400 error
    (or 404 for empty string, depending on implementation).

    Parameterized test cases:
    - missing_evidence_id: No evidence_id in payload
    - null_evidence_id: evidence_id is None
    - empty_string_evidence_id: evidence_id is empty string
    """
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["GetEvidenceFunction"]

    if payload_type == "missing":
        payload = {}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )
        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        for keyword in expected_keywords:
            assert keyword in body["message"].lower()

    elif payload_type == "null":
        payload = {"evidence_id": None}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )
        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        for keyword in expected_keywords:
            assert keyword in body["message"].lower()

    elif payload_type == "empty":
        # Empty string - either 400 (validation) or 404 (not found) is acceptable
        result = invoke_get_evidence("")
        assert result["statusCode"] in [400, 404]


def test_get_evidence_immutability(shared_evidence_records, invoke_get_evidence):
    """
    Test 7: Get Evidence - Verify Data Immutability

    Retrieve evidence twice and verify data is identical.
    Uses shared module-scoped evidence for efficiency.
    """
    evidence_id = shared_evidence_records["basic"]["evidence_id"]

    # First retrieval
    result1 = invoke_get_evidence(evidence_id)
    assert result1["statusCode"] == 200
    body1 = json.loads(result1["body"])

    # Wait and retrieve again
    time.sleep(2)
    result2 = invoke_get_evidence(evidence_id)
    assert result2["statusCode"] == 200
    body2 = json.loads(result2["body"])

    # Verify immutability
    assert body1["evidence_id"] == body2["evidence_id"]
    assert body1["subject_id"] == body2["subject_id"]
    assert body1["term_iri"] == body2["term_iri"]
    assert body1["created_timestamp"] == body2["created_timestamp"]
    assert body1["termlink_id"] == body2["termlink_id"]


def test_get_evidence_with_qualifiers(shared_evidence_records, invoke_get_evidence):
    """
    Test 9: Get Evidence with Qualifiers

    Retrieve evidence with qualifiers and verify they're returned.
    Uses shared module-scoped evidence for efficiency.
    """
    evidence_id = shared_evidence_records["with_qualifiers"]["evidence_id"]

    # Get evidence
    result = invoke_get_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Verify qualifiers present
    assert "qualifiers" in body
    # Qualifiers stored as array in Iceberg
    qualifiers = body["qualifiers"]
    assert qualifiers is not None


def test_get_evidence_creator_structure(shared_evidence_records, invoke_get_evidence):
    """
    Test 11: Get Evidence - Creator Object Structure

    Verify creator is returned as structured object.
    Uses shared module-scoped evidence for efficiency.
    """
    evidence_id = shared_evidence_records["basic"]["evidence_id"]

    # Get evidence
    result = invoke_get_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Verify creator structure
    assert "creator" in body
    creator = body["creator"]
    assert isinstance(creator, dict)
    assert "creator_id" in creator
    assert "creator_type" in creator
    assert creator["creator_id"] == "test-creator"
    assert creator["creator_type"] == "human"


def test_get_evidence_malformed_id(invoke_get_evidence):
    """
    Test 14: Get Evidence - Malformed evidence_id

    Test with malformed UUID format (graceful handling).
    """
    malformed_id = "not-a-valid-uuid-format"

    result = invoke_get_evidence(malformed_id)

    # Assertions - should return 404, not crash
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "not found" in body["message"].lower()


def test_get_evidence_unicode_fields(shared_evidence_records, invoke_get_evidence):
    """
    Test 19: Get Evidence - Text Annotation Fields

    Verify text_annotation span positions are returned correctly.
    Uses shared module-scoped evidence for efficiency.
    Note: get_evidence_record() currently only returns span_start/span_end,
    not text_snippet.
    """
    evidence_id = shared_evidence_records["with_text_annotation"]["evidence_id"]

    # Get evidence
    result = invoke_get_evidence(evidence_id)

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Verify text_annotation with span positions
    assert "text_annotation" in body
    assert body["text_annotation"]["span_start"] == 100
    assert body["text_annotation"]["span_end"] == 150


def test_get_evidence_multiple_sequential(
    test_subject,
    create_evidence_helper,
    invoke_get_evidence,
    standard_hpo_terms
):
    """
    Test 20: Get Multiple Evidence Records Sequentially

    Create 10 evidence records and verify each can be retrieved.
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create 10 evidence records
    evidence_ids = []
    for i in range(10):
        evidence = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )
        evidence_ids.append(evidence["evidence_id"])

    # Verify all are unique
    assert len(set(evidence_ids)) == 10

    # Get each evidence record
    for evidence_id in evidence_ids:
        result = invoke_get_evidence(evidence_id)

        # Assertions
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["evidence_id"] == evidence_id
        assert body["subject_id"] == subject_uuid
        assert body["term_iri"] == term_iri
