"""
Integration tests for get_subject lambda function.

These tests verify subject retrieval via two query modes:
- By internal subject_id (direct Iceberg query)
- By project_subject_iri (DynamoDB lookup + Iceberg query)

Key verification:
- Subject data returned correctly with terms and evidence counts
- DynamoDB mappings resolve correctly
- Iceberg analytical tables queried correctly
- Edge cases handled (no terms, not found, invalid input)
"""

import pytest
import json
import uuid
from phebee.utils.aws import get_client


pytestmark = pytest.mark.integration


# ============================================================================
# Helper Functions
# ============================================================================

def get_subject(subject_id: str = None, project_subject_iri: str = None, physical_resources: dict = None):
    """Helper to invoke GetSubjectFunction."""
    lambda_client = get_client("lambda")

    payload = {}
    if subject_id:
        payload["subject_id"] = subject_id
    if project_subject_iri:
        payload["project_subject_iri"] = project_subject_iri

    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())
    return result


# ============================================================================
# Test Cases
# ============================================================================

def test_get_subject_by_id_success(physical_resources, test_subject, test_project_id, query_athena,
                                     standard_hpo_terms, create_evidence_helper):
    """
    Test 1: Get Subject by subject_id (Happy Path)

    Verifies that a subject can be retrieved by its internal subject_id,
    with correct aggregation of terms and evidence counts.
    """
    subject_uuid, project_subject_iri = test_subject

    # Setup: Create evidence for 2 different terms
    term1 = standard_hpo_terms["seizure"]  # HP_0001250
    term2 = standard_hpo_terms["abnormal_heart_morphology"]  # HP_0001627

    # Create 2 evidence for term1
    evidence1 = create_evidence_helper(subject_id=subject_uuid, term_iri=term1)
    evidence2 = create_evidence_helper(subject_id=subject_uuid, term_iri=term1)

    # Create 1 evidence for term2
    evidence3 = create_evidence_helper(subject_id=subject_uuid, term_iri=term2)

    # Action: Get subject by subject_id
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    # Assertions: Response structure
    assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}: {result.get('body', '')}"

    body = json.loads(result["body"])
    assert body["subject_id"] == subject_uuid
    assert body["subject_iri"] == f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_uuid}"
    assert "terms" in body
    assert len(body["terms"]) == 2, f"Expected 2 terms, got {len(body['terms'])}"

    # Assertions: Terms data
    terms = body["terms"]

    # Find term1 and term2 in response
    term1_data = next((t for t in terms if t["term_iri"] == term1), None)
    term2_data = next((t for t in terms if t["term_iri"] == term2), None)

    assert term1_data is not None, f"Term {term1} not found in response"
    assert term2_data is not None, f"Term {term2} not found in response"

    # Verify evidence counts
    assert term1_data["evidence_count"] == 2, f"Expected evidence_count=2 for {term1}"
    assert term2_data["evidence_count"] == 1, f"Expected evidence_count=1 for {term2}"

    # Verify each term has termlink_id
    assert "termlink_id" in term1_data
    assert "termlink_id" in term2_data

    # Verify qualifiers array present (should be empty or populated)
    assert "qualifiers" in term1_data
    assert "qualifiers" in term2_data


def test_get_subject_by_project_iri_success(physical_resources, test_subject, test_project_id,
                                              standard_hpo_terms, query_athena, create_evidence_helper):
    """
    Test 2: Get Subject by project_subject_iri (Happy Path)

    Verifies that a subject can be retrieved using its project-scoped IRI,
    which requires DynamoDB lookup to resolve to internal subject_id.
    """
    subject_uuid, project_subject_iri = test_subject

    # Setup: Create evidence
    term_iri = standard_hpo_terms["seizure"]
    evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)

    # Action: Get subject by project_subject_iri
    result = get_subject(project_subject_iri=project_subject_iri, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}: {result.get('body', '')}"

    body = json.loads(result["body"])
    assert body["subject_id"] == subject_uuid
    assert body["subject_iri"] == f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_uuid}"
    assert body["project_subject_iri"] == project_subject_iri, "Should echo back project_subject_iri"
    assert "terms" in body
    assert len(body["terms"]) > 0


def test_get_subject_no_terms(physical_resources, test_subject):
    """
    Test 3: Get Subject with No Terms (Empty Subject)

    Verifies behavior when subject exists in DynamoDB but has no evidence/terms.
    According to get_subject.py:77-82, this returns 200 with empty terms array.
    """
    subject_uuid, project_subject_iri = test_subject

    # Action: Get subject immediately after creation (no evidence created)
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    # Assertions: Based on code, should return 404 when queried by subject_id
    # because query_subject_by_id returns None when no terms found
    assert result["statusCode"] == 404, \
        f"Expected 404 for subject with no terms, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "message" in body
    assert "not found" in body["message"].lower()

    # BUT: When queried by project_subject_iri, should return 200 with empty terms
    result2 = get_subject(project_subject_iri=project_subject_iri, physical_resources=physical_resources)

    assert result2["statusCode"] == 200, \
        f"Expected 200 for project_subject_iri query, got {result2['statusCode']}"

    body2 = json.loads(result2["body"])
    assert body2["subject_id"] == subject_uuid
    assert body2["terms"] == [], "Should return empty terms array when no evidence"


def test_get_subject_by_id_not_found(physical_resources):
    """
    Test 4: Get Subject - Subject Does Not Exist (by subject_id)

    Verifies that querying a non-existent subject_id returns 404.
    """
    # Generate random UUID that doesn't exist
    nonexistent_id = str(uuid.uuid4())

    # Action
    result = get_subject(subject_id=nonexistent_id, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 404, f"Expected 404, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "message" in body
    assert "not found" in body["message"].lower()


def test_get_subject_by_project_iri_not_found(physical_resources, test_project_id):
    """
    Test 5: Get Subject - Subject Does Not Exist (by project_subject_iri)

    Verifies that querying with a non-existent project_subject_iri returns 404.
    """
    # Create project_subject_iri for non-existent subject
    nonexistent_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}/nonexistent-patient"

    # Action
    result = get_subject(project_subject_iri=nonexistent_iri, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 404, f"Expected 404, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "message" in body
    assert "not found" in body["message"].lower()


def test_get_subject_missing_both_parameters(physical_resources):
    """
    Test 6: Missing Both Parameters

    Verifies that calling without subject_id or project_subject_iri returns 400.
    """
    # Action: Empty payload
    result = get_subject(physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "message" in body
    assert "must provide either" in body["message"].lower()


def test_get_subject_both_parameters_provided(physical_resources, test_subject):
    """
    Test 7: Both Parameters Provided (Ambiguous)

    Verifies behavior when both subject_id and project_subject_iri are provided.
    According to get_subject.py:20-23, project_subject_iri takes precedence.
    """
    subject_uuid, project_subject_iri = test_subject

    # Action: Provide both parameters
    result = get_subject(
        subject_id=subject_uuid,
        project_subject_iri=project_subject_iri,
        physical_resources=physical_resources
    )

    # Assertions: project_subject_iri takes precedence (checked first)
    # Subject has no terms, so by project_subject_iri returns 200 with empty terms
    assert result["statusCode"] == 200, \
        f"Expected 200 (project_subject_iri precedence), got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "project_subject_iri" in body, \
        "Should include project_subject_iri in response (proves it was used)"


def test_get_subject_with_qualified_terms(physical_resources, test_subject, test_project_id,
                                           standard_hpo_terms, create_evidence_helper,
                                           query_athena):
    """
    Test 8: Get Subject with Qualified Terms

    Verifies that qualified and unqualified annotations for the same term
    appear as separate entries with distinct termlink_ids.
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Setup: Create evidence with and without qualifiers
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=["negated"]
    )

    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri
        # No qualifiers
    )

    # Action: Get subject
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 200

    body = json.loads(result["body"])
    terms = body["terms"]

    # Find all entries for this term_iri
    matching_terms = [t for t in terms if t["term_iri"] == term_iri]

    assert len(matching_terms) == 2, \
        f"Expected 2 entries for {term_iri} (qualified and unqualified), got {len(matching_terms)}"

    # Verify they have different termlink_ids
    termlink_ids = [t["termlink_id"] for t in matching_terms]
    assert len(set(termlink_ids)) == 2, "Qualified and unqualified should have different termlink_ids"

    # Verify each has evidence_count = 1
    for term in matching_terms:
        assert term["evidence_count"] == 1


def test_get_subject_evidence_count_aggregation(physical_resources, test_subject, test_project_id,
                                                 standard_hpo_terms, create_evidence_helper,
                                                 query_athena):
    """
    Test 9: Get Subject with Multiple Evidence per Term Link

    Verifies that evidence_count correctly aggregates multiple evidence records
    for the same term link.
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Setup: Create 5 evidence records for same term (no qualifiers)
    evidence_ids = []
    for _ in range(5):
        evidence = create_evidence_helper(subject_id=subject_uuid, term_iri=term_iri)
        evidence_ids.append(evidence["evidence_id"])

    # Action: Get subject
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 200

    body = json.loads(result["body"])
    terms = body["terms"]

    assert len(terms) == 1, "Should have single term entry"
    assert terms[0]["term_iri"] == term_iri
    assert terms[0]["evidence_count"] == 5, \
        f"Expected evidence_count=5, got {terms[0]['evidence_count']}"


def test_get_subject_invalid_project_iri_format(physical_resources):
    """
    Test 11: Invalid project_subject_iri Format

    Verifies that malformed project_subject_iri returns 404.
    """
    # Action: Invalid IRI format
    result = get_subject(
        project_subject_iri="not-a-valid-iri-format",
        physical_resources=physical_resources
    )

    # Assertions
    assert result["statusCode"] == 404, \
        f"Expected 404 for invalid IRI, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "message" in body


def test_get_subject_incomplete_project_iri(physical_resources, test_project_id):
    """
    Test 12: project_subject_iri with Missing Segments

    Verifies that incomplete IRI (missing project_subject_id) returns 404.
    """
    # Action: IRI missing project_subject_id segment
    incomplete_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}"

    result = get_subject(
        project_subject_iri=incomplete_iri,
        physical_resources=physical_resources
    )

    # Assertions
    assert result["statusCode"] == 404, \
        f"Expected 404 for incomplete IRI, got {result['statusCode']}"


def test_get_subject_termlink_id_consistency(physical_resources, test_subject, test_project_id,
                                              standard_hpo_terms, create_evidence_helper,
                                              query_athena):
    """
    Test 19: Verify termlink_id Consistency

    Verifies that termlink_id returned by GetSubject matches the termlink_id
    in the evidence table (consistent hash computation).
    """
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]
    qualifiers = ["negated", "family"]

    # Setup: Create evidence with specific qualifiers
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=qualifiers
    )

    # Get termlink_id from evidence table
    evidence_results = query_athena(f"""
        SELECT termlink_id FROM phebee.evidence
        WHERE evidence_id = '{evidence["evidence_id"]}'
    """)
    expected_termlink_id = evidence_results[0]["termlink_id"]

    # Action: Get subject
    result = get_subject(subject_id=subject_uuid, physical_resources=physical_resources)

    # Assertions
    assert result["statusCode"] == 200

    body = json.loads(result["body"])
    terms = body["terms"]

    assert len(terms) == 1
    assert terms[0]["termlink_id"] == expected_termlink_id, \
        "termlink_id from GetSubject should match evidence table"
