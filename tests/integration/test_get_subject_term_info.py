"""
Integration tests for get_subject_term_info Lambda function.

This Lambda retrieves detailed information about a specific term link for a subject,
including the term IRI, qualifiers, and evidence count from the Iceberg analytical tables.
"""

import json
import uuid
import boto3
import pytest
from concurrent.futures import ThreadPoolExecutor


@pytest.fixture
def invoke_get_subject_term_info(physical_resources):
    """Fixture to invoke the get_subject_term_info Lambda function."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["GetSubjectTermInfoFunction"]

    def _invoke(subject_id, term_iri, qualifiers=None):
        payload = {
            "subject_id": subject_id,
            "term_iri": term_iri
        }
        if qualifiers is not None:
            payload["qualifiers"] = qualifiers

        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )
        return json.loads(response["Payload"].read())

    return _invoke


def test_get_subject_term_info_no_qualifiers_success(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test retrieving term info for a term with no qualifiers (happy path)."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create 3 evidence records with same term and no qualifiers
    evidence_ids = []
    for _ in range(3):
        evidence = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )
        evidence_ids.append(evidence["evidence_id"])

    # Get subject term info
    result = invoke_get_subject_term_info(subject_uuid, term_iri)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert body["term_iri"] == term_iri
    assert body["qualifiers"] == []
    assert body["evidence_count"] == 3
    assert "term_links" in body
    assert len(body["term_links"]) == 1
    assert body["term_links"][0]["evidence_count"] == 3


def test_get_subject_term_info_with_qualifiers_success(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test retrieving term info for a term with qualifiers."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create qualifier dictionary
    qualifiers_dict = {
        "http://purl.obolibrary.org/obo/HP_0012823": True,  # Clinical modifier - Severe
        "http://purl.obolibrary.org/obo/HP_0003577": True   # Temporal pattern - Congenital onset
    }

    # Create 2 evidence records with qualifiers
    evidence_ids = []
    for _ in range(2):
        evidence = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri,
            qualifiers=qualifiers_dict
        )
        evidence_ids.append(evidence["evidence_id"])

    # Get subject term info (Lambda expects qualifier IRIs as a list)
    result = invoke_get_subject_term_info(subject_uuid, term_iri, qualifiers=list(qualifiers_dict.keys()))

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert body["term_iri"] == term_iri
    # Qualifiers returned as full IRIs
    assert "http://purl.obolibrary.org/obo/HP_0012823" in body["qualifiers"]
    assert "http://purl.obolibrary.org/obo/HP_0003577" in body["qualifiers"]
    assert body["evidence_count"] == 2
    assert len(body["qualifiers"]) == 2


def test_get_subject_term_info_term_not_found(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test querying for a term that does not exist for the subject."""
    subject_uuid, _ = test_subject
    term_iri_with_evidence = standard_hpo_terms["neoplasm"]
    term_iri_without_evidence = standard_hpo_terms["seizure"]

    # Create evidence with one term
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri_with_evidence
    )

    # Query for a different term that does NOT have evidence
    result = invoke_get_subject_term_info(subject_uuid, term_iri_without_evidence)

    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "Term not found for subject" in body["message"]


def test_get_subject_term_info_subject_not_found(
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test querying for a non-existent subject."""
    nonexistent_subject = str(uuid.uuid4())
    term_iri = standard_hpo_terms["seizure"]

    result = invoke_get_subject_term_info(nonexistent_subject, term_iri)

    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "Term not found for subject" in body["message"]


def test_get_subject_term_info_missing_subject_id(invoke_get_subject_term_info, standard_hpo_terms):
    """Test that missing subject_id returns 400."""
    term_iri = standard_hpo_terms["seizure"]

    # Invoke with None subject_id
    result = invoke_get_subject_term_info(None, term_iri)

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "Missing required field: subject_id" in body["message"]


def test_get_subject_term_info_missing_term_iri(test_subject, invoke_get_subject_term_info):
    """Test that missing term_iri returns 400."""
    subject_uuid, _ = test_subject

    # Invoke with None term_iri
    result = invoke_get_subject_term_info(subject_uuid, None)

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "Missing required field: term_iri" in body["message"]


def test_get_subject_term_info_qualifiers_mismatch(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test that querying with different qualifiers returns 404 (different termlinks)."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with specific qualifiers
    qualifiers_dict = {
        "http://purl.obolibrary.org/obo/HP_0012823": True  # Severe
    }
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=qualifiers_dict
    )

    # Query with DIFFERENT qualifiers (creates a different termlink_id)
    different_qualifiers = ["http://purl.obolibrary.org/obo/HP_0003577"]  # Congenital onset
    result = invoke_get_subject_term_info(subject_uuid, term_iri, qualifiers=different_qualifiers)

    # Should return 404 because different qualifiers = different termlink
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "Term not found for subject" in body["message"]


def test_get_subject_term_info_empty_qualifiers(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test that empty qualifiers array is equivalent to no qualifiers."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence without qualifiers
    evidence = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri
    )

    # Query with explicit empty qualifiers array
    result = invoke_get_subject_term_info(subject_uuid, term_iri, qualifiers=[])

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["qualifiers"] == []
    assert body["evidence_count"] == 1


def test_get_subject_term_info_multiple_termlinks_same_term(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test that multiple term links for same term are correctly differentiated by qualifiers."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create evidence with no qualifiers (termlink A)
    evidence1 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri
    )

    # Create evidence with qualifiers (termlink B - different termlink_id)
    qualifiers_dict = {
        "http://purl.obolibrary.org/obo/HP_0012823": True  # Severe
    }
    evidence2 = create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=term_iri,
        qualifiers=qualifiers_dict
    )

    # Query for unqualified term link
    result1 = invoke_get_subject_term_info(subject_uuid, term_iri, qualifiers=[])
    assert result1["statusCode"] == 200
    body1 = json.loads(result1["body"])
    assert body1["qualifiers"] == []
    assert body1["evidence_count"] == 1  # Only unqualified evidence
    termlink_id_1 = body1["term_links"][0]["termlink_iri"]

    # Query for qualified term link
    result2 = invoke_get_subject_term_info(subject_uuid, term_iri, qualifiers=list(qualifiers_dict.keys()))
    assert result2["statusCode"] == 200
    body2 = json.loads(result2["body"])
    assert "http://purl.obolibrary.org/obo/HP_0012823" in body2["qualifiers"]
    assert body2["evidence_count"] == 1  # Only qualified evidence
    termlink_id_2 = body2["term_links"][0]["termlink_iri"]

    # Verify different termlink IDs
    assert termlink_id_1 != termlink_id_2


def test_get_subject_term_info_evidence_count_accuracy(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test that evidence count is accurate when multiple evidence records exist."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Create 7 evidence records
    evidence_ids = []
    for _ in range(7):
        evidence = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )
        evidence_ids.append(evidence["evidence_id"])

    # Get term info
    result = invoke_get_subject_term_info(subject_uuid, term_iri)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["evidence_count"] == 7


def test_get_subject_term_info_after_evidence_deletion(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    physical_resources,
    standard_hpo_terms
):
    """Test that term info updates correctly after evidence deletion."""
    subject_uuid, _ = test_subject
    term_iri = standard_hpo_terms["seizure"]
    lambda_client = boto3.client("lambda")
    remove_evidence_function = physical_resources["RemoveEvidenceFunction"]

    # Create 5 evidence records
    evidence_ids = []
    for _ in range(5):
        evidence = create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )
        evidence_ids.append(evidence["evidence_id"])

    # Verify initial count
    result = invoke_get_subject_term_info(subject_uuid, term_iri)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["evidence_count"] == 5

    # Delete 3 evidence records
    for evidence_id in evidence_ids[:3]:
        lambda_client.invoke(
            FunctionName=remove_evidence_function,
            Payload=json.dumps({"evidence_id": evidence_id}).encode("utf-8")
        )

    # Verify decremented count
    result = invoke_get_subject_term_info(subject_uuid, term_iri)
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["evidence_count"] == 2

    # Delete remaining 2 evidence records
    for evidence_id in evidence_ids[3:]:
        lambda_client.invoke(
            FunctionName=remove_evidence_function,
            Payload=json.dumps({"evidence_id": evidence_id}).encode("utf-8")
        )

    # Verify term link removed when evidence_count = 0
    result = invoke_get_subject_term_info(subject_uuid, term_iri)
    assert result["statusCode"] == 404


def test_get_subject_term_info_invalid_term_iri(
    test_subject,
    invoke_get_subject_term_info
):
    """Test that invalid term IRI format returns 404."""
    subject_uuid, _ = test_subject

    result = invoke_get_subject_term_info(subject_uuid, "not-a-valid-iri")

    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "Term not found for subject" in body["message"]


def test_get_subject_term_info_concurrent(
    test_subject,
    create_evidence_helper,
    invoke_get_subject_term_info,
    standard_hpo_terms
):
    """Test concurrent queries for multiple terms on the same subject."""
    subject_uuid, _ = test_subject

    # Create evidence for 5 different terms
    terms = [
        standard_hpo_terms["seizure"],
        standard_hpo_terms["neoplasm"],
        standard_hpo_terms["abnormality_of_head"],
        standard_hpo_terms["abnormal_heart_morphology"],
        standard_hpo_terms["disturbance_in_speech"]
    ]

    for term_iri in terms:
        create_evidence_helper(
            subject_id=subject_uuid,
            term_iri=term_iri
        )

    # Query all 5 terms concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(invoke_get_subject_term_info, subject_uuid, term_iri)
            for term_iri in terms
        ]
        results = [future.result() for future in futures]

    # Verify all queries succeeded
    assert len(results) == 5
    for i, result in enumerate(results):
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["term_iri"] == terms[i]
        assert body["evidence_count"] == 1
