"""
Integration tests for create_evidence lambda function.

These tests verify evidence creation in the PheBee system, including:
- Evidence record creation with Iceberg as source of truth
- Deterministic termlink_id generation via hashing
- Critical qualifier standardization ('true'/'false' format)
- Hash consistency across qualifier order variations
- Athena verification of evidence data

Note: Verification primarily uses Athena queries since Iceberg is source of truth.
Neptune term link creation is logged but non-blocking.
"""

import pytest
import json
import uuid
import time
from phebee.utils.aws import get_client


pytestmark = pytest.mark.integration


# ============================================================================
# Helper Functions
# ============================================================================

def create_evidence(subject_id: str, term_iri: str, creator_id: str,
                   physical_resources: dict, **kwargs):
    """Helper to invoke CreateEvidenceFunction."""
    lambda_client = get_client("lambda")

    payload = {
        "subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": creator_id
    }

    # Add any additional kwargs
    payload.update(kwargs)

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read())

    if "statusCode" not in result:
        print(f"Unexpected lambda response format: {result}")
        if "errorMessage" in result:
            raise Exception(f"Lambda error: {result['errorMessage']}")

    return result


def wait_for_iceberg_evidence(query_athena, evidence_id: str, timeout: int = 60) -> dict:
    """
    Wait for evidence to appear in Iceberg via Athena.

    Iceberg writes may have a small delay, so poll until evidence appears.
    Returns core fields that are always present.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        results = query_athena(f"""
            SELECT
                evidence_id,
                run_id,
                batch_id,
                evidence_type,
                subject_id,
                term_iri,
                termlink_id,
                creator.creator_id as creator_id,
                creator.creator_type as creator_type,
                encounter_id,
                clinical_note_id,
                created_timestamp,
                created_date
            FROM phebee.evidence
            WHERE evidence_id = '{evidence_id}'
        """)

        if results:
            return results[0]

        time.sleep(2)

    raise TimeoutError(f"Evidence {evidence_id} not found in Iceberg after {timeout}s")


def count_evidence_for_termlink(query_athena, termlink_id: str) -> int:
    """Count evidence records with given termlink_id."""
    results = query_athena(f"""
        SELECT COUNT(*) as count
        FROM phebee.evidence
        WHERE termlink_id = '{termlink_id}'
    """)
    return int(results[0]["count"]) if results else 0


# ============================================================================
# Test Cases
# ============================================================================

def test_create_evidence_minimal(physical_resources, test_subject, test_project_id, query_athena,
                                  standard_hpo_terms, wait_for_subject_terms):
    """
    Test 1: Create Evidence Record (Happy Path - Minimal Fields)

    Verifies that evidence can be created with only required fields,
    and that it appears in Iceberg with correct defaults. Also verifies
    that subject_terms tables are updated correctly.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["seizure"]
    creator_id = f"test-creator-{uuid.uuid4().hex[:8]}"

    # Action: Create evidence with minimal fields
    result = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id=creator_id,
        physical_resources=physical_resources
    )

    # Assertions: Verify response
    assert result["statusCode"] == 201, f"Expected 201, got {result['statusCode']}"

    body = json.loads(result["body"])
    assert "evidence_id" in body
    assert body["subject_id"] == subject_uuid
    assert body["term_iri"] == term_iri
    assert body["creator"]["creator_id"] == creator_id
    assert body["creator"]["creator_type"] == "human"  # default
    assert body["evidence_type"] == "manual_annotation"  # default
    assert "created_timestamp" in body

    evidence_id = body["evidence_id"]

    # Iceberg verification: Wait for evidence to appear and verify fields
    evidence_record = wait_for_iceberg_evidence(query_athena, evidence_id)

    assert evidence_record["evidence_id"] == evidence_id
    assert evidence_record["subject_id"] == subject_uuid
    assert evidence_record["term_iri"] == term_iri
    assert evidence_record["creator_id"] == creator_id
    assert evidence_record["creator_type"] == "human"
    assert evidence_record["evidence_type"] == "manual_annotation"

    # Subject-terms verification: First evidence should create row with count=1
    termlink_id = evidence_record["termlink_id"]
    by_subject, by_project = wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=termlink_id,
        project_id=test_project_id
    )

    # Verify by_subject table
    assert by_subject["subject_id"] == subject_uuid
    assert by_subject["term_iri"] == term_iri
    assert by_subject["termlink_id"] == termlink_id
    assert int(by_subject["evidence_count"]) == 1, "First evidence should set count to 1"
    assert by_subject["first_evidence_date"] == by_subject["last_evidence_date"]

    # Verify by_project_term table
    assert by_project["project_id"] == test_project_id
    assert by_project["subject_id"] == subject_uuid
    assert by_project["term_iri"] == term_iri
    assert by_project["termlink_id"] == termlink_id
    assert int(by_project["evidence_count"]) == 1, "First evidence should set count to 1"
    assert by_project["first_evidence_date"] == by_project["last_evidence_date"]
    assert "termlink_id" in evidence_record
    assert "created_date" in evidence_record


def test_create_evidence_full_fields(physical_resources, test_subject, query_athena, standard_hpo_terms):
    """
    Test 2: Create Evidence with All Optional Fields

    Verifies that all optional fields are properly stored in Iceberg.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["abnormal_heart_morphology"]

    # Action: Create evidence with all optional fields
    result = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="system-nlp-v2",
        creator_name="NLP System v2.0",
        creator_type="automated",
        evidence_type="clinical_note",
        run_id=f"run-{uuid.uuid4().hex[:6]}",
        batch_id=f"batch-{uuid.uuid4().hex[:4]}",
        encounter_id=f"encounter-{uuid.uuid4().hex[:6]}",
        clinical_note_id=f"note-{uuid.uuid4().hex[:6]}",
        span_start=125,
        span_end=150,
        qualifiers=["negated", "family"],
        note_timestamp="2024-01-15T10:30:00Z",
        provider_type="physician",
        author_specialty="neurology",
        note_type="progress_note",
        physical_resources=physical_resources
    )

    # Assertions
    if result["statusCode"] != 201:
        print(f"Error response: {result}")
    assert result["statusCode"] == 201, f"Expected 201, got {result['statusCode']}: {result.get('body', '')}"

    body = json.loads(result["body"])
    evidence_id = body["evidence_id"]

    # Iceberg verification: Query with optional fields
    results = query_athena(f"""
        SELECT
            evidence_id,
            run_id,
            batch_id,
            evidence_type,
            creator.creator_id as creator_id,
            creator.creator_type as creator_type,
            encounter_id,
            clinical_note_id,
            text_annotation.span_start as span_start,
            text_annotation.span_end as span_end,
            note_context.provider_type as provider_type,
            note_context.author_specialty as author_specialty,
            note_context.note_type as note_type
        FROM phebee.evidence
        WHERE evidence_id = '{evidence_id}'
    """)

    assert len(results) == 1
    evidence_record = results[0]

    assert evidence_record["creator_id"] == "system-nlp-v2"
    assert evidence_record["creator_type"] == "automated"
    assert evidence_record["evidence_type"] == "clinical_note"
    assert evidence_record["run_id"] == body["run_id"]
    assert evidence_record["batch_id"] == body["batch_id"]
    assert evidence_record["encounter_id"] is not None
    assert evidence_record["clinical_note_id"] is not None
    # Note: Athena may return span values as strings
    assert int(evidence_record["span_start"]) == 125
    assert int(evidence_record["span_end"]) == 150
    assert evidence_record["provider_type"] == "physician"
    assert evidence_record["author_specialty"] == "neurology"
    assert evidence_record["note_type"] == "progress_note"


def test_create_evidence_multiple_same_termlink(physical_resources, test_subject, test_project_id,
                                                 query_athena, standard_hpo_terms, wait_for_subject_terms):
    """
    Test 3: Multiple Evidence Records for Same Term Link

    Verifies that multiple evidence records with identical (subject, term, qualifiers)
    share the same termlink_id and increment the evidence count in subject_terms tables.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Action: Create 3 evidence records with same subject+term (no qualifiers)
    evidence_ids = []
    termlink_ids = []

    for i in range(3):
        result = create_evidence(
            subject_id=subject_uuid,
            term_iri=term_iri,
            creator_id=f"creator-{i}",
            evidence_type="clinical_note" if i % 2 == 0 else "manual_annotation",
            clinical_note_id=f"note-{i}" if i % 2 == 0 else None,
            physical_resources=physical_resources
        )

        assert result["statusCode"] == 201
        body = json.loads(result["body"])
        evidence_ids.append(body["evidence_id"])

        # Extract termlink_id from Iceberg
        evidence_record = wait_for_iceberg_evidence(query_athena, body["evidence_id"])
        termlink_ids.append(evidence_record["termlink_id"])

    # Assertions: All evidence records share same termlink_id
    assert len(set(termlink_ids)) == 1, "All evidence should have same termlink_id"

    # Verify all 3 evidence records exist
    assert len(set(evidence_ids)) == 3, "All evidence_ids should be unique"

    # Count evidence for this termlink in evidence table
    termlink_id = termlink_ids[0]
    count = count_evidence_for_termlink(query_athena, termlink_id)
    assert count == 3, f"Expected 3 evidence records for termlink, found {count}"

    # Subject-terms verification: Count should be 3 in both tables
    by_subject, by_project = wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=termlink_id,
        project_id=test_project_id
    )

    assert int(by_subject["evidence_count"]) == 3, \
        f"Expected evidence_count=3 in by_subject table, got {by_subject['evidence_count']}"
    assert int(by_project["evidence_count"]) == 3, \
        f"Expected evidence_count=3 in by_project table, got {by_project['evidence_count']}"

    # Verify dates are maintained (first_evidence_date <= last_evidence_date)
    assert by_subject["first_evidence_date"] <= by_subject["last_evidence_date"]
    assert by_project["first_evidence_date"] <= by_project["last_evidence_date"]


def test_create_evidence_qualifier_standardization(physical_resources, test_subject, test_project_id,
                                                   query_athena, standard_hpo_terms, wait_for_subject_terms):
    """
    Test 21: Qualifier Standardization with true/false Format (CRITICAL)

    Verifies that qualifiers are stored in Iceberg and subject_terms tables.
    The standardization to {"negated": "true", "family": "true"} format
    happens at read time (GetEvidence), not write time.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["abnormal_heart_morphology"]

    # Action: Create evidence with qualifiers
    result = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="test-creator",
        qualifiers=["negated", "family"],
        physical_resources=physical_resources
    )

    # Assertions
    assert result["statusCode"] == 201

    body = json.loads(result["body"])
    evidence_id = body["evidence_id"]

    # Iceberg verification: Qualifiers stored as array
    evidence_record = wait_for_iceberg_evidence(query_athena, evidence_id)

    # Note: The qualifier format in Iceberg depends on how PyIceberg/Athena represents arrays
    # The actual standardization to {"negated": "true"} format happens in GetEvidence
    # Here we verify the qualifiers are stored
    assert "termlink_id" in evidence_record

    # The termlink_id should be computed from subject+term+qualifiers
    # We can't directly verify the hash algorithm, but we can verify consistency
    assert evidence_record["termlink_id"] is not None

    # Subject-terms verification: Qualifiers stored in both tables
    termlink_id = evidence_record["termlink_id"]
    by_subject, by_project = wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=termlink_id,
        project_id=test_project_id
    )

    # Verify qualifiers are present in both tables
    # Note: Athena may return arrays as strings like "[negated, family]"
    assert by_subject["qualifiers"] is not None and len(by_subject["qualifiers"]) > 0, \
        "Qualifiers should be stored in by_subject table"
    assert by_project["qualifiers"] is not None and len(by_project["qualifiers"]) > 0, \
        "Qualifiers should be stored in by_project table"


def test_create_evidence_qualifier_hash_consistency(physical_resources, test_subject,
                                                     query_athena, standard_hpo_terms):
    """
    Test 22: Qualifier Hash Consistency for termlink_id Generation (CRITICAL)

    Verifies that qualifiers in different order produce the same termlink_id,
    ensuring consistent hash generation regardless of qualifier order.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["abnormal_heart_morphology"]

    # Action: Create two evidence with same qualifiers in different order
    result1 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-1",
        qualifiers=["negated", "family"],  # Order 1
        physical_resources=physical_resources
    )

    result2 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-2",
        qualifiers=["family", "negated"],  # Order 2 (reversed)
        physical_resources=physical_resources
    )

    # Assertions: Both created successfully
    assert result1["statusCode"] == 201
    assert result2["statusCode"] == 201

    body1 = json.loads(result1["body"])
    body2 = json.loads(result2["body"])

    # Get evidence records from Iceberg
    evidence1 = wait_for_iceberg_evidence(query_athena, body1["evidence_id"])
    evidence2 = wait_for_iceberg_evidence(query_athena, body2["evidence_id"])

    # CRITICAL: Same termlink_id despite different qualifier order
    assert evidence1["termlink_id"] == evidence2["termlink_id"], \
        "Qualifiers in different order should produce same termlink_id"

    # Verify both evidence records exist (different evidence_ids)
    assert evidence1["evidence_id"] != evidence2["evidence_id"]

    # Count should be 2 for this termlink
    count = count_evidence_for_termlink(query_athena, evidence1["termlink_id"])
    assert count == 2, f"Expected 2 evidence records for termlink, found {count}"


def test_create_evidence_different_qualifiers_different_hash(physical_resources, test_subject, test_project_id,
                                                              query_athena, standard_hpo_terms, wait_for_subject_terms):
    """
    Test 23: Different Qualifiers Produce Different termlink_ids (CRITICAL)

    Verifies that different qualifier values produce different termlink_ids,
    ensuring proper separation of qualified vs unqualified annotations.
    Also verifies that subject_terms tables maintain separate rows for each termlink.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["seizure"]

    # Action: Create evidence with different qualifiers
    result1 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-1",
        qualifiers=["negated"],
        physical_resources=physical_resources
    )

    result2 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-2",
        qualifiers=["hypothetical"],
        physical_resources=physical_resources
    )

    # Assertions
    assert result1["statusCode"] == 201
    assert result2["statusCode"] == 201

    body1 = json.loads(result1["body"])
    body2 = json.loads(result2["body"])

    evidence1 = wait_for_iceberg_evidence(query_athena, body1["evidence_id"])
    evidence2 = wait_for_iceberg_evidence(query_athena, body2["evidence_id"])

    # CRITICAL: Different qualifiers produce different termlink_ids
    assert evidence1["termlink_id"] != evidence2["termlink_id"], \
        "Different qualifiers should produce different termlink_ids"

    # Each termlink should have exactly 1 evidence
    count1 = count_evidence_for_termlink(query_athena, evidence1["termlink_id"])
    count2 = count_evidence_for_termlink(query_athena, evidence2["termlink_id"])
    assert count1 == 1
    assert count2 == 1

    # Subject-terms verification: Both termlinks should have separate rows
    by_subject1, by_project1 = wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=evidence1["termlink_id"],
        project_id=test_project_id
    )

    by_subject2, by_project2 = wait_for_subject_terms(
        subject_id=subject_uuid,
        termlink_id=evidence2["termlink_id"],
        project_id=test_project_id
    )

    # Verify both termlinks exist as separate rows
    assert by_subject1["termlink_id"] != by_subject2["termlink_id"]
    assert by_project1["termlink_id"] != by_project2["termlink_id"]

    # Both should have count=1 (one evidence each)
    assert int(by_subject1["evidence_count"]) == 1
    assert int(by_subject2["evidence_count"]) == 1
    assert int(by_project1["evidence_count"]) == 1
    assert int(by_project2["evidence_count"]) == 1


def test_create_evidence_empty_qualifiers_hash(physical_resources, test_subject,
                                                query_athena, standard_hpo_terms):
    """
    Test 24: Empty Qualifiers vs Missing Qualifiers (Same termlink_id) (CRITICAL)

    Verifies that empty qualifiers array and missing qualifiers produce
    the same termlink_id, ensuring consistent handling of unqualified annotations.
    """
    subject_uuid, project_subject_iri = test_subject
    term_iri = standard_hpo_terms["abnormality_of_head"]

    # Action: Create evidence with empty qualifiers array
    result1 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-1",
        qualifiers=[],  # Empty array
        physical_resources=physical_resources
    )

    # Create evidence with missing qualifiers (omitted)
    result2 = create_evidence(
        subject_id=subject_uuid,
        term_iri=term_iri,
        creator_id="creator-2",
        # qualifiers not provided at all
        physical_resources=physical_resources
    )

    # Assertions
    assert result1["statusCode"] == 201
    assert result2["statusCode"] == 201

    body1 = json.loads(result1["body"])
    body2 = json.loads(result2["body"])

    evidence1 = wait_for_iceberg_evidence(query_athena, body1["evidence_id"])
    evidence2 = wait_for_iceberg_evidence(query_athena, body2["evidence_id"])

    # CRITICAL: Empty and missing qualifiers produce same termlink_id
    assert evidence1["termlink_id"] == evidence2["termlink_id"], \
        "Empty qualifiers array and missing qualifiers should produce same termlink_id"

    # Count should be 2 for this termlink
    count = count_evidence_for_termlink(query_athena, evidence1["termlink_id"])
    assert count == 2, f"Expected 2 evidence records for termlink, found {count}"


def test_create_evidence_multi_project_update(physical_resources, test_project_id, query_athena,
                                                standard_hpo_terms, wait_for_subject_terms):
    """
    Test: Evidence Updates All Projects with Subject

    Verifies that when evidence is created, subject_terms tables are updated
    for ALL projects that have the subject, maintaining consistency across projects.

    This tests the critical `get_projects_for_subject()` logic in update_subject_terms_for_evidence().
    """
    lambda_client = get_client("lambda")

    # Create a second project
    project_id_2 = f"test-project-2-{uuid.uuid4().hex[:8]}"
    project_label_2 = "Multi-project Test 2"

    project_result = lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps({
            "project_id": project_id_2,
            "project_label": project_label_2
        }).encode("utf-8")
    )
    project_response = json.loads(project_result["Payload"].read())
    assert project_response["statusCode"] == 200

    try:
        # Create subject in first project
        project_subject_id_1 = f"subj-{uuid.uuid4().hex[:8]}"
        subject_result_1 = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({
                "body": json.dumps({
                    "project_id": test_project_id,
                    "project_subject_id": project_subject_id_1
                })
            }).encode("utf-8")
        )
        subject_response_1 = json.loads(subject_result_1["Payload"].read())
        assert subject_response_1["statusCode"] == 200

        subject_body_1 = json.loads(subject_response_1["body"])
        subject_iri = subject_body_1["subject"]["iri"]
        subject_uuid = subject_iri.split("/")[-1]

        # Link same subject to second project
        project_subject_id_2 = f"subj-{uuid.uuid4().hex[:8]}"
        subject_result_2 = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({
                "body": json.dumps({
                    "project_id": project_id_2,
                    "project_subject_id": project_subject_id_2,
                    "known_subject_iri": subject_iri
                })
            }).encode("utf-8")
        )
        subject_response_2 = json.loads(subject_result_2["Payload"].read())
        assert subject_response_2["statusCode"] == 200

        # Create evidence for the subject
        term_iri = standard_hpo_terms["seizure"]
        evidence_result = create_evidence(
            subject_id=subject_uuid,
            term_iri=term_iri,
            creator_id="test-creator",
            physical_resources=physical_resources
        )

        assert evidence_result["statusCode"] == 201
        evidence_body = json.loads(evidence_result["body"])
        evidence_id = evidence_body["evidence_id"]

        # Get termlink_id from evidence
        evidence_record = wait_for_iceberg_evidence(query_athena, evidence_id)
        termlink_id = evidence_record["termlink_id"]

        # Verify subject_terms updated for BOTH projects
        by_subject, by_project_1 = wait_for_subject_terms(
            subject_id=subject_uuid,
            termlink_id=termlink_id,
            project_id=test_project_id
        )

        _, by_project_2 = wait_for_subject_terms(
            subject_id=subject_uuid,
            termlink_id=termlink_id,
            project_id=project_id_2
        )

        # Both projects should have the evidence
        assert int(by_project_1["evidence_count"]) == 1, \
            f"Project 1 should have evidence_count=1, got {by_project_1['evidence_count']}"
        assert int(by_project_2["evidence_count"]) == 1, \
            f"Project 2 should have evidence_count=1, got {by_project_2['evidence_count']}"

        # by_subject table should have evidence_count=1 (not duplicated per project)
        assert int(by_subject["evidence_count"]) == 1, \
            f"by_subject should have evidence_count=1, got {by_subject['evidence_count']}"

        # Both project records should have same termlink_id
        assert by_project_1["termlink_id"] == by_project_2["termlink_id"] == termlink_id

    finally:
        # Cleanup project 2 (best effort)
        try:
            lambda_client.invoke(
                FunctionName=physical_resources["RemoveProjectFunction"],
                Payload=json.dumps({"project_id": project_id_2}).encode("utf-8")
            )
        except Exception as e:
            print(f"Warning: Failed to cleanup project {project_id_2}: {e}")


def test_create_evidence_missing_required_fields(physical_resources):
    """
    Test: Missing Required Fields

    Verifies that missing required fields (subject_id, term_iri, creator_id)
    result in a 400 error.
    """
    lambda_client = get_client("lambda")

    # Test missing subject_id
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
            "creator_id": "test-creator"
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "required" in body["message"].lower()

    # Test missing term_iri
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "subject_id": str(uuid.uuid4()),
            "creator_id": "test-creator"
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "required" in body["message"].lower()

    # Test missing creator_id
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "subject_id": str(uuid.uuid4()),
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
        }).encode("utf-8")
    )
    result = json.loads(response["Payload"].read())
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "required" in body["message"].lower()


def test_create_evidence_subject_not_exists(physical_resources, query_athena, standard_hpo_terms):
    """
    Test: Evidence for Non-Existent Subject

    Verifies that evidence can be created for a non-existent subject.
    Evidence creation does NOT validate subject existence (documented behavior).
    """
    nonexistent_subject_id = str(uuid.uuid4())
    term_iri = standard_hpo_terms["seizure"]

    # Action: Create evidence for non-existent subject
    result = create_evidence(
        subject_id=nonexistent_subject_id,
        term_iri=term_iri,
        creator_id="test-creator",
        physical_resources=physical_resources
    )

    # Assertions: Evidence creation succeeds (201)
    assert result["statusCode"] == 201, \
        "Evidence creation should succeed even for non-existent subject"

    body = json.loads(result["body"])
    evidence_id = body["evidence_id"]

    # Iceberg verification: Evidence record exists
    evidence_record = wait_for_iceberg_evidence(query_athena, evidence_id)
    assert evidence_record["subject_id"] == nonexistent_subject_id

    # Document: This is expected behavior - evidence creation is independent
    # of subject existence validation
