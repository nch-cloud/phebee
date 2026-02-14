"""
Integration tests for get_subjects_pheno Lambda function.

Tests the GetSubjectsPheno API endpoint which queries subjects by project with
optional term filtering, returns as JSON or phenopackets, with compression.
"""

import pytest
import json
import boto3
import uuid
import time
import gzip
import base64
from typing import Dict, List


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def invoke_get_subjects_pheno(physical_resources):
    """Helper to invoke GetSubjectsPheno Lambda."""
    lambda_client = boto3.client("lambda")
    function_name = physical_resources["GetSubjectsPhenotypesFunction"]

    def _invoke(**params):
        """Invoke GetSubjectsPheno with given parameters."""
        payload = {"body": json.dumps(params)}
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())

        # Check if Lambda function error occurred
        if "FunctionError" in response:
            # Function error - result is the error details
            return {"statusCode": 500, "body": json.dumps(result)}

        # If response is compressed, decompress it
        if result.get("statusCode") == 200 and result.get("isBase64Encoded"):
            compressed_data = base64.b64decode(result["body"])
            decompressed = gzip.decompress(compressed_data).decode('utf-8')
            result["decompressed_body"] = json.loads(decompressed)

        return result

    return _invoke


@pytest.fixture
def create_subject_with_evidence(physical_resources, test_project_id, create_evidence_helper, wait_for_subject_terms, query_athena):
    """Helper to create a subject with evidence and wait for indexing."""
    lambda_client = boto3.client("lambda")

    def wait_for_iceberg_evidence(evidence_id: str, timeout: int = 60) -> dict:
        """Wait for evidence to appear in Iceberg via Athena."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            results = query_athena(f"""
                SELECT evidence_id, termlink_id
                FROM phebee.evidence
                WHERE evidence_id = '{evidence_id}'
            """)

            if results:
                return results[0]

            time.sleep(2)

        raise TimeoutError(f"Evidence {evidence_id} not found in Iceberg after {timeout}s")

    def _create(project_subject_id=None, term_iri="http://purl.obolibrary.org/obo/HP_0001249", qualifiers=None, evidence_count=1):
        """Create a subject with specified evidence.

        Args:
            project_subject_id: Subject ID (generated if not provided)
            term_iri: Phenotype term IRI
            qualifiers: List of qualifiers (e.g., ["negated"])
            evidence_count: Number of evidence records to create

        Returns:
            Dict with subject_uuid, project_subject_id, project_id, termlink_id
        """
        if project_subject_id is None:
            project_subject_id = f"test-subj-{uuid.uuid4().hex[:8]}"

        # Create subject
        create_payload = {
            "project_id": test_project_id,
            "project_subject_id": project_subject_id
        }

        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateSubjectFunction"],
            Payload=json.dumps({"body": json.dumps(create_payload)}).encode("utf-8")
        )

        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 200

        body = json.loads(result["body"])
        subject_iri = body["subject"]["iri"]
        subject_uuid = subject_iri.split("/")[-1]

        # Create evidence
        termlink_ids = []
        for _ in range(evidence_count):
            evidence = create_evidence_helper(
                subject_id=subject_uuid,
                term_iri=term_iri,
                qualifiers=qualifiers or []
            )
            # Wait for evidence to appear in Iceberg to get termlink_id
            evidence_record = wait_for_iceberg_evidence(evidence["evidence_id"])
            termlink_ids.append(evidence_record["termlink_id"])

        # Wait for subject_terms tables to update (using first termlink_id)
        wait_for_subject_terms(
            subject_id=subject_uuid,
            termlink_id=termlink_ids[0],
            project_id=test_project_id
        )

        return {
            "subject_uuid": subject_uuid,
            "project_subject_id": project_subject_id,
            "project_id": test_project_id,
            "termlink_id": termlink_ids[0]
        }

    return _create


# ============================================================================
# BASIC QUERY TESTS
# ============================================================================

def test_get_subjects_all_in_project(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 1: Query all subjects in a project without filters.

    Setup: Create 3 subjects with evidence
    Action: Query with only project_id
    Verify: All 3 subjects returned
    """
    # Create 3 subjects
    subj1 = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
    subj2 = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0002297")
    subj3 = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001250")

    # Query all subjects in project
    result = invoke_get_subjects_pheno(project_id=subj1["project_id"])

    # Verify
    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    assert "body" in body
    assert "pagination" in body
    assert body["n_subjects"] >= 3

    # Extract subject IDs
    subjects = body["body"]
    project_subject_ids = [s["project_subject_id"] for s in subjects]

    assert subj1["project_subject_id"] in project_subject_ids
    assert subj2["project_subject_id"] in project_subject_ids
    assert subj3["project_subject_id"] in project_subject_ids


def test_get_subjects_missing_project_id(invoke_get_subjects_pheno):
    """
    Test 14: Missing project_id parameter should return error.

    Action: Call without project_id
    Verify: Returns 500 with error about missing project_id
    """
    result = invoke_get_subjects_pheno()

    assert result["statusCode"] == 500
    assert "project_id" in result["body"].lower()


def test_get_subjects_filtered_by_term(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 2: Filter subjects by specific term_iri.

    Setup: Create subjects with different terms
    Action: Query with specific term_iri
    Verify: Only subjects with that term are returned
    """
    # Create subjects with different terms
    subj_match1 = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
    subj_match2 = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
    subj_no_match = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0002297")

    # Query for HP_0001249 only
    result = invoke_get_subjects_pheno(
        project_id=subj_match1["project_id"],
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        include_child_terms=False
    )

    # Verify
    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]
    project_subject_ids = [s["project_subject_id"] for s in subjects]

    assert subj_match1["project_subject_id"] in project_subject_ids
    assert subj_match2["project_subject_id"] in project_subject_ids
    assert subj_no_match["project_subject_id"] not in project_subject_ids


def test_get_subjects_exclude_child_terms(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 4: Verify include_child_terms=false returns exact term only.

    Setup: Create subject with specific term
    Action: Query with include_child_terms=false
    Verify: Only exact term matches returned
    """
    subj = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")

    result = invoke_get_subjects_pheno(
        project_id=subj["project_id"],
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        include_child_terms=False
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]

    assert len(subjects) >= 1
    assert subj["project_subject_id"] in [s["project_subject_id"] for s in subjects]


# ============================================================================
# QUALIFIER FILTERING TESTS
# ============================================================================

def test_get_subjects_exclude_qualified(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 5: Verify include_qualified=false filters out qualified terms.

    Setup: Create subjects with qualified and unqualified evidence
    Action: Query with include_qualified=false
    Verify: Only unqualified subjects returned
    """
    # Create subject with unqualified evidence
    subj_unqualified = create_subject_with_evidence(
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        qualifiers=[]
    )

    # Create subject with negated evidence
    subj_negated = create_subject_with_evidence(
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        qualifiers=["negated"]
    )

    # Query with include_qualified=false
    result = invoke_get_subjects_pheno(
        project_id=subj_unqualified["project_id"],
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        include_child_terms=False,
        include_qualified=False
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]
    project_subject_ids = [s["project_subject_id"] for s in subjects]

    # Unqualified subject should be present
    assert subj_unqualified["project_subject_id"] in project_subject_ids
    # Negated subject should NOT be present
    assert subj_negated["project_subject_id"] not in project_subject_ids


def test_get_subjects_include_qualified(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 6: Verify include_qualified=true includes all subjects.

    Setup: Create subjects with qualified and unqualified evidence
    Action: Query with include_qualified=true
    Verify: Both qualified and unqualified subjects returned
    """
    # Create subject with unqualified evidence
    subj_unqualified = create_subject_with_evidence(
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        qualifiers=[]
    )

    # Create subject with negated evidence
    subj_negated = create_subject_with_evidence(
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        qualifiers=["negated"]
    )

    # Query with include_qualified=true
    result = invoke_get_subjects_pheno(
        project_id=subj_unqualified["project_id"],
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        include_child_terms=False,
        include_qualified=True
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]
    project_subject_ids = [s["project_subject_id"] for s in subjects]

    # Both should be present
    assert subj_unqualified["project_subject_id"] in project_subject_ids
    assert subj_negated["project_subject_id"] in project_subject_ids


# ============================================================================
# PAGINATION TESTS
# ============================================================================

def test_get_subjects_pagination_basic(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 7 & 25: Verify pagination with limit parameter.

    Setup: Create 4 subjects
    Action: Query with limit=2
    Verify: Exactly 2 subjects returned, has_more=true
    """
    # Create 4 subjects
    subjs = [
        create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
        for _ in range(4)
    ]

    # Query with limit=2
    result = invoke_get_subjects_pheno(
        project_id=subjs[0]["project_id"],
        limit=2
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]
    pagination = body["pagination"]

    assert len(subjects) == 2
    assert pagination["limit"] == 2
    assert pagination["has_more"] is True
    assert pagination["next_cursor"] is not None


def test_get_subjects_pagination_cursor(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 26: Verify cursor-based pagination across multiple pages.

    Setup: Create 5 subjects
    Action: Query with limit=2, then use cursor to get next page
    Verify: All subjects retrieved across pages, no duplicates
    """
    # Create 5 subjects
    subjs = [
        create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
        for _ in range(5)
    ]

    all_retrieved_ids = []

    # First page
    result = invoke_get_subjects_pheno(
        project_id=subjs[0]["project_id"],
        limit=2
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    all_retrieved_ids.extend([s["project_subject_id"] for s in body["body"]])
    pagination = body["pagination"]

    # Continue paginating
    while pagination["has_more"]:
        cursor = pagination["next_cursor"]
        result = invoke_get_subjects_pheno(
            project_id=subjs[0]["project_id"],
            limit=2,
            cursor=cursor
        )

        assert result["statusCode"] == 200
        body = result["decompressed_body"]
        all_retrieved_ids.extend([s["project_subject_id"] for s in body["body"]])
        pagination = body["pagination"]

    # Verify all subjects retrieved
    expected_ids = [s["project_subject_id"] for s in subjs]
    for expected_id in expected_ids:
        assert expected_id in all_retrieved_ids

    # Verify no duplicates
    assert len(all_retrieved_ids) == len(set(all_retrieved_ids))


def test_get_subjects_pagination_empty_cursor(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 27: Verify empty cursor is handled gracefully.

    Action: Query with empty cursor string
    Verify: Treated as first page, returns results normally
    """
    subj = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")

    result = invoke_get_subjects_pheno(
        project_id=subj["project_id"],
        limit=2,
        cursor=""
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    assert "body" in body
    assert "pagination" in body


# ============================================================================
# PROJECT_SUBJECT_IDS FILTER TESTS
# ============================================================================

def test_get_subjects_project_subject_ids_filter(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 17 & 22: Filter by specific project_subject_ids.

    Setup: Create 4 subjects
    Action: Query with project_subject_ids filter for 2 of them
    Verify: Only specified subjects returned
    """
    # Create 4 subjects
    subj_a = create_subject_with_evidence(project_subject_id="subject-a")
    subj_b = create_subject_with_evidence(project_subject_id="subject-b")
    subj_c = create_subject_with_evidence(project_subject_id="subject-c")
    subj_d = create_subject_with_evidence(project_subject_id="subject-d")

    # Query with filter for subject-b and subject-c
    result = invoke_get_subjects_pheno(
        project_id=subj_a["project_id"],
        project_subject_ids=["subject-b", "subject-c"]
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]
    project_subject_ids = [s["project_subject_id"] for s in subjects]

    assert len(subjects) == 2
    assert "subject-b" in project_subject_ids
    assert "subject-c" in project_subject_ids
    assert "subject-a" not in project_subject_ids
    assert "subject-d" not in project_subject_ids


# ============================================================================
# OUTPUT FORMAT TESTS
# ============================================================================

def test_get_subjects_json_output(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 8: Verify JSON output format (default).

    Action: Query with output_type=json (or omit for default)
    Verify: Response is valid JSON with expected structure
    """
    subj = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")

    result = invoke_get_subjects_pheno(
        project_id=subj["project_id"],
        output_type="json"
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]

    # Verify JSON structure
    assert "body" in body
    assert "n_subjects" in body
    assert "pagination" in body
    assert isinstance(body["body"], list)


def test_get_subjects_gzip_compression(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 11: Verify response is gzip compressed.

    Action: Query subjects
    Verify: Response has Content-Encoding: gzip, body is base64-encoded
    """
    subj = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")

    result = invoke_get_subjects_pheno(project_id=subj["project_id"])

    assert result["statusCode"] == 200
    assert result.get("isBase64Encoded") is True
    assert result.get("headers", {}).get("Content-Encoding") == "gzip"

    # Verify we can decompress
    compressed_data = base64.b64decode(result["body"])
    decompressed = gzip.decompress(compressed_data).decode('utf-8')
    body = json.loads(decompressed)

    assert "body" in body
    assert "pagination" in body


# ============================================================================
# EMPTY RESULT TESTS
# ============================================================================

def test_get_subjects_empty_result_set(invoke_get_subjects_pheno, test_project_id):
    """
    Test 31: Query with non-existent term returns empty result (not error).

    Action: Query with non-existent term
    Verify: Returns 200 with empty list
    """
    result = invoke_get_subjects_pheno(
        project_id=test_project_id,
        term_iri="http://purl.obolibrary.org/obo/HP_9999999",
        term_source="hpo"
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]

    assert body["body"] == []
    assert body["n_subjects"] == 0


def test_get_subjects_empty_result_project_subjects(invoke_get_subjects_pheno, test_project_id):
    """
    Test 32: Query with non-existent project_subject_ids returns empty result.

    Action: Query with non-existent project_subject_ids
    Verify: Returns 200 with empty list
    """
    result = invoke_get_subjects_pheno(
        project_id=test_project_id,
        project_subject_ids=["non_existent_1", "non_existent_2"]
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]

    assert body["body"] == []
    assert body["n_subjects"] == 0


# ============================================================================
# TERM ID CONVERSION TEST
# ============================================================================

def test_get_subjects_term_id_conversion(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 16: Verify term_iri is correctly converted to term_id for Iceberg query.

    Action: Query with term_iri in IRI format
    Verify: Query succeeds (conversion works)
    """
    subj = create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")

    result = invoke_get_subjects_pheno(
        project_id=subj["project_id"],
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        include_child_terms=False
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    subjects = body["body"]

    assert len(subjects) >= 1
    assert subj["project_subject_id"] in [s["project_subject_id"] for s in subjects]


# ============================================================================
# ROOT TERM REJECTION TEST
# ============================================================================

def test_get_subjects_root_term_rejected(invoke_get_subjects_pheno, test_project_id):
    """
    Test 15: Query with root term HP:0000001 and include_child_terms=true should error.

    Action: Query with root term
    Verify: Returns error to prevent full scan
    """
    result = invoke_get_subjects_pheno(
        project_id=test_project_id,
        term_iri="http://purl.obolibrary.org/obo/HP_0000001",
        include_child_terms=True
    )

    assert result["statusCode"] == 500
    assert "root" in result["body"].lower() or "all subjects" in result["body"].lower()


# ============================================================================
# RESPONSE STRUCTURE TESTS
# ============================================================================

def test_get_subjects_response_structure(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Verify the response has correct structure with subject and phenotype data.

    Action: Query subjects
    Verify: Response structure matches API contract
    """
    subj = create_subject_with_evidence(
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        evidence_count=2
    )

    result = invoke_get_subjects_pheno(project_id=subj["project_id"])

    assert result["statusCode"] == 200
    body = result["decompressed_body"]

    # Verify top-level structure
    assert "body" in body
    assert "n_subjects" in body
    assert "pagination" in body

    # Verify subjects structure
    subjects = body["body"]
    assert len(subjects) >= 1

    # Find our subject
    our_subject = None
    for s in subjects:
        if s["project_subject_id"] == subj["project_subject_id"]:
            our_subject = s
            break

    assert our_subject is not None

    # Verify subject fields
    assert "subject_iri" in our_subject
    assert "project_subject_iri" in our_subject
    assert "project_subject_id" in our_subject
    assert "phenotypes" in our_subject

    # Verify phenotypes structure
    phenotypes = our_subject["phenotypes"]
    assert len(phenotypes) >= 1

    phenotype = phenotypes[0]
    assert "term" in phenotype
    assert "iri" in phenotype["term"]
    assert "id" in phenotype["term"]
    assert "label" in phenotype["term"]
    assert "qualifiers" in phenotype
    assert "termlink_id" in phenotype
    assert "evidence_count" in phenotype

    # Verify evidence count is aggregated correctly
    assert phenotype["evidence_count"] == 2


def test_get_subjects_pagination_info(invoke_get_subjects_pheno, create_subject_with_evidence):
    """
    Test 19: Verify pagination metadata is correct.

    Action: Query with pagination
    Verify: Pagination includes total, has_more, next_cursor
    """
    # Create 3 subjects
    subjs = [
        create_subject_with_evidence(term_iri="http://purl.obolibrary.org/obo/HP_0001249")
        for _ in range(3)
    ]

    result = invoke_get_subjects_pheno(
        project_id=subjs[0]["project_id"],
        limit=2
    )

    assert result["statusCode"] == 200
    body = result["decompressed_body"]
    pagination = body["pagination"]

    # Verify pagination fields
    assert "limit" in pagination
    assert "cursor" in pagination
    assert "next_cursor" in pagination
    assert "has_more" in pagination

    assert pagination["limit"] == 2
    assert pagination["has_more"] is True
    assert pagination["next_cursor"] is not None
