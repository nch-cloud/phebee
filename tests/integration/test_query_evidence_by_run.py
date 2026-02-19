"""
Integration tests for QueryEvidenceByRun Lambda.

Tests the Athena-based evidence query with pagination, limit handling,
and structured field parsing (creator struct).
"""
import json
import pytest
import time
import concurrent.futures
from phebee.utils.aws import get_client


def invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=None, next_token=None):
    """Helper to invoke QueryEvidenceByRun lambda."""
    lambda_client = get_client("lambda")

    payload = {"run_id": run_id}
    if limit is not None:
        payload["limit"] = limit
    if next_token is not None:
        payload["next_token"] = next_token

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-QueryEvidenceByRunFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    return result


@pytest.fixture(scope="module")
def module_test_project(physical_resources):
    """Module-scoped test project."""
    import boto3
    import uuid

    lambda_client = boto3.client("lambda")
    project_id = f"test-proj-query-evidence-{uuid.uuid4().hex[:8]}"
    project_label = "Test Project for Query Evidence"

    print(f"\n[MODULE FIXTURE] Creating project {project_id}")

    # Create project
    payload = {
        "project_id": project_id,
        "project_label": project_label
    }

    lambda_client.invoke(
        FunctionName=physical_resources["CreateProjectFunction"],
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
    )

    yield project_id

    # Cleanup
    try:
        print(f"\n[MODULE FIXTURE] Cleaning up project {project_id}")
        lambda_client.invoke(
            FunctionName=physical_resources["RemoveProjectFunction"],
            Payload=json.dumps({
                "pathParameters": {"project_id": project_id}
            }).encode("utf-8")
        )
    except Exception as e:
        print(f"[MODULE FIXTURE] Cleanup warning: {e}")


@pytest.fixture(scope="module")
def module_test_subject(physical_resources, module_test_project):
    """
    Module-scoped subject for shared evidence fixture.
    Creates one subject that persists for all tests.
    """
    import boto3
    import uuid

    lambda_client = boto3.client("lambda")
    project_id = module_test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:8]}"

    print(f"\n[MODULE FIXTURE] Creating subject in project {project_id}")

    # Create subject
    payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id
    }

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectFunction"],
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    assert result["statusCode"] == 200, f"Failed to create test subject: {result}"

    body = json.loads(result["body"])
    subject_iri = body["subject"]["iri"]

    # Extract UUID from IRI
    subject_uuid = subject_iri.split("/")[-1]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    print(f"[MODULE FIXTURE] Created subject {subject_uuid}")

    return (subject_uuid, project_subject_iri)


@pytest.fixture(scope="module")
def shared_run_evidence(physical_resources, module_test_subject, standard_hpo_terms):
    """
    Module-scoped fixture that creates 20 evidence records once.
    Most tests can reuse this data for efficiency.
    """
    import boto3

    lambda_client = boto3.client("lambda")
    subject_uuid, _ = module_test_subject
    run_id = f"shared-test-run-{int(time.time())}"

    print(f"\n[FIXTURE] Creating 20 evidence records for shared run_id: {run_id}")

    # Create 20 evidence records with timestamps spread out
    for i in range(20):
        payload = {
            "subject_id": subject_uuid,
            "term_iri": standard_hpo_terms["seizure"],
            "evidence_type": "phenotype_assertion",
            "run_id": run_id,
            "batch_id": f"batch-{i}",
            "creator_id": "test-creator",
            "creator_type": "human"
        }

        response = lambda_client.invoke(
            FunctionName=physical_resources["CreateEvidenceFunction"],
            Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8")
        )

        # Check for errors (CreateEvidence returns 201)
        result = json.loads(response["Payload"].read().decode("utf-8"))
        if result.get("statusCode") not in [200, 201]:
            print(f"[FIXTURE] Error creating evidence {i}: {result}")
            raise Exception(f"Failed to create evidence: {result}")

        if i < 19:
            time.sleep(0.05)  # Small delay for timestamp ordering

    print(f"[FIXTURE] Created 20 evidence records")

    return {
        "run_id": run_id,
        "subject_id": subject_uuid,
        "count": 20
    }


def test_query_evidence_by_run_success(cloudformation_stack, shared_run_evidence):
    """Test 1: Query run_id returns all evidence."""
    run_id = shared_run_evidence["run_id"]

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert body["run_id"] == run_id
    assert body["evidence_count"] == 20
    assert len(body["evidence"]) == 20
    assert body["has_more"] is False
    assert "next_token" not in body
    assert body["total_count"] == 20


def test_query_evidence_pagination(cloudformation_stack, shared_run_evidence):
    """Test 2: Pagination with limit=8 across 3 pages."""
    run_id = shared_run_evidence["run_id"]

    # First page with limit=8
    result1 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=8)
    assert result1["statusCode"] == 200
    body1 = json.loads(result1["body"])

    assert body1["evidence_count"] == 8
    assert body1["has_more"] is True
    assert "next_token" in body1
    assert body1["total_count"] == 20  # Total count on first page

    # Second page
    result2 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=8, next_token=body1["next_token"])
    body2 = json.loads(result2["body"])

    assert body2["evidence_count"] == 8
    assert body2["has_more"] is True
    assert "total_count" not in body2  # No count on subsequent pages

    # Third page
    result3 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=8, next_token=body2["next_token"])
    body3 = json.loads(result3["body"])

    assert body3["evidence_count"] == 4  # Remaining records
    assert body3["has_more"] is False


def test_query_evidence_limit_parameter(cloudformation_stack, shared_run_evidence):
    """Test 3: Limit parameter restricts results correctly."""
    run_id = shared_run_evidence["run_id"]

    # Query with limit=5
    result = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=5)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert body["evidence_count"] == 5
    assert body["limit"] == 5
    assert body["has_more"] is True


def test_query_evidence_max_10k_limit(cloudformation_stack):
    """Test 4: Limit capped at 10k even if higher requested."""
    result = invoke_query_evidence_by_run(
        "nonexistent-run",
        cloudformation_stack,
        limit=20000
    )

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Limit should be capped at 10000
    assert body["limit"] == 10000


def test_query_evidence_empty_run(cloudformation_stack):
    """Test 5: Query for run_id with no data returns empty array."""
    run_id = f"nonexistent-run-{int(time.time())}"

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert body["run_id"] == run_id
    assert body["evidence_count"] == 0
    assert body["evidence"] == []
    assert body["has_more"] is False
    assert body["total_count"] == 0


def test_query_evidence_missing_run_id(cloudformation_stack):
    """Test 6: Missing run_id returns 400 error."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-QueryEvidenceByRunFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({}).encode("utf-8")  # No run_id
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "run_id is required" in body["message"]


def test_query_evidence_total_count_first_page(cloudformation_stack, shared_run_evidence):
    """Test 7: First page includes total_count."""
    run_id = shared_run_evidence["run_id"]

    # First page query
    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Total count should be present on first page
    assert "total_count" in body
    assert body["total_count"] == 20


def test_query_evidence_creator_struct_parsed(
    cloudformation_stack,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms
):
    """Test 9: Creator field parsed from ROW format to dict."""
    subject_uuid, _ = test_subject
    run_id = f"test-run-creator-{int(time.time())}"

    # Create evidence with specific creator info
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri=standard_hpo_terms["seizure"],
        run_id=run_id,
        evidence_creator_id="test-creator-123",
        evidence_creator_type="automated",
        evidence_creator_name="Test Creator"
    )

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert len(body["evidence"]) == 1
    evidence = body["evidence"][0]

    # Verify creator is parsed as dict
    assert "creator" in evidence
    creator = evidence["creator"]
    assert isinstance(creator, dict)
    assert creator.get("creator_id") == "test-creator-123"
    assert creator.get("creator_type") == "automated"


def test_query_evidence_fields_complete(cloudformation_stack, shared_run_evidence):
    """Test 10: All evidence fields present in response."""
    run_id = shared_run_evidence["run_id"]

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=1)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    evidence = body["evidence"][0]

    # Required fields
    required_fields = [
        "evidence_id",
        "run_id",
        "batch_id",
        "evidence_type",
        "subject_id",
        "term_iri",
        "creator",
        "created_timestamp"
    ]

    for field in required_fields:
        assert field in evidence, f"Missing field: {field}"

    assert evidence["run_id"] == run_id
    assert evidence["subject_id"] == shared_run_evidence["subject_id"]


def test_query_evidence_ordered_by_timestamp(cloudformation_stack, shared_run_evidence):
    """Test 11: Results ordered by created_timestamp ascending."""
    run_id = shared_run_evidence["run_id"]

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    timestamps = [e["created_timestamp"] for e in body["evidence"]]

    # Verify timestamps are in ascending order
    assert timestamps == sorted(timestamps)


def test_query_evidence_concurrent_queries(
    cloudformation_stack,
    test_subject,
    create_evidence_helper,
    standard_hpo_terms
):
    """Test 14: Concurrent queries for different runs don't interfere."""
    subject_uuid, _ = test_subject

    # Create evidence for 3 different runs (2 records each)
    run_ids = [f"test-run-concurrent-{int(time.time())}-{i}" for i in range(3)]

    for run_id in run_ids:
        for j in range(2):
            create_evidence_helper(
                subject_id=subject_uuid,
                term_iri=standard_hpo_terms["seizure"],
                run_id=run_id,
                batch_id=f"batch-{j}"
            )

    # Query all runs concurrently
    def query_run(run_id):
        return invoke_query_evidence_by_run(run_id, cloudformation_stack)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(query_run, run_id) for run_id in run_ids]
        results = [future.result() for future in futures]

    # Verify all queries succeeded with correct data
    for i, result in enumerate(results):
        assert result["statusCode"] == 200
        body = json.loads(result["body"])

        assert body["run_id"] == run_ids[i]
        assert body["evidence_count"] == 2
        assert body["total_count"] == 2


def test_query_evidence_idempotent(cloudformation_stack, shared_run_evidence):
    """Test 16: Same query twice returns same results."""
    run_id = shared_run_evidence["run_id"]

    # Query twice
    result1 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=10)
    result2 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=10)

    assert result1["statusCode"] == result2["statusCode"]

    body1 = json.loads(result1["body"])
    body2 = json.loads(result2["body"])

    assert body1["evidence_count"] == body2["evidence_count"]
    assert body1["total_count"] == body2["total_count"]
    assert body1["has_more"] == body2["has_more"]


def test_query_evidence_has_more_flag(cloudformation_stack, shared_run_evidence):
    """Test 19: has_more flag indicates more pages available."""
    run_id = shared_run_evidence["run_id"]

    # Query with limit=10 (has more since we have 20)
    result1 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=10)
    body1 = json.loads(result1["body"])

    assert body1["has_more"] is True

    # Query all (no more)
    result2 = invoke_query_evidence_by_run(run_id, cloudformation_stack, limit=30)
    body2 = json.loads(result2["body"])

    assert body2["has_more"] is False


def test_query_evidence_response_structure(cloudformation_stack, shared_run_evidence):
    """Test: Response has all required fields."""
    run_id = shared_run_evidence["run_id"]

    result = invoke_query_evidence_by_run(run_id, cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # Required response fields
    required_fields = [
        "run_id",
        "evidence_count",
        "limit",
        "has_more",
        "evidence"
    ]

    for field in required_fields:
        assert field in body, f"Missing field: {field}"

    assert isinstance(body["evidence"], list)
    assert isinstance(body["evidence_count"], int)
    assert isinstance(body["has_more"], bool)


def test_query_evidence_null_run_id(cloudformation_stack):
    """Test: Null run_id returns 400 error."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-QueryEvidenceByRunFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"run_id": None}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 400


def test_query_evidence_empty_string_run_id(cloudformation_stack):
    """Test: Empty string run_id returns 400 error."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-QueryEvidenceByRunFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"run_id": ""}).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    assert result["statusCode"] == 400
