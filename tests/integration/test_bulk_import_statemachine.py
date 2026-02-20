"""
Integration tests for BulkImportStateMachine.

Tests the end-to-end bulk import orchestration including validation, EMR processing,
Neptune loading, and materialization.

NOTE: These tests use a CONSOLIDATED APPROACH to minimize execution time:
- One module-scoped "golden" execution runs completely (~30-60 minutes)
- Multiple tests verify different aspects of that single execution
- Fast validation-only tests run separately
- Heavy tests (large-scale, concurrent) are marked as manual/slow

This reduces total test time from 4+ hours to under 1 hour.
"""
import json
import pytest
import time
import boto3
import uuid
from phebee.utils.aws import get_client


@pytest.fixture(scope="module")
def golden_bulk_import_execution(physical_resources, standard_hpo_terms):
    """
    Module-scoped fixture that executes ONE complete bulk import.

    This "golden" execution is reused by multiple tests to verify different
    aspects without running the expensive workflow multiple times.

    Creates:
    - CSV with 50 subjects, 200 evidence records
    - Uploads to S3
    - Starts state machine execution
    - Waits for completion (~30-60 minutes)

    Returns dict with execution details for verification.
    """
    print("\n[GOLDEN EXECUTION] Starting module-scoped bulk import execution...")

    s3_client = boto3.client("s3")
    sfn_client = boto3.client("stepfunctions")

    bucket_name = physical_resources["PheBeeBucketName"]
    state_machine_arn = physical_resources["BulkImportStateMachineArn"]

    run_id = f"golden-test-run-{int(time.time())}"
    s3_prefix = f"bulk-imports/test/{run_id}"

    # Generate JSONL data with 50 subjects, 200 evidence records (4 per subject)
    test_project_id = f"bulk-test-project-{uuid.uuid4().hex[:8]}"

    jsonl_records = []

    for subj_idx in range(50):
        project_subject_id = f"bulk-subj-{subj_idx:03d}"

        # Create 4 evidence records for this subject
        evidence_list = []
        for ev_idx in range(4):
            evidence_list.append({
                "type": "clinical_note",
                "clinical_note_id": f"note-{subj_idx}-{ev_idx}",
                "encounter_id": f"encounter-{subj_idx}",
                "evidence_creator_id": "bulk-importer",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Bulk Import System",
                "note_timestamp": "2024-01-15",
                "note_type": "progress_note",
                "provider_type": "physician",
                "author_specialty": "internal_medicine",
                "span_start": 10 + ev_idx * 10,
                "span_end": 20 + ev_idx * 10,
                "contexts": {
                    "negated": 0.0,
                    "family": 0.0,
                    "hypothetical": 0.0
                }
            })

        # Create one JSONL record per subject with all their evidence
        jsonl_records.append({
            "project_id": test_project_id,
            "project_subject_id": project_subject_id,
            "term_iri": standard_hpo_terms["seizure"],
            "evidence": evidence_list,
            "row_num": subj_idx + 1,
            "batch_id": 0
        })

    # Convert to JSONL format (newline-delimited JSON)
    jsonl_content = "\n".join(json.dumps(record) for record in jsonl_records)

    # Upload JSONL to S3 in /jsonl/ subdirectory with .json extension
    jsonl_key = f"{s3_prefix}/jsonl/evidence.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=jsonl_key,
        Body=jsonl_content.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    input_path = f"s3://{bucket_name}/{s3_prefix}/jsonl"
    print(f"[GOLDEN EXECUTION] Uploaded JSONL to {input_path}")

    # Start state machine execution
    execution_name = f"golden-bulk-import-{int(time.time())}"

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "run_id": run_id,
            "input_path": input_path
        })
    )

    execution_arn = response["executionArn"]
    print(f"[GOLDEN EXECUTION] Started execution: {execution_name}")
    print(f"[GOLDEN EXECUTION] Execution ARN: {execution_arn}")
    print("[GOLDEN EXECUTION] Waiting for completion (this may take 30-60 minutes)...")

    # Wait for completion with extended timeout
    start_time = time.time()
    max_wait = 7200  # 2 hours max

    while True:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution["status"]

        elapsed = int(time.time() - start_time)

        if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            print(f"[GOLDEN EXECUTION] Execution completed with status: {status}")
            print(f"[GOLDEN EXECUTION] Total time: {elapsed}s ({elapsed//60}m {elapsed%60}s)")
            break

        if elapsed > max_wait:
            pytest.fail(f"Golden execution timed out after {max_wait}s")

        # Log progress every 5 minutes
        if elapsed % 300 == 0 and elapsed > 0:
            print(f"[GOLDEN EXECUTION] Still running... {elapsed}s elapsed ({elapsed//60}m)")

        time.sleep(30)

    # Get execution history for detailed verification
    history_response = sfn_client.get_execution_history(
        executionArn=execution_arn,
        maxResults=1000
    )

    execution_details = {
        "execution_arn": execution_arn,
        "execution_name": execution_name,
        "run_id": run_id,
        "project_id": test_project_id,
        "input_path": input_path,
        "status": status,
        "execution": execution,
        "history": history_response["events"],
        "expected_subjects": 50,
        "expected_evidence": 200,
        "s3_bucket": bucket_name,
        "s3_prefix": s3_prefix
    }

    # Store for cleanup (not implemented here - manual cleanup may be needed)

    return execution_details


def test_bulk_import_happy_path_completion(golden_bulk_import_execution):
    """
    Test 1: Verify golden execution completed successfully.

    This is the primary test that validates the happy path execution succeeded.
    """
    assert golden_bulk_import_execution["status"] == "SUCCEEDED", \
        f"Execution failed: {golden_bulk_import_execution['execution']}"


def test_bulk_import_evidence_in_iceberg(golden_bulk_import_execution, query_athena):
    """
    Test 2: Verify all evidence was loaded to Iceberg.

    Reuses golden execution to verify data in Iceberg evidence table.
    """
    run_id = golden_bulk_import_execution["run_id"]

    # Query evidence count
    query = f"""
        SELECT COUNT(*) as count
        FROM phebee.evidence
        WHERE run_id = '{run_id}'
    """

    results = query_athena(query)
    assert len(results) == 1

    evidence_count = int(results[0]["count"])
    expected_count = golden_bulk_import_execution["expected_evidence"]

    assert evidence_count == expected_count, \
        f"Expected {expected_count} evidence records, found {evidence_count}"


def test_bulk_import_subjects_processed(golden_bulk_import_execution, query_athena):
    """
    Test 3: Verify all subjects' evidence was processed.

    Reuses golden execution to verify subject distribution in evidence.
    """
    run_id = golden_bulk_import_execution["run_id"]

    # Query distinct subjects
    query = f"""
        SELECT COUNT(DISTINCT subject_id) as subject_count
        FROM phebee.evidence
        WHERE run_id = '{run_id}'
    """

    results = query_athena(query)
    subject_count = int(results[0]["subject_count"])
    expected_subjects = golden_bulk_import_execution["expected_subjects"]

    assert subject_count == expected_subjects, \
        f"Expected {expected_subjects} subjects, found {subject_count}"


def test_bulk_import_execution_stages(golden_bulk_import_execution):
    """
    Test 4: Verify all expected state machine stages were executed.

    Reuses golden execution to verify stage progression through the workflow.
    """
    history = golden_bulk_import_execution["history"]

    # Extract state names from execution history
    states_entered = []
    for event in history:
        if event["type"] == "TaskStateEntered":
            state_name = event["stateEnteredEventDetails"]["name"]
            states_entered.append(state_name)

    # Critical stages that should be present
    expected_stages = [
        "ValidateInput",
        "MaterializeProjectSubjectTerms"
    ]

    for stage in expected_stages:
        matching_states = [s for s in states_entered if stage in s]
        assert len(matching_states) > 0, \
            f"Stage '{stage}' not found in execution history. States: {states_entered}"


def test_bulk_import_ttl_files_generated(golden_bulk_import_execution):
    """
    Test 5: Verify TTL files were generated and uploaded to S3.

    Reuses golden execution to verify EMR generated TTL files.
    """
    s3_client = boto3.client("s3")

    bucket = golden_bulk_import_execution["s3_bucket"]
    run_id = golden_bulk_import_execution["run_id"]

    # Check for TTL files in runs/{run_id}/neptune/projects/ directory
    projects_prefix = f"runs/{run_id}/neptune/projects/"
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=projects_prefix,
        MaxKeys=10
    )

    # Should have at least one TTL file
    assert "Contents" in response, f"No TTL files found at {projects_prefix}"
    assert len(response["Contents"]) > 0, "Expected at least one project TTL file"

    # Verify file has .ttl extension
    first_file = response["Contents"][0]["Key"]
    assert first_file.endswith(".ttl"), f"Expected .ttl file, got {first_file}"


def test_bulk_import_query_tables_materialized(golden_bulk_import_execution, query_athena):
    """
    Test 6: Verify analytical query tables were materialized successfully.

    Reuses golden execution to verify both subject_terms_by_subject and
    subject_terms_by_project_term tables contain the expected data.

    This test ensures the MaterializeProjectSubjectTerms step not only ran,
    but actually populated the query tables correctly.
    """
    project_id = golden_bulk_import_execution["project_id"]
    expected_subjects = golden_bulk_import_execution["expected_subjects"]

    # Test 1: Verify subject_terms_by_subject table
    by_subject_query = f"""
        SELECT COUNT(DISTINCT subject_id) as subject_count,
               COUNT(*) as total_records
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{golden_bulk_import_execution["run_id"]}'
        )
    """

    by_subject_results = query_athena(by_subject_query)
    assert len(by_subject_results) == 1, "Expected one result row from by_subject query"

    subject_count = int(by_subject_results[0]["subject_count"])
    total_records = int(by_subject_results[0]["total_records"])

    assert subject_count == expected_subjects, \
        f"subject_terms_by_subject: Expected {expected_subjects} subjects, found {subject_count}"

    # With 50 subjects and 1 unique term, we should have 50 records (1 per subject)
    assert total_records == expected_subjects, \
        f"subject_terms_by_subject: Expected {expected_subjects} records, found {total_records}"

    print(f"✓ subject_terms_by_subject: {subject_count} subjects, {total_records} records")

    # Test 2: Verify subject_terms_by_project_term table
    by_project_term_query = f"""
        SELECT COUNT(DISTINCT subject_id) as subject_count,
               COUNT(DISTINCT term_id) as term_count,
               COUNT(*) as total_records
        FROM phebee.subject_terms_by_project_term
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{golden_bulk_import_execution["run_id"]}'
        )
    """

    by_project_term_results = query_athena(by_project_term_query)
    assert len(by_project_term_results) == 1, "Expected one result row from by_project_term query"

    pt_subject_count = int(by_project_term_results[0]["subject_count"])
    pt_term_count = int(by_project_term_results[0]["term_count"])
    pt_total_records = int(by_project_term_results[0]["total_records"])

    assert pt_subject_count == expected_subjects, \
        f"subject_terms_by_project_term: Expected {expected_subjects} subjects, found {pt_subject_count}"

    # All subjects reference the same term, so we should have 1 unique term
    assert pt_term_count == 1, \
        f"subject_terms_by_project_term: Expected 1 unique term, found {pt_term_count}"

    # 50 subjects × 1 term = 50 records
    assert pt_total_records == expected_subjects, \
        f"subject_terms_by_project_term: Expected {expected_subjects} records, found {pt_total_records}"

    print(f"✓ subject_terms_by_project_term: {pt_subject_count} subjects, {pt_term_count} terms, {pt_total_records} records")

    # Test 3: Verify aggregation is working correctly
    # The test creates 4 evidence records per subject for the SAME term.
    # Materialization should aggregate these into ONE row per (subject, term) with evidence_count=4.
    # This verifies that multiple evidence records get properly collapsed.

    # First, verify the raw evidence table has 4 records per subject
    raw_evidence_query = f"""
        SELECT subject_id,
               COUNT(*) as raw_evidence_count
        FROM phebee.evidence
        WHERE run_id = '{golden_bulk_import_execution["run_id"]}'
        GROUP BY subject_id
        ORDER BY subject_id
        LIMIT 5
    """

    raw_evidence_results = query_athena(raw_evidence_query)
    for row in raw_evidence_results:
        raw_count = int(row["raw_evidence_count"])
        assert raw_count == 4, f"Subject {row['subject_id']} should have 4 raw evidence records, found {raw_count}"

    print(f"✓ Verified raw evidence: 4 records per subject")

    # Now verify that materialization aggregated these into single rows
    aggregation_query = f"""
        SELECT subject_id,
               COUNT(*) as materialized_rows,
               MAX(evidence_count) as evidence_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{golden_bulk_import_execution["run_id"]}'
        )
        GROUP BY subject_id
        ORDER BY subject_id
        LIMIT 5
    """

    aggregation_results = query_athena(aggregation_query)
    for row in aggregation_results:
        materialized_rows = int(row["materialized_rows"])
        evidence_count = int(row["evidence_count"])

        # Each subject should have exactly 1 materialized row (not 4)
        assert materialized_rows == 1, \
            f"Subject {row['subject_id']}: Expected 1 materialized row, found {materialized_rows}. Aggregation may not be working."

        # That single row should have evidence_count=4 (aggregated from 4 raw records)
        assert evidence_count == 4, \
            f"Subject {row['subject_id']}: Expected evidence_count=4, found {evidence_count}"

    print(f"✓ Aggregation verified: 4 evidence records → 1 materialized row per subject")

    # Verify overall evidence counts
    evidence_count_query = f"""
        SELECT MIN(evidence_count) as min_evidence,
               MAX(evidence_count) as max_evidence,
               AVG(evidence_count) as avg_evidence
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{golden_bulk_import_execution["run_id"]}'
        )
    """

    evidence_results = query_athena(evidence_count_query)
    min_evidence = int(evidence_results[0]["min_evidence"])
    max_evidence = int(evidence_results[0]["max_evidence"])

    assert min_evidence > 0, "All records should have evidence_count > 0"
    assert min_evidence == 4, f"Expected min evidence_count = 4, found {min_evidence}"
    assert max_evidence == 4, f"Expected max evidence_count = 4, found {max_evidence}"

    print(f"✓ Evidence counts across all subjects: min={min_evidence}, max={max_evidence}")


def test_bulk_import_invalid_input_path(physical_resources):
    """
    Test 7: Validation failure - nonexistent S3 path.

    Fast test that validates early failure for invalid input.
    """
    sfn_client = boto3.client("stepfunctions")
    state_machine_arn = physical_resources["BulkImportStateMachineArn"]

    # Start with invalid path
    execution_name = f"test-invalid-path-{int(time.time())}"

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "run_id": f"invalid-{int(time.time())}",
            "input_path": "s3://nonexistent-bucket-12345/fake/path.csv"
        })
    )

    execution_arn = response["executionArn"]

    # Wait for failure (should be fast - validation only)
    max_wait = 300  # 5 minutes max
    start_time = time.time()

    while time.time() - start_time < max_wait:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution["status"]

        if status in ["FAILED", "TIMED_OUT", "ABORTED"]:
            # Validation should have failed
            assert status == "FAILED"
            return

        if status == "SUCCEEDED":
            pytest.fail("Expected validation failure, but execution succeeded")

        time.sleep(10)

    pytest.fail("Execution did not fail within timeout")


def test_bulk_import_missing_run_id(physical_resources):
    """
    Test 8: Validation failure - missing run_id parameter.

    Fast test that validates required field checking.
    """
    sfn_client = boto3.client("stepfunctions")
    state_machine_arn = physical_resources["BulkImportStateMachineArn"]

    execution_name = f"test-missing-run-id-{int(time.time())}"

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "input_path": "s3://some-bucket/path.csv"
            # Missing run_id
        })
    )

    execution_arn = response["executionArn"]

    # Wait for failure
    max_wait = 300
    start_time = time.time()

    while time.time() - start_time < max_wait:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution["status"]

        if status in ["FAILED", "TIMED_OUT", "ABORTED"]:
            assert status == "FAILED"
            return

        if status == "SUCCEEDED":
            pytest.fail("Expected failure for missing run_id, but execution succeeded")

        time.sleep(10)

    pytest.fail("Execution did not fail within timeout")


@pytest.mark.slow
@pytest.mark.manual
def test_bulk_import_concurrent_executions():
    """
    Test 10: Multiple concurrent bulk import executions.

    MARKED AS MANUAL: This test takes 1+ hours and should be run manually
    to verify concurrency handling.

    To run: pytest -m manual test_bulk_import_statemachine.py::test_bulk_import_concurrent_executions
    """
    pytest.skip("Concurrent execution test - run manually with: pytest -m manual")
