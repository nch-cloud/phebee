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
    test_run_suffix = int(time.time())  # Unique suffix to avoid collisions with previous test runs

    jsonl_records = []

    for subj_idx in range(50):
        project_subject_id = f"bulk-subj-{test_run_suffix}-{subj_idx:03d}"

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
        "test_run_suffix": test_run_suffix,
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


def test_bulk_import_materialization_succeeded(golden_bulk_import_execution):
    """
    Test 4.5: Verify EMR materialization step completed successfully.

    Reuses golden execution to verify MaterializeProjectSubjectTerms EMR job
    ran and completed without errors.
    """
    history = golden_bulk_import_execution["history"]

    # Find MaterializeProjectSubjectTerms events
    materialization_entered = False
    materialization_succeeded = False
    materialization_failed = False

    for event in history:
        event_type = event["type"]

        if event_type == "TaskStateEntered":
            state_name = event["stateEnteredEventDetails"]["name"]
            if "MaterializeProjectSubjectTerms" in state_name:
                materialization_entered = True

        elif event_type == "TaskStateExited":
            state_name = event["stateExitedEventDetails"]["name"]
            if "MaterializeProjectSubjectTerms" in state_name:
                # Check if the output indicates success
                output = event["stateExitedEventDetails"].get("output")
                if output:
                    materialization_succeeded = True

        elif event_type == "TaskStateFailed":
            state_name = event.get("stateFailedEventDetails", {}).get("name", "")
            if "MaterializeProjectSubjectTerms" in state_name:
                materialization_failed = True

    assert materialization_entered, \
        "MaterializeProjectSubjectTerms step was never entered"

    assert not materialization_failed, \
        "MaterializeProjectSubjectTerms EMR job failed"

    assert materialization_succeeded, \
        "MaterializeProjectSubjectTerms step did not complete successfully"

    print("✓ EMR materialization step completed successfully")


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

    This test validates the OUTPUT of the EMR materialization job, ensuring
    the MaterializeProjectSubjectTerms step not only completed successfully
    (verified by test 4.5), but also populated the query tables correctly with
    properly aggregated data.

    NOTE: This test MUST run before the incremental tests to verify initial state.
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


@pytest.fixture(scope="module")
def incremental_bulk_import_execution(golden_bulk_import_execution, physical_resources, standard_hpo_terms):
    """
    Module-scoped fixture that executes TWO bulk imports for the SAME project/subjects.

    Uses a separate project from golden execution to avoid test interference.

    Runs two executions:
    1. First import: 4 evidence per subject, date=2024-01-15
    2. Second import: 3 evidence per subject, date=2024-02-20, SAME subjects

    This allows testing that incremental MERGE:
    - Adds evidence counts (4 + 3 = 7)
    - Extends date ranges (2024-01-15 to 2024-02-20)
    - Unions qualifiers
    - Doesn't create duplicate rows
    """
    print("\n[INCREMENTAL EXECUTION] Starting TWO bulk imports for incremental MERGE testing...")

    # Use DIFFERENT project to avoid interfering with golden execution tests
    incremental_project_id = f"incr-test-project-{uuid.uuid4().hex[:8]}"
    incremental_test_run_suffix = int(time.time())

    s3_client = boto3.client("s3")
    sfn_client = boto3.client("stepfunctions")

    bucket_name = physical_resources["PheBeeBucketName"]
    state_machine_arn = physical_resources["BulkImportStateMachineArn"]

    # ==================== FIRST IMPORT: 4 evidence per subject ====================
    print("[INCREMENTAL 1/2] Creating first import with 4 evidence per subject...")
    first_run_id = f"incr-first-{int(time.time())}"
    first_s3_prefix = f"bulk-imports/test/{first_run_id}"

    first_jsonl_records = []
    for subj_idx in range(50):
        project_subject_id = f"incr-subj-{incremental_test_run_suffix}-{subj_idx:03d}"

        evidence_list = []
        for ev_idx in range(4):  # 4 evidence per subject in first import
            evidence_list.append({
                "type": "clinical_note",
                "clinical_note_id": f"first-note-{subj_idx}-{ev_idx}",
                "encounter_id": f"first-encounter-{subj_idx}",
                "evidence_creator_id": "bulk-importer",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Bulk Import System",
                "note_timestamp": "2024-01-15",  # First import date
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

        first_jsonl_records.append({
            "project_id": incremental_project_id,
            "project_subject_id": project_subject_id,
            "term_iri": standard_hpo_terms["seizure"],
            "evidence": evidence_list,
            "row_num": subj_idx + 1,
            "batch_id": 0
        })

    first_jsonl_content = "\n".join(json.dumps(record) for record in first_jsonl_records)
    first_jsonl_key = f"{first_s3_prefix}/jsonl/evidence.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=first_jsonl_key,
        Body=first_jsonl_content.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    first_input_path = f"s3://{bucket_name}/{first_s3_prefix}/jsonl"
    print(f"[INCREMENTAL 1/2] Uploaded JSONL to {first_input_path}")

    # Start first execution
    first_execution_name = f"incr-first-{int(time.time())}"

    first_response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=first_execution_name,
        input=json.dumps({
            "run_id": first_run_id,
            "input_path": first_input_path
        })
    )

    first_execution_arn = first_response["executionArn"]
    print(f"[INCREMENTAL 1/2] Started execution: {first_execution_name}")
    print("[INCREMENTAL 1/2] Waiting for completion...")

    # Wait for first execution to complete
    start_time = time.time()
    max_wait = 7200

    while True:
        execution = sfn_client.describe_execution(executionArn=first_execution_arn)
        first_status = execution["status"]

        elapsed = int(time.time() - start_time)

        if first_status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            print(f"[INCREMENTAL 1/2] Completed with status: {first_status}")
            print(f"[INCREMENTAL 1/2] Total time: {elapsed}s ({elapsed//60}m {elapsed%60}s)")
            break

        if elapsed > max_wait:
            pytest.fail(f"First incremental execution timed out after {max_wait}s")

        if elapsed % 300 == 0 and elapsed > 0:
            print(f"[INCREMENTAL 1/2] Still running... {elapsed}s elapsed")

        time.sleep(30)

    if first_status != "SUCCEEDED":
        pytest.fail(f"First incremental execution failed with status: {first_status}")

    # Capture state after first import for comparison
    print("[INCREMENTAL 1/2] Capturing materialized state after first import...")
    time.sleep(10)  # Brief pause to ensure Athena can see the data

    # ==================== SECOND IMPORT: 3 evidence per subject, SAME subjects ====================
    print("[INCREMENTAL 2/2] Creating second import with 3 evidence per subject for SAME subjects...")
    time.sleep(5)  # Brief pause between executions

    second_run_id = f"incr-second-{int(time.time())}"
    second_s3_prefix = f"bulk-imports/test/{second_run_id}"

    second_jsonl_records = []
    for subj_idx in range(50):
        project_subject_id = f"incr-subj-{incremental_test_run_suffix}-{subj_idx:03d}"  # SAME subject IDs!

        evidence_list = []
        for ev_idx in range(3):  # 3 evidence per subject in second import
            evidence_list.append({
                "type": "clinical_note",
                "clinical_note_id": f"second-note-{subj_idx}-{ev_idx}",
                "encounter_id": f"second-encounter-{subj_idx}",
                "evidence_creator_id": "bulk-importer",
                "evidence_creator_type": "automated",
                "evidence_creator_name": "Bulk Import System",
                "note_timestamp": "2024-02-20",  # Second import date (later)
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

        second_jsonl_records.append({
            "project_id": incremental_project_id,  # SAME project
            "project_subject_id": project_subject_id,  # SAME subjects
            "term_iri": standard_hpo_terms["seizure"],  # SAME term
            "evidence": evidence_list,
            "row_num": subj_idx + 1,
            "batch_id": 0
        })

    second_jsonl_content = "\n".join(json.dumps(record) for record in second_jsonl_records)
    second_jsonl_key = f"{second_s3_prefix}/jsonl/evidence.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=second_jsonl_key,
        Body=second_jsonl_content.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    second_input_path = f"s3://{bucket_name}/{second_s3_prefix}/jsonl"
    print(f"[INCREMENTAL 2/2] Uploaded JSONL to {second_input_path}")

    # Start second execution
    second_execution_name = f"incr-second-{int(time.time())}"

    second_response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=second_execution_name,
        input=json.dumps({
            "run_id": second_run_id,
            "input_path": second_input_path
        })
    )

    second_execution_arn = second_response["executionArn"]
    print(f"[INCREMENTAL 2/2] Started execution: {second_execution_name}")
    print("[INCREMENTAL 2/2] Waiting for completion...")

    # Wait for second execution to complete
    start_time = time.time()

    while True:
        execution = sfn_client.describe_execution(executionArn=second_execution_arn)
        second_status = execution["status"]

        elapsed = int(time.time() - start_time)

        if second_status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            print(f"[INCREMENTAL 2/2] Completed with status: {second_status}")
            print(f"[INCREMENTAL 2/2] Total time: {elapsed}s ({elapsed//60}m {elapsed%60}s)")
            break

        if elapsed > max_wait:
            pytest.fail(f"Second incremental execution timed out after {max_wait}s")

        if elapsed % 300 == 0 and elapsed > 0:
            print(f"[INCREMENTAL 2/2] Still running... {elapsed}s elapsed")

        time.sleep(30)

    return {
        "first_execution_arn": first_execution_arn,
        "first_execution_name": first_execution_name,
        "first_run_id": first_run_id,
        "second_execution_arn": second_execution_arn,
        "execution_name": second_execution_name,  # For compatibility
        "run_id": second_run_id,  # This is what tests query
        "project_id": incremental_project_id,
        "status": second_status,
        "expected_subjects": 50,
        "expected_evidence_first": 200,  # 4 per subject
        "expected_evidence_second": 150,  # 3 per subject
        "first_run_evidence": 4,
        "second_run_evidence": 3,
        "expected_total_evidence": 7  # Should be 4 + 3 = 7 after merge
    }


def test_bulk_import_incremental_merge_evidence_counts(incremental_bulk_import_execution, query_athena):
    """
    Test 6a: Verify incremental MERGE adds evidence counts correctly.

    After running two bulk imports for the same subjects:
    - First import: 4 evidence per subject
    - Second import: 3 evidence per subject
    - Expected result: evidence_count = 7 per subject
    """
    assert incremental_bulk_import_execution["status"] == "SUCCEEDED", \
        "Incremental execution must succeed for this test"

    run_id = incremental_bulk_import_execution["run_id"]

    # Query the materialized table to verify evidence counts were added
    query = f"""
        SELECT subject_id,
               evidence_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{run_id}'
        )
        ORDER BY subject_id
        LIMIT 10
    """

    results = query_athena(query)

    expected_count = incremental_bulk_import_execution["expected_total_evidence"]

    for row in results:
        evidence_count = int(row["evidence_count"])
        assert evidence_count == expected_count, \
            f"Subject {row['subject_id']}: Expected evidence_count={expected_count} (4+3), found {evidence_count}"

    print(f"✓ Evidence counts correctly added: {expected_count} per subject")


def test_bulk_import_incremental_merge_date_ranges(incremental_bulk_import_execution, query_athena):
    """
    Test 6b: Verify incremental MERGE extends date ranges correctly.

    After running two imports:
    - First import: 4 evidence per subject
    - Second import: 3 evidence per subject (SAME subjects)

    Verifies that the materialized table's date range encompasses evidence
    from both imports (uses MIN/MAX from both run_ids).
    """
    assert incremental_bulk_import_execution["status"] == "SUCCEEDED", \
        "Incremental execution must succeed for this test"

    first_run_id = incremental_bulk_import_execution["first_run_id"]
    second_run_id = incremental_bulk_import_execution["run_id"]

    # Get note_date range from the raw evidence table for both imports
    # Using note_context.note_date (the clinical observation date) not created_date (system import date)
    # Cast to date to match the format in the materialized table
    evidence_dates_query = f"""
        SELECT
            CAST(MIN(note_context.note_date) AS DATE) as min_evidence_date,
            CAST(MAX(note_context.note_date) AS DATE) as max_evidence_date
        FROM phebee.evidence
        WHERE run_id IN ('{first_run_id}', '{second_run_id}')
    """

    evidence_dates = query_athena(evidence_dates_query)
    expected_min_date = evidence_dates[0]["min_evidence_date"]
    expected_max_date = evidence_dates[0]["max_evidence_date"]

    print(f"Clinical observation date range from both imports: {expected_min_date} to {expected_max_date}")

    # Get date range from materialized table
    materialized_query = f"""
        SELECT subject_id,
               first_evidence_date,
               last_evidence_date
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{second_run_id}'
        )
        ORDER BY subject_id
        LIMIT 5
    """

    results = query_athena(materialized_query)
    assert len(results) > 0, "Should have results for date range test"

    for row in results:
        first_date = row["first_evidence_date"]
        last_date = row["last_evidence_date"]

        # Verify dates are present (not null)
        assert first_date is not None, \
            f"Subject {row['subject_id']}: first_evidence_date should not be null"
        assert last_date is not None, \
            f"Subject {row['subject_id']}: last_evidence_date should not be null"

        # Verify the materialized dates match the evidence date range
        # The MERGE should use MIN for first_evidence_date and MAX for last_evidence_date
        assert first_date == expected_min_date, \
            f"Subject {row['subject_id']}: first_evidence_date ({first_date}) should match min from evidence ({expected_min_date})"

        assert last_date == expected_max_date, \
            f"Subject {row['subject_id']}: last_evidence_date ({last_date}) should match max from evidence ({expected_max_date})"

        # Verify first_date <= last_date
        assert first_date <= last_date, \
            f"Subject {row['subject_id']}: first_evidence_date ({first_date}) should be <= last_evidence_date ({last_date})"

    print(f"✓ Date ranges correctly computed: {expected_min_date} to {expected_max_date}")


def test_bulk_import_incremental_merge_no_duplicates(incremental_bulk_import_execution, query_athena):
    """
    Test 6c: Verify incremental MERGE doesn't create duplicate rows.

    After running two imports for the same (subject, term) combinations,
    we should still have only ONE row per (subject, term), not two.
    """
    assert incremental_bulk_import_execution["status"] == "SUCCEEDED", \
        "Incremental execution must succeed for this test"

    run_id = incremental_bulk_import_execution["run_id"]

    # Count rows per subject - should be exactly 1 (not 2)
    query = f"""
        SELECT subject_id,
               COUNT(*) as row_count
        FROM phebee.subject_terms_by_subject
        WHERE subject_id IN (
            SELECT DISTINCT subject_id
            FROM phebee.evidence
            WHERE run_id = '{run_id}'
        )
        GROUP BY subject_id
        ORDER BY subject_id
    """

    results = query_athena(query)

    for row in results:
        row_count = int(row["row_count"])
        assert row_count == 1, \
            f"Subject {row['subject_id']}: Expected 1 row (no duplicates), found {row_count}"

    print(f"✓ No duplicate rows created: 1 row per (subject, term)")


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
    Test 9: Multiple concurrent bulk import executions.

    MARKED AS MANUAL: This test takes 1+ hours and should be run manually
    to verify concurrency handling.

    To run: pytest -m manual test_bulk_import_statemachine.py::test_bulk_import_concurrent_executions
    """
    pytest.skip("Concurrent execution test - run manually with: pytest -m manual")
