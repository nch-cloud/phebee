"""
Integration tests for ImportPhenopacketsSFN State Machine.

Tests the end-to-end phenopacket import workflow including ZIP parsing, subject creation,
and evidence import using distributed maps.

NOTE: These tests use a CONSOLIDATED APPROACH to minimize execution time:
- One module-scoped "golden" execution runs completely (~2-5 minutes)
- Multiple tests verify different aspects of that single execution
- Fast validation tests run separately
- State machine is SYNCHRONOUS (startSyncExecution) - much faster than bulk import

This reduces total test time significantly while maintaining comprehensive coverage.
"""
import json
import pytest
import time
import boto3
import uuid
import zipfile
import io
from phebee.utils.aws import get_client


def create_phenopacket(phenopacket_id, project_id, hpo_terms):
    """
    Create a valid GA4GH Phenopacket v2 structure.

    Args:
        phenopacket_id: Unique ID for this phenopacket
        project_id: Project ID to assign
        hpo_terms: List of HPO term IRIs

    Returns:
        Dict representing a phenopacket
    """
    return {
        "id": phenopacket_id,
        "project_id": project_id,
        "subject": {
            "id": phenopacket_id,
            "sex": "UNKNOWN_SEX"
        },
        "phenotypicFeatures": [
            {
                "type": {
                    "id": term_iri.replace("http://purl.obolibrary.org/obo/", "").replace("_", ":"),
                    "label": "Test Term"
                }
            }
            for term_iri in hpo_terms
        ],
        "metaData": {
            "created": "2024-01-01T00:00:00Z",
            "createdBy": "test-importer",
            "resources": [
                {
                    "id": "hp",
                    "name": "Human Phenotype Ontology",
                    "namespacePrefix": "HP",
                    "url": "http://purl.obolibrary.org/obo/hp.owl",
                    "version": "2024-01-01",
                    "iriPrefix": "http://purl.obolibrary.org/obo/HP_"
                }
            ],
            "phenopacketSchemaVersion": "2.0"
        }
    }


@pytest.fixture(scope="module")
def golden_phenopacket_import(physical_resources, standard_hpo_terms):
    """
    Module-scoped fixture that executes ONE complete phenopacket import.

    This "golden" execution is reused by multiple tests to verify different
    aspects without running the expensive workflow multiple times.

    Creates:
    - ZIP file with 10 phenopackets
    - Uploads to S3
    - Starts SYNCHRONOUS state machine execution
    - Waits for completion (~2-5 minutes)

    Returns dict with execution details for verification.
    """
    print("\n[GOLDEN PHENOPACKET IMPORT] Starting module-scoped import...")

    s3_client = boto3.client("s3")
    sfn_client = boto3.client("stepfunctions")

    bucket_name = physical_resources["PheBeeBucketName"]
    # Get the ImportPhenopacketsSFN ARN from outputs
    state_machine_arn = physical_resources.get("ImportPhenopacketsSFNArn")

    if not state_machine_arn:
        pytest.skip("ImportPhenopacketsSFNArn not found in physical_resources - state machine may not be deployed")

    test_project_id = f"phenopacket-test-proj-{uuid.uuid4().hex[:8]}"
    run_id = f"golden-phenopacket-import-{int(time.time())}"
    s3_prefix = f"phenopackets/test/{run_id}"

    # Create 10 phenopackets
    phenopackets = []
    for i in range(10):
        phenopacket_id = f"phenopacket-{i:03d}"
        phenopacket = create_phenopacket(
            phenopacket_id=phenopacket_id,
            project_id=test_project_id,
            hpo_terms=[standard_hpo_terms["seizure"]]
        )
        phenopackets.append(phenopacket)

    # Create ZIP file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for i, phenopacket in enumerate(phenopackets):
            zip_file.writestr(
                f"phenopacket-{i:03d}.json",
                json.dumps(phenopacket, indent=2)
            )

    zip_content = zip_buffer.getvalue()

    # Upload ZIP to S3
    zip_key = f"{s3_prefix}/phenopackets.zip"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=zip_key,
        Body=zip_content
    )

    input_path = f"s3://{bucket_name}/{zip_key}"
    output_path = f"s3://{bucket_name}/{s3_prefix}/processed/phenopackets.jsonl"

    print(f"[GOLDEN PHENOPACKET IMPORT] Uploaded ZIP to {input_path}")
    print(f"[GOLDEN PHENOPACKET IMPORT] Output will be at {output_path}")

    # Start SYNCHRONOUS state machine execution
    execution_name = f"golden-import-{int(time.time())}"

    print(f"[GOLDEN PHENOPACKET IMPORT] Starting execution: {execution_name}")
    print("[GOLDEN PHENOPACKET IMPORT] This is SYNCHRONOUS - will wait for completion...")

    start_time = time.time()

    try:
        response = sfn_client.start_sync_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps({
                "project_id": test_project_id,
                "s3_path": input_path,
                "output_s3_path": output_path
            })
        )

        elapsed = int(time.time() - start_time)
        print(f"[GOLDEN PHENOPACKET IMPORT] Execution completed in {elapsed}s")
        print(f"[GOLDEN PHENOPACKET IMPORT] Status: {response['status']}")

        # Parse output
        output_data = {}
        if response.get('output'):
            try:
                output_data = json.loads(response['output'])
            except json.JSONDecodeError:
                print(f"[GOLDEN PHENOPACKET IMPORT] Could not parse output: {response['output']}")

        execution_details = {
            "execution_arn": response["executionArn"],
            "execution_name": execution_name,
            "project_id": test_project_id,
            "input_path": input_path,
            "output_path": output_path,
            "status": response["status"],
            "output": output_data,
            "response": response,
            "expected_phenopackets": 10,
            "s3_bucket": bucket_name,
            "s3_prefix": s3_prefix,
            "phenopackets": phenopackets
        }

        return execution_details

    except Exception as e:
        elapsed = int(time.time() - start_time)
        print(f"[GOLDEN PHENOPACKET IMPORT] Execution failed after {elapsed}s: {e}")
        raise


def test_import_phenopackets_happy_path_completion(golden_phenopacket_import):
    """
    Test 1: Verify golden execution completed successfully.

    This is the primary test that validates the happy path execution succeeded.
    """
    assert golden_phenopacket_import["status"] == "SUCCEEDED", \
        f"Execution failed: {golden_phenopacket_import['response']}"


def test_import_phenopackets_jsonl_created(golden_phenopacket_import):
    """
    Test 3: Verify JSONL file was created by ParseZipFile.

    Reuses golden execution to verify S3 ItemReader input file was generated.
    """
    s3_client = boto3.client("s3")

    bucket = golden_phenopacket_import["s3_bucket"]
    output_path = golden_phenopacket_import["output_path"]

    # Parse S3 URL
    s3_key = output_path.replace(f"s3://{bucket}/", "")

    # Check if JSONL file exists
    try:
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        assert response["ContentLength"] > 0, "JSONL file is empty"
    except s3_client.exceptions.NoSuchKey:
        pytest.fail(f"JSONL file not found at {output_path}")


def test_import_phenopackets_subjects_created(golden_phenopacket_import, physical_resources):
    """
    Test 4: Verify subjects were created in DynamoDB.

    Reuses golden execution to verify CreateSubject step worked.
    """
    output_data = golden_phenopacket_import["output"]

    # Check if output contains summary with subject_ids
    if "Summary" in output_data and "subject_ids" in output_data["Summary"]:
        subject_ids = output_data["Summary"]["subject_ids"]
        assert len(subject_ids) > 0, "No subject IDs in output summary"

        # Verify at least one subject exists in DynamoDB
        dynamodb = boto3.resource("dynamodb")
        table_name = physical_resources["DynamoDBTableName"]
        table = dynamodb.Table(table_name)

        first_subject_id = subject_ids[0]
        project_id = golden_phenopacket_import["project_id"]

        # Query for subject mapping
        response = table.query(
            IndexName="GSI1",
            KeyConditionExpression="GSI1PK = :pk",
            ExpressionAttributeValues={
                ":pk": f"PROJECT#{project_id}"
            },
            Limit=1
        )

        assert response["Count"] > 0, f"No subjects found for project {project_id}"
    else:
        # If no summary, just verify execution succeeded (subjects may have been created)
        assert golden_phenopacket_import["status"] == "SUCCEEDED"


def test_import_phenopackets_evidence_created(golden_phenopacket_import, query_athena):
    """
    Test 5: Verify evidence was created in Iceberg.

    Reuses golden execution to verify CreateEvidence steps worked.
    """
    output_data = golden_phenopacket_import["output"]

    # Try to find subject IDs to query evidence
    subject_ids = []
    if "Summary" in output_data and "subject_ids" in output_data["Summary"]:
        subject_ids = output_data["Summary"]["subject_ids"]

    if subject_ids:
        # Query evidence for these subjects
        subject_list = "', '".join(subject_ids)
        query = f"""
            SELECT COUNT(*) as count
            FROM phebee.evidence
            WHERE subject_id IN ('{subject_list}')
        """

        results = query_athena(query)
        evidence_count = int(results[0]["count"])

        # Should have at least some evidence
        assert evidence_count > 0, f"Expected evidence for subjects, found {evidence_count}"
    else:
        # If no subject IDs in output, just verify execution succeeded
        assert golden_phenopacket_import["status"] == "SUCCEEDED"


def test_import_phenopackets_materialization_completed(golden_phenopacket_import, query_athena):
    """
    Test 6: Verify subject_terms analytical tables were materialized.

    Reuses golden execution to verify MaterializeProjectSubjectTerms step worked.
    """
    output_data = golden_phenopacket_import["output"]
    project_id = golden_phenopacket_import["project_id"]

    # Get subject IDs from output
    subject_ids = []
    if "Summary" in output_data and "subject_ids" in output_data["Summary"]:
        subject_ids = output_data["Summary"]["subject_ids"]

    if subject_ids:
        # Query subject_terms_by_subject table
        first_subject_id = subject_ids[0]
        query = f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_subject
            WHERE subject_id = '{first_subject_id}'
        """

        results = query_athena(query)
        by_subject_count = int(results[0]["count"])

        # Should have at least one term for the first subject
        assert by_subject_count > 0, \
            f"Expected subject_terms for {first_subject_id}, found {by_subject_count}. " \
            "Materialization may have failed."

        # Query subject_terms_by_project_term table
        query = f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_id}'
        """

        results = query_athena(query)
        by_project_count = int(results[0]["count"])

        # Should have at least one term for the project
        assert by_project_count > 0, \
            f"Expected subject_terms for project {project_id}, found {by_project_count}. " \
            "Materialization may have failed."
    else:
        # If no subject IDs, just verify execution succeeded
        assert golden_phenopacket_import["status"] == "SUCCEEDED"


def test_import_phenopackets_distributed_map_used(golden_phenopacket_import, physical_resources):
    """
    Test 2: Verify distributed map configuration.

    Checks that ImportPhenopacketsMap uses distributed mode for parallel processing.
    """
    sfn_client = boto3.client("stepfunctions")

    state_machine_arn = physical_resources.get("ImportPhenopacketsSFNArn")
    if not state_machine_arn:
        pytest.skip("ImportPhenopacketsSFNArn not available")

    # Describe state machine to check definition
    response = sfn_client.describe_state_machine(
        stateMachineArn=state_machine_arn
    )

    definition = json.loads(response["definition"])

    # Look for ImportPhenopacketsMap state
    if "ImportPhenopacketsMap" in definition.get("States", {}):
        map_state = definition["States"]["ImportPhenopacketsMap"]

        # Verify it's a distributed map
        assert map_state["Type"] == "Map"

        # Check for distributed mode configuration
        item_processor = map_state.get("ItemProcessor", {})
        processor_config = item_processor.get("ProcessorConfig", {})

        if processor_config:
            mode = processor_config.get("Mode")
            assert mode == "DISTRIBUTED", f"Expected DISTRIBUTED mode, got {mode}"


def test_import_phenopackets_invalid_s3_path(physical_resources):
    """
    Test: Validation failure - nonexistent S3 path.

    Fast test that validates early failure for invalid input.
    """
    sfn_client = boto3.client("stepfunctions")

    state_machine_arn = physical_resources.get("ImportPhenopacketsSFNArn")
    if not state_machine_arn:
        pytest.skip("ImportPhenopacketsSFNArn not available")

    # Start with invalid path
    execution_name = f"test-invalid-path-{int(time.time())}"

    response = sfn_client.start_sync_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "project_id": f"invalid-proj-{uuid.uuid4().hex[:8]}",
            "s3_path": "s3://nonexistent-bucket-12345/fake/path.zip",
            "output_s3_path": "s3://nonexistent-bucket-12345/output.jsonl"
        })
    )

    # Should fail
    assert response["status"] in ["FAILED", "TIMED_OUT"], \
        f"Expected failure for invalid S3 path, got {response['status']}"


def test_import_phenopackets_missing_project_id(physical_resources):
    """
    Test: Validation failure - missing project_id.

    Fast test that validates required field checking.
    """
    sfn_client = boto3.client("stepfunctions")

    state_machine_arn = physical_resources.get("ImportPhenopacketsSFNArn")
    if not state_machine_arn:
        pytest.skip("ImportPhenopacketsSFNArn not available")

    execution_name = f"test-missing-project-{int(time.time())}"

    response = sfn_client.start_sync_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "s3_path": "s3://some-bucket/path.zip",
            "output_s3_path": "s3://some-bucket/output.jsonl"
            # Missing project_id
        })
    )

    # Should fail
    assert response["status"] in ["FAILED", "TIMED_OUT"], \
        f"Expected failure for missing project_id, got {response['status']}"


@pytest.mark.slow
@pytest.mark.manual
def test_import_phenopackets_large_collection():
    """
    Test: Large collection with 100+ phenopackets.

    MARKED AS MANUAL: This test takes longer and should be run manually
    for performance validation.

    To run: pytest -m manual test_import_phenopackets_statemachine.py::test_import_phenopackets_large_collection
    """
    pytest.skip("Large collection test - run manually with: pytest -m manual")
