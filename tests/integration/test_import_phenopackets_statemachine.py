"""
Integration tests for ImportPhenopacketsSFN State Machine.

Tests the end-to-end phenopacket import workflow including ZIP parsing, subject creation,
and evidence import using distributed maps.

NOTE: These tests use a CONSOLIDATED APPROACH to minimize execution time:
- One module-scoped "golden" execution runs completely (~2-5 minutes)
- Multiple tests verify different aspects of that single execution
- Fast validation tests run separately
- State machine uses STANDARD type with async execution and polling

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
    - ZIP file with 100 phenopackets
    - Uploads to S3
    - Starts SYNCHRONOUS state machine execution
    - Waits for completion (~5-15 minutes)

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

    # Create project first
    print(f"[GOLDEN PHENOPACKET IMPORT] Creating test project: {test_project_id}")
    lambda_client = boto3.client("lambda")
    create_project_function = physical_resources.get("CreateProjectFunction")

    if create_project_function:
        response = lambda_client.invoke(
            FunctionName=create_project_function,
            Payload=json.dumps({
                "body": json.dumps({
                    "project_id": test_project_id,
                    "project_label": "Test Project for Phenopacket Import"
                })
            }).encode("utf-8")
        )
        result = json.loads(response["Payload"].read().decode("utf-8"))
        if result.get("statusCode") not in [200, 201]:
            pytest.fail(f"Failed to create test project: {result}")
        print(f"[GOLDEN PHENOPACKET IMPORT] Project created successfully")
    else:
        pytest.fail("CreateProjectFunction not found in physical_resources")

    # Create 100 phenopackets
    phenopackets = []
    for i in range(100):
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
    print("[GOLDEN PHENOPACKET IMPORT] Using async execution - will poll for completion...")

    start_time = time.time()

    try:
        # Start async execution (works with STANDARD state machines)
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps({
                "project_id": test_project_id,
                "s3_path": input_path,
                "output_s3_path": output_path
            })
        )

        execution_arn = response['executionArn']
        print(f"[GOLDEN PHENOPACKET IMPORT] Execution ARN: {execution_arn}")
        print("[GOLDEN PHENOPACKET IMPORT] Polling for completion...")

        # Poll for completion
        while True:
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            status = execution['status']

            elapsed = int(time.time() - start_time)
            if elapsed % 10 == 0 and elapsed > 0:  # Print every 10 seconds
                print(f"[GOLDEN PHENOPACKET IMPORT] Status: {status} (elapsed: {elapsed}s)")

            if status == 'SUCCEEDED':
                elapsed = int(time.time() - start_time)
                print(f"[GOLDEN PHENOPACKET IMPORT] Execution completed in {elapsed}s")
                print(f"[GOLDEN PHENOPACKET IMPORT] Status: {status}")

                # Parse output
                output_data = {}
                if execution.get('output'):
                    try:
                        output_data = json.loads(execution['output'])
                    except json.JSONDecodeError:
                        print(f"[GOLDEN PHENOPACKET IMPORT] Could not parse output: {execution['output']}")

                execution_details = {
                    "execution_arn": execution_arn,
                    "execution_name": execution_name,
                    "project_id": test_project_id,
                    "input_path": input_path,
                    "output_path": output_path,
                    "status": status,
                    "output": output_data,
                    "response": execution,
                    "expected_phenopackets": 100,
                    "s3_bucket": bucket_name,
                    "s3_prefix": s3_prefix,
                    "phenopackets": phenopackets
                }

                # Yield for tests, then cleanup
                yield execution_details

                # Cleanup: Remove project
                print(f"\n[GOLDEN PHENOPACKET IMPORT] Cleaning up project: {test_project_id}")
                remove_project_function = physical_resources.get("RemoveProjectFunction")
                if remove_project_function:
                    try:
                        lambda_client.invoke(
                            FunctionName=remove_project_function,
                            Payload=json.dumps({
                                "pathParameters": {"project_id": test_project_id}
                            }).encode("utf-8")
                        )
                        print(f"[GOLDEN PHENOPACKET IMPORT] Project removed successfully")
                    except Exception as cleanup_error:
                        print(f"[GOLDEN PHENOPACKET IMPORT] Cleanup warning: {cleanup_error}")

                return  # Exit after cleanup

            elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
                elapsed = int(time.time() - start_time)
                error = execution.get('cause', 'Unknown error')
                print(f"[GOLDEN PHENOPACKET IMPORT] Execution {status} after {elapsed}s: {error}")
                raise Exception(f"State machine execution {status}: {error}")

            time.sleep(2)  # Poll every 2 seconds

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
    Test 4: Verify subjects were created in DynamoDB with exact count.

    Reuses golden execution to verify CreateSubject step worked.
    Validates that exactly 100 subjects were created.
    """
    project_id = golden_phenopacket_import["project_id"]
    expected_count = golden_phenopacket_import["expected_phenopackets"]

    # Query DynamoDB directly for subject mappings
    dynamodb = boto3.resource("dynamodb")
    table_name = physical_resources["DynamoDBTableName"]
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={
            ":pk": f"PROJECT#{project_id}"
        }
    )

    # Extract subject_ids from response
    subject_ids = [item['subject_id'] for item in response.get('Items', []) if 'subject_id' in item]

    assert len(subject_ids) == expected_count, \
        f"Expected {expected_count} subjects in DynamoDB, found {len(subject_ids)}"


def test_import_phenopackets_subject_content_accuracy(golden_phenopacket_import, physical_resources):
    """
    Test 4b: Verify subject content accuracy in DynamoDB.

    Validates that:
    - All 100 expected project_subject_ids exist in mappings
    - Each phenopacket ID corresponds to a valid subject_id UUID
    - Bidirectional mappings exist (PROJECT->SUBJECT and SUBJECT->PROJECT)
    """
    project_id = golden_phenopacket_import["project_id"]
    expected_count = golden_phenopacket_import["expected_phenopackets"]

    # Query DynamoDB for all subject mappings
    dynamodb = boto3.resource("dynamodb")
    table_name = physical_resources["DynamoDBTableName"]
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={
            ":pk": f"PROJECT#{project_id}"
        }
    )

    # Build map of project_subject_id -> subject_id
    mappings = {}
    for item in response.get('Items', []):
        if 'subject_id' in item:
            # Extract project_subject_id from SK: "SUBJECT#{project_subject_id}"
            sk = item['SK']
            if sk.startswith('SUBJECT#'):
                project_subject_id = sk.replace('SUBJECT#', '')
                mappings[project_subject_id] = item['subject_id']

    # Verify we have exactly 100 mappings
    assert len(mappings) == expected_count, \
        f"Expected {expected_count} subject mappings, found {len(mappings)}"

    # Verify all expected phenopacket IDs are present
    expected_phenopacket_ids = [f"phenopacket-{i:03d}" for i in range(expected_count)]
    missing_ids = set(expected_phenopacket_ids) - set(mappings.keys())
    assert len(missing_ids) == 0, \
        f"Missing {len(missing_ids)} expected phenopacket IDs: {sorted(list(missing_ids))[:5]}"

    # Verify all subject_ids are valid UUIDs (36 chars with hyphens)
    import re
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    for project_subject_id, subject_id in list(mappings.items())[:5]:  # Check first 5
        assert uuid_pattern.match(subject_id), \
            f"subject_id for {project_subject_id} is not a valid UUID: {subject_id}"

    # Verify bidirectional mapping exists for first few subjects
    for project_subject_id, subject_id in list(mappings.items())[:3]:
        reverse_response = table.query(
            KeyConditionExpression="PK = :pk AND SK = :sk",
            ExpressionAttributeValues={
                ":pk": f"SUBJECT#{subject_id}",
                ":sk": f"PROJECT#{project_id}#SUBJECT#{project_subject_id}"
            }
        )
        assert reverse_response['Count'] == 1, \
            f"Missing reverse mapping for subject {subject_id} -> {project_subject_id}"


def test_import_phenopackets_evidence_created(golden_phenopacket_import, query_athena, physical_resources):
    """
    Test 5: Verify evidence was created in Iceberg with exact count.

    Reuses golden execution to verify CreateEvidence steps worked.
    Validates that exactly 100 evidence records were created (one per phenopacket).
    """
    project_id = golden_phenopacket_import["project_id"]
    expected_count = golden_phenopacket_import["expected_phenopackets"]

    # Query DynamoDB to get actual subject IDs for this project
    dynamodb = boto3.resource("dynamodb")
    table_name = physical_resources["DynamoDBTableName"]
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={
            ":pk": f"PROJECT#{project_id}"
        }
    )

    # Extract subject_ids from DynamoDB response
    subject_ids = [item['subject_id'] for item in response.get('Items', []) if 'subject_id' in item]

    assert len(subject_ids) == expected_count, \
        f"Expected {expected_count} subjects in DynamoDB, found {len(subject_ids)}"

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

        # Should have exactly one evidence record per phenopacket (each has 1 HPO term)
        assert evidence_count == expected_count, \
            f"Expected {expected_count} evidence records, found {evidence_count}"
    else:
        pytest.fail(f"No subjects found in DynamoDB for project {project_id}")


def test_import_phenopackets_content_accuracy(golden_phenopacket_import, query_athena, physical_resources, standard_hpo_terms):
    """
    Test 5b: Verify content accuracy of imported evidence.

    Validates that:
    - All evidence has the correct term IRI (seizure term)
    - Evidence creator is set to phenopacket importer
    - Qualifiers are empty (none specified in test phenopackets)
    """
    project_id = golden_phenopacket_import["project_id"]
    expected_term_iri = standard_hpo_terms["seizure"]
    expected_count = golden_phenopacket_import["expected_phenopackets"]

    # Query DynamoDB to get actual subject IDs for this project
    dynamodb = boto3.resource("dynamodb")
    table_name = physical_resources["DynamoDBTableName"]
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={
            ":pk": f"PROJECT#{project_id}"
        }
    )

    # Extract subject_ids from DynamoDB response
    subject_ids = [item['subject_id'] for item in response.get('Items', []) if 'subject_id' in item]

    if subject_ids:
        # Query evidence to verify term IRIs
        subject_list = "', '".join(subject_ids)
        query = f"""
            SELECT term_iri, COUNT(*) as count
            FROM phebee.evidence
            WHERE subject_id IN ('{subject_list}')
            GROUP BY term_iri
        """

        results = query_athena(query)

        # Should have exactly one distinct term (seizure)
        assert len(results) == 1, \
            f"Expected 1 distinct term IRI, found {len(results)}: {[r['term_iri'] for r in results]}"

        # Verify it's the correct term
        actual_term_iri = results[0]["term_iri"]
        actual_count = int(results[0]["count"])

        assert actual_term_iri == expected_term_iri, \
            f"Expected term IRI {expected_term_iri}, found {actual_term_iri}"

        assert actual_count == expected_count, \
            f"Expected {expected_count} evidence records for term {expected_term_iri}, found {actual_count}"

        # Query to verify creator and qualifiers
        query = f"""
            SELECT creator.creator_id as creator_id, cardinality(qualifiers) as qualifier_count
            FROM phebee.evidence
            WHERE subject_id IN ('{subject_list}')
            LIMIT 5
        """

        results = query_athena(query)

        # Check first few records
        for record in results:
            # Creator should be from phenopacket import
            creator_id = record.get("creator_id", "")
            assert "phenopacket" in creator_id.lower() or "importer" in creator_id.lower(), \
                f"Expected phenopacket importer as creator, found: {creator_id}"

            # Qualifiers should be empty (cardinality 0)
            # Handle None from cardinality() when qualifiers array is NULL
            qualifier_count = int(record.get("qualifier_count") or 0)
            assert qualifier_count == 0, \
                f"Expected 0 qualifiers, found {qualifier_count}"
    else:
        pytest.fail(f"No subjects found in DynamoDB for project {project_id}")


def test_import_phenopackets_materialization_completed(golden_phenopacket_import, query_athena, physical_resources):
    """
    Test 6: Verify subject_terms analytical tables were materialized with exact counts.

    Reuses golden execution to verify MaterializeProjectSubjectTerms step worked.
    Each subject should have exactly one term in both analytical tables.
    """
    project_id = golden_phenopacket_import["project_id"]
    expected_count = golden_phenopacket_import["expected_phenopackets"]

    # Query DynamoDB to get actual subject IDs for this project
    dynamodb = boto3.resource("dynamodb")
    table_name = physical_resources["DynamoDBTableName"]
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={
            ":pk": f"PROJECT#{project_id}"
        }
    )

    # Extract subject_ids from DynamoDB response
    subject_ids = [item['subject_id'] for item in response.get('Items', []) if 'subject_id' in item]

    if subject_ids:
        # Query subject_terms_by_subject table for all subjects
        subject_list = "', '".join(subject_ids)
        query = f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_subject
            WHERE subject_id IN ('{subject_list}')
        """

        results = query_athena(query)
        by_subject_count = int(results[0]["count"])

        # Should have exactly one term per subject (100 total)
        assert by_subject_count == expected_count, \
            f"Expected {expected_count} subject_terms in subject_terms_by_subject, found {by_subject_count}. " \
            "Materialization may have failed."

        # Query subject_terms_by_project_term table
        query = f"""
            SELECT COUNT(*) as count
            FROM phebee.subject_terms_by_project_term
            WHERE project_id = '{project_id}'
        """

        results = query_athena(query)
        by_project_count = int(results[0]["count"])

        # Should have exactly one term per subject (100 total)
        assert by_project_count == expected_count, \
            f"Expected {expected_count} subject_terms in subject_terms_by_project_term, found {by_project_count}. " \
            "Materialization may have failed."
    else:
        pytest.fail(f"No subjects found in DynamoDB for project {project_id}")


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

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "project_id": f"invalid-proj-{uuid.uuid4().hex[:8]}",
            "s3_path": "s3://nonexistent-bucket-12345/fake/path.zip",
            "output_s3_path": "s3://nonexistent-bucket-12345/output.jsonl"
        })
    )

    execution_arn = response['executionArn']

    # Poll for completion (should fail quickly)
    max_wait = 60  # 60 seconds max
    start_time = time.time()
    while time.time() - start_time < max_wait:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            break

        time.sleep(2)

    # Should fail
    assert status in ["FAILED", "TIMED_OUT"], \
        f"Expected failure for invalid S3 path, got {status}"


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

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "s3_path": "s3://some-bucket/path.zip",
            "output_s3_path": "s3://some-bucket/output.jsonl"
            # Missing project_id
        })
    )

    execution_arn = response['executionArn']

    # Poll for completion (should fail quickly)
    max_wait = 60  # 60 seconds max
    start_time = time.time()
    while time.time() - start_time < max_wait:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            break

        time.sleep(2)

    # Should fail
    assert status in ["FAILED", "TIMED_OUT"], \
        f"Expected failure for missing project_id, got {status}"
