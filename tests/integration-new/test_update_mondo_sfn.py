"""
Integration tests for UpdateMondoSFN State Machine.

Tests the MONDO (Monarch Disease Ontology) update workflow which:
1. Downloads latest MONDO release from GitHub
2. Installs to Neptune via InstallRDFXMLSFN
3. Materializes ontology hierarchy to Iceberg tables
4. Fires update event to EventBridge
"""
import json
import pytest
import time
from phebee.utils.aws import get_client


@pytest.fixture(scope="module")
def update_mondo_sfn_arn(cloudformation_stack):
    """Get the UpdateMondoSFN state machine ARN from CloudFormation outputs."""
    cf_client = get_client("cloudformation")

    # Get stack outputs (physical_resources only has resources, not outputs)
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = response['Stacks'][0]['Outputs']

    # Find UpdateMondoSFNArn output
    for output in outputs:
        if output['OutputKey'] == 'UpdateMondoSFNArn':
            return output['OutputValue']

    pytest.fail(f"UpdateMondoSFNArn output not found in stack {cloudformation_stack}")


@pytest.fixture(scope="module")
def run_mondo_update_once(update_mondo_sfn_arn):
    """
    Run the MONDO update state machine once per test module.

    Returns the execution ARN for test verification.
    This fixture ensures MONDO data is available for all tests.
    """
    sfn_client = get_client("stepfunctions")

    # Start execution
    execution_name = f"test-mondo-update-{int(time.time())}"
    print(f"\n[MONDO UPDATE] Starting execution: {execution_name}")

    response = sfn_client.start_execution(
        stateMachineArn=update_mondo_sfn_arn,
        name=execution_name,
        input=json.dumps({"test": True})
    )

    execution_arn = response['executionArn']
    print(f"[MONDO UPDATE] Execution ARN: {execution_arn}")

    # Wait for completion (can take 15-45 minutes)
    print("[MONDO UPDATE] Waiting for execution to complete (this may take 15-45 minutes)...")
    start_time = time.time()

    while True:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        elapsed = int(time.time() - start_time)
        print(f"[MONDO UPDATE] Status: {status} (elapsed: {elapsed//60}m {elapsed%60}s)")

        if status == 'SUCCEEDED':
            duration = time.time() - start_time
            print(f"[MONDO UPDATE] Execution completed successfully in {duration//60:.0f}m {duration%60:.0f}s")
            break
        elif status == 'FAILED':
            error = execution.get('cause', 'Unknown error')
            pytest.fail(f"MONDO update failed: {error}")
        elif status in ['TIMED_OUT', 'ABORTED']:
            pytest.fail(f"MONDO update {status}")

        time.sleep(30)  # Check every 30 seconds

    return execution_arn


def test_update_mondo_execution_succeeds(run_mondo_update_once, update_mondo_sfn_arn):
    """Test that the UpdateMondoSFN state machine executes successfully."""
    sfn_client = get_client("stepfunctions")

    execution = sfn_client.describe_execution(executionArn=run_mondo_update_once)

    assert execution['status'] == 'SUCCEEDED'
    assert 'output' in execution

    # Parse output
    output = json.loads(execution['output'])
    print(f"\n[TEST] Execution output keys: {list(output.keys())}")

    # Output structure varies based on whether update was needed:
    # If downloaded=False: output contains 'mondo' with Payload.downloaded=False
    # If downloaded=True: output contains EventBridge response after full workflow
    assert 'mondo' in output or 'FailedEntryCount' in output, \
        "Output should contain either MONDO download info or EventBridge response"

    # If MONDO info present, check if download occurred
    if 'mondo' in output:
        mondo_payload = output['mondo'].get('Payload', {})
        print(f"[TEST] MONDO Payload: downloaded={mondo_payload.get('downloaded')}, version={mondo_payload.get('version')}")
        assert 'downloaded' in mondo_payload, "MONDO payload should indicate if download occurred"


def test_mondo_dynamodb_timestamp_updated(run_mondo_update_once, cloudformation_stack):
    """Test that DynamoDB SOURCE record is updated with installation timestamp."""
    dynamodb = get_client("dynamodb")
    cf_client = get_client("cloudformation")

    # Get DynamoDB table name from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    table_name = outputs['DynamoDBTableName']

    # Query for all SOURCE~mondo records
    response = dynamodb.query(
        TableName=table_name,
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={
            ':pk': {'S': 'SOURCE~mondo'}
        }
    )

    assert response['Count'] > 0, "DynamoDB should have SOURCE~mondo records"

    # Find the most recent record by InstallTimestamp
    items = response['Items']
    items_with_install = [item for item in items if 'InstallTimestamp' in item]
    assert len(items_with_install) > 0, "Should have at least one record with InstallTimestamp"

    # Sort by InstallTimestamp descending and get the most recent
    latest = sorted(items_with_install, key=lambda x: x['InstallTimestamp']['S'], reverse=True)[0]
    print(f"\n[TEST] Latest DynamoDB record: {latest}")

    # Verify required fields
    assert 'Version' in latest, "Should have Version"
    assert 'InstallTimestamp' in latest, "Should have InstallTimestamp"
    assert 'GraphName' in latest, "Should have GraphName"

    # Verify timestamp is recent (within last 24 hours)
    from datetime import datetime, timezone, timedelta
    timestamp_str = latest['InstallTimestamp']['S']
    # Parse timestamp and ensure it's timezone-aware (UTC)
    timestamp = datetime.fromisoformat(timestamp_str)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    assert (now - timestamp) < timedelta(hours=24), \
        f"InstallTimestamp should be recent, but was {timestamp}"


def test_mondo_hierarchy_table_populated(run_mondo_update_once, query_athena, cloudformation_stack):
    """Test that ontology_hierarchy Iceberg table is populated with MONDO terms."""
    cf_client = get_client("cloudformation")

    # Get table names from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    database_name = outputs['AthenaDatabase']
    table_name = outputs.get('AthenaOntologyHierarchyTable', 'ontology_hierarchy')

    # Query the table to verify it has MONDO data
    query = f"""
    SELECT COUNT(*) as term_count
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
    """

    results = query_athena(query)

    assert len(results) > 0, "No results returned from ontology_hierarchy table"
    term_count = int(results[0]['term_count'])

    # MONDO has ~25,000+ terms (as of 2024)
    assert term_count > 20000, \
        f"Expected more than 20,000 MONDO terms, but found {term_count}"

    print(f"\n[TEST] Ontology hierarchy table contains {term_count} MONDO terms")


def test_mondo_hierarchy_has_required_columns(run_mondo_update_once, query_athena, cloudformation_stack):
    """Test that hierarchy table has correct structure with required columns."""
    cf_client = get_client("cloudformation")

    # Get table names from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    database_name = outputs['AthenaDatabase']
    table_name = outputs.get('AthenaOntologyHierarchyTable', 'ontology_hierarchy')

    # Query a sample row to verify structure
    query = f"""
    SELECT *
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
    LIMIT 1
    """

    results = query_athena(query)

    assert len(results) > 0, "Should return at least one MONDO term"

    row = results[0]
    required_columns = ['ontology_source', 'term_id', 'term_label', 'depth']

    for col in required_columns:
        assert col in row, f"Missing required column: {col}"

    # Verify data types
    assert isinstance(row['term_id'], str)
    assert isinstance(row['term_label'], str)
    assert isinstance(row['depth'], (int, str))  # Athena may return as string

    print(f"\n[TEST] Sample row: {row}")


def test_mondo_root_term_exists_at_depth_zero(run_mondo_update_once, query_athena, cloudformation_stack):
    """Test that MONDO root term (MONDO:0000001 'disease or disorder') exists at depth 0."""
    cf_client = get_client("cloudformation")

    # Get table names from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    database_name = outputs['AthenaDatabase']
    table_name = outputs.get('AthenaOntologyHierarchyTable', 'ontology_hierarchy')

    query = f"""
    SELECT term_id, term_label, depth
    FROM {database_name}.{table_name}
    WHERE ontology_source = 'mondo'
      AND term_id = 'MONDO:0000001'
    """

    results = query_athena(query)

    assert len(results) == 1, "Should find exactly one root term MONDO:0000001"

    root = results[0]
    assert root['term_id'] == 'MONDO:0000001'
    assert int(root['depth']) == 0, f"Root term should have depth 0, got {root['depth']}"

    print(f"\n[TEST] Root term: {root['term_label']} (depth={root['depth']})")


def test_mondo_idempotency_second_run_skips(update_mondo_sfn_arn, run_mondo_update_once):
    """
    Test that running MONDO update again immediately skips download when version unchanged.

    This verifies the idempotency check works correctly.
    """
    sfn_client = get_client("stepfunctions")

    # Run second execution immediately after first
    execution_name = f"test-mondo-idempotent-{int(time.time())}"
    print(f"\n[TEST] Starting second execution to test idempotency: {execution_name}")

    response = sfn_client.start_execution(
        stateMachineArn=update_mondo_sfn_arn,
        name=execution_name,
        input=json.dumps({"test": False})
    )

    execution_arn = response['executionArn']
    start_time = time.time()

    # Wait for completion (should be much faster - under 2 minutes)
    while True:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        if status == 'SUCCEEDED':
            duration = time.time() - start_time
            print(f"[TEST] Second execution completed in {duration:.1f}s")

            # Verify quick completion (< 2 minutes indicates skip)
            assert duration < 120, \
                f"Idempotent execution should be fast (< 120s), but took {duration:.1f}s"

            # Verify it skipped installation by checking execution history
            history = sfn_client.get_execution_history(executionArn=execution_arn)

            # Look for Install MONDO state
            install_states = [
                e for e in history['events']
                if e.get('stateEnteredEventDetails', {}).get('name') == 'Install MONDO'
            ]

            assert len(install_states) == 0, \
                "Second execution should skip Install MONDO step when version unchanged"

            print(f"[TEST] ✅ Idempotency verified - second run skipped installation")
            break

        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            pytest.fail(f"Second execution {status}")

        if time.time() - start_time > 300:  # 5 minute timeout
            pytest.fail("Second execution took too long (> 5 minutes)")

        time.sleep(5)
