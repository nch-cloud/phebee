"""
Integration tests for UpdateECOSFN State Machine.

Tests the ECO (Evidence & Conclusion Ontology) update workflow which:
1. Downloads latest ECO release from GitHub
2. Installs to Neptune via InstallRDFXMLSFN
3. Materializes ontology hierarchy to Iceberg tables
4. Fires update event to EventBridge
"""
import json
import pytest
import time
from phebee.utils.aws import get_client


@pytest.fixture(scope="module")
def update_eco_sfn_arn(cloudformation_stack):
    """Get the UpdateECOSFN state machine ARN from CloudFormation outputs."""
    cf_client = get_client("cloudformation")

    # Get stack outputs (physical_resources only has resources, not outputs)
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = response['Stacks'][0]['Outputs']

    # Find UpdateECOSFNArn output
    for output in outputs:
        if output['OutputKey'] == 'UpdateECOSFNArn':
            return output['OutputValue']

    pytest.fail(f"UpdateECOSFNArn output not found in stack {cloudformation_stack}")


@pytest.fixture(scope="module")
def run_eco_update_once(update_eco_sfn_arn):
    """
    Run the ECO update state machine once per test module.

    Returns the execution ARN for test verification.
    This fixture ensures ECO data is available for all tests.
    """
    sfn_client = get_client("stepfunctions")

    # Start execution
    execution_name = f"test-eco-update-{int(time.time())}"
    print(f"\n[ECO UPDATE] Starting execution: {execution_name}")

    response = sfn_client.start_execution(
        stateMachineArn=update_eco_sfn_arn,
        name=execution_name,
        input=json.dumps({"test": True})
    )

    execution_arn = response['executionArn']
    print(f"[ECO UPDATE] Execution ARN: {execution_arn}")

    # Wait for completion (ECO is smaller, typically 5-15 minutes)
    print("[ECO UPDATE] Waiting for execution to complete (this may take 5-15 minutes)...")
    start_time = time.time()

    while True:
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        status = execution['status']

        elapsed = int(time.time() - start_time)
        print(f"[ECO UPDATE] Status: {status} (elapsed: {elapsed//60}m {elapsed%60}s)")

        if status == 'SUCCEEDED':
            duration = time.time() - start_time
            print(f"[ECO UPDATE] Execution completed successfully in {duration//60:.0f}m {duration%60:.0f}s")
            break
        elif status == 'FAILED':
            error = execution.get('cause', 'Unknown error')
            pytest.fail(f"ECO update failed: {error}")
        elif status in ['TIMED_OUT', 'ABORTED']:
            pytest.fail(f"ECO update {status}")

        time.sleep(30)  # Check every 30 seconds

    return execution_arn


def test_update_eco_execution_succeeds(run_eco_update_once, update_eco_sfn_arn):
    """Test that the UpdateECOSFN state machine executes successfully."""
    sfn_client = get_client("stepfunctions")

    execution = sfn_client.describe_execution(executionArn=run_eco_update_once)

    assert execution['status'] == 'SUCCEEDED'
    assert 'output' in execution

    # Parse output
    output = json.loads(execution['output'])
    print(f"\n[TEST] Execution output keys: {list(output.keys())}")

    # Output structure varies based on whether update was needed:
    # If downloaded=False: output contains 'eco' with Payload.downloaded=False
    # If downloaded=True: output contains EventBridge response after full workflow
    assert 'eco' in output or 'FailedEntryCount' in output, \
        "Output should contain either ECO download info or EventBridge response"

    # If ECO info present, check if download occurred
    if 'eco' in output:
        eco_payload = output['eco'].get('Payload', {})
        print(f"[TEST] ECO Payload: downloaded={eco_payload.get('downloaded')}, version={eco_payload.get('version')}")
        assert 'downloaded' in eco_payload, "ECO payload should indicate if download occurred"


def test_eco_dynamodb_timestamp_updated(run_eco_update_once, cloudformation_stack):
    """Test that DynamoDB SOURCE record is updated with installation timestamp."""
    dynamodb = get_client("dynamodb")
    cf_client = get_client("cloudformation")

    # Get DynamoDB table name from stack outputs
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    table_name = outputs['DynamoDBTableName']

    # Query for all SOURCE~eco records
    response = dynamodb.query(
        TableName=table_name,
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={
            ':pk': {'S': 'SOURCE~eco'}
        }
    )

    assert response['Count'] > 0, "DynamoDB should have SOURCE~eco records"

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


def test_eco_idempotency_second_run_skips(update_eco_sfn_arn, run_eco_update_once):
    """
    Test that running ECO update again immediately skips download when version unchanged.

    This verifies the idempotency check works correctly.
    """
    sfn_client = get_client("stepfunctions")

    # Run second execution immediately after first
    execution_name = f"test-eco-idempotent-{int(time.time())}"
    print(f"\n[TEST] Starting second execution to test idempotency: {execution_name}")

    response = sfn_client.start_execution(
        stateMachineArn=update_eco_sfn_arn,
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

            # Look for Install ECO state
            install_states = [
                e for e in history['events']
                if e.get('stateEnteredEventDetails', {}).get('name') == 'Install ECO'
            ]

            assert len(install_states) == 0, \
                "Second execution should skip Install ECO step when version unchanged"

            print(f"[TEST] ✅ Idempotency verified - second run skipped installation")
            break

        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            pytest.fail(f"Second execution {status}")

        if time.time() - start_time > 300:  # 5 minute timeout
            pytest.fail("Second execution took too long (> 5 minutes)")

        time.sleep(5)
