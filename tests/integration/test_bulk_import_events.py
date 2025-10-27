import json
import os
import uuid
import pytest
import boto3
import time
from phebee.utils.aws import get_client
from test_bulk_upload import bulk_upload_run


def invoke_lambda(name, payload):
    """Helper function to invoke a Lambda function."""
    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=name,
        Payload=json.dumps(payload).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    return json.loads(response["Payload"].read().decode("utf-8"))


@pytest.mark.integration
def test_bulk_import_success_event(physical_resources, test_project_id):
    """Test that BULK_IMPORT_SUCCESS event is fired when loads complete."""
    
    sqs_client = boto3.client("sqs")
    events_client = boto3.client("events")
    stack_name = os.environ.get("STACK_NAME", "phebee-dev-2")
    event_bus_name = f"phebee-bus-{stack_name}"
    
    # Create test SQS queue
    queue_name = f"phebee-test-events-{uuid.uuid4().hex[:8]}"
    queue_response = sqs_client.create_queue(QueueName=queue_name)
    queue_url = queue_response["QueueUrl"]
    queue_attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = queue_attrs["Attributes"]["QueueArn"]
    
    # Allow EventBridge to send to queue
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn
        }]
    }
    sqs_client.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)})
    
    # Create EventBridge rule to capture bulk import events
    rule_name = f"phebee-test-rule-{uuid.uuid4().hex[:8]}"
    events_client.put_rule(
        Name=rule_name,
        EventBusName=event_bus_name,
        EventPattern=json.dumps({
            "source": ["PheBee"],
            "detail-type": ["bulk_import_success", "bulk_import_failure"]
        }),
        State="ENABLED"
    )
    
    # Add queue as target
    events_client.put_targets(
        Rule=rule_name,
        EventBusName=event_bus_name,
        Targets=[{"Id": "1", "Arn": queue_arn}]
    )
    
    try:
        # Simple test payload
        test_payload = [{
            "project_id": test_project_id,
            "project_subject_id": f"subj-{uuid.uuid4()}",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
            "evidence_code": "IEA"
        }]
        
        # Run bulk upload (this will trigger the Step Function)
        run_id, domain_load_id, prov_load_id = bulk_upload_run([test_payload], physical_resources)
        
        # Wait for Step Function to complete and fire event
        time.sleep(75)
        
        # Check SQS queue for event
        messages = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5)
        
        assert "Messages" in messages, f"No events received in queue for run_id: {run_id}"
        
        # Find message with our run_id
        event_found = False
        for msg in messages["Messages"]:
            body = json.loads(msg["Body"])
            if body.get("detail", {}).get("run_id") == run_id:
                assert body["detail-type"] == "bulk_import_success", f"Wrong event type: {body['detail-type']}"
                event_found = True
                print(f"✓ BULK_IMPORT_SUCCESS event delivered to EventBridge for run_id: {run_id}")
                print(f"  Event detail: {body['detail']}")
                break
        
        assert event_found, f"Event with run_id {run_id} not found in queue messages"
        
    finally:
        # Cleanup
        events_client.remove_targets(Rule=rule_name, EventBusName=event_bus_name, Ids=["1"])
        events_client.delete_rule(Name=rule_name, EventBusName=event_bus_name)
        sqs_client.delete_queue(QueueUrl=queue_url)


def _verify_step_function_success(run_id, physical_resources):
    """Fallback verification via Step Function execution status."""
    sfn_client = boto3.client("stepfunctions")
    cf_client = boto3.client("cloudformation")
    stack_name = physical_resources["stack_name"]
    
    outputs = cf_client.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
    sfn_arn = next(o["OutputValue"] for o in outputs if o["OutputKey"] == "BulkLoadMonitorSFNArn")
    
    executions = sfn_client.list_executions(
        stateMachineArn=sfn_arn,
        statusFilter="SUCCEEDED",
        maxResults=10
    )
    
    our_execution = next((e for e in executions["executions"] if run_id in e.get("name", "")), None)
    assert our_execution is not None, f"No Step Function execution found for run_id: {run_id}"
    assert our_execution["status"] == "SUCCEEDED", f"Step Function failed: {our_execution['status']}"
    
    print(f"✓ Step Function completed successfully for run_id: {run_id}")


@pytest.mark.integration  
def test_bulk_import_failure_event(physical_resources, test_project_id):
    """Test that BULK_IMPORT_FAILURE event is fired when loads fail."""
    
    # This is harder to test reliably since we'd need to force Neptune load failures
    # For now, just test that the CheckBulkLoadStatusFunction works correctly
    
    check_fn = physical_resources["CheckBulkLoadStatusFunction"]
    
    # Test with invalid load IDs (should return FAILED)
    response = invoke_lambda(check_fn, {
        "run_id": "test-run",
        "domain_load_id": "invalid-load-id", 
        "prov_load_id": "invalid-load-id"
    })
    
    assert response["status"] == "FAILED"
    assert response["run_id"] == "test-run"
    assert "error" in response
    
    print("✓ CheckBulkLoadStatusFunction handles invalid load IDs correctly")


@pytest.mark.integration
def test_fire_bulk_event_function(physical_resources):
    """Test that FireBulkEventFunction works correctly."""
    
    fire_fn = physical_resources["FireBulkEventFunction"]
    
    # Test success event
    response = invoke_lambda(fire_fn, {
        "event_type": "BULK_IMPORT_SUCCESS",
        "run_id": "test-run-123",
        "domain_load_id": "domain-123",
        "prov_load_id": "prov-123"
    })
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "Event BULK_IMPORT_SUCCESS fired successfully" in body["message"]
    assert body["run_id"] == "test-run-123"
    
    # Test failure event  
    response = invoke_lambda(fire_fn, {
        "event_type": "BULK_IMPORT_FAILURE",
        "run_id": "test-run-456",
        "error": "Test error"
    })
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "Event BULK_IMPORT_FAILURE fired successfully" in body["message"]
    
    print("✓ FireBulkEventFunction works for both success and failure events")
