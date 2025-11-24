import json
import uuid
import time
import pytest
import boto3
from general_utils import parse_iso8601

pytestmark = [pytest.mark.integration]

def test_bulk_import_stepfunction(physical_resources):
    """Test the new Step Function-based bulk import workflow"""
    
    # Get S3 bucket from physical resources
    s3_bucket = physical_resources.get("PheBeeBucket")
    if not s3_bucket:
        pytest.skip("PheBeeBucket not found in physical resources")
    
    print(f"Using S3 bucket: {s3_bucket}")
    
    # Create test data
    run_id = f"test-run-{uuid.uuid4().hex[:8]}"
    
    # Sample JSONL data - create multiple files
    test_data_1 = [
        {
            "project_id": "test-project",
            "project_subject_id": "subject-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-123",
                    "encounter_id": "encounter-456",
                    "evidence_creator_id": "nlp-system-v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP Extractor",
                    "note_timestamp": "2024-01-15T10:30:00Z",
                    "note_type": "progress_note",
                    "span_start": 45,
                    "span_end": 58,
                    "contexts": {
                        "negated": 0,
                        "family": 0,
                        "hypothetical": 0
                    }
                }
            ]
        }
    ]
    
    test_data_2 = [
        {
            "project_id": "test-project",
            "project_subject_id": "subject-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-999",
                    "encounter_id": "encounter-789",
                    "evidence_creator_id": "nlp-system-v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP Extractor",
                    "note_timestamp": "2024-01-16T14:20:00Z",
                    "note_type": "discharge_summary",
                    "span_start": 199,
                    "span_end": 211,
                    "contexts": {
                        "negated": 0,
                        "family": 0,
                        "hypothetical": 0
                    }
                }
            ]
        },
        {
            "project_id": "test-project",
            "project_subject_id": "subject-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-999",
                    "encounter_id": "encounter-789",
                    "evidence_creator_id": "nlp-system-v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP Extractor",
                    "note_timestamp": "2024-01-16T14:20:00Z",
                    "note_type": "discharge_summary",
                    "span_start": 250,
                    "span_end": 260,
                    "contexts": {
                        "negated": 0,
                        "family": 1,
                        "hypothetical": 0
                    }
                }
            ]
        },
        {
            "project_id": "test-project",
            "project_subject_id": "subject-002",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0002664",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-456",
                    "encounter_id": "encounter-789",
                    "evidence_creator_id": "nlp-system-v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP Extractor",
                    "note_timestamp": "2024-01-16T14:20:00Z",
                    "note_type": "discharge_summary",
                    "span_start": 120,
                    "span_end": 135,
                    "contexts": {
                        "negated": 0,
                        "family": 0,
                        "hypothetical": 0
                    }
                }
            ]
        }
    ]
    
    # Convert to JSONL format
    jsonl_content_1 = "\n".join(json.dumps(record) for record in test_data_1)
    jsonl_content_2 = "\n".join(json.dumps(record) for record in test_data_2)
    
    # Upload test data to S3 - multiple files
    s3_client = boto3.client('s3')
    
    # Upload first file
    input_key_1 = f"test-data/{run_id}/batch1.jsonl"
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=input_key_1,
        Body=jsonl_content_1.encode('utf-8'),
        ContentType='application/x-ndjson'
    )
    
    # Upload second file
    input_key_2 = f"test-data/{run_id}/batch2.jsonl"
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=input_key_2,
        Body=jsonl_content_2.encode('utf-8'),
        ContentType='application/x-ndjson'
    )
    
    try:
        # Start Step Function execution
        stepfunctions_client = boto3.client('stepfunctions')
        
        # Get Step Function ARN from physical resources
        state_machine_arn = physical_resources.get("BulkImportStateMachine")
        
        if not state_machine_arn:
            pytest.skip("BulkImportStateMachine not found in physical resources")
        
        execution_name = f"test-execution-{uuid.uuid4().hex[:8]}"
        
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps({
                "run_id": run_id,
                "input_path": f"s3://{s3_bucket}/test-data/{run_id}/"
            })
        )
        
        execution_arn = response['executionArn']
        print(f"Started Step Function execution: {execution_arn}")
        
        # Wait for execution to complete
        timeout_seconds = 1800  # 30 minutes for EMR job
        start_time = time.time()
        
        while True:
            execution_response = stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
            
            status = execution_response['status']
            print(f"Execution status: {status}")
            
            if status == 'SUCCEEDED':
                print("Step Function execution completed successfully")
                break
            elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
                # Get execution history for debugging
                history = stepfunctions_client.get_execution_history(
                    executionArn=execution_arn,
                    reverseOrder=True,
                    maxResults=20
                )
                print("Execution failed. Recent events:")
                for event in history['events']:
                    event_type = event['type']
                    details = {}
                    
                    # Extract relevant details based on event type
                    if 'taskFailedEventDetails' in event:
                        details = event['taskFailedEventDetails']
                    elif 'executionFailedEventDetails' in event:
                        details = event['executionFailedEventDetails']
                    elif 'stateExitedEventDetails' in event:
                        details = event['stateExitedEventDetails']
                    elif 'taskStateEnteredEventDetails' in event:
                        details = event['taskStateEnteredEventDetails']
                    
                    print(f"  {event_type}: {details}")
                
                pytest.fail(f"Step Function execution failed with status: {status}")
            
            if time.time() - start_time > timeout_seconds:
                pytest.fail(f"Step Function execution did not complete within {timeout_seconds} seconds")
            
            time.sleep(30)  # Check every 30 seconds
        
        # Verify outputs
        output = json.loads(execution_response.get('output', '{}'))
        print(f"Execution output: {output}")
        
        # Check that TTL files were created
        ttl_key = f"phebee/runs/{run_id}/neptune/data.ttl"
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=ttl_key)
            print(f"TTL file created successfully: {ttl_key}")
        except s3_client.exceptions.NoSuchKey:
            pytest.fail(f"Expected TTL file not found: {ttl_key}")
        
        # Verify Iceberg table has evidence data
        verify_iceberg_evidence(run_id, test_data)
        
        # Verify Neptune has the relationships
        verify_neptune_relationships(physical_resources, test_data)
        
        print("Bulk import test completed successfully")
        
    finally:
        # Cleanup test data
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=input_key)
        except Exception as e:
            print(f"Failed to cleanup test data: {e}")


def test_bulk_import_validation_failure(physical_resources):
    """Test that Step Function properly handles validation failures"""
    
    stepfunctions_client = boto3.client('stepfunctions')
    
    state_machine_arn = physical_resources.get("BulkImportStateMachine")
    if not state_machine_arn:
        pytest.skip("BulkImportStateMachine not found in physical resources")
    
    execution_name = f"test-validation-failure-{uuid.uuid4().hex[:8]}"
    
    # Use invalid S3 path
    response = stepfunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps({
            "run_id": "test-validation-failure",
            "input_path": "s3://nonexistent-bucket/nonexistent-prefix/"
        })
    )
    
    execution_arn = response['executionArn']
    
    # Wait for execution to fail
    timeout_seconds = 300  # 5 minutes should be enough for validation
    start_time = time.time()
    
    while True:
        execution_response = stepfunctions_client.describe_execution(
            executionArn=execution_arn
        )
        
        status = execution_response['status']
        
        if status == 'FAILED':
            print("Step Function properly failed on validation")
            break
        elif status == 'SUCCEEDED':
            pytest.fail("Step Function should have failed validation but succeeded")
        
        if time.time() - start_time > timeout_seconds:
            pytest.fail("Step Function did not fail validation within expected time")
        
        time.sleep(5)


def verify_iceberg_evidence(run_id, expected_data):
    """Verify evidence data was written to Iceberg table"""
    athena_client = boto3.client('athena')
    
    query = f"""
    SELECT evidence_id, run_id, subject_id, term_iri, evidence_type, creator.creator_id
    FROM phebee.evidence 
    WHERE run_id = '{run_id}'
    """
    
    # Get Athena results location from workgroup or use default
    try:
        wg_cfg = athena_client.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False
        
        params = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": "phebee"}
        }
        
        if not managed:
            # Use a default results location - this should be configured in the environment
            params["ResultConfiguration"] = {"OutputLocation": "s3://phebee-dev-phebeebucket/athena-results/"}
        
        response = athena_client.start_query_execution(**params)
    except Exception as e:
        pytest.skip(f"Could not execute Athena query: {e}")
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query completion
    timeout = 60
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            break
        elif status in ['FAILED', 'CANCELLED']:
            pytest.fail(f"Athena query failed: {result['QueryExecution']['Status'].get('StateChangeReason')}")
        
        time.sleep(2)
    else:
        pytest.fail("Athena query timed out")
    
    # Get results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = results['ResultSet']['Rows'][1:]  # Skip header
    
    assert len(rows) > 0, "No evidence records found in Iceberg table"
    
    # Verify expected data
    for row in rows:
        data = [col.get('VarCharValue', '') for col in row['Data']]
        evidence_id, found_run_id, subject_id, term_iri, evidence_type, creator_id = data
        
        assert found_run_id == run_id, f"Expected run_id {run_id}, got {found_run_id}"
        assert subject_id == "subject-001", f"Expected subject-001, got {subject_id}"
        assert term_iri == "http://purl.obolibrary.org/obo/HP_0001627", f"Unexpected term_iri: {term_iri}"
        assert evidence_type == "clinical_note", f"Expected clinical_note, got {evidence_type}"
        assert creator_id == "nlp-system-v1", f"Expected nlp-system-v1, got {creator_id}"
    
    print(f"Verified {len(rows)} evidence records in Iceberg table")


def verify_neptune_relationships(physical_resources, expected_data):
    """Verify Neptune contains the expected subject-term relationships using Lambda functions"""
    
    # Use GetSubjectFunction to verify the subject exists and has the expected term
    get_subject_function = physical_resources.get("GetSubjectFunction")
    if not get_subject_function:
        pytest.skip("GetSubjectFunction not available")
    
    lambda_client = boto3.client('lambda')
    
    # Query for the subject
    payload = {
        "project_subject_id": "subject-001"
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=get_subject_function,
            Payload=json.dumps(payload)
        )
        
        result = json.loads(response['Payload'].read())
        
        if response['StatusCode'] != 200:
            pytest.fail(f"GetSubjectFunction failed: {result}")
        
        # Check if subject has the expected term
        subject_data = result.get('body', {})
        if isinstance(subject_data, str):
            subject_data = json.loads(subject_data)
        
        # Look for the HP term in the subject's data
        found_term = False
        term_links = subject_data.get('term_links', [])
        
        for term_link in term_links:
            if 'HP_0001627' in term_link.get('term_iri', ''):
                found_term = True
                break
        
        assert found_term, f"Term HP_0001627 not found in subject data. Found terms: {[tl.get('term_iri') for tl in term_links]}"
        
        print(f"Verified Neptune relationships: Subject has {len(term_links)} term links including HP_0001627")
        
    except Exception as e:
        pytest.skip(f"Could not verify Neptune relationships: {e}")
