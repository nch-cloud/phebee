import json
import uuid
import time
import pytest
import boto3
from general_utils import parse_iso8601

pytestmark = [pytest.mark.integration]

def test_emr_duplicate_subject_handling(physical_resources, test_project_id):
    """Test that running same data twice doesn't create duplicates in DynamoDB"""
    
    # Get resources
    s3_bucket = physical_resources.get("PheBeeBucket")
    dynamodb_table = physical_resources.get("DynamoDBTable")
    state_machine_arn = physical_resources.get("BulkImportStateMachine")
    
    if not all([s3_bucket, dynamodb_table, state_machine_arn]):
        pytest.skip("Required resources not found")
    
    # Create test data with multiple subjects
    run_id = f"duplicate-test-{uuid.uuid4().hex[:8]}"
    
    test_data = [
        {
            "project_id": test_project_id,
            "project_subject_id": "dup-subject-001",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-001",
                    "encounter_id": "encounter-001",
                    "evidence_creator_id": "nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP System",
                    "note_timestamp": "2024-01-15",
                    "note_type": "progress_note",
                    "provider_type": "physician",
                    "author_specialty": "cardiology",
                    "span_start": 45,
                    "span_end": 58,
                    "contexts": {
                        "negated": 0.0,
                        "family": 0.0,
                        "hypothetical": 0.0
                    }
                }
            ],
            "row_num": 1,
            "batch_id": 0
        },
        {
            "project_id": test_project_id,
            "project_subject_id": "dup-subject-002",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0002664",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-002",
                    "encounter_id": "encounter-002",
                    "evidence_creator_id": "nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP System",
                    "note_timestamp": "2024-01-16",
                    "note_type": "discharge_summary",
                    "provider_type": "physician",
                    "author_specialty": "oncology",
                    "span_start": 120,
                    "span_end": 135,
                    "contexts": {
                        "negated": 0.0,
                        "family": 0.0,
                        "hypothetical": 0.0
                    }
                }
            ],
            "row_num": 2,
            "batch_id": 0
        },
        {
            "project_id": test_project_id,
            "project_subject_id": "dup-subject-003",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
            "evidence": [],
            "row_num": 3,
            "batch_id": 0
        }
    ]
    
    # Convert to JSONL
    jsonl_content = "\n".join(json.dumps(record) for record in test_data)
    
    # Upload test data
    s3_client = boto3.client('s3')
    input_key = f"duplicate-test/{run_id}/jsonl/data.json"
    
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=input_key,
        Body=jsonl_content.encode('utf-8'),
        ContentType='application/x-ndjson'
    )
    
    try:
        # Get initial DynamoDB counts
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(dynamodb_table)
        
        initial_project_subjects = count_project_subjects(table, test_project_id)
        initial_total_subjects = count_total_subjects(table)
        
        print(f"Initial counts - Project subjects: {initial_project_subjects}, Total subjects: {initial_total_subjects}")
        
        # Run Step Function FIRST time
        stepfunctions_client = boto3.client('stepfunctions')
        
        execution_name_1 = f"duplicate-test-run1-{uuid.uuid4().hex[:8]}"
        
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name_1,
            input=json.dumps({
                "run_id": run_id + "-run1",
                "input_path": f"s3://{s3_bucket}/duplicate-test/{run_id}/jsonl"
            })
        )
        
        execution_arn_1 = response['executionArn']
        print(f"Started first execution: {execution_arn_1}")
        
        # Wait for first execution to complete
        wait_for_execution_completion(stepfunctions_client, execution_arn_1)
        
        # Get counts after first run
        after_first_project_subjects = count_project_subjects(table, test_project_id)
        after_first_total_subjects = count_total_subjects(table)
        
        print(f"After first run - Project subjects: {after_first_project_subjects}, Total subjects: {after_first_total_subjects}")
        
        # Verify first run created expected subjects
        expected_new_subjects = 3  # 3 unique project_subject_ids in test data
        assert after_first_project_subjects == initial_project_subjects + expected_new_subjects, \
            f"Expected {expected_new_subjects} new project subjects, got {after_first_project_subjects - initial_project_subjects}"
        
        # Get the actual subject mappings created
        first_run_mappings = get_subject_mappings(table, test_project_id, ["dup-subject-001", "dup-subject-002", "dup-subject-003"])
        
        print(f"First run mappings: {first_run_mappings}")
        assert len(first_run_mappings) == 3, f"Expected 3 subject mappings, got {len(first_run_mappings)}"
        
        # Run Step Function SECOND time with SAME data
        execution_name_2 = f"duplicate-test-run2-{uuid.uuid4().hex[:8]}"
        
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name_2,
            input=json.dumps({
                "run_id": run_id + "-run2",
                "input_path": f"s3://{s3_bucket}/duplicate-test/{run_id}/jsonl"
            })
        )
        
        execution_arn_2 = response['executionArn']
        print(f"Started second execution: {execution_arn_2}")
        
        # Wait for second execution to complete
        wait_for_execution_completion(stepfunctions_client, execution_arn_2)
        
        # Get counts after second run
        after_second_project_subjects = count_project_subjects(table, test_project_id)
        after_second_total_subjects = count_total_subjects(table)
        
        print(f"After second run - Project subjects: {after_second_project_subjects}, Total subjects: {after_second_total_subjects}")
        
        # CRITICAL TEST: Counts should be IDENTICAL after second run
        assert after_second_project_subjects == after_first_project_subjects, \
            f"Subject count changed on second run! First: {after_first_project_subjects}, Second: {after_second_project_subjects}"
        
        assert after_second_total_subjects == after_first_total_subjects, \
            f"Total subject count changed on second run! First: {after_first_total_subjects}, Second: {after_second_total_subjects}"
        
        # Verify same subject mappings exist
        second_run_mappings = get_subject_mappings(table, test_project_id, ["dup-subject-001", "dup-subject-002", "dup-subject-003"])
        
        print(f"Second run mappings: {second_run_mappings}")
        
        # Subject mappings should be IDENTICAL
        assert first_run_mappings == second_run_mappings, \
            f"Subject mappings changed between runs!\nFirst: {first_run_mappings}\nSecond: {second_run_mappings}"
        
        # Verify all expected subjects are still present
        for project_subject_id in ["dup-subject-001", "dup-subject-002", "dup-subject-003"]:
            assert project_subject_id in first_run_mappings, f"Missing subject mapping: {project_subject_id}"
            assert project_subject_id in second_run_mappings, f"Missing subject mapping after second run: {project_subject_id}"
        
        print("✅ Duplicate handling test passed:")
        print(f"  - No duplicate subjects created")
        print(f"  - All {len(first_run_mappings)} subject mappings preserved")
        print(f"  - Identical UUIDs maintained across runs")
        
    finally:
        # Cleanup
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=input_key)
            
            # Clean up any test files created
            cleanup_prefix = f"duplicate-test/{run_id}/"
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=cleanup_prefix):
                if 'Contents' in page:
                    objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=s3_bucket,
                            Delete={'Objects': objects_to_delete}
                        )
                        
            # Clean up run outputs
            for run_suffix in ["-run1", "-run2"]:
                run_cleanup_prefix = f"runs/{run_id}{run_suffix}/"
                for page in paginator.paginate(Bucket=s3_bucket, Prefix=run_cleanup_prefix):
                    if 'Contents' in page:
                        objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects_to_delete:
                            s3_client.delete_objects(
                                Bucket=s3_bucket,
                                Delete={'Objects': objects_to_delete}
                            )
                            
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")


def count_project_subjects(table, project_id):
    """Count subjects for a specific project"""
    response = table.query(
        KeyConditionExpression='PK = :pk',
        ExpressionAttributeValues={':pk': f'PROJECT#{project_id}'},
        Select='COUNT'
    )
    return response['Count']


def count_total_subjects(table):
    """Count total subjects in table (rough estimate using scan)"""
    response = table.scan(
        FilterExpression='begins_with(PK, :prefix)',
        ExpressionAttributeValues={':prefix': 'SUBJECT#'},
        Select='COUNT'
    )
    return response['Count']


def get_subject_mappings(table, project_id, project_subject_ids):
    """Get subject mappings for specific project subjects"""
    mappings = {}
    
    for project_subject_id in project_subject_ids:
        try:
            response = table.get_item(
                Key={
                    'PK': f'PROJECT#{project_id}',
                    'SK': f'SUBJECT#{project_subject_id}'
                }
            )
            if 'Item' in response:
                mappings[project_subject_id] = response['Item']['subject_id']
        except Exception as e:
            print(f"Error getting mapping for {project_subject_id}: {e}")
    
    return mappings


def wait_for_execution_completion(stepfunctions_client, execution_arn, timeout_seconds=1800):
    """Wait for Step Function execution to complete"""
    start_time = time.time()
    
    while True:
        execution_response = stepfunctions_client.describe_execution(
            executionArn=execution_arn
        )
        
        status = execution_response['status']
        
        if status == 'SUCCEEDED':
            print(f"Execution completed successfully: {execution_arn}")
            break
        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            # Get execution history for debugging
            history = stepfunctions_client.get_execution_history(
                executionArn=execution_arn,
                reverseOrder=True,
                maxResults=10
            )
            print("Execution failed. Recent events:")
            for event in history['events']:
                event_type = event['type']
                details = {}
                
                if 'taskFailedEventDetails' in event:
                    details = event['taskFailedEventDetails']
                elif 'executionFailedEventDetails' in event:
                    details = event['executionFailedEventDetails']
                
                print(f"  {event_type}: {details}")
            
            pytest.fail(f"Step Function execution failed with status: {status}")
        
        if time.time() - start_time > timeout_seconds:
            pytest.fail(f"Step Function execution did not complete within {timeout_seconds} seconds")
        
        time.sleep(30)  # Check every 30 seconds
