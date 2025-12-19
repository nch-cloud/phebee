import json
import uuid
import time
import pytest
import boto3
from general_utils import parse_iso8601

pytestmark = [pytest.mark.integration]

def test_bulk_import_stepfunction(physical_resources, test_project_id):
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
            "project_id": test_project_id,
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
        }
    ]
    
    test_data_2 = [
        {
            "project_id": test_project_id,
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
                    "note_timestamp": "2024-01-16",
                    "note_type": "discharge_summary",
                    "provider_type": "nurse_practitioner", 
                    "author_specialty": "internal_medicine",
                    "span_start": 199,
                    "span_end": 211,
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
                    "note_timestamp": "2024-01-16",
                    "note_type": "discharge_summary",
                    "provider_type": "physician_assistant",
                    "author_specialty": "emergency_medicine",
                    "span_start": 250,
                    "span_end": 260,
                    "contexts": {
                        "negated": 0.0,
                        "family": 1.0,
                        "hypothetical": 0.0
                    }
                }
            ],
            "row_num": 3,
            "batch_id": 0
        },
        {
            "project_id": test_project_id,
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
            "row_num": 4,
            "batch_id": 0
        }
    ]
    
    # Convert to JSONL format
    jsonl_content_1 = "\n".join(json.dumps(record) for record in test_data_1)
    jsonl_content_2 = "\n".join(json.dumps(record) for record in test_data_2)
    
    # Upload test data to S3 - multiple files in jsonl subdirectory
    s3_client = boto3.client('s3')
    
    # Upload first file
    input_key_1 = f"test-data/{run_id}/jsonl/batch1.json"
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=input_key_1,
        Body=jsonl_content_1.encode('utf-8'),
        ContentType='application/x-ndjson'
    )
    
    # Upload second file
    input_key_2 = f"test-data/{run_id}/jsonl/batch2.json"
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
                "input_path": f"s3://{s3_bucket}/test-data/{run_id}/jsonl"
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
        
        # Check that TTL files were created (updated from N-Quads to TTL)
        ttl_prefix = f"{run_id}/neptune/"
        
        # List objects with the TTL prefix to find generated files
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=ttl_prefix)
        ttl_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.ttl')]
        
        if not ttl_files:
            pytest.fail(f"No TTL files found with prefix: {ttl_prefix}")
        
        print(f"TTL files created successfully: {ttl_files}")
        
        # Find a subjects TTL file that should contain HP terms
        subjects_ttl_file = None
        for ttl_file in ttl_files:
            if '/subjects/' in ttl_file:
                subjects_ttl_file = ttl_file
                break
        
        if not subjects_ttl_file:
            subjects_ttl_file = ttl_files[0]  # Fallback to first file
            
        print(f"Using TTL file for validation: {subjects_ttl_file}")
        try:
            obj_response = s3_client.head_object(Bucket=s3_bucket, Key=subjects_ttl_file)
            if obj_response['ContentLength'] == 0:
                pytest.fail(f"TTL file is empty: {subjects_ttl_file}")
            
            # Check TTL content structure
            ttl_obj = s3_client.get_object(Bucket=s3_bucket, Key=subjects_ttl_file)
            ttl_content = ttl_obj['Body'].read().decode('utf-8')
            
            # Validate basic TTL structure
            print(f"Checking TTL file: {subjects_ttl_file}")
            print(f"TTL content length: {len(ttl_content)} characters")
            print(f"TTL content preview (first 1000 chars): {ttl_content[:1000]}")
            
            required_patterns = ['phebee:', 'HP_']  # HP terms appear as HP_ in full URIs
            for pattern in required_patterns:
                if pattern not in ttl_content:
                    pytest.fail(f"Missing required pattern in TTL: {pattern}")
            
            # Check for basic phenotype triples
            if 'hasTermLink' not in ttl_content:
                pytest.fail("No termlink relationships found in TTL")
            
            # Basic validation - check that we have subjects and termlinks
            subject_count = ttl_content.count('Subject')
            termlink_count = ttl_content.count('TermLink')
            
            print(f"TTL validation - Subjects: {subject_count}, TermLinks: {termlink_count}")
            
            if subject_count == 0:
                pytest.fail("No subjects found in TTL content")
            if termlink_count == 0:
                pytest.fail("No termlinks found in TTL content")
                
            print(f"TTL content validation passed for {subjects_ttl_file}")
                
            print(f"TTL content validation passed for {subjects_ttl_file}")
            
        except s3_client.exceptions.NoSuchKey:
            pytest.fail(f"TTL file not accessible: {subjects_ttl_file}")
        
        # Validate Iceberg data was written correctly
        print("Validating Iceberg data...")
        
        # Query Iceberg table using Athena to verify data
        athena_client = boto3.client('athena', region_name='us-east-2')
        
        # Query to count records for this run
        count_query = f"""
        SELECT COUNT(*) as record_count, 
               COUNT(DISTINCT subject_id) as subject_count,
               COUNT(DISTINCT term_iri) as term_count
        FROM phebee.evidence 
        WHERE run_id = '{run_id}'
        """
        
        query_response = athena_client.start_query_execution(
            QueryString=count_query,
            QueryExecutionContext={'Database': 'phebee'},
            ResultConfiguration={'OutputLocation': f's3://{s3_bucket}/athena-results/'},
            WorkGroup='primary'
        )
        
        query_execution_id = query_response['QueryExecutionId']
        
        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                pytest.fail(f"Athena query failed: {result['QueryExecution']['Status']}")
            time.sleep(2)
        
        # Get query results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        data_row = results['ResultSet']['Rows'][1]['Data']  # Skip header
        
        record_count = int(data_row[0]['VarCharValue'])
        subject_count = int(data_row[1]['VarCharValue'])
        term_count = int(data_row[2]['VarCharValue'])
        
        print(f"Iceberg validation - Records: {record_count}, Subjects: {subject_count}, Terms: {term_count}")
        
        # Basic validation - we expect some data
        if record_count == 0:
            pytest.fail("No evidence records found in Iceberg table")
        if subject_count == 0:
            pytest.fail("No subjects found in Iceberg table")
        if term_count == 0:
            pytest.fail("No terms found in Iceberg table")
            
        # Validate we have data from both test files
        # Expected: 2 subjects total (subject-001 appears in both files, subject-002 in second file)
        expected_subjects = 2  # Based on test_data_1 and test_data_2
        if subject_count != expected_subjects:
            print(f"Warning: Expected {expected_subjects} subjects, found {subject_count}")
            
        # Expected terms from test data
        expected_terms = ["http://purl.obolibrary.org/obo/HP_0001627", "http://purl.obolibrary.org/obo/HP_0002664"]
        
        # Query for specific terms to ensure both test files were processed
        terms_query = f"""
        SELECT DISTINCT term_iri
        FROM phebee.evidence 
        WHERE run_id = '{run_id}'
        ORDER BY term_iri
        """
        
        query_response = athena_client.start_query_execution(
            QueryString=terms_query,
            QueryExecutionContext={'Database': 'phebee'},
            ResultConfiguration={'OutputLocation': f's3://{s3_bucket}/athena-results/'},
            WorkGroup='primary'
        )
        
        query_execution_id = query_response['QueryExecutionId']
        
        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                pytest.fail(f"Terms validation query failed: {result['QueryExecution']['Status']}")
            time.sleep(2)
        
        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        found_terms = [row['Data'][0]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]  # Skip header
        
        print(f"Found terms in Iceberg: {found_terms}")
        
        # Validate expected terms are present
        for expected_term in expected_terms:
            if expected_term not in found_terms:
                pytest.fail(f"Expected term not found in Iceberg data: {expected_term}")
                
        print(f"Validated presence of expected terms from both test files")
            
        # Validate run_id is correct in all records for THIS run
        run_id_query = f"""
        SELECT COUNT(*) as total_records
        FROM phebee.evidence 
        WHERE run_id = '{run_id}'
        """
        
        query_response = athena_client.start_query_execution(
            QueryString=run_id_query,
            QueryExecutionContext={'Database': 'phebee'},
            ResultConfiguration={'OutputLocation': f's3://{s3_bucket}/athena-results/'},
            WorkGroup='primary'
        )
        
        query_execution_id = query_response['QueryExecutionId']
        
        # Wait for query completion
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                pytest.fail(f"Run ID validation query failed: {result['QueryExecution']['Status']}")
            time.sleep(2)
        
        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        our_record_count = int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        if our_record_count != record_count:
            pytest.fail(f"Mismatch in record counts: expected {record_count}, found {our_record_count}")
            
        print(f"Confirmed all {our_record_count} records have correct run_id")
        print("Iceberg data validation passed")
        
        # Verify Iceberg table has evidence data
        verify_iceberg_evidence(run_id)
        
        print("Bulk import test completed successfully")
        
    finally:
        # Cleanup test data
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=input_key)
        except Exception as e:
            print(f"Failed to cleanup test data: {e}")


def test_bulk_import_validation_failure(physical_resources, test_project_id):
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


def verify_iceberg_evidence(run_id):
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
    
    # Verify expected data - use the actual mapped subject IDs from the subject mapping
    # These are the UUIDs that get stored in the Iceberg table, not the original project_subject_ids
    expected_terms = {
        "http://purl.obolibrary.org/obo/HP_0001627",  # From both files
        "http://purl.obolibrary.org/obo/HP_0002664"   # From test_data_2
    }
    
    found_subjects = set()
    found_terms = set()
    
    for row in rows:
        data = [col.get('VarCharValue', '') for col in row['Data']]
        evidence_id, found_run_id, subject_id, term_iri, evidence_type, creator_id = data
        
        assert found_run_id == run_id, f"Expected run_id {run_id}, got {found_run_id}"
        assert evidence_type == "http://purl.obolibrary.org/obo/ECO_0006162", f"Expected ECO_0006162 (automated clinical note), got {evidence_type}"
        assert creator_id == "nlp-system-v1", f"Expected nlp-system-v1, got {creator_id}"
        
        found_subjects.add(subject_id)
        found_terms.add(term_iri)
    
    # Verify we found expected terms (subjects are dynamic UUIDs)
    missing_terms = expected_terms - found_terms
    if missing_terms:
        pytest.fail(f"Missing expected terms: {missing_terms}")
    
    # Verify we have the expected number of subjects (2 unique from test data)
    if len(found_subjects) != 2:
        pytest.fail(f"Expected 2 subjects, found {len(found_subjects)}: {found_subjects}")
    
    print(f"Verified {len(rows)} evidence records in Iceberg table")
    print(f"Found subjects: {found_subjects}")
    print(f"Found terms: {found_terms}")


@pytest.mark.integration
def test_ttl_generation_comprehensive(physical_resources, test_project_id):
    """Test comprehensive TTL generation with subjects, termlinks, terms, and qualifiers"""
    
    s3_bucket = physical_resources.get("PheBeeBucket")
    if not s3_bucket:
        pytest.skip("PheBeeBucket not found in physical resources")
    
    run_id = f"ttl-test-{uuid.uuid4().hex[:8]}"
    
    # Create test data with various qualifier combinations
    test_data = [
        {
            "project_id": test_project_id,
            "project_subject_id": "ttl-subject-001",
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
                    "author_specialty": "neurology",
                    "span_start": 45,
                    "span_end": 58,
                    "contexts": {
                        "negated": 0.0,
                        "family": 0.0,
                        "hypothetical": 0.0
                    }
                },
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-002",
                    "encounter_id": "encounter-002",
                    "evidence_creator_id": "nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP System",
                    "note_timestamp": "2024-01-16",
                    "note_type": "discharge_summary",
                    "provider_type": "nurse",
                    "author_specialty": "critical_care",
                    "span_start": 100,
                    "span_end": 115,
                    "contexts": {
                        "negated": 1.0,
                        "family": 0.0,
                        "hypothetical": 0.0
                    }
                },
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-003",
                    "encounter_id": "encounter-003",
                    "evidence_creator_id": "nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP System",
                    "note_timestamp": "2024-01-17",
                    "note_type": "consultation",
                    "provider_type": "specialist",
                    "author_specialty": "psychiatry",
                    "span_start": 200,
                    "span_end": 220,
                    "contexts": {
                        "negated": 0.0,
                        "family": 1.0,
                        "hypothetical": 0.0
                    }
                }
            ],
            "row_num": 1,
            "batch_id": 0
        },
        {
            "project_id": test_project_id,
            "project_subject_id": "ttl-subject-002",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0002664",
            "evidence": [
                {
                    "type": "clinical_note",
                    "clinical_note_id": "note-004",
                    "encounter_id": "encounter-004",
                    "evidence_creator_id": "nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "NLP System",
                    "note_timestamp": "2024-01-18",
                    "note_type": "progress_note",
                    "provider_type": "resident",
                    "author_specialty": "pediatrics",
                    "span_start": 75,
                    "span_end": 90,
                    "contexts": {
                        "negated": 0.0,
                        "family": 0.0,
                        "hypothetical": 1.0
                    }
                }
            ],
            "row_num": 2,
            "batch_id": 0
        }
    ]
    
    # Upload test data to jsonl subdirectory
    jsonl_content = "\n".join(json.dumps(record) for record in test_data)
    s3_client = boto3.client('s3')
    input_key = f"ttl-test-data/{run_id}/jsonl/data.json"
    
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=input_key,
        Body=jsonl_content.encode('utf-8'),
        ContentType='application/x-ndjson'
    )
    
    try:
        # Run Step Function
        stepfunctions_client = boto3.client('stepfunctions')
        state_machine_arn = physical_resources.get("BulkImportStateMachine")
        
        if not state_machine_arn:
            pytest.skip("BulkImportStateMachine not found")
        
        execution_name = f"ttl-test-{uuid.uuid4().hex[:8]}"
        
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps({
                "run_id": run_id,
                "input_path": f"s3://{s3_bucket}/ttl-test-data/{run_id}/jsonl"
            })
        )
        
        execution_arn = response['executionArn']
        
        # Wait for completion
        timeout_seconds = 1800
        start_time = time.time()
        
        while True:
            execution_response = stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
            
            status = execution_response['status']
            
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
                pytest.fail(f"Step Function execution failed: {status}")
            
            if time.time() - start_time > timeout_seconds:
                pytest.fail("Step Function execution timed out")
            
            time.sleep(30)
        
        # Validate TTL files
        ttl_prefix = f"{run_id}/neptune/"
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=ttl_prefix)
        ttl_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.ttl')]
        
        print(f"TTL prefix: s3://{s3_bucket}/{ttl_prefix}")
        print(f"TTL files found:")
        for ttl_file in ttl_files:
            print(f"  - s3://{s3_bucket}/{ttl_file}")
        
        assert ttl_files, f"No TTL files found with prefix: {ttl_prefix}"
        
        # Download and analyze TTL content
        all_ttl_content = ""
        for ttl_file in ttl_files:
            ttl_obj = s3_client.get_object(Bucket=s3_bucket, Key=ttl_file)
            ttl_content = ttl_obj['Body'].read().decode('utf-8')
            all_ttl_content += ttl_content + "\n"
        
        # Comprehensive TTL validation
        validate_ttl_structure(all_ttl_content, test_data)
        
        print("Comprehensive N-Quads validation passed")
        
    finally:
        # Cleanup
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=input_key)
        except:
            pass


def validate_ttl_structure(ttl_content: str, test_data: list):
    """Validate comprehensive TTL structure including subjects, termlinks, terms, and qualifiers"""
    
    # 1. Validate URI patterns (TTL may use prefixes or full URIs)
    required_uris = [
        'http://ods.nationwidechildrens.org/phebee/',
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'http://purl.obolibrary.org/obo/HP_'
    ]
    
    for uri in required_uris:
        assert uri in ttl_content, f"Missing required URI pattern: {uri}"
    
    lines = ttl_content.split('\n')
    
    # 2. Extract all subjects, termlinks, and terms from TTL
    subjects_in_ttl = set()
    termlinks_in_ttl = set()
    subject_termlink_pairs = set()
    termlink_term_pairs = set()
    
    for line in lines:
        line = line.strip()
        if 'phebee#Subject' in line or ':Subject' in line:
            # Extract subject URI from TTL format
            if '<' in line and '>' in line:
                subject = line.split()[0]
                subjects_in_ttl.add(subject)
        elif 'phebee#TermLink' in line or ':TermLink' in line:
            if '<' in line and '>' in line:
                termlink = line.split()[0]
                termlinks_in_ttl.add(termlink)
        elif 'phebee#hasTermLink' in line or ':hasTermLink' in line:
            parts = line.split()
            if len(parts) >= 3:
                subject = parts[0]
                termlink = parts[2].rstrip(' .;')
                subject_termlink_pairs.add((subject, termlink))
        elif 'phebee#hasTerm' in line or ':hasTerm' in line:
            parts = line.split()
            if len(parts) >= 3:
                termlink = parts[0]
                term = parts[2].rstrip(' .;')
                termlink_term_pairs.add((termlink, term))
    
    # 3. Build expected data from input
    expected_evidence_count = 0
    expected_subjects = set()
    expected_terms = set()
    expected_subject_term_pairs = set()
    
    for record in test_data:
        project_id = record['project_id']
        project_subject_id = record['project_subject_id']
        term_iri = record['term_iri']
        
        # Convert term IRI to expected format (TTL may use full URIs or prefixes)
        expected_term = term_iri  # Keep full URI format
        expected_terms.add(expected_term)
        
        for evidence in record['evidence']:
            expected_evidence_count += 1
            # Each evidence creates a unique subject-term pair (different qualifiers = different termlinks)
            expected_subject_term_pairs.add((project_subject_id, expected_term, str(evidence.get('contexts', {}))))
    
    # 4. Validate exact counts
    print(f"Expected evidence records: {expected_evidence_count}")
    print(f"Found termlinks: {len(termlinks_in_ttl)}")
    print(f"Expected unique subject-term-qualifier combinations: {len(expected_subject_term_pairs)}")
    
    # Should have one termlink per evidence record (each has unique qualifiers)
    assert len(termlinks_in_ttl) == expected_evidence_count, \
        f"Expected {expected_evidence_count} termlinks, found {len(termlinks_in_ttl)}"
    
    # 5. Validate all expected terms are present
    found_terms = set()
    for _, term in termlink_term_pairs:
        # Remove angle brackets from URIs for comparison
        clean_term = term.strip('<>')
        found_terms.add(clean_term)
    
    for expected_term in expected_terms:
        assert expected_term in found_terms, f"Expected term not found: {expected_term}"
    
    # 5.5. Validate qualifiers
    qualifier_assertions = []
    
    for line in lines:
        if 'hasQualifyingTerm' in line or ':hasQualifyingTerm' in line:
            qualifier_assertions.append(line.strip())
    
    # Count expected positive qualifiers from test data
    expected_positive_qualifiers = 0
    for record in test_data:
        for evidence in record['evidence']:
            contexts = evidence.get('contexts', {})
            for qualifier_type, qualifier_value in contexts.items():
                if qualifier_value in [1, 1.0, '1', True, 'true']:
                    expected_positive_qualifiers += 1
    
    print(f"Expected positive qualifiers: {expected_positive_qualifiers}")
    print(f"Found qualifier assertions: {len(qualifier_assertions)}")
    print(f"Qualifier assertions found: {qualifier_assertions}")
    
    # Should have qualifier assertions for positive qualifiers
    assert len(qualifier_assertions) == expected_positive_qualifiers, \
        f"Expected {expected_positive_qualifiers} qualifier assertions, found {len(qualifier_assertions)}"
    
    # 6. Validate subject count (unique project subjects)
    expected_unique_subjects = len(set((r['project_id'], r['project_subject_id']) for r in test_data))
    assert len(subjects_in_ttl) >= expected_unique_subjects, \
        f"Expected at least {expected_unique_subjects} subjects, found {len(subjects_in_ttl)}"
    
    # 7. Validate RDF structure integrity
    assert len(subject_termlink_pairs) == len(termlinks_in_ttl), \
        f"Mismatch: {len(subject_termlink_pairs)} subject-termlink pairs vs {len(termlinks_in_ttl)} termlinks"
    
    assert len(termlink_term_pairs) == len(termlinks_in_ttl), \
        f"Mismatch: {len(termlink_term_pairs)} termlink-term pairs vs {len(termlinks_in_ttl)} termlinks"
    
    # 8. Validate each termlink connects exactly one subject to one term
    termlinks_with_subjects = set(termlink for _, termlink in subject_termlink_pairs)
    termlinks_with_terms = set(termlink for termlink, _ in termlink_term_pairs)
    
    assert termlinks_with_subjects == termlinks_in_ttl, \
        "Not all termlinks have subject connections"
    
    assert termlinks_with_terms == termlinks_in_ttl, \
        "Not all termlinks have term connections"
    
    # 9. Validate no orphaned entities
    subjects_referenced = set(subject for subject, _ in subject_termlink_pairs)
    assert subjects_referenced == subjects_in_ttl, \
        "Some subjects are declared but not connected to termlinks"
    
    print(f"✅ Exact TTL validation passed:")
    print(f"  - Subjects: {len(subjects_in_ttl)}")
    print(f"  - TermLinks: {len(termlinks_in_ttl)}")
    print(f"  - Terms: {len(found_terms)}")
    print(f"  - Subject-TermLink connections: {len(subject_termlink_pairs)}")
    print(f"  - TermLink-Term connections: {len(termlink_term_pairs)}")
    print(f"  - Qualifier assertions: {len(qualifier_assertions)}")
    
    return True
