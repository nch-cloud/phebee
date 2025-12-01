import json
import time
import uuid
import pytest
import boto3
from phebee.utils.aws import get_client


@pytest.mark.skip(reason="Performance test - run manually with valid AWS credentials")
@pytest.mark.integration
@pytest.mark.performance
def test_large_scale_performance(physical_resources, test_project_id):
    """Test performance with large numbers of subjects and evidence records"""
    
    # Configuration - adjust these for different scale tests
    NUM_SUBJECTS = 500
    NOTES_PER_SUBJECT = 10000
    
    print(f"Performance test: {NUM_SUBJECTS} subjects, {NOTES_PER_SUBJECT} notes per subject")
    
    # Generate test data
    test_data = []
    subject_ids = []
    
    for i in range(NUM_SUBJECTS):
        subject_id = f"perf-subject-{i:06d}-{uuid.uuid4().hex[:8]}"
        subject_ids.append(subject_id)
        
        for j in range(NOTES_PER_SUBJECT):
            test_data.append({
                "project_id": test_project_id,
                "project_subject_id": subject_id,
                "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormal heart morphology
                "evidence": [{
                    "type": "clinical_note",
                    "clinical_note_id": f"note-{i:06d}-{j:03d}-{uuid.uuid4().hex[:8]}",
                    "encounter_id": f"encounter-{i:06d}-{j:03d}",
                    "evidence_creator_id": "perf-test-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Performance Test System",
                    "note_timestamp": "2024-01-15T10:30:00Z",
                    "note_type": "progress_note",
                    "span_start": 45,
                    "span_end": 58,
                    "contexts": {
                        "negated": 0,
                        "family": 0,
                        "hypothetical": 0
                    }
                }]
            })
    
    print(f"Generated {len(test_data)} evidence records")
    
    # Upload and process data via bulk import
    start_time = time.time()
    run_id = bulk_load_performance_data(test_data, physical_resources)
    load_time = time.time() - start_time
    
    print(f"Bulk load completed in {load_time:.2f} seconds")
    print(f"Rate: {len(test_data)/load_time:.2f} records/second")
    
    # Test API performance
    api_url = physical_resources["HttpApiUrl"]
    
    # Test 1: Query subjects by project
    start_time = time.time()
    response = get_client("requests").get(f"{api_url}/subjects", params={"project_id": test_project_id})
    query_time = time.time() - start_time
    
    assert response.status_code == 200
    subjects_data = response.json()
    
    print(f"Project query: {query_time:.3f}s for {len(subjects_data.get('subjects', []))} subjects")
    
    # Test 2: Query subjects by term
    start_time = time.time()
    response = get_client("requests").get(f"{api_url}/subjects", params={
        "project_id": test_project_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001627"
    })
    term_query_time = time.time() - start_time
    
    assert response.status_code == 200
    term_subjects = response.json()
    
    print(f"Term query: {term_query_time:.3f}s for {len(term_subjects.get('subjects', []))} subjects")
    
    # Test 3: Get individual subject details (sample 10 subjects)
    sample_subjects = subject_ids[:10]
    subject_times = []
    
    for subject_id in sample_subjects:
        start_time = time.time()
        response = get_client("requests").get(f"{api_url}/subjects/{test_project_id}/{subject_id}")
        subject_time = time.time() - start_time
        subject_times.append(subject_time)
        
        assert response.status_code == 200
    
    avg_subject_time = sum(subject_times) / len(subject_times)
    print(f"Individual subject queries: avg {avg_subject_time:.3f}s (sample of {len(sample_subjects)})")
    
    # Test 4: Pagination performance
    start_time = time.time()
    response = get_client("requests").get(f"{api_url}/subjects", params={
        "project_id": test_project_id,
        "limit": 100
    })
    pagination_time = time.time() - start_time
    
    assert response.status_code == 200
    paginated_data = response.json()
    
    print(f"Paginated query (limit=100): {pagination_time:.3f}s")
    
    # Performance assertions (adjust thresholds as needed)
    assert query_time < 30.0, f"Project query too slow: {query_time:.3f}s"
    assert term_query_time < 30.0, f"Term query too slow: {term_query_time:.3f}s"
    assert avg_subject_time < 5.0, f"Individual subject queries too slow: {avg_subject_time:.3f}s"
    assert pagination_time < 10.0, f"Pagination too slow: {pagination_time:.3f}s"
    
    print("All performance tests passed!")


def bulk_load_performance_data(test_data, physical_resources):
    """Load performance test data via bulk import using multiple smaller files"""
    run_id = f"perf-test-{uuid.uuid4().hex[:8]}"
    
    # Upload to S3 in chunks
    s3_bucket = physical_resources["PheBeeBucket"]
    s3_client = boto3.client('s3')
    
    # Split data into smaller files (10,000 records per file)
    chunk_size = 10000
    file_count = 0
    
    for i in range(0, len(test_data), chunk_size):
        chunk = test_data[i:i + chunk_size]
        jsonl_content = "\n".join(json.dumps(record) for record in chunk)
        
        input_key = f"performance-test/{run_id}/input_{file_count:04d}.jsonl"
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=input_key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/x-ndjson'
        )
        
        file_count += 1
        print(f"Uploaded file {file_count}: {len(chunk)} records")
    
    print(f"Total files uploaded: {file_count}")
    
    # Start Step Function
    stepfunctions_client = boto3.client('stepfunctions')
    state_machine_arn = physical_resources["BulkImportStateMachine"]
    
    input_path = f"s3://{s3_bucket}/performance-test/{run_id}/"
    
    stepfunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=run_id,
        input=json.dumps({
            "run_id": run_id,
            "input_path": input_path
        })
    )
    
    # Wait for completion (with timeout)
    max_wait = 1800  # 30 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        time.sleep(30)  # Check every 30 seconds
        
        try:
            execution = stepfunctions_client.describe_execution(
                executionArn=f"arn:aws:states:us-east-2:595936048629:execution:phebee-dev-BulkImportStateMachine:{run_id}"
            )
            
            if execution['status'] == 'SUCCEEDED':
                print(f"Bulk import completed successfully")
                return run_id
            elif execution['status'] == 'FAILED':
                raise Exception(f"Bulk import failed: {execution.get('error', 'Unknown error')}")
                
        except stepfunctions_client.exceptions.ExecutionDoesNotExist:
            continue
    
    raise Exception(f"Bulk import timed out after {max_wait} seconds")
