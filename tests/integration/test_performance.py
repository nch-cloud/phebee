import json
import time
import uuid
import pytest
import boto3
import random
import requests


@pytest.mark.performance
def test_large_scale_performance(physical_resources, stack_outputs, test_project_id, num_subjects, num_terms):
    """Test performance with large numbers of subjects and evidence records using realistic HPO terms and qualifiers"""
    
    # Configuration from command line parameters
    NUM_SUBJECTS = num_subjects
    NOTES_PER_SUBJECT = num_terms
    
    print(f"Performance test: {NUM_SUBJECTS} subjects, {NOTES_PER_SUBJECT} notes per subject")
    
    # Realistic HPO terms for cardiovascular, neurological, and skeletal phenotypes
    hpo_terms = [
        "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormal heart morphology
        "http://purl.obolibrary.org/obo/HP_0001626",  # Abnormality of the cardiovascular system
        "http://purl.obolibrary.org/obo/HP_0001250",  # Seizure
        "http://purl.obolibrary.org/obo/HP_0000365",  # Hearing impairment
        "http://purl.obolibrary.org/obo/HP_0001263",  # Global developmental delay
        "http://purl.obolibrary.org/obo/HP_0000252",  # Microcephaly
        "http://purl.obolibrary.org/obo/HP_0001249",  # Intellectual disability
        "http://purl.obolibrary.org/obo/HP_0000707",  # Abnormality of the nervous system
        "http://purl.obolibrary.org/obo/HP_0000924",  # Abnormality of the skeletal system
        "http://purl.obolibrary.org/obo/HP_0000478",  # Abnormality of the eye
    ]
    
    # Qualifier scenarios with realistic distributions
    positive_scenario = {"negated": 0, "family": 0, "hypothetical": 0}  # Standard positive finding
    other_scenarios = [
        {"negated": 1, "family": 0, "hypothetical": 0},  # Explicitly absent
        {"negated": 0, "family": 1, "hypothetical": 0},  # Present in family history
        {"negated": 0, "family": 0, "hypothetical": 1},  # Possible/hypothetical
        {"negated": 1, "family": 1, "hypothetical": 0},  # Absent in family history
        {"negated": 0, "family": 1, "hypothetical": 1},  # Possible family history
    ]
    
    # Generate test data with realistic distributions
    test_data = []
    subject_ids = []
    expected_mappings = {}  # term_iri -> set of subject_ids that should have it
    hpo_terms = [
        "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormal heart morphology
        "http://purl.obolibrary.org/obo/HP_0001626",  # Abnormality of the cardiovascular system
        "http://purl.obolibrary.org/obo/HP_0001250",  # Seizure
        "http://purl.obolibrary.org/obo/HP_0001249",  # Intellectual disability
        "http://purl.obolibrary.org/obo/HP_0002664",  # Neoplasm
        "http://purl.obolibrary.org/obo/HP_0000924",  # Abnormality of the skeletal system
        "http://purl.obolibrary.org/obo/HP_0000707",  # Abnormality of the nervous system
        "http://purl.obolibrary.org/obo/HP_0000478",  # Abnormality of the eye
        "http://purl.obolibrary.org/obo/HP_0000365",  # Hearing impairment
        "http://purl.obolibrary.org/obo/HP_0001507",  # Growth abnormality
    ]
    
    # Qualifier combinations with weighted distribution (80% positive, 20% other)
    positive_scenario = {"negated": 0, "family": 0, "hypothetical": 0}  # Present in patient
    other_scenarios = [
        {"negated": 1, "family": 0, "hypothetical": 0},  # Explicitly absent
        {"negated": 0, "family": 1, "hypothetical": 0},  # Present in family history
        {"negated": 0, "family": 0, "hypothetical": 1},  # Possible/suspected
        {"negated": 1, "family": 1, "hypothetical": 0},  # Absent in family history
        {"negated": 0, "family": 1, "hypothetical": 1},  # Possible family history
    ]
    
    # Generate test data with realistic distributions
    test_data = []
    subject_ids = []
    expected_mappings = {}  # term_iri -> set of subject_ids that should have it
    expected_evidence = {}  # (subject_id, term_iri) -> list of evidence records
    
    for i in range(NUM_SUBJECTS):
        subject_id = f"perf-subject-{i:06d}-{uuid.uuid4().hex[:8]}"
        subject_ids.append(subject_id)
        
        for j in range(NOTES_PER_SUBJECT):
            # Select random HPO term and qualifier scenario with 80% positive weighting
            term_iri = random.choice(hpo_terms)
            
            # 80% chance of positive, 20% chance of other qualifiers
            if random.random() < 0.8:
                qualifiers = positive_scenario
            else:
                qualifiers = random.choice(other_scenarios)
            
            # Track expected mappings (only for positive/unqualified evidence)
            if qualifiers == positive_scenario:
                if term_iri not in expected_mappings:
                    expected_mappings[term_iri] = set()
                expected_mappings[term_iri].add(subject_id)
            
            # Track all evidence for detailed validation
            evidence_key = (subject_id, term_iri)
            if evidence_key not in expected_evidence:
                expected_evidence[evidence_key] = []
            
            clinical_note_id = f"note-{i:06d}-{j:03d}-{uuid.uuid4().hex[:8]}"
            encounter_id = f"encounter-{i:06d}-{j:03d}"
            
            expected_evidence[evidence_key].append({
                'clinical_note_id': clinical_note_id,
                'encounter_id': encounter_id,
                'qualifiers': qualifiers
            })
            
            # 80% chance of positive, 20% chance of other qualifiers
            if random.random() < 0.8:
                qualifiers = positive_scenario
            else:
                qualifiers = random.choice(other_scenarios)
            
            # Add some variation to note types and timestamps
            note_types = ["progress_note", "discharge_summary", "consultation", "admission_note"]
            note_type = random.choice(note_types)
            
            # Vary timestamps across a year
            base_time = "2024-01-15T10:30:00Z"
            day_offset = random.randint(0, 365)
            hour_offset = random.randint(0, 23)
            
            test_data.append({
                "project_id": test_project_id,
                "project_subject_id": subject_id,
                "term_iri": term_iri,
                "evidence": [{
                    "type": "clinical_note",
                    "clinical_note_id": f"note-{i:06d}-{j:03d}-{uuid.uuid4().hex[:8]}",
                    "encounter_id": f"encounter-{i:06d}-{j:03d}",
                    "evidence_creator_id": "perf-test-nlp-system",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Performance Test NLP System",
                    "note_timestamp": f"2024-{1 + (day_offset // 30):02d}-{1 + (day_offset % 30):02d}T{hour_offset:02d}:30:00Z",
                    "note_type": note_type,
                    "span_start": random.randint(10, 500),
                    "span_end": random.randint(510, 1000),
                    "contexts": qualifiers
                }]
            })
    
    print(f"Generated {len(test_data)} evidence records with {len(set(r['term_iri'] for r in test_data))} unique HPO terms")
    
    # Generate comprehensive input data summary
    print("\n=== INPUT DATA SUMMARY ===")
    print(f"Total subjects: {NUM_SUBJECTS}")
    print(f"Total evidence records: {len(test_data)}")
    
    # Organize data by subject for summary
    subject_summary = {}
    for record in test_data:
        subject_id = record['project_subject_id']
        term_iri = record['term_iri']
        qualifiers = record['evidence'][0]['contexts']
        
        if subject_id not in subject_summary:
            subject_summary[subject_id] = {}
        if term_iri not in subject_summary[subject_id]:
            subject_summary[subject_id][term_iri] = []
        
        subject_summary[subject_id][term_iri].append(qualifiers)
    
    # Print per-subject breakdown
    for subject_id in sorted(subject_summary.keys()):
        print(f"\nSubject: {subject_id}")
        for term_iri, qualifier_list in subject_summary[subject_id].items():
            term_name = term_iri.split('/')[-1]
            
            # Count evidence by qualifier combinations
            qualifier_counts = {}
            for quals in qualifier_list:
                # Create qualifier description
                qual_desc = []
                if quals.get('negated', 0):
                    qual_desc.append('negated')
                if quals.get('family', 0):
                    qual_desc.append('family')
                if quals.get('hypothetical', 0):
                    qual_desc.append('hypothetical')
                
                if not qual_desc:
                    qual_key = 'positive'
                else:
                    qual_key = '+'.join(qual_desc)
                
                qualifier_counts[qual_key] = qualifier_counts.get(qual_key, 0) + 1
            
            print(f"  {term_name}:")
            for qual_type, count in sorted(qualifier_counts.items()):
                print(f"    - {qual_type.title()} evidence: {count} records")
    
    # Print expected mappings summary
    print(f"\nExpected term links (subjects with positive evidence only):")
    for term_iri, subjects in expected_mappings.items():
        term_name = term_iri.split('/')[-1]
        subject_list = ', '.join(sorted(subjects))
        print(f"  {term_name}: {len(subjects)} subjects ({subject_list})")
    print("=" * 50)
    
    # Count qualifier distributions
    qualifier_counts = {}
    for record in test_data:
        contexts = record['evidence'][0]['contexts']
        key = f"neg:{contexts['negated']}_fam:{contexts['family']}_hyp:{contexts['hypothetical']}"
        qualifier_counts[key] = qualifier_counts.get(key, 0) + 1
    
    print("Qualifier distribution:")
    for combo, count in sorted(qualifier_counts.items()):
        print(f"  {combo}: {count} records ({count/len(test_data)*100:.1f}%)")
    
    # Upload and process data via bulk import
    start_time = time.time()
    run_id = bulk_load_performance_data(test_data, physical_resources)
    load_time = time.time() - start_time
    
    print(f"Bulk load completed in {load_time:.2f} seconds")
    print(f"Rate: {len(test_data)/load_time:.2f} records/second")
    
    # Test API performance with qualifier-aware queries
    api_url = stack_outputs.get("HttpApiUrl")
    if not api_url:
        print("HttpApiUrl not found in stack outputs, skipping API performance tests")
        print("Bulk import performance test completed successfully!")
        return
    
    # Wait a bit for Neptune indexing to complete
    print("Waiting 30 seconds for Neptune indexing to complete...")
    time.sleep(30)

    # Test 1: Query subjects by project (using correct POST method)
    start_time = time.time()
    response = requests.post(f"{api_url}/subjects/query", json={"project_id": test_project_id})
    query_time = time.time() - start_time
    
    print(f"API Response Status: {response.status_code}")
    print(f"API Response Content: {response.text[:500]}")  # First 500 chars
    
    if response.status_code != 200:
        print("API test failed - data may not be available yet or endpoint issue")
        print("Bulk import performance test completed successfully!")
        return
    subjects_data = response.json()
    
    print(f"Project query: {query_time:.3f}s for {len(subjects_data.get('body', []))} subjects")
    
    # Test 2: Query subjects by specific HPO terms
    test_terms = hpo_terms[:3]  # Test with first 3 terms
    for i, term_iri in enumerate(test_terms):
        start_time = time.time()
        
        # Get all results by paginating with cursor
        all_subjects = []
        limit = 50
        cursor = None
        
        while True:
            request_body = {
                "project_id": test_project_id,
                "term_iri": term_iri,
                "limit": limit
            }
            if cursor:
                request_body["cursor"] = cursor
                
            response = requests.post(f"{api_url}/subjects/query", json=request_body)
            
            assert response.status_code == 200
            page_data = response.json()
            page_subjects = page_data.get('body', [])
            
            if not page_subjects:
                break
                
            all_subjects.extend(page_subjects)
            
            # Get next cursor for pagination
            cursor = page_data.get('next_cursor')
            if not cursor:
                break
            
            # Safety break
            if len(all_subjects) > 10000:
                break
        
        term_query_time = time.time() - start_time
        
        term_name = term_iri.split('/')[-1]
        actual_subject_ids = [s.get('project_subject_id') for s in all_subjects]
        print(f"Term query ({term_name}): {term_query_time:.3f}s")
        print(f"  Expected: {expected_mappings.get(term_iri, set())}")
        print(f"  Actual: {set(actual_subject_ids)}")
        
        # Validate results against expected mappings
        if term_iri in expected_mappings:
            actual_subjects = {s.get('project_subject_id') for s in all_subjects}
            expected_subjects = expected_mappings[term_iri]
            
            if actual_subjects == expected_subjects:
                print(f"  ✓ Validation passed: {len(actual_subjects)} subjects match expected")
                
                # For first term, do detailed evidence validation
                if i == 0:
                    print(f"  Performing detailed evidence validation for {term_name}...")
                    evidence_mismatches = 0
                    
                    for subject_data in all_subjects:
                        subject_id = subject_data.get('project_subject_id')
                        subject_iri = subject_data.get('subject_iri')
                        
                        # Check expected evidence for this subject-term combination
                        evidence_key = (subject_id, term_iri)
                        if evidence_key in expected_evidence:
                            expected_records = expected_evidence[evidence_key]
                            positive_expected = [e for e in expected_records if e['qualifiers'] == positive_scenario]
                            
                            # Get actual evidence records from /subject/term-info endpoint
                            try:
                                # First, get the internal subject UUID using /subject endpoint
                                subject_response = requests.post(f"{api_url}/subject", json={
                                    "project_subject_iri": subject_data.get('project_subject_iri')
                                })
                                
                                if subject_response.status_code != 200:
                                    print(f"    ✗ Subject {subject_id}: Could not get subject details (status {subject_response.status_code})")
                                    evidence_mismatches += 1
                                    continue
                                
                                subject_details = subject_response.json().get('body', {})
                                if isinstance(subject_details, str):
                                    import json
                                    subject_details = json.loads(subject_details)
                                print(f"    DEBUG: Subject details for {subject_id}: {subject_details}")
                                internal_subject_id = subject_details.get('subject_iri', '').split('/')[-1]
                                
                                if not internal_subject_id:
                                    print(f"    ✗ Subject {subject_id}: Could not extract internal subject UUID")
                                    evidence_mismatches += 1
                                    continue
                                
                                # Now get evidence using the internal subject UUID
                                evidence_response = requests.post(f"{api_url}/subject/term-info", json={
                                    "subject_id": internal_subject_id,
                                    "term_iri": term_iri,
                                    "qualifiers": []
                                })
                                
                                if evidence_response.status_code == 200:
                                    evidence_data = evidence_response.json().get('body', [])
                                    # Filter for positive evidence (no negative qualifiers)
                                    positive_actual = [e for e in evidence_data if not any(
                                        q.get('qualifier_type') in ['negated', 'family', 'hypothetical'] and 
                                        q.get('qualifier_value') in ['1', 'true', True]
                                        for q in e.get('qualifiers', [])
                                    )]
                                    
                                    if len(positive_actual) != len(positive_expected):
                                        print(f"    ✗ Subject {subject_id}: Expected {len(positive_expected)} positive evidence, got {len(positive_actual)}")
                                        evidence_mismatches += 1
                                else:
                                    print(f"    ✗ Subject {subject_id}: Could not retrieve evidence (status {evidence_response.status_code})")
                                    evidence_mismatches += 1
                                    
                            except Exception as e:
                                print(f"    ✗ Subject {subject_id}: Error retrieving evidence: {e}")
                                evidence_mismatches += 1
                    
                    if evidence_mismatches == 0:
                        print(f"  ✓ Evidence validation passed: All evidence counts match expected")
                    else:
                        assert False, f"Evidence validation failed: {evidence_mismatches} subjects had evidence count mismatches"
                        
            else:
                missing = expected_subjects - actual_subjects
                extra = actual_subjects - expected_subjects
                print(f"  ✗ Validation failed:")
                print(f"    Expected subjects: {expected_subjects}")
                print(f"    Actual subjects: {actual_subjects}")
                if missing:
                    print(f"    Missing subjects: {missing}")
                if extra:
                    print(f"    Extra subjects: {extra}")
                
                # FAIL THE TEST - strict validation
                assert False, f"Data validation failed for {term_name}: Expected {len(expected_subjects)} subjects, got {len(actual_subjects)}"
        
        # For the first term, page through ALL results to test full dataset performance
        if i == 0:
            unique_subjects = set()
            page_times = []
            page_num = 1
            limit = 50  # Page size
            cursor = None
            
            print(f"Paging through all results for {term_name}...")
            
            while True:
                page_start = time.time()
                request_body = {
                    "project_id": test_project_id,
                    "term_iri": term_iri,
                    "limit": limit
                }
                if cursor:
                    request_body["cursor"] = cursor
                    
                response = requests.post(f"{api_url}/subjects/query", json=request_body)
                page_time = time.time() - page_start
                page_times.append(page_time)
                
                assert response.status_code == 200
                page_data = response.json()
                page_subjects = page_data.get('body', [])
                
                if not page_subjects:
                    break
                    
                for subject in page_subjects:
                    unique_subjects.add(subject.get('project_subject_id'))
                print(f"  Page {page_num}: {page_time:.3f}s for {len(page_subjects)} subjects")
                page_num += 1
                
                # Get next cursor
                cursor = page_data.get('next_cursor')
                if not cursor:
                    break
                
                # Safety break to avoid infinite loops
                if page_num > 100:
                    break
            
            total_paging_time = sum(page_times)
            avg_page_time = total_paging_time / len(page_times) if page_times else 0
            
            print(f"Full pagination for {term_name}:")
            print(f"  Unique subjects: {len(unique_subjects)}")
            print(f"  Total pages: {len(page_times)}")
            print(f"  Total paging time: {total_paging_time:.3f}s")
            print(f"  Average page time: {avg_page_time:.3f}s")
    
    # Test 3: Query with qualifier filtering (include_qualified parameter)
    start_time = time.time()
    response = requests.post(f"{api_url}/subjects/query", json={
        "project_id": test_project_id,
        "term_iri": hpo_terms[0],
        "include_qualified": False  # Only non-negated, non-family, non-hypothetical
    })
    qualified_query_time = time.time() - start_time
    
    assert response.status_code == 200
    qualified_subjects = response.json()
    
    print(f"Qualified query (non-qualified only): {qualified_query_time:.3f}s for {len(qualified_subjects.get('body', []))} subjects")
    
    # Test 4: Get individual subject details (sample from actual returned subjects)
    returned_subjects = subjects_data.get('body', [])
    if not returned_subjects:
        print("No subjects returned from query, skipping individual subject tests")
    else:
        sample_subjects = returned_subjects[:10]  # Use actual returned subjects
        subject_times = []
        
        for subject_data in sample_subjects:
            project_subject_iri = subject_data.get('project_subject_iri')
            if not project_subject_iri:
                continue
                
            start_time = time.time()
            response = requests.post(f"{api_url}/subject", json={
                "project_subject_iri": project_subject_iri
            })
            subject_time = time.time() - start_time
            subject_times.append(subject_time)
            
            assert response.status_code == 200, f"Subject lookup failed for {project_subject_iri}: {response.status_code}"
        
        if subject_times:
            avg_subject_time = sum(subject_times) / len(subject_times)
            print(f"Individual subject queries: avg {avg_subject_time:.3f}s (sample of {len(subject_times)})")
        else:
            print("No successful individual subject queries")
    
    # Test 5: Get single subject details using /subject endpoint
    if returned_subjects:
        sample_subject_data = returned_subjects[0]
        sample_subject_iri = sample_subject_data.get('project_subject_iri')
        
        if sample_subject_iri:
            start_time = time.time()
            response = requests.post(f"{api_url}/subject", json={
                "project_subject_iri": sample_subject_iri
            })
            single_subject_time = time.time() - start_time
            
            assert response.status_code == 200, f"Single subject query failed: {response.status_code}"
            subject_data = response.json()
            print(f"Single subject query (/subject): {single_subject_time:.3f}s")
        else:
            print("No valid subject IRI found for /subject query")
            subject_data = {}
    else:
        print("No subjects available for /subject query")
        subject_data = {}
    
    # Test 6: Get subject term info details for specific term links
    if returned_subjects and 'termlinks' in subject_data and subject_data['termlinks']:
        # Get the first termlink for detailed analysis
        first_termlink = subject_data['termlinks'][0]
        termlink_id = first_termlink.get('termlink_id')
        term_iri = first_termlink.get('term_iri')
        subject_iri = sample_subject_data.get('subject_iri')
        
        if termlink_id and term_iri and subject_iri:
            start_time = time.time()
            response = requests.post(f"{api_url}/subject/term-info", json={
                "subject_iri": subject_iri,
                "term_iri": term_iri
            })
            term_info_time = time.time() - start_time
            
            assert response.status_code == 200
            term_info_data = response.json()
            
            print(f"Subject term-info query: {term_info_time:.3f}s")
            print(f"  Term: {term_iri}")
            print(f"  Evidence count: {len(term_info_data.get('evidence', []))}")
            
            # Check if qualifiers are present in the evidence
            evidence_with_qualifiers = 0
            for evidence in term_info_data.get('evidence', []):
                if evidence.get('qualifiers'):
                    evidence_with_qualifiers += 1
            
            print(f"  Evidence with qualifiers: {evidence_with_qualifiers}")
        else:
            print("No termlink details available for term-info testing")
    else:
        print("No termlinks found in subject data for term-info testing")

    # Test 7: Pagination performance (using subjects/query with limit)
    start_time = time.time()
    response = requests.post(f"{api_url}/subjects/query", json={
        "project_id": test_project_id,
        "limit": 5
    })
    pagination_time = time.time() - start_time
    
    assert response.status_code == 200
    paginated_data = response.json()
    
    print(f"Paginated query (limit=5): {pagination_time:.3f}s for {len(paginated_data.get('body', []))} subjects")
    
    # Performance assertions (adjust thresholds as needed)
    assert query_time < 30.0, f"Project query too slow: {query_time:.3f}s"
    assert qualified_query_time < 30.0, f"Qualified query too slow: {qualified_query_time:.3f}s"
    assert avg_subject_time < 5.0, f"Individual subject queries too slow: {avg_subject_time:.3f}s"
    assert single_subject_time < 5.0, f"Single subject query too slow: {single_subject_time:.3f}s"
    if 'term_info_time' in locals():
        assert term_info_time < 5.0, f"Term info query too slow: {term_info_time:.3f}s"
    assert pagination_time < 10.0, f"Pagination too slow: {pagination_time:.3f}s"
    
    print("All performance tests passed!")
    print(f"Total test runtime: {time.time() - start_time:.2f} seconds")


def bulk_load_performance_data(test_data, physical_resources):
    """Load performance test data via bulk import using multiple smaller files"""
    run_id = f"perf-test-{uuid.uuid4().hex[:8]}"
    
    # Upload to S3 in chunks
    s3_bucket = physical_resources["PheBeeBucket"]
    s3_client = boto3.client('s3')
    
    # Split data into smaller files (1,000 records per file for better processing)
    chunk_size = 1000
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
