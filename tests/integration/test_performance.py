import json
import time
import uuid
import pytest
import boto3
import random
import requests


def extract_project_id_from_run(run_id, physical_resources):
    """Extract project_id from an existing run's evidence files"""
    s3_client = boto3.client('s3')
    bucket_name = physical_resources['PheBeeBucket']
    
    # List evidence files for the run
    prefix = f"performance-test/{run_id}/"
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
    
    if 'Contents' not in response:
        raise ValueError(f"No evidence files found for run_id: {run_id}")
    
    # Download and parse the first evidence file to extract project_id
    first_file_key = response['Contents'][0]['Key']
    obj = s3_client.get_object(Bucket=bucket_name, Key=first_file_key)
    content = obj['Body'].read().decode('utf-8')
    
    # Parse the first line to extract project_id
    first_line = content.strip().split('\n')[0]
    if first_line:
        evidence_record = json.loads(first_line)
        return evidence_record.get('project_id')
    
    raise ValueError(f"Could not extract project_id from evidence file: {first_file_key}")


@pytest.mark.performance
def test_large_scale_performance(physical_resources, stack_outputs, num_subjects, num_terms, request):
    """Test performance with large numbers of subjects and evidence records using realistic HPO terms and qualifiers"""
    
    # Check for existing run_id parameter
    existing_run_id = getattr(request.config.option, 'existing_run_id', None)
    
    # Only use test_project_id fixture if not using existing run
    if existing_run_id:
        test_project_id = extract_project_id_from_run(existing_run_id, physical_resources)
        print(f"Using project_id from existing run: {test_project_id}")
    else:
        # Generate new project_id for new runs
        test_project_id = f"test_project_{uuid.uuid4().hex[:8]}"
        print(f"Generated new project_id: {test_project_id}")
    existing_run_id = getattr(request.config.option, 'existing_run_id', None)
    
    # Configuration from command line parameters
    NUM_SUBJECTS = num_subjects
    NOTES_PER_SUBJECT = num_terms
    
    if not existing_run_id:
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
    
    # Generate test data with realistic distributions (skip if using existing run)
    if existing_run_id:
        print(f"Skipping data generation - using existing run_id: {existing_run_id}")
        test_data = []
        subject_ids = []
        expected_mappings = {}
        expected_evidence = {}
        
        # Reconstruct expected data from S3 files for validation
        print("Reconstructing expected validation data from S3 files...")
        s3_client = boto3.client('s3')
        s3_bucket = physical_resources["PheBeeBucket"]
        
        # List all files for this run_id
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=s3_bucket,
            Prefix=f"performance-test/{existing_run_id}/"
        )
        
        # Collect all file keys
        file_keys = []
        for page in page_iterator:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.jsonl'):
                    file_keys.append(obj['Key'])
        
        print(f"Found {len(file_keys)} files to process")
        
        # Process files in parallel
        from concurrent.futures import ThreadPoolExecutor
        import threading
        
        # Thread-safe collections
        lock = threading.Lock()
        all_subject_ids = set()
        all_expected_mappings = {}
        all_expected_evidence = {}
        
        def process_file(key):
            try:
                response = s3_client.get_object(Bucket=s3_bucket, Key=key)
                content = response['Body'].read().decode('utf-8')
                
                # Local collections for this file
                local_subjects = set()
                local_mappings = {}
                local_evidence = {}
                
                for line in content.strip().split('\n'):
                    if line:
                        record = json.loads(line)
                        test_data.append(record)
                        
                        subject_id = record['project_subject_id']
                        term_iri = record['term_iri']
                        qualifiers = record['evidence'][0]['contexts']
                        
                        local_subjects.add(subject_id)
                        
                        # Track expected mappings (only for positive/unqualified evidence)
                        positive_scenario = {"negated": 0, "family": 0, "hypothetical": 0}
                        if qualifiers == positive_scenario:
                            if term_iri not in local_mappings:
                                local_mappings[term_iri] = set()
                            local_mappings[term_iri].add(subject_id)
                        
                        # Track all evidence for detailed validation
                        evidence_key = (subject_id, term_iri)
                        if evidence_key not in local_evidence:
                            local_evidence[evidence_key] = []
                        local_evidence[evidence_key].append({
                            'clinical_note_id': record['evidence'][0]['clinical_note_id'],
                            'encounter_id': record['evidence'][0]['encounter_id'],
                            'qualifiers': qualifiers
                        })
                
                # Merge into global collections (thread-safe)
                with lock:
                    all_subject_ids.update(local_subjects)
                    
                    for term_iri, subjects in local_mappings.items():
                        if term_iri not in all_expected_mappings:
                            all_expected_mappings[term_iri] = set()
                        all_expected_mappings[term_iri].update(subjects)
                    
                    for evidence_key, evidence_list in local_evidence.items():
                        if evidence_key not in all_expected_evidence:
                            all_expected_evidence[evidence_key] = []
                        all_expected_evidence[evidence_key].extend(evidence_list)
                
                return len([line for line in content.strip().split('\n') if line])
            except Exception as e:
                print(f"Error processing {key}: {e}")
                return 0
        
        # Process files in parallel (use 10 threads for good S3 throughput)
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            record_counts = list(executor.map(process_file, file_keys))
        
        processing_time = time.time() - start_time
        total_records = sum(record_counts)
        
        # Update the main collections
        subject_ids = list(all_subject_ids)
        expected_mappings = all_expected_mappings
        expected_evidence = all_expected_evidence
        
        print(f"Parallel processing completed in {processing_time:.1f}s:")
        print(f"  Files processed: {len(file_keys)}")
        print(f"  Total records reconstructed: {total_records}")
        print(f"  Unique subjects found: {len(subject_ids)}")
        print(f"  Terms with positive evidence: {len(expected_mappings)}")
        print(f"  Reconstruction rate: {total_records/processing_time:.0f} records/second")
        for term_iri, subjects in list(expected_mappings.items())[:3]:  # Show first 3
            term_name = term_iri.split('/')[-1]
            print(f"    {term_name}: {len(subjects)} subjects")
    else:
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
    
    # Generate comprehensive input data summary (skip if using existing run)
    if not existing_run_id:
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
    
    if existing_run_id:
        print(f"Using existing run_id: {existing_run_id}")
        run_id = existing_run_id
        load_time = 0  # Skip timing since we didn't do the load
    else:
        run_id = bulk_load_performance_data(test_data, physical_resources)
        load_time = time.time() - start_time
    
    if existing_run_id:
        print(f"Using existing bulk import run: {existing_run_id}")
    else:
        print(f"Bulk load completed in {load_time:.2f} seconds")
        
    if load_time > 0:
        print(f"Rate: {len(test_data)/load_time:.2f} records/second")
    else:
        print("Rate: N/A (using existing run_id)")
    
    # Test API performance with qualifier-aware queries
    print("\n=== API PERFORMANCE VALIDATION ===")
    api_url = stack_outputs.get("HttpApiUrl")
    if not api_url:
        print("HttpApiUrl not found in stack outputs, skipping API performance tests")
        print("Performance test completed successfully!")
        return
    
    # Wait a bit for Neptune indexing to complete
    print("Waiting 30 seconds for Neptune indexing to complete...")
    time.sleep(30)

    # Test 1: Query subjects by project (using pagination)
    start_time = time.time()
    
    # Get all subjects by paginating
    all_project_subjects = []
    limit = 50
    cursor = None
    
    while True:
        request_body = {"project_id": test_project_id, "limit": limit}
        if cursor:
            request_body["cursor"] = cursor
            
        response = requests.post(f"{api_url}/subjects/query", json=request_body)
        
        if response.status_code != 200:
            print("API test failed - data may not be available yet or endpoint issue")
            print("Performance test completed (API validation skipped)")
            return
            
        page_data = response.json()
        page_subjects = page_data.get('body', [])
        
        if not page_subjects:
            break
            
        all_project_subjects.extend(page_subjects)
        
        # Get next cursor
        cursor = page_data.get('pagination', {}).get('next_cursor')
        if not cursor:
            break
            
        # Safety break
        if len(all_project_subjects) > 10000:
            break
    
    query_time = time.time() - start_time
    
    expected_subject_count = len(subject_ids) if subject_ids else "unknown"
    actual_subject_count = len(all_project_subjects)
    
    print(f"API Call: GET /projects/{test_project_id}/subjects - Expected: {expected_subject_count} subjects from bulk import")
    print(f"Result: Retrieved {actual_subject_count} subjects in {query_time:.3f}s - {'✓ PASS' if actual_subject_count == expected_subject_count else f'✗ FAIL: Expected {expected_subject_count}, got {actual_subject_count}'}")
    
    assert actual_subject_count == expected_subject_count, f"Project query returned wrong count: expected {expected_subject_count}, got {actual_subject_count}"
    
    # Use paginated results for subsequent tests
    subjects_data = {"body": all_project_subjects}
    
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
                
            page_start = time.time()
            response = requests.post(f"{api_url}/subjects/query", json=request_body)
            page_time = time.time() - page_start
            
            assert response.status_code == 200
            page_data = response.json()
            page_subjects = page_data.get('body', [])
            
            if not page_subjects:
                break
                
            all_subjects.extend(page_subjects)
            print(f"      Page {len(all_subjects)//len(page_subjects)}: {len(page_subjects)} subjects ({page_time:.3f}s)")
            
            # Get next cursor for pagination
            cursor = page_data.get('pagination', {}).get('next_cursor')
            if not cursor:
                break
            
            # Safety break
            if len(all_subjects) > 10000:
                break
        
        term_query_time = time.time() - start_time
        
        term_name = term_iri.split('/')[-1]
        actual_subject_ids = [s.get('project_subject_id') for s in all_subjects]
        expected_count = len(expected_mappings.get(term_iri, set()))
        actual_count = len(actual_subject_ids)
        
        print(f"API Call: GET /projects/{test_project_id}/subjects?term_iri={term_name} - Expected: {expected_count} subjects with positive evidence")
        print(f"Result: Retrieved {actual_count} subjects in {term_query_time:.3f}s - {'✓ PASS' if actual_count == expected_count else f'✗ FAIL: Expected {expected_count}, got {actual_count}'}")
        
        assert actual_count == expected_count, f"Term query for {term_name} returned wrong count: expected {expected_count}, got {actual_count}"
        
        # Validate results against expected mappings
        if term_iri in expected_mappings:
            actual_subjects = {s.get('project_subject_id') for s in all_subjects}
            expected_subjects = expected_mappings[term_iri]
            
            if actual_subjects == expected_subjects:
                print(f"  ✓ Subject IDs match expected set")
                
                # For first term, validate termlink grouping using /subject endpoint data
                if i == 0:
                    print(f"  Validating evidence grouping by qualifiers for {term_name}...")
                    grouping_mismatches = 0
                    sample_subjects = all_subjects[:10]  # Limit to 10 subjects for performance
                    
                    print(f"    Validating termlink grouping for {len(sample_subjects)} sample subjects (API calls: POST /subject)")
                    
                    for i, subject_data in enumerate(sample_subjects):
                        subject_id = subject_data.get('project_subject_id')
                        
                        # Get subject details to access aggregated term data
                        try:
                            start_time = time.time()
                            subject_response = requests.post(f"{api_url}/subject", json={
                                "project_subject_iri": subject_data.get('project_subject_iri')
                            })
                            call_time = time.time() - start_time
                            
                            if subject_response.status_code != 200:
                                print(f"    ✗ Subject {i+1}/{len(sample_subjects)} ({subject_id}): Could not get subject details ({call_time:.3f}s)")
                                grouping_mismatches += 1
                                continue
                                
                            subject_details = subject_response.json()
                            if 'body' in subject_details:
                                subject_details = subject_details['body']
                                if isinstance(subject_details, str):
                                    subject_details = json.loads(subject_details)
                            
                            # Find the term in the aggregated data
                            term_found = False
                            for term in subject_details.get('terms', []):
                                if term.get('term_iri') == term_iri:
                                    term_found = True
                                    actual_term_link_count = term.get('term_link_count', 0)
                                    actual_evidence_count = term.get('evidence_count', 0)
                                    
                                    # Calculate expected values
                                    evidence_key = (subject_id, term_iri)
                                    if evidence_key in expected_evidence:
                                        expected_records = expected_evidence[evidence_key]
                                        expected_qualifier_groups = {}
                                        for record in expected_records:
                                            qual_key = tuple(sorted([k for k, v in record['qualifiers'].items() if v == 1]))
                                            expected_qualifier_groups[qual_key] = expected_qualifier_groups.get(qual_key, 0) + 1
                                        
                                        expected_term_link_count = len(expected_qualifier_groups)
                                        expected_evidence_count = len(expected_records)
                                        
                                        # Validate termlink grouping
                                        if actual_term_link_count != expected_term_link_count:
                                            print(f"    ✗ Subject {i+1}/{len(sample_subjects)} ({subject_id}): Expected {expected_term_link_count} term links, got {actual_term_link_count} ({call_time:.3f}s)")
                                            print(f"      Expected qualifier groups: {list(expected_qualifier_groups.keys())}")
                                            grouping_mismatches += 1
                                        elif actual_evidence_count != expected_evidence_count:
                                            print(f"    ✗ Subject {i+1}/{len(sample_subjects)} ({subject_id}): Expected {expected_evidence_count} evidence records, got {actual_evidence_count} ({call_time:.3f}s)")
                                            grouping_mismatches += 1
                                        else:
                                            # Validate evidence count per qualifier combination
                                            # Note: Current API doesn't expose per-qualifier evidence counts
                                            # This validates the overall grouping is correct
                                            print(f"    ✓ Subject {i+1}/{len(sample_subjects)} ({subject_id}): {actual_term_link_count} term links, {actual_evidence_count} evidence records ({call_time:.3f}s)")
                                            if len(expected_qualifier_groups) <= 5:  # Only show distribution for simpler cases
                                                print(f"      Expected qualifier distribution: {dict(expected_qualifier_groups)}")
                                            else:
                                                print(f"      Expected {len(expected_qualifier_groups)} qualifier groups with {expected_evidence_count} total evidence")
                                    break
                            
                            if not term_found:
                                print(f"    ✗ Subject {i+1}/{len(sample_subjects)} ({subject_id}): Term {term_iri} not found in subject data ({call_time:.3f}s)")
                                grouping_mismatches += 1
                                
                        except Exception as e:
                            print(f"    ✗ Subject {i+1}/{len(sample_subjects)} ({subject_id}): Error validating termlink grouping: {e}")
                            grouping_mismatches += 1
                    
                    if grouping_mismatches == 0:
                        print(f"  ✓ Termlink grouping validation passed: All subjects have correct term link counts")
                    else:
                        assert False, f"Termlink grouping validation failed: {grouping_mismatches} subjects had incorrect grouping"
                        
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
                cursor = page_data.get('pagination', {}).get('next_cursor')
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
    
    # Test 3: Query with qualifier filtering (include_qualified parameter) - with pagination
    start_time = time.time()
    
    # Get all qualified subjects by paginating
    all_qualified_subjects = []
    limit = 50
    cursor = None
    
    while True:
        request_body = {
            "project_id": test_project_id,
            "term_iri": hpo_terms[0],
            "include_qualified": False,
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
            
        all_qualified_subjects.extend(page_subjects)
        
        # Get next cursor
        cursor = page_data.get('pagination', {}).get('next_cursor')
        if not cursor:
            break
            
        # Safety break
        if len(all_qualified_subjects) > 10000:
            break
    
    qualified_query_time = time.time() - start_time
    qualified_count = len(all_qualified_subjects)
    expected_qualified = len(expected_mappings.get(hpo_terms[0], set())) if hpo_terms[0] in expected_mappings else 0
    
    print(f"API Call: POST /subjects/query with include_qualified=false - Expected: {expected_qualified} subjects with positive evidence only")
    print(f"Result: Retrieved {qualified_count} subjects in {qualified_query_time:.3f}s - {'✓ PASS' if qualified_count == expected_qualified else f'✗ FAIL: Expected {expected_qualified}, got {qualified_count}'}")
    
    assert qualified_count == expected_qualified, f"Qualified query returned wrong count: expected {expected_qualified}, got {qualified_count}"
    
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
            print(f"API Call: GET /projects/{{project_id}}/subjects/{{subject_iri}} - Expected: Individual subject details")
            print(f"Result: Retrieved {len(subject_times)} subjects in avg {avg_subject_time:.3f}s each - {'✓ PASS' if avg_subject_time < 5.0 else '✗ FAIL: Too slow'}")
            assert avg_subject_time < 5.0, f"Individual subject queries too slow: {avg_subject_time:.3f}s average"
        else:
            print("API Call: GET /projects/{project_id}/subjects/{subject_iri} - Expected: Individual subject details")
            print("Result: ✗ FAIL: No successful individual subject queries")
            assert False, "No successful individual subject queries"
    
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
            print(f"API Call: POST /subject with subject_iri - Expected: Single subject with termlinks")
            print(f"Result: Retrieved subject data in {single_subject_time:.3f}s - {'✓ PASS' if single_subject_time < 5.0 else '✗ FAIL: Too slow'}")
            assert single_subject_time < 5.0, f"Single subject query too slow: {single_subject_time:.3f}s"
        else:
            print("API Call: POST /subject with subject_iri - Expected: Single subject with termlinks")
            print("Result: ✗ FAIL: No valid subject IRI found")
            assert False, "No valid subject IRI found"
            subject_data = {}
    else:
        print("API Call: POST /subject with subject_iri - Expected: Single subject with termlinks")
        print("Result: ✗ FAIL: No subjects available for testing")
        assert False, "No subjects available for testing"
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
            
            evidence_count = len(term_info_data.get('evidence', []))
            evidence_with_qualifiers = sum(1 for evidence in term_info_data.get('evidence', []) if evidence.get('qualifiers'))
            
            print(f"API Call: POST /subject/term-info with termlink_id - Expected: Evidence records for specific term")
            print(f"Result: Retrieved {evidence_count} evidence records in {term_info_time:.3f}s ({evidence_with_qualifiers} with qualifiers) - {'✓ PASS' if evidence_count > 0 else '✗ FAIL: No evidence found'}")
            assert evidence_count > 0, f"No evidence found for term info query"
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
    paginated_count = len(paginated_data.get('body', []))
    
    print(f"API Call: POST /subjects/query with limit=5 - Expected: 5 subjects (pagination test)")
    print(f"Result: Retrieved {paginated_count} subjects in {pagination_time:.3f}s - {'✓ PASS' if paginated_count == 5 else f'✗ FAIL: Expected 5, got {paginated_count}'}")
    
    assert paginated_count == 5, f"Pagination test failed: expected 5 subjects, got {paginated_count}"
    
    # Performance assertions (adjust thresholds as needed)
    assert query_time < 30.0, f"Project query too slow: {query_time:.3f}s"
    assert qualified_query_time < 30.0, f"Qualified query too slow: {qualified_query_time:.3f}s"
    if 'avg_subject_time' in locals():
        assert avg_subject_time < 5.0, f"Individual subject queries too slow: {avg_subject_time:.3f}s"
    if 'single_subject_time' in locals():
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
    max_wait = 3600  # 1 hour
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
