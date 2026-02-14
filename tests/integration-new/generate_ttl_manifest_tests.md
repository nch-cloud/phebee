# Integration Test Plan: generate_ttl_manifest.py

## Lambda Metadata

**Purpose**: Generate manifest of Athena result pages for parallel TTL processing, splitting evidence data into hash-distributed pages.

**Dependencies**:
- Athena: Count records, define page queries
- S3: Write manifest.json

**Key Operations**:
- Counts total evidence records for run_id
- Calculates number of pages using hash distribution
- Generates page queries using MOD(xxhash64(subject_id), 64)
- Writes manifest with page definitions
- Returns page_numbers array for Step Functions Map

---

## Integration Test Scenarios

### Test 1: test_generate_manifest_1000_records (1000 records, manifest with pages created)
### Test 2: test_generate_manifest_empty_run (0 records, skip_processing=true, no pages)
### Test 3: test_generate_manifest_hash_distribution (Pages use MOD(xxhash64(subject_id), 64) for even distribution)
### Test 4: test_generate_manifest_max_64_pages (Large dataset, capped at 64 pages)
### Test 5: test_generate_manifest_min_1000_per_page (At least 1000 records per page)
### Test 6: test_generate_manifest_s3_location (manifest.json written to s3://bucket/run_id/)
### Test 7: test_generate_manifest_page_queries (Each page has SQL query in manifest)
### Test 8: test_generate_manifest_total_count (total_records in manifest matches actual count)
### Test 9: test_generate_manifest_page_numbers_array (page_numbers is [0,1,2,...,n-1])
### Test 10: test_generate_manifest_step_functions_map (page_numbers compatible with Map state ItemReader)
### Test 11: test_generate_manifest_missing_run_id (run_id not provided, error)
### Test 12: test_generate_manifest_nonexistent_run (run_id with no data, 0 records, skip)
### Test 13: test_generate_manifest_athena_workgroup (Uses primary workgroup, managed results handling)
### Test 14: test_generate_manifest_count_query_performance (Count query completes in <10s)
### Test 15: test_generate_manifest_concurrent_runs (Generate manifests for 3 runs concurrently, isolated)
### Test 16: test_generate_manifest_response_structure (Returns total_records, total_pages, manifest_location)
### Test 17: test_generate_manifest_skip_processing_flag (Empty run sets skip_processing=true)
### Test 18: test_generate_manifest_integration_with_ttl_generation (Manifest used by generate_ttl_from_iceberg)
### Test 19: test_generate_manifest_idempotent (Generate twice, same manifest produced)
### Test 20: test_generate_manifest_large_dataset (1M records, 64 pages, completes without timeout)
