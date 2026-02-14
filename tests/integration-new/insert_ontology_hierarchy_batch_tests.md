# Integration Test Plan: insert_ontology_hierarchy_batch.py

## Lambda Metadata

**Purpose**: Insert a single batch of ontology terms into hierarchy table, invoked by Distributed Map state.

**Dependencies**:
- S3: Read batch file (if from Map) or receive direct payload
- Athena: Execute INSERT query
- Environment: ICEBERG_DATABASE, ICEBERG_ONTOLOGY_HIERARCHY_TABLE, PHEBEE_BUCKET_NAME

**Key Operations**:
- Reads batch from S3 or direct invocation
- Builds INSERT VALUES statement with term data
- Executes Athena INSERT
- Returns batch statistics

---

## Integration Test Scenarios

### Test 1: test_insert_batch_from_s3 (Distributed Map invocation, reads batch from S3 key)
### Test 2: test_insert_batch_direct (Direct payload with terms array, no S3 read)
### Test 3: test_insert_100_terms (Batch of 100 terms inserted, all present in table)
### Test 4: test_insert_200_terms (Full batch size, no truncation)
### Test 5: test_insert_empty_batch (0 terms, skipped gracefully, no error)
### Test 6: test_insert_term_with_ancestors (ancestor_term_ids array inserted correctly)
### Test 7: test_insert_root_term (depth=0, empty ancestors, valid)
### Test 8: test_insert_deep_term (depth=10, many ancestors, all preserved)
### Test 9: test_insert_unicode_labels (Unicode characters in term_label, no encoding errors)
### Test 10: test_insert_special_chars_in_label (Single quotes escaped properly, no SQL injection)
### Test 11: test_insert_concurrent_batches (10 batches inserted concurrently, all succeed, no conflicts)
### Test 12: test_insert_idempotent (Insert same batch twice, duplicates created - Iceberg allows, or handled)
### Test 13: test_insert_updates_last_updated (last_updated timestamp set to current time)
### Test 14: test_query_inserted_terms (After insert, query by term_id, data retrievable)
### Test 15: test_query_by_ontology_version (Partition pruning works, query specific version)
### Test 16: test_athena_query_failure (Invalid data, INSERT fails, error raised)
### Test 17: test_missing_environment_variables (ICEBERG_DATABASE not set, error)
### Test 18: test_batch_from_materialize_ontology (Integration with materialize_ontology_hierarchy output)
### Test 19: test_response_structure (Returns batch_key, terms_inserted, status)
### Test 20: test_step_functions_map_integration (Invoked by Map state, results aggregated)
