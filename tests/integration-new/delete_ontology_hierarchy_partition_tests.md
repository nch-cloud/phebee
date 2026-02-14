# Integration Test Plan: delete_ontology_hierarchy_partition.py

## Lambda Metadata

**Purpose**: Delete ontology hierarchy data for a specific ontology source and version before loading new version.

**Dependencies**:
- Athena: Execute DELETE query
- Iceberg: Partition deletion
- Environment: ICEBERG_DATABASE, ICEBERG_ONTOLOGY_HIERARCHY_TABLE

**Key Operations**:
- Executes DELETE WHERE ontology_source=X AND version=Y
- Removes partition data before new load
- Returns success status

---

## Integration Test Scenarios

### Test 1: test_delete_partition_success (HPO version deleted, data removed from table)
### Test 2: test_delete_specific_version (Delete v2024-01, v2024-02 remains)
### Test 3: test_delete_nonexistent_partition (No data matches, succeeds without error)
### Test 4: test_delete_then_query (After delete, query returns 0 rows for that partition)
### Test 5: test_delete_preserves_other_ontologies (Delete HPO, MONDO data unchanged)
### Test 6: test_delete_preserves_other_versions (Delete old version, new version intact)
### Test 7: test_delete_before_insert (Delete old → insert new, clean replacement)
### Test 8: test_delete_missing_ontology_source (ontology_source not provided, error)
### Test 9: test_delete_missing_version (version not provided, error)
### Test 10: test_delete_special_chars_in_source (Ontology source with special chars, handled safely)
### Test 11: test_delete_athena_failure (DELETE query fails, error raised)
### Test 12: test_delete_concurrent_operations (Delete partition while queries running, Iceberg handles)
### Test 13: test_delete_idempotent (Delete twice, both succeed, no error on second)
### Test 14: test_delete_response_structure (Returns ontology_source, version, status)
### Test 15: test_delete_environment_variables (ICEBERG_DATABASE missing, error)
### Test 16: test_delete_iceberg_snapshots (New Iceberg snapshot created, versioned)
### Test 17: test_delete_s3_data_files (Partition data files marked for deletion, eventually removed)
### Test 18: test_delete_integration_with_materialize (Delete old → materialize new workflow)
### Test 19: test_delete_large_partition (Delete partition with 10k+ terms, completes without timeout)
### Test 20: test_delete_query_performance (DELETE completes in <5 seconds)
