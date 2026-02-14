# Integration Test Plan: create_ontology_hierarchy_table.py

## Lambda Metadata

**Purpose**: CloudFormation custom resource to create ontology_hierarchy Iceberg table partitioned by (ontology_source, version).

**Dependencies**:
- Athena, CloudFormation

**Key Operations**:
- Creates table with term_id, term_iri, term_label, ancestor_term_ids array, depth
- Partitioned by ontology_source (hpo, mondo) and version (date)
- Handles CF custom resource lifecycle

---

## Integration Test Scenarios

### Test 1: test_create_hierarchy_table (Table created with correct schema, partitioning by source+version)
### Test 2: test_ancestor_array_field (ancestor_term_ids is array<string>, can query/unnest)
### Test 3: test_depth_field_integer (depth field is int, used for hierarchy queries)
### Test 4: test_multiple_ontology_sources (HPO and MONDO data coexist, partitions separate)
### Test 5: test_multiple_versions (Different versions side-by-side, queries by version work)
### Test 6: test_partition_pruning (Query specific ontology+version, only scans one partition)
### Test 7: test_create_idempotent (IF NOT EXISTS prevents error on retry)
### Test 8: test_cloudformation_success (CF stack receives SUCCESS, deployment continues)
### Test 9: test_update_handling (Update request, no changes, success)
### Test 10: test_delete_handling (Delete request, logged, table preserved)
### Test 11: test_athena_query_wait (Lambda waits for CREATE TABLE to complete)
### Test 12: test_s3_location_configured (LOCATION points to correct S3 path)
### Test 13: test_iceberg_format (table_type=ICEBERG, format=parquet in TBLPROPERTIES)
### Test 14: test_glue_catalog_visibility (Table in Glue, visible to Athena/Spark)
### Test 15: test_last_updated_timestamp (last_updated timestamp field present, tracks loads)
### Test 16: test_query_term_ancestors (Unnest ancestor_term_ids, traverse hierarchy)
### Test 17: test_concurrent_cf_stacks (Multiple deployments, isolated tables)
### Test 18: test_missing_properties (Missing Database/Table, CF receives FAILED)
### Test 19: test_athena_failure (Query fails, error propagated to CF)
### Test 20: test_table_metadata_files (Iceberg metadata in S3, versioned snapshots)
