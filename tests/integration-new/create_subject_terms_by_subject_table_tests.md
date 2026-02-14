# Integration Test Plan: create_subject_terms_by_subject_table.py

## Lambda Metadata

**Purpose**: CloudFormation custom resource to create subject_terms table partitioned by subject_id for individual subject lookups.

**Dependencies**:
- Athena: CREATE TABLE
- CloudFormation: Custom resource

**Key Operations**:
- Creates Iceberg table partitioned by subject_id only
- Optimized for "get all terms for subject X" queries
- Same schema as by_project_term but different partitioning
- Handles CF custom resource lifecycle

---

## Integration Test Scenarios

### Test 1: test_create_table_subject_partition (Partitioned by subject_id only, single subject queries fast)
### Test 2: test_query_single_subject (Get all terms for subject, partition pruning efficient)
### Test 3: test_schema_matches_by_project_term (Same columns, different partitioning strategy)
### Test 4: test_subject_isolation (Each subject in own partition, no cross-subject scanning)
### Test 5: test_create_idempotent (IF NOT EXISTS, no error on re-creation)
### Test 6: test_cloudformation_integration (CF stack creates table, receives SUCCESS)
### Test 7: test_s3_location_independent (Separate S3 path from other subject_terms tables)
### Test 8: test_concurrent_subject_queries (Multiple subjects queried concurrently, no interference)
### Test 9: test_update_noop (Update request, no changes, success response)
### Test 10: test_delete_logged (Delete request, logged but table kept)
### Test 11: test_athena_workgroup_config (Uses primary workgroup, managed results handling)
### Test 12: test_table_queryable_immediately (Table usable right after creation)
### Test 13: test_partition_count_grows (As subjects added, partitions increase dynamically)
### Test 14: test_iceberg_metadata (Iceberg metadata files in S3, snapshots tracked)
### Test 15: test_glue_catalog_registration (Table in Glue catalog, cross-service accessible)
### Test 16: test_missing_table_name (Table property missing, CF failure)
### Test 17: test_athena_timeout (Long query, timeout handling, CF notified)
### Test 18: test_concurrent_cf_deployments (Multiple stacks, isolated tables)
### Test 19: test_table_properties_verification (Verify TBLPROPERTIES set correctly)
### Test 20: test_query_performance (Single subject query <100ms)
