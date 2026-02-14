# Integration Test Plan: create_subject_terms_by_project_term_table.py

## Lambda Metadata

**Purpose**: CloudFormation custom resource to create subject_terms table partitioned by (project_id, term_id) for term-filtered queries.

**Dependencies**:
- Athena: CREATE TABLE execution
- CloudFormation: Custom resource handler

**Key Operations**:
- Creates Iceberg table partitioned by project_id and term_id
- Optimized for "all subjects with term X in project Y" queries
- Schema includes subject info, term info, qualifiers, evidence_count, dates
- Handles CF lifecycle events

---

## Integration Test Scenarios

### Test 1: test_create_table_dual_partition (Partitioned by (project_id, term_id), efficient for term queries)
### Test 2: test_query_by_project_and_term (Query subjects by project+term, uses partition pruning)
### Test 3: test_query_all_subjects_in_project (Query by project_id only, efficient)
### Test 4: test_schema_includes_qualifiers (qualifiers array field present and queryable)
### Test 5: test_schema_includes_evidence_dates (first_evidence_date, last_evidence_date present)
### Test 6: test_multiple_projects_isolated (Data from different projects in separate partitions)
### Test 7: test_create_idempotent (Create twice, IF NOT EXISTS prevents error)
### Test 8: test_cloudformation_success_response (CF receives SUCCESS, stack continues)
### Test 9: test_athena_query_completion_wait (Lambda waits for query SUCCEEDED status)
### Test 10: test_s3_location_separate_from_by_subject (Different S3 location than by_subject table)
### Test 11: test_table_metadata_includes_termlink_id (termlink_id field present for joins)
### Test 12: test_concurrent_cf_stack_deployment (Multiple stacks, no conflicts)
### Test 13: test_update_request_handling (RequestType: Update, no schema changes, success)
### Test 14: test_delete_request_handling (RequestType: Delete, logged, table not dropped)
### Test 15: test_partition_performance (Query with project_id+term_id very fast, <1s)
### Test 16: test_table_in_glue_catalog (Table registered in Glue, visible to other services)
### Test 17: test_iceberg_table_properties (TBLPROPERTIES correct, Iceberg format)
### Test 18: test_query_results_location (Athena results written to correct S3 path)
### Test 19: test_missing_database_property (Database not provided, CF receives FAILED)
### Test 20: test_athena_failure_handling (Query fails, error sent to CloudFormation)
