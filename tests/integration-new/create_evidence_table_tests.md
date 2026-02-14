# Integration Test Plan: create_evidence_table.py

## Lambda Metadata

**Purpose**: CloudFormation custom resource to create Iceberg evidence table with optimized schema and partitioning.

**Dependencies**:
- Athena: Execute CREATE TABLE SQL
- S3: Table location, query results
- CloudFormation: Custom resource lifecycle (Create/Update/Delete)

**Key Operations**:
- Handles CloudFormation RequestType (Create/Update/Delete)
- Creates Iceberg table with complex schema (structs, arrays)
- Partitions by bucket(64, subject_id) and created_date
- Waits for Athena query completion
- Sends success/failure response to CloudFormation

---

## Integration Test Scenarios

### Test 1: test_create_table_success (RequestType: Create, table created successfully, CloudFormation receives SUCCESS)
### Test 2: test_create_table_already_exists (CREATE IF NOT EXISTS, no error if exists, idempotent)
### Test 3: test_create_table_schema_verification (Query table, verify all columns present, types correct)
### Test 4: test_create_table_partitioning (Verify bucket(64, subject_id) and created_date partitions)
### Test 5: test_create_table_struct_fields (term_source, note_context, creator structs queryable)
### Test 6: test_create_table_array_qualifiers (qualifiers array field works, can query/filter)
### Test 7: test_update_request_noop (RequestType: Update, table not modified, success returned)
### Test 8: test_delete_request_noop (RequestType: Delete, logged only, table not dropped)
### Test 9: test_athena_query_timeout (Query exceeds 60s, timeout error, CF failure sent)
### Test 10: test_athena_query_failure (Invalid SQL, failure status, error sent to CF)
### Test 11: test_workgroup_managed_results (Workgroup has managed results, OutputLocation not needed)
### Test 12: test_workgroup_unmanaged_results (Unmanaged workgroup, OutputLocation required and used)
### Test 13: test_cloudformation_response_sent (Verify PUT to ResponseURL succeeds, CF stack continues)
### Test 14: test_physical_resource_id (PhysicalResourceId is database.table format)
### Test 15: test_missing_required_properties (Database or Table missing, CF receives FAILED)
### Test 16: test_concurrent_table_creation (Multiple stacks create tables, no conflicts)
### Test 17: test_s3_location_validation (Verify table LOCATION points to correct S3 path)
### Test 18: test_table_properties (TBLPROPERTIES has table_type=ICEBERG, format=parquet)
### Test 19: test_query_results_cleanup (Athena result files cleaned up or retained per config)
### Test 20: test_create_in_different_database (Create table in non-default database, succeeds)
