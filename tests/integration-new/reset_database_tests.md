# Integration Test Plan: reset_database.py

## Lambda Metadata

**Purpose**: Reset both DynamoDB table and Neptune database, clearing all data (used for testing/cleanup).

**Dependencies**:
- DynamoDB: reset_dynamodb_table utility
- Neptune: reset_neptune_database utility

**Key Operations**:
- Calls reset_dynamodb_table (clears all items)
- Calls reset_neptune_database (drops all graphs/triples)
- Returns success status

---

## Integration Test Scenarios

### Test 1: test_reset_database_success (Both DynamoDB and Neptune reset, statusCode 200)
### Test 2: test_reset_database_dynamodb_cleared (After reset, DynamoDB table empty)
### Test 3: test_reset_database_neptune_cleared (After reset, Neptune has no triples)
### Test 4: test_reset_database_response_structure (Returns statusCode, success field)
### Test 5: test_reset_database_error_handling (One reset fails, statusCode 500, error message)
### Test 6: test_reset_database_logging (Reset actions logged)
### Test 7: test_reset_database_concurrent_resets (Multiple resets queued, handled safely)
### Test 8: test_reset_database_idempotent (Reset twice, both succeed, no issues)
### Test 9: test_reset_database_dynamodb_error (DynamoDB unavailable, error caught)
### Test 10: test_reset_database_neptune_error (Neptune unavailable, error caught)
### Test 11: test_reset_database_partial_failure (DynamoDB succeeds, Neptune fails, error returned)
### Test 12: test_reset_database_after_data_load (Load data → Reset → Verify empty)
### Test 13: test_reset_database_integration_test_setup (Used in test setup to clean environment)
### Test 14: test_reset_database_preserves_table_structure (DynamoDB table schema intact after reset)
### Test 15: test_reset_database_removes_all_graphs (Neptune named graphs all dropped)
### Test 16: test_reset_database_performance (Reset completes in <30s)
### Test 17: test_reset_database_test_only_operation (Not used in production, test-only function)
### Test 18: test_reset_database_authorization (Should require admin permissions)
### Test 19: test_reset_database_audit_logging (Reset action logged to audit trail)
### Test 20: test_reset_database_recovery (After reset, new data can be loaded successfully)
