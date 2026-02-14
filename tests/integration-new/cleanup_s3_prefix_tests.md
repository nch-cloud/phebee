# Integration Test Plan: cleanup_s3_prefix.py

## Lambda Metadata

**Purpose**: Delete all objects under an S3 prefix, used for cleaning up temporary files and old batches.

**Dependencies**:
- S3: list_objects_v2, delete_objects
- Input: bucket, prefix

**Key Operations**:
- Lists all objects with prefix (paginated)
- Deletes in batches of 1000 (S3 limit)
- Returns total deleted count

---

## Integration Test Scenarios

### Test 1: test_cleanup_small_prefix (100 objects, all deleted)
### Test 2: test_cleanup_large_prefix (5000 objects, deleted in batches of 1000)
### Test 3: test_cleanup_empty_prefix (No objects, returns 0, no error)
### Test 4: test_cleanup_nested_prefix (Prefix with subdirectories, all objects deleted)
### Test 5: test_cleanup_missing_bucket (bucket not provided, error)
### Test 6: test_cleanup_missing_prefix (prefix not provided, error)
### Test 7: test_cleanup_nonexistent_bucket (Bucket doesn't exist, error)
### Test 8: test_cleanup_nonexistent_prefix (Prefix with no objects, deleted_count=0)
### Test 9: test_cleanup_response_structure (Returns deleted_count and status)
### Test 10: test_cleanup_special_chars_in_prefix (Prefix with special chars, handled correctly)
### Test 11: test_cleanup_unicode_in_keys (Object keys with Unicode, deleted properly)
### Test 12: test_cleanup_large_files (Prefix with large files, no size issues)
### Test 13: test_cleanup_concurrent_cleanups (Clean 3 prefixes concurrently, no interference)
### Test 14: test_cleanup_idempotent (Clean same prefix twice, second returns 0)
### Test 15: test_cleanup_integration_with_materialize (Clean old batches after materialization)
### Test 16: test_cleanup_s3_error (S3 unavailable, error raised)
### Test 17: test_cleanup_logging (Prefix and count logged)
### Test 18: test_cleanup_pagination (Handles >1000 objects with pagination)
### Test 19: test_cleanup_performance (1000 objects deleted in <10s)
### Test 20: test_cleanup_step_functions_integration (Invoked in cleanup step, succeeds)
