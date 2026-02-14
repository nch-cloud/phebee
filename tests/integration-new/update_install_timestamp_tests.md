# Integration Test Plan: update_install_timestamp.py

## Lambda Metadata

**Purpose**: Update InstallTimestamp in DynamoDB source record to track when a version was installed/loaded into Neptune.

**Dependencies**:
- DynamoDB: Update item with InstallTimestamp
- Input: source, version

**Key Operations**:
- Queries DynamoDB for source records
- Finds record matching version
- Updates InstallTimestamp to current time
- Returns updated attributes

---

## Integration Test Scenarios

### Test 1: test_update_install_timestamp_success (Update HPO v2024-04-26, InstallTimestamp set)
### Test 2: test_update_install_timestamp_missing_source (source not provided, error)
### Test 3: test_update_install_timestamp_missing_version (version not provided, error)
### Test 4: test_update_install_timestamp_nonexistent_source (Source not in DynamoDB, error)
### Test 5: test_update_install_timestamp_nonexistent_version (Version not found, error)
### Test 6: test_update_install_timestamp_format (InstallTimestamp is ISO 8601 UTC string)
### Test 7: test_update_install_timestamp_idempotent (Update twice, timestamp updated both times)
### Test 8: test_update_install_timestamp_response (Returns updated attributes)
### Test 9: test_update_install_timestamp_preserves_other_fields (Other fields like Version, Assets unchanged)
### Test 10: test_update_install_timestamp_multiple_versions (Multiple versions, only specified one updated)
### Test 11: test_update_install_timestamp_concurrent_updates (Update HPO and MONDO concurrently)
### Test 12: test_update_install_timestamp_integration_with_download (Download → Load → Update timestamp workflow)
### Test 13: test_update_install_timestamp_dynamodb_error (DynamoDB unavailable, error)
### Test 14: test_update_install_timestamp_logging (Source and version logged)
### Test 15: test_update_install_timestamp_region_hardcoded (us-east-2 region hardcoded, works correctly)
### Test 16: test_update_install_timestamp_sk_selection (Correct SK selected when multiple records for version)
### Test 17: test_update_install_timestamp_get_source_records (Uses utility to fetch records)
### Test 18: test_update_install_timestamp_special_chars_in_source (Source name with special chars)
### Test 19: test_update_install_timestamp_performance (<500ms update time)
### Test 20: test_update_install_timestamp_tracks_installations (Multiple installs tracked by timestamp updates)
