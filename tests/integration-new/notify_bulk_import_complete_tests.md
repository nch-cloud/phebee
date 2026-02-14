# Integration Test Plan: notify_bulk_import_complete.py

## Lambda Metadata

**Purpose**: Send completion notification for successful bulk import process and fire EventBridge event for downstream workflows.

**Dependencies**:
- EventBridge: fire_event utility
- Input: run_id, status, emr_job_id, load_id, domain_load_id, prov_load_id

**Key Operations**:
- Logs completion details
- Fires "bulk_import_success" EventBridge event
- Returns completion metadata

---

## Integration Test Scenarios

### Test 1: test_notify_successful_completion
- Complete a bulk import workflow
- Invoke with all completion metadata
- Verify event fired with correct structure
- Verify response includes all metadata
- Check EventBridge event appears in event bus
- Verify downstream listeners triggered

### Test 2: test_event_includes_all_required_fields
- Invoke with complete metadata
- Verify EventBridge event contains run_id, timestamp, domain_load_id, prov_load_id
- Verify event detail-type is "bulk_import_success"
- Verify event source is correct

### Test 3: test_missing_run_id
- Invoke without run_id
- Verify lambda handles gracefully
- Verify response indicates missing field
- Verify no event fired (or event with null run_id)

### Test 4: test_optional_fields_handling
- Invoke with only run_id
- Verify lambda succeeds
- Verify event fired with available fields
- Verify optional fields can be null

### Test 5: test_completion_timestamp_format
- Invoke notification
- Verify timestamp is ISO 8601 format
- Verify timestamp is UTC
- Verify timestamp accuracy (within 1 minute of actual time)

### Test 6: test_logging_includes_metadata
- Invoke with all metadata
- Check CloudWatch logs
- Verify EMR job ID logged
- Verify Neptune load ID logged
- Verify run_id logged

### Test 7: test_multiple_notifications_different_runs
- Complete 3 separate bulk imports
- Send notifications for each
- Verify 3 separate events fired
- Verify no cross-contamination of run data

### Test 8: test_notification_idempotency
- Send same notification twice
- Verify both succeed (200 response)
- Verify two events fired (not idempotent by design)
- Verify events have different timestamps

### Test 9: test_eventbridge_failure_handling
- Simulate EventBridge unavailable
- Invoke notification
- Verify lambda logs error
- Verify error response but doesn't crash

### Test 10: test_response_structure
- Invoke notification
- Verify statusCode: 200
- Verify body.message present
- Verify body.run_id present
- Verify body.completion_time present

### Test 11: test_concurrent_notifications
- Invoke 5 notifications concurrently for different runs
- Verify all succeed
- Verify 5 events fired
- Verify no interference

### Test 12: test_special_characters_in_run_id
- Use run_id with special characters
- Verify notification succeeds
- Verify event fired correctly
- Verify run_id preserved in event

### Test 13: test_large_load_ids
- Use very long load_id strings
- Verify notification succeeds
- Verify event payload size acceptable
- Verify no truncation

### Test 14: test_status_field_variations
- Test with status="SUCCESS"
- Test with status="COMPLETED"
- Verify both accepted
- Verify status included in response

### Test 15: test_event_targeting
- Configure EventBridge rule to capture bulk_import_success
- Send notification
- Verify rule triggered
- Verify downstream lambda/SNS/SQS receives event

### Test 16: test_notification_after_retry
- Simulate failed bulk import
- Retry and succeed
- Send completion notification
- Verify event indicates successful completion
- Verify retry history not exposed

### Test 17: test_json_serialization
- Include all metadata fields
- Verify response is valid JSON
- Verify event payload is valid JSON
- Verify no serialization errors

### Test 18: test_unicode_in_metadata
- Use Unicode characters in optional fields
- Verify notification succeeds
- Verify Unicode preserved in event
- Verify no encoding errors

### Test 19: test_step_functions_integration
- Invoke as final step in Step Functions workflow
- Verify response format compatible
- Verify workflow completes successfully
- Verify execution metadata captured

### Test 20: test_error_response_structure
- Invoke with malformed input to cause error
- Verify statusCode: 500
- Verify body.error present
- Verify body.run_id present (if available)
