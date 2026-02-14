# Integration Test Plan: notify_bulk_import_failure.py

## Lambda Metadata

**Purpose**: Send failure notification for failed bulk import process and fire EventBridge event for error handling workflows.

**Dependencies**:
- EventBridge: fire_event utility
- Input: run_id, status, error

**Key Operations**:
- Logs failure details
- Fires "bulk_import_failure" EventBridge event
- Returns failure metadata with error details

---

## Integration Test Scenarios

### Test 1: test_notify_failure_with_error_details
- Simulate failed bulk import
- Invoke with run_id, status="FAILED", error details
- Verify event fired with error information
- Verify error logged to CloudWatch
- Verify response includes error

### Test 2: test_event_includes_error_context
- Invoke with detailed error object
- Verify EventBridge event contains run_id, timestamp, error
- Verify event detail-type is "bulk_import_failure"
- Verify error structure preserved in event

### Test 3: test_missing_run_id
- Invoke without run_id
- Verify lambda handles gracefully
- Verify response indicates missing field
- Verify event still fired (or error handled)

### Test 4: test_missing_error_field
- Invoke without error details
- Verify lambda succeeds with default error
- Verify event fired with empty/default error
- Verify no crash from missing error

### Test 5: test_error_types
- Test with string error
- Test with dict error (exception details)
- Test with nested error object
- Verify all formats handled correctly

### Test 6: test_failure_timestamp_format
- Invoke notification
- Verify failure_time is ISO 8601
- Verify timestamp is UTC
- Verify accuracy

### Test 7: test_error_logging
- Invoke with specific error
- Check CloudWatch logs
- Verify error logged at ERROR level
- Verify run_id in log context
- Verify error details logged

### Test 8: test_multiple_failure_notifications
- Fail 3 separate bulk imports
- Send failure notifications
- Verify 3 events fired
- Verify each has correct error context

### Test 9: test_failure_notification_idempotency
- Send same failure notification twice
- Verify both succeed
- Verify two events fired
- Verify timestamps differ

### Test 10: test_response_structure
- Invoke failure notification
- Verify statusCode: 200 (notification succeeded)
- Verify body.message: "Bulk import failed"
- Verify body.status: "FAILED"
- Verify body.error present

### Test 11: test_exception_during_notification
- Simulate EventBridge failure
- Invoke notification
- Verify lambda catches exception
- Verify statusCode: 500
- Verify error in response

### Test 12: test_unicode_in_error_message
- Use Unicode error message
- Verify notification succeeds
- Verify Unicode preserved
- Verify no encoding issues

### Test 13: test_large_error_payload
- Send error with large stack trace
- Verify notification succeeds
- Verify event payload size acceptable
- Verify no truncation or size limits exceeded

### Test 14: test_error_with_nested_exception
- Send error with nested exception chain
- Verify full error structure captured
- Verify event includes all error levels
- Verify error is serializable

### Test 15: test_alert_system_integration
- Configure EventBridge rule for bulk_import_failure
- Connect to SNS topic for alerts
- Send failure notification
- Verify SNS alert triggered
- Verify alert contains error details

### Test 16: test_status_field_values
- Test with status="FAILED"
- Test with status="ERROR"
- Verify both accepted
- Verify status reflected in event and response

### Test 17: test_concurrent_failure_notifications
- Simulate 5 concurrent failures
- Send notifications concurrently
- Verify all succeed
- Verify 5 events fired
- Verify no cross-contamination

### Test 18: test_failure_after_partial_success
- Simulate bulk import that partially completed
- Send failure with context
- Verify error includes partial completion info
- Verify event useful for debugging

### Test 19: test_json_serialization_of_errors
- Send complex error object
- Verify JSON serialization succeeds
- Verify no circular reference issues
- Verify error structure maintained

### Test 20: test_monitoring_integration
- Send failure notification
- Verify CloudWatch metrics updated
- Verify alarms triggered (if configured)
- Verify failure visible in monitoring dashboards
