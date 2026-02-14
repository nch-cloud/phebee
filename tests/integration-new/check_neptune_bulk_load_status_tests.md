# Integration Test Plan: check_neptune_bulk_load_status.py

## Lambda Metadata

**Purpose**: Check status of a single Neptune bulk load using load_id.

**Dependencies**:
- Neptune: get_load_status API

**Key Operations**:
- Queries Neptune loader API for load status
- Returns status (LOAD_COMPLETED, LOAD_FAILED, LOAD_IN_PROGRESS, etc.)
- Logs error details if failed
- Returns payload with full load details

---

## Integration Test Scenarios

### Test 1: test_check_load_completed (Load succeeded, status: LOAD_COMPLETED)
### Test 2: test_check_load_in_progress (Load running, status: LOAD_IN_PROGRESS)
### Test 3: test_check_load_failed (Load failed, status: LOAD_FAILED, error details logged)
### Test 4: test_check_load_cancelled (Load cancelled, status: LOAD_CANCELLED)
### Test 5: test_check_load_missing_id (load_id not provided, error)
### Test 6: test_check_load_invalid_id (Nonexistent load_id, error from Neptune)
### Test 7: test_check_load_response_structure (Returns statusCode, body.load_id, body.status, body.details)
### Test 8: test_check_load_details_payload (Payload includes Neptune load metadata)
### Test 9: test_check_load_error_logs (Failed load, errorLogs present in payload)
### Test 10: test_check_load_full_uri (fullUri in response for detailed view)
### Test 11: test_check_load_concurrent_checks (Check 5 loads concurrently, all succeed)
### Test 12: test_check_load_polling_workflow (Poll load every 10s until completed)
### Test 13: test_check_load_step_functions_choice (Status used in Choice state routing)
### Test 14: test_check_load_idempotent (Check same load twice, same status)
### Test 15: test_check_load_status_transitions (Status progresses: queued → in_progress → completed)
### Test 16: test_check_load_integration_with_start (Check load started by start_neptune_bulk_load)
### Test 17: test_check_load_neptune_api_error (Neptune unavailable, error handled gracefully)
### Test 18: test_check_load_response_on_failure (Failed load, statusCode 200 but status=LOAD_FAILED)
### Test 19: test_check_load_logs_monitoring (Load details logged for observability)
### Test 20: test_check_load_performance (Status check completes in <1s)
