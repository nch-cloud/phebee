# Integration Test Plan: get_load_status.py

## Lambda Metadata

**Purpose**: Get Neptune load job status by load_job_id.

**Dependencies**:
- Neptune: get_load_status utility

**Key Operations**:
- Calls get_load_status with load_job_id
- Returns Neptune load status response
- Raises exception if status not "200 OK"

---

## Integration Test Scenarios

### Test 1: test_get_load_status_completed (Load completed, status 200 OK, payload returned)
### Test 2: test_get_load_status_in_progress (Load running, status OK, payload shows progress)
### Test 3: test_get_load_status_failed (Load failed, status OK but payload shows failure)
### Test 4: test_get_load_status_missing_id (load_job_id not provided, error)
### Test 5: test_get_load_status_invalid_id (Nonexistent load_job_id, Neptune error)
### Test 6: test_get_load_status_response_structure (Contains status, payload with Neptune details)
### Test 7: test_get_load_status_raises_on_non_200 (Neptune returns non-200, exception raised)
### Test 8: test_get_load_status_payload_details (Payload includes overallStatus, errors, etc.)
### Test 9: test_get_load_status_concurrent_checks (Check 5 loads concurrently)
### Test 10: test_get_load_status_idempotent (Check same load twice, same result)
### Test 11: test_get_load_status_integration_with_start_load (Check load started by start_load)
### Test 12: test_get_load_status_step_functions (Invoked by Step Functions, result used for routing)
### Test 13: test_get_load_status_polling (Poll until completion workflow)
### Test 14: test_get_load_status_neptune_unavailable (Neptune API down, error handled)
### Test 15: test_get_load_status_logging (Load ID and status logged)
### Test 16: test_get_load_status_performance (<1s response)
### Test 17: test_get_load_status_cancelled_load (Cancelled load, status reflected)
### Test 18: test_get_load_status_queued_load (Queued load, status shows queue position)
### Test 19: test_get_load_status_error_details (Failed load, error details in payload)
### Test 20: test_get_load_status_multiple_runs (Check loads from different runs, isolated)
