# Integration Test Plan: check_bulk_load_status.py

## Lambda Metadata

**Purpose**: Check the status of both domain and provenance Neptune bulk loads during the bulk import state machine workflow.

**Dependencies**:
- Neptune: get_load_status utility function
- Input: domain_load_id, prov_load_id, run_id

**Key Operations**:
- Queries Neptune loader API for both domain and provenance load status
- Determines overall status based on both loads
- Returns SUCCESS if both completed, FAILED if either failed, PENDING if in progress

---

## Integration Test Scenarios

### Test 1: test_both_loads_completed_successfully
**Description**: Both domain and provenance loads completed

**Setup**:
- Start two Neptune bulk loads (domain and provenance)
- Wait for both to complete successfully

**Action**:
```json
{
  "domain_load_id": "domain-load-123",
  "prov_load_id": "prov-load-456",
  "run_id": "run-789"
}
```

**Assertions**:
- status: "SUCCESS"
- run_id: "run-789"
- domain_load_id and prov_load_id included
- No error field present

**Cleanup**:
- None (loads already completed)

### Test 2: test_both_loads_in_progress
**Description**: Both loads still running

**Setup**:
- Start two Neptune bulk loads
- Query immediately before completion

**Action**:
```json
{
  "domain_load_id": "domain-load-234",
  "prov_load_id": "prov-load-567",
  "run_id": "run-890"
}
```

**Assertions**:
- status: "PENDING"
- run_id: "run-890"
- domain_status: "LOAD_IN_PROGRESS"
- prov_status: "LOAD_IN_PROGRESS"

**Cleanup**:
- Wait for loads to complete

### Test 3: test_domain_load_failed
**Description**: Domain load failed, provenance succeeded

**Setup**:
- Start domain load with invalid data (to cause failure)
- Start provenance load with valid data
- Wait for both to finish

**Action**:
```json
{
  "domain_load_id": "domain-load-fail",
  "prov_load_id": "prov-load-345",
  "run_id": "run-901"
}
```

**Assertions**:
- status: "FAILED"
- run_id: "run-901"
- domain_status: "LOAD_FAILED"
- prov_status: "LOAD_COMPLETED" or in progress

**Cleanup**:
- Clean up failed load artifacts

### Test 4: test_prov_load_failed
**Description**: Provenance load failed, domain succeeded

**Setup**:
- Start provenance load with invalid data
- Start domain load with valid data
- Wait for both to finish

**Action**:
```json
{
  "domain_load_id": "domain-load-456",
  "prov_load_id": "prov-load-fail",
  "run_id": "run-012"
}
```

**Assertions**:
- status: "FAILED"
- run_id: "run-012"
- domain_status: "LOAD_COMPLETED" or in progress
- prov_status: "LOAD_FAILED"

**Cleanup**:
- Clean up failed load artifacts

### Test 5: test_both_loads_failed
**Description**: Both loads failed

**Setup**:
- Start both loads with invalid data
- Wait for failure

**Action**:
```json
{
  "domain_load_id": "domain-fail-1",
  "prov_load_id": "prov-fail-1",
  "run_id": "run-123"
}
```

**Assertions**:
- status: "FAILED"
- domain_status: "LOAD_FAILED"
- prov_status: "LOAD_FAILED"

**Cleanup**:
- Clean up failed loads

### Test 6: test_domain_load_cancelled
**Description**: Domain load was cancelled

**Setup**:
- Start domain load
- Cancel it before completion
- Complete provenance load normally

**Action**:
```json
{
  "domain_load_id": "domain-cancelled",
  "prov_load_id": "prov-load-678",
  "run_id": "run-234"
}
```

**Assertions**:
- status: "FAILED"
- domain_status: "LOAD_CANCELLED"

**Cleanup**:
- Clean up cancelled load

### Test 7: test_missing_domain_load_id
**Description**: Handle missing domain_load_id parameter

**Setup**:
- None

**Action**:
```json
{
  "prov_load_id": "prov-load-789",
  "run_id": "run-345"
}
```

**Assertions**:
- status: "FAILED"
- domain_status: "UNKNOWN"
- prov_status: "UNKNOWN"
- error: "Missing required parameters"

**Cleanup**:
- None

### Test 8: test_missing_prov_load_id
**Description**: Handle missing prov_load_id parameter

**Setup**:
- None

**Action**:
```json
{
  "domain_load_id": "domain-load-890",
  "run_id": "run-456"
}
```

**Assertions**:
- status: "FAILED"
- domain_status: "UNKNOWN"
- prov_status: "UNKNOWN"
- error: "Missing required parameters"

**Cleanup**:
- None

### Test 9: test_missing_run_id
**Description**: Handle missing run_id parameter

**Setup**:
- None

**Action**:
```json
{
  "domain_load_id": "domain-load-901",
  "prov_load_id": "prov-load-012"
}
```

**Assertions**:
- status: "FAILED"
- run_id: None
- error: "Missing required parameters"

**Cleanup**:
- None

### Test 10: test_invalid_domain_load_id
**Description**: Handle non-existent domain load ID

**Setup**:
- None

**Action**:
```json
{
  "domain_load_id": "nonexistent-load-id",
  "prov_load_id": "prov-load-123",
  "run_id": "run-567"
}
```

**Assertions**:
- status: "FAILED"
- error field present
- Error indicates load ID not found

**Cleanup**:
- None

### Test 11: test_invalid_prov_load_id
**Description**: Handle non-existent provenance load ID

**Setup**:
- Start valid domain load

**Action**:
```json
{
  "domain_load_id": "domain-load-valid",
  "prov_load_id": "nonexistent-prov",
  "run_id": "run-678"
}
```

**Assertions**:
- status: "FAILED"
- error field present
- Error indicates prov load ID not found

**Cleanup**:
- None

### Test 12: test_domain_complete_prov_pending
**Description**: Domain completed, provenance still in progress

**Setup**:
- Start domain load (small, completes quickly)
- Start provenance load (larger, takes longer)
- Wait for domain to complete

**Action**:
```json
{
  "domain_load_id": "domain-quick",
  "prov_load_id": "prov-slow",
  "run_id": "run-789"
}
```

**Assertions**:
- status: "PENDING"
- domain_status: "LOAD_COMPLETED"
- prov_status: "LOAD_IN_PROGRESS" or other pending state

**Cleanup**:
- Wait for prov load to complete

### Test 13: test_prov_complete_domain_pending
**Description**: Provenance completed, domain still in progress

**Setup**:
- Start provenance load (small)
- Start domain load (larger)
- Wait for provenance to complete

**Action**:
```json
{
  "domain_load_id": "domain-slow",
  "prov_load_id": "prov-quick",
  "run_id": "run-890"
}
```

**Assertions**:
- status: "PENDING"
- domain_status: "LOAD_IN_PROGRESS"
- prov_status: "LOAD_COMPLETED"

**Cleanup**:
- Wait for domain load to complete

### Test 14: test_neptune_api_error
**Description**: Handle Neptune API errors gracefully

**Setup**:
- Simulate Neptune API unavailable or timeout

**Action**:
```json
{
  "domain_load_id": "domain-test",
  "prov_load_id": "prov-test",
  "run_id": "run-901"
}
```

**Assertions**:
- status: "FAILED"
- error field contains exception message
- Lambda doesn't crash

**Cleanup**:
- None

### Test 15: test_concurrent_status_checks
**Description**: Multiple status checks in parallel don't interfere

**Setup**:
- Start single set of loads

**Action** (invoke lambda 5 times concurrently):
```json
{
  "domain_load_id": "domain-concurrent",
  "prov_load_id": "prov-concurrent",
  "run_id": "run-concurrent"
}
```

**Assertions**:
- All 5 invocations return same status
- No race conditions
- Status is consistent across concurrent calls
- No cross-contamination

**Cleanup**:
- Wait for loads to complete

### Test 16: test_step_functions_integration
**Description**: Verify response format works with Step Functions Choice state

**Setup**:
- Complete both loads

**Action**:
```json
{
  "domain_load_id": "domain-sf",
  "prov_load_id": "prov-sf",
  "run_id": "run-sf"
}
```

**Assertions**:
- Response has top-level "status" field
- status is one of: "SUCCESS", "FAILED", "PENDING"
- Step Functions can route based on $.status
- run_id passed through for tracking

**Cleanup**:
- None

### Test 17: test_status_transition_sequence
**Description**: Track load status through completion lifecycle

**Setup**:
- Start both loads

**Action** (poll multiple times):
```json
{
  "domain_load_id": "domain-lifecycle",
  "prov_load_id": "prov-lifecycle",
  "run_id": "run-lifecycle"
}
```

**Assertions**:
- First call: status "PENDING", both "LOAD_IN_PROGRESS"
- Subsequent calls: status remains "PENDING" until both complete
- Final call: status "SUCCESS", both "LOAD_COMPLETED"
- Status never regresses

**Cleanup**:
- None

### Test 18: test_response_includes_all_load_ids
**Description**: Verify all IDs included in success response

**Setup**:
- Complete both loads

**Action**:
```json
{
  "domain_load_id": "domain-ids",
  "prov_load_id": "prov-ids",
  "run_id": "run-ids"
}
```

**Assertions**:
- Response includes domain_load_id
- Response includes prov_load_id
- Response includes run_id
- All IDs match input values

**Cleanup**:
- None

### Test 19: test_idempotency
**Description**: Same input returns same result

**Setup**:
- Complete both loads

**Action** (invoke twice with same params):
```json
{
  "domain_load_id": "domain-idem",
  "prov_load_id": "prov-idem",
  "run_id": "run-idem"
}
```

**Assertions**:
- Both invocations return identical status
- Both return "SUCCESS"
- Response is deterministic
- No side effects from repeated checks

**Cleanup**:
- None

### Test 20: test_error_details_in_failure
**Description**: Verify error details are included for failed loads

**Setup**:
- Start load with invalid data to cause specific error

**Action**:
```json
{
  "domain_load_id": "domain-error-details",
  "prov_load_id": "prov-error-details",
  "run_id": "run-error"
}
```

**Assertions**:
- status: "FAILED"
- domain_status or prov_status shows failure
- Error message is descriptive
- Enough context to diagnose issue

**Cleanup**:
- Clean up failed loads
