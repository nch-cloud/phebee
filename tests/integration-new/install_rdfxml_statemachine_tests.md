# InstallRDFXMLSFN State Machine Integration Tests

## Overview

The InstallRDFXMLSFN state machine orchestrates the loading of RDF/XML ontology files into Neptune. It starts a bulk load job, polls for completion, and handles success/failure states.

**State Machine**: `InstallRDFXMLSFN`
**Type**: Reusable workflow (called by ontology update state machines)
**Execution Mode**: Synchronous (startExecution.sync:2)

## Architecture

```
Start Load
    ↓
  Wait (30s)
    ↓
Get Load Status
    ↓
  Choice
    ├─ IN_PROGRESS/NOT_STARTED/IN_QUEUE → Wait (loop)
    ├─ COMPLETED → Success
    └─ Other → Fail
```

## Test Scenarios

### 1. Happy Path - Successful Load

**Objective**: Verify complete successful ontology load workflow

**Test Steps**:
1. Start state machine execution with valid RDF/XML file
   ```json
   {
     "source": "s3://bucket/ontologies/test.owl",
     "graph_name": "test_ontology~v1.0"
   }
   ```
2. Verify StartLoadFunction is invoked with correct parameters
3. Verify state machine waits 30 seconds
4. Verify GetLoadStatusFunction is invoked with load_id from start response
5. Poll load status until LOAD_COMPLETED
6. Verify execution completes with Success state

**Expected Results**:
- State machine execution status: SUCCEEDED
- Load job status: LOAD_COMPLETED
- RDF/XML data loaded to Neptune graph
- Execution time: ~30s + load time (varies by file size)

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/test.owl",
        "graph_name": "test_ontology~v1.0"
    })
)

# Wait for completion
execution_arn = response['executionArn']
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=execution_arn)

# Verify result
execution = sfn_client.describe_execution(executionArn=execution_arn)
assert execution['status'] == 'SUCCEEDED'
```

---

### 2. Load in Progress - Multiple Polling Cycles

**Objective**: Verify state machine correctly polls until load completes

**Test Steps**:
1. Start execution with large RDF/XML file requiring multiple poll cycles
2. Monitor execution events
3. Count number of Wait → GetLoadStatus cycles
4. Verify load eventually completes

**Expected Results**:
- Multiple Wait/GetLoadStatus cycles observed
- Each cycle waits 30 seconds
- Final status: SUCCEEDED
- Load status transitions: LOAD_IN_QUEUE → LOAD_IN_PROGRESS → LOAD_COMPLETED

**Verification**:
```python
# Track state transitions
history = sfn_client.get_execution_history(executionArn=execution_arn)
wait_states = [e for e in history['events'] if e['type'] == 'TaskStateEntered' and 'Wait' in e.get('stateEnteredEventDetails', {}).get('name', '')]
assert len(wait_states) >= 2  # At least 2 polling cycles
```

---

### 3. Load Failure - Neptune Error

**Objective**: Verify state machine handles Neptune load failures

**Test Steps**:
1. Start execution with invalid RDF/XML file (malformed syntax)
2. Wait for load to fail
3. Verify state machine transitions to Fail state

**Expected Results**:
- State machine execution status: FAILED
- Load status: LOAD_FAILED
- Error details logged

**Verification**:
```python
# Start with invalid file
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/invalid.owl",
        "graph_name": "test_invalid"
    })
)

# Wait for failure
waiter = sfn_client.get_waiter('execution_failed')
waiter.wait(executionArn=response['executionArn'])

execution = sfn_client.describe_execution(executionArn=response['executionArn'])
assert execution['status'] == 'FAILED'
```

---

### 4. Missing S3 File

**Objective**: Verify state machine handles missing source files

**Test Steps**:
1. Start execution with non-existent S3 path
2. Verify StartLoadFunction returns error
3. Verify state machine fails appropriately

**Expected Results**:
- StartLoadFunction raises exception
- State machine execution status: FAILED
- Error indicates S3 file not found

**Verification**:
```python
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/nonexistent.owl",
        "graph_name": "test_missing"
    })
)

# Check for S3 error
execution = sfn_client.describe_execution(executionArn=response['executionArn'])
assert 'NoSuchKey' in execution.get('cause', '') or 'not found' in execution.get('cause', '').lower()
```

---

### 5. Lambda Retry Logic

**Objective**: Verify retry configuration for Lambda invocations

**Test Steps**:
1. Monitor Lambda invocation attempts during transient failures
2. Verify exponential backoff (2x) with max 6 attempts
3. Verify interval starts at 2 seconds

**Expected Results**:
- Retry configuration: IntervalSeconds=2, MaxAttempts=6, BackoffRate=2
- Retries on: Lambda.ServiceException, Lambda.AWSLambdaException, Lambda.SdkClientException
- Successful retry recovers from transient errors

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(stateMachineArn=state_machine_arn)
definition = json.loads(sm_def['definition'])

# Verify retry config for Start Load state
start_load_retries = definition['States']['Start Load']['Retry']
assert start_load_retries[0]['IntervalSeconds'] == 2
assert start_load_retries[0]['MaxAttempts'] == 6
assert start_load_retries[0]['BackoffRate'] == 2
```

---

### 6. Load Cancellation

**Objective**: Verify behavior when state machine execution is cancelled

**Test Steps**:
1. Start execution with large file
2. Cancel execution while load is in progress
3. Verify Neptune load job status

**Expected Results**:
- State machine execution status: ABORTED
- Neptune load job may continue or be cancelled (implementation-dependent)
- No data corruption in Neptune

**Verification**:
```python
# Start execution
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/large.owl",
        "graph_name": "test_cancel"
    })
)

# Wait for it to be in progress
time.sleep(35)

# Stop execution
sfn_client.stop_execution(executionArn=response['executionArn'])

# Verify stopped
execution = sfn_client.describe_execution(executionArn=response['executionArn'])
assert execution['status'] == 'ABORTED'
```

---

### 7. Concurrent Executions

**Objective**: Verify multiple concurrent loads don't interfere

**Test Steps**:
1. Start 3 concurrent executions with different ontology files
2. Monitor all executions
3. Verify all complete successfully

**Expected Results**:
- All 3 executions succeed independently
- Load jobs execute in parallel or queue appropriately
- No data corruption or conflicts

**Verification**:
```python
# Start concurrent executions
execution_arns = []
for i in range(3):
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"concurrent-load-{i}-{int(time.time())}",
        input=json.dumps({
            "source": f"s3://bucket/ontologies/test{i}.owl",
            "graph_name": f"test_concurrent_{i}~v1.0"
        })
    )
    execution_arns.append(response['executionArn'])

# Wait for all to complete
for arn in execution_arns:
    waiter = sfn_client.get_waiter('execution_succeeded')
    waiter.wait(executionArn=arn)
```

---

### 8. Named Graph Configuration

**Objective**: Verify correct named graph URI handling

**Test Steps**:
1. Start execution with specific graph_name
2. Verify StartLoadFunction receives correct graph_name parameter
3. Verify data loaded to correct named graph in Neptune

**Expected Results**:
- graph_name passed correctly to Neptune loader
- Data queryable in specified named graph
- No data leaks to default graph

**Verification**:
```python
# Start execution with specific graph
graph_name = "test_ontology~v2.0"
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/test.owl",
        "graph_name": graph_name
    })
)

# Wait for completion
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify through get_source_info
lambda_client = boto3.client('lambda')
result = lambda_client.invoke(
    FunctionName='GetSourceInfoFunction',
    Payload=json.dumps({"source": "test_ontology", "version": "v2.0"})
)
assert result['StatusCode'] == 200
```

---

### 9. IAM Role Configuration

**Objective**: Verify correct Neptune loader role is passed

**Test Steps**:
1. Extract LoaderRole from state machine definition substitutions
2. Start execution
3. Verify StartLoadFunction receives correct role ARN

**Expected Results**:
- LoaderRole substitution correctly resolved
- StartLoadFunction payload includes correct IAM role
- Neptune loader has permissions to read S3

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(stateMachineArn=state_machine_arn)
definition = json.loads(sm_def['definition'])

# Extract LoaderRole parameter
start_load_params = definition['States']['Start Load']['Parameters']['Payload']
assert 'role' in start_load_params
```

---

### 10. Large File Timeout

**Objective**: Verify state machine doesn't timeout on large files

**Test Steps**:
1. Start execution with very large RDF/XML file (100MB+)
2. Monitor execution duration
3. Verify successful completion despite long load time

**Expected Results**:
- Execution completes successfully
- No timeout errors
- Multiple polling cycles executed

**Verification**:
```python
# Start with large file
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/very-large.owl",
        "graph_name": "test_large~v1.0"
    })
)

# Wait with extended timeout
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=response['executionArn'],
    WaiterConfig={'MaxAttempts': 120}  # 120 attempts * 15s = 30 minutes
)
```

---

### 11. Invalid Graph Name

**Objective**: Verify handling of invalid graph names

**Test Steps**:
1. Start execution with invalid graph_name (special characters, too long)
2. Verify appropriate error handling

**Expected Results**:
- Execution fails with validation error
- Error message indicates invalid graph name

**Verification**:
```python
# Test with invalid characters
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "source": "s3://bucket/ontologies/test.owl",
        "graph_name": "test/invalid*name"
    })
)

# Should fail
waiter = sfn_client.get_waiter('execution_failed')
waiter.wait(executionArn=response['executionArn'])
```

---

### 12. ResultPath Preservation

**Objective**: Verify state outputs preserve execution context

**Test Steps**:
1. Start execution
2. Examine execution output
3. Verify startLoader and loadStatus are preserved in output

**Expected Results**:
- Final output contains startLoader.Payload.payload.loadId
- Final output contains loadStatus.Payload
- Original input preserved

**Verification**:
```python
# Get execution output
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

# Verify structure
assert 'startLoader' in output
assert 'loadStatus' in output
assert 'source' in output  # Original input preserved
assert 'graph_name' in output
```

---

## Test Data Requirements

### Valid RDF/XML Files
- Small file (< 1MB): `s3://bucket/test-data/ontologies/small.owl`
- Medium file (10MB): `s3://bucket/test-data/ontologies/medium.owl`
- Large file (100MB): `s3://bucket/test-data/ontologies/large.owl`

### Invalid Files
- Malformed XML: `s3://bucket/test-data/ontologies/malformed.owl`
- Empty file: `s3://bucket/test-data/ontologies/empty.owl`
- Non-XML file: `s3://bucket/test-data/ontologies/not-xml.txt`

### Graph Names
- Valid: `hpo~v2024-04-26`, `mondo~v2024-01-01`, `eco~v2023-09-14`
- Invalid: `test/invalid`, `name*with*special`, `verylongnamethatexceedslimits...`

---

## Performance Benchmarks

| File Size | Expected Load Time | Expected Execution Time | Max Poll Cycles |
|-----------|-------------------|------------------------|-----------------|
| 1 MB      | 10-30s            | ~60s                   | 2-3             |
| 10 MB     | 30-60s            | ~90s                   | 3-4             |
| 100 MB    | 2-5 min           | ~6 min                 | 10-12           |

---

## Error Scenarios

| Error Type | Trigger | Expected State Machine Behavior |
|------------|---------|--------------------------------|
| S3 NoSuchKey | Non-existent file | Fail at Start Load step |
| S3 Access Denied | Missing IAM permissions | Fail at Start Load step |
| Malformed RDF | Invalid XML/RDF | Load status returns LOAD_FAILED → Fail state |
| Neptune unavailable | Cluster down | Retry Start Load up to 6 times → Fail |
| Load timeout | Very large file | Continue polling until complete |

---

## Notes

- This state machine is invoked synchronously by ontology update workflows
- Wait time of 30 seconds is fixed - consider making configurable for performance tuning
- No explicit timeout on the state machine - relies on Neptune load job completion
- Load status polling continues indefinitely until terminal state reached
- Multiple simultaneous loads are supported via Neptune's queueing mechanism
