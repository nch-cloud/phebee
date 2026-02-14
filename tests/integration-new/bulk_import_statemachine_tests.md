# BulkImportStateMachine Integration Tests

## Overview

The BulkImportStateMachine orchestrates large-scale evidence import using EMR Serverless for parallel processing. It validates input, resolves subject mappings via DynamoDB, processes evidence to Iceberg, generates TTL files, loads to Neptune, materializes analytical tables, and fires completion events.

**State Machine**: `BulkImportStateMachine`
**Type**: Long-running distributed workflow
**Execution Mode**: Asynchronous
**Max Duration**: Hours (depends on data volume)

## Architecture

```
ValidateInput
    ↓
CheckValidation
    ↓
ResolveSubjectMappings (EMR)
    ↓
ProcessWithEMR (EMR - Evidence to Iceberg + TTL)
    ↓
GenerateTTLWithEMR (EMR - TTL generation)
    ↓
StartProjectLoad
    ↓
CheckProjectLoadsExist → MonitorProjectLoad (loop) → CheckProjectLoadComplete
    ↓
StartSubjectsLoad
    ↓
CheckSubjectLoadsExist → MonitorSubjectsLoad (loop) → CheckSubjectsLoadComplete
    ↓
MaterializeProjectSubjectTerms
    ↓
CheckMaterializationStatus
    ↓
NotifySuccess

(All errors → NotifyFailure → FailExecution)
```

## Test Scenarios

### 1. Happy Path - Complete Bulk Import

**Objective**: Verify full end-to-end bulk import workflow

**Test Steps**:
1. Prepare bulk import data:
   - Create CSV with 100 subjects, 1000 evidence records
   - Upload to S3 at `s3://bucket/bulk-imports/test-run-001/evidence.csv`
   - Ensure subjects exist in DynamoDB or will be created
2. Start state machine execution:
   ```json
   {
     "run_id": "test-run-001",
     "input_path": "s3://bucket/bulk-imports/test-run-001/evidence.csv"
   }
   ```
3. Monitor execution through all stages
4. Verify completion and data consistency

**Expected Results**:
- Execution status: SUCCEEDED
- ValidateInput: validates CSV structure and S3 path
- ResolveSubjectMappings EMR job: creates subject_mapping.json
- ProcessWithEMR job: inserts evidence to Iceberg
- GenerateTTLWithEMR job: creates TTL files in projects/ and subjects/ directories
- StartProjectLoad: starts Neptune bulk load for project graphs
- StartSubjectsLoad: starts Neptune bulk load for subjects graph
- MaterializeProjectSubjectTerms: updates analytical tables
- NotifySuccess: fires BulkImportComplete event to EventBridge
- Evidence queryable in Iceberg
- Term links present in Neptune
- Analytical tables updated

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    name=f"bulk-import-test-{int(time.time())}",
    input=json.dumps({
        "run_id": "test-run-001",
        "input_path": "s3://bucket/bulk-imports/test-run-001/evidence.csv"
    })
)

# Wait for completion (may take 30+ minutes)
execution_arn = response['executionArn']
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=execution_arn,
    WaiterConfig={'Delay': 60, 'MaxAttempts': 120}  # Up to 2 hours
)

# Verify evidence in Iceberg
athena_client = boto3.client('athena')
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{evidence_table}
    WHERE run_id = 'test-run-001'
"""
result = execute_athena_query(query)
assert result[0]['count'] == 1000

# Verify EventBridge event fired
events_client = boto3.client('events')
# Check event rule was triggered (requires CloudWatch integration)
```

---

### 2. Validation Failure - Invalid Input Path

**Objective**: Verify validation catches invalid S3 paths

**Test Steps**:
1. Start execution with non-existent S3 path
2. Verify ValidateInput fails
3. Verify NotifyFailure is called

**Expected Results**:
- Execution status: FAILED
- ValidateInput returns validation error
- NotifyFailure fires BulkImportFailed event
- No EMR jobs started

**Verification**:
```python
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-invalid-001",
        "input_path": "s3://bucket/nonexistent/path.csv"
    })
)

# Wait for failure
waiter = sfn_client.get_waiter('execution_failed')
waiter.wait(executionArn=response['executionArn'])

# Verify no EMR jobs were started
emr_client = boto3.client('emr-serverless')
jobs = emr_client.list_job_runs(applicationId=emr_app_id)
test_jobs = [j for j in jobs['jobRuns'] if 'test-invalid-001' in j['name']]
assert len(test_jobs) == 0
```

---

### 3. EMR Job Failure - Subject Mapping Resolution

**Objective**: Verify handling of EMR failures during subject mapping

**Test Steps**:
1. Start execution with CSV containing invalid subject IDs
2. ResolveSubjectMappings EMR job fails
3. Verify workflow transitions to NotifyFailure

**Expected Results**:
- Execution status: FAILED
- ResolveSubjectMappings EMR job status: FAILED
- Error captured in $.error
- NotifyFailure called with error details
- No downstream processing occurs

**Verification**:
```python
# CSV with invalid subject IDs
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-mapping-fail-001",
        "input_path": "s3://bucket/bulk-imports/invalid-subjects.csv"
    })
)

waiter = sfn_client.get_waiter('execution_failed')
waiter.wait(executionArn=response['executionArn'])

# Check execution history for EMR failure
history = sfn_client.get_execution_history(executionArn=response['executionArn'])
emr_failure = [e for e in history['events'] if 'TaskFailed' in e['type'] and 'ResolveSubjectMappings' in str(e)]
assert len(emr_failure) > 0
```

---

### 4. Neptune Load Monitoring - Projects Load

**Objective**: Verify proper monitoring of project load jobs

**Test Steps**:
1. Start execution with data for multiple projects
2. Monitor StartProjectLoad and subsequent polling
3. Verify MonitorProjectLoad executes repeatedly
4. Verify CheckProjectLoadComplete eventually succeeds

**Expected Results**:
- StartProjectLoad returns multiple load IDs (one per project)
- CheckProjectLoadsExist determines loads exist
- MonitorProjectLoad polls every 30 seconds
- Load status transitions: LOAD_IN_QUEUE → LOAD_IN_PROGRESS → LOAD_COMPLETED
- Workflow proceeds to StartSubjectsLoad

**Verification**:
```python
# Monitor execution events
execution_arn = response['executionArn']

# Poll until project loads complete
while True:
    execution = sfn_client.describe_execution(executionArn=execution_arn)
    if execution['status'] != 'RUNNING':
        break

    history = sfn_client.get_execution_history(executionArn=execution_arn)

    # Count MonitorProjectLoad invocations
    monitor_events = [e for e in history['events']
                      if e.get('stateEnteredEventDetails', {}).get('name') == 'MonitorProjectLoad']

    print(f"MonitorProjectLoad called {len(monitor_events)} times")
    time.sleep(30)

# Verify loads completed
assert execution['status'] == 'SUCCEEDED'
```

---

### 5. No Project Loads - Skip to Subjects

**Objective**: Verify workflow skips project monitoring when no project loads

**Test Steps**:
1. Start execution with subject-only data (no project-specific term links)
2. Verify CheckProjectLoadsExist evaluates to 0
3. Verify workflow skips to StartSubjectsLoad directly

**Expected Results**:
- StartProjectLoad returns total_loads: 0
- CheckProjectLoadsExist default branch taken
- MonitorProjectLoad never executed
- StartSubjectsLoad executed immediately

**Verification**:
```python
# Subject-only data
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-subjects-only-001",
        "input_path": "s3://bucket/bulk-imports/subjects-only.csv"
    })
)

# Wait for completion
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify MonitorProjectLoad was never called
history = sfn_client.get_execution_history(executionArn=response['executionArn'])
monitor_project = [e for e in history['events']
                   if e.get('stateEnteredEventDetails', {}).get('name') == 'MonitorProjectLoad']
assert len(monitor_project) == 0
```

---

### 6. Neptune Load Failure - Project Load

**Objective**: Verify handling of Neptune load failures

**Test Steps**:
1. Start execution with malformed TTL files
2. Project load fails
3. Verify workflow transitions to NotifyFailure

**Expected Results**:
- Project load status: LOAD_FAILED
- CheckProjectLoadComplete detects failure
- NotifyFailure called
- Execution status: FAILED

**Verification**:
```python
# Use malformed TTL
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-load-fail-001",
        "input_path": "s3://bucket/bulk-imports/malformed-ttl-test.csv"
    })
)

waiter = sfn_client.get_waiter('execution_failed')
waiter.wait(executionArn=response['executionArn'])

# Verify failure at Neptune load stage
execution = sfn_client.describe_execution(executionArn=response['executionArn'])
output = json.loads(execution.get('output', '{}'))
assert 'LOAD_FAILED' in str(output)
```

---

### 7. Materialization Failure - Continue Anyway

**Objective**: Verify workflow continues despite materialization failures

**Test Steps**:
1. Start execution with valid data
2. Force MaterializeProjectSubjectTerms to fail (e.g., Athena timeout)
3. Verify CheckMaterializationStatus proceeds to NotifySuccess anyway

**Expected Results**:
- MaterializeProjectSubjectTerms fails
- CheckMaterializationStatus: "Always proceed to success - materialization is optional optimization"
- NotifySuccess called
- Execution status: SUCCEEDED

**Verification**:
```python
# Monitor execution that has materialization error
# (Exact trigger depends on implementation - may need to simulate Athena failure)

execution = sfn_client.describe_execution(executionArn=execution_arn)
assert execution['status'] == 'SUCCEEDED'

# Verify materialization was attempted but didn't block success
history = sfn_client.get_execution_history(executionArn=execution_arn)
materialize_events = [e for e in history['events']
                      if 'MaterializeProjectSubjectTerms' in str(e)]
assert len(materialize_events) > 0
```

---

### 8. Large Scale Import - 10K Subjects

**Objective**: Verify scalability with large datasets

**Test Steps**:
1. Generate CSV with 10,000 subjects, 50,000 evidence records
2. Upload to S3
3. Start execution
4. Monitor EMR job metrics (executors, memory, duration)
5. Verify completion

**Expected Results**:
- All EMR jobs complete successfully
- EMR scales appropriately (multiple executors)
- Evidence successfully loaded to Iceberg
- Neptune loads complete
- Analytical tables materialized
- Total duration: < 2 hours

**Verification**:
```python
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-large-scale-001",
        "input_path": "s3://bucket/bulk-imports/10k-subjects.csv"
    })
)

# Wait with extended timeout
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=response['executionArn'],
    WaiterConfig={'Delay': 120, 'MaxAttempts': 60}  # 120 minutes max
)

# Verify all evidence loaded
query = f"""
    SELECT COUNT(DISTINCT subject_id) as subject_count,
           COUNT(*) as evidence_count
    FROM {database}.{evidence_table}
    WHERE run_id = 'test-large-scale-001'
"""
result = execute_athena_query(query)
assert result[0]['subject_count'] == 10000
assert result[0]['evidence_count'] == 50000
```

---

### 9. Concurrent Bulk Imports

**Objective**: Verify multiple bulk imports can run concurrently

**Test Steps**:
1. Start 3 concurrent executions with different run_ids
2. Monitor all executions
3. Verify all complete successfully without interference

**Expected Results**:
- All 3 executions succeed
- EMR jobs run concurrently
- Neptune loads queue appropriately
- No data corruption or cross-contamination

**Verification**:
```python
execution_arns = []
for i in range(3):
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"concurrent-import-{i}-{int(time.time())}",
        input=json.dumps({
            "run_id": f"test-concurrent-{i}",
            "input_path": f"s3://bucket/bulk-imports/concurrent-{i}.csv"
        })
    )
    execution_arns.append(response['executionArn'])

# Wait for all to complete
for arn in execution_arns:
    waiter = sfn_client.get_waiter('execution_succeeded')
    waiter.wait(executionArn=arn, WaiterConfig={'Delay': 60, 'MaxAttempts': 120})

# Verify data isolation
for i in range(3):
    query = f"SELECT COUNT(*) FROM {database}.{evidence_table} WHERE run_id = 'test-concurrent-{i}'"
    result = execute_athena_query(query)
    assert result[0]['count'] > 0
```

---

### 10. EMR Spark Configuration

**Objective**: Verify EMR jobs use correct Iceberg configuration

**Test Steps**:
1. Start execution
2. Inspect EMR job configuration
3. Verify Spark parameters include Iceberg extensions and catalog config

**Expected Results**:
- SparkSubmitParameters include:
  - `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
  - `spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.glue_catalog.warehouse=s3://.../iceberg-warehouse`
  - `spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`
  - `spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO`

**Verification**:
```python
# Get EMR job run details
emr_client = boto3.client('emr-serverless')
jobs = emr_client.list_job_runs(applicationId=emr_app_id)
test_job = [j for j in jobs['jobRuns'] if 'test-run-001' in j['name']][0]

job_details = emr_client.get_job_run(
    applicationId=emr_app_id,
    jobRunId=test_job['id']
)

spark_params = job_details['jobRun']['jobDriver']['sparkSubmit']['sparkSubmitParameters']
assert 'IcebergSparkSessionExtensions' in spark_params
assert 'GlueCatalog' in spark_params
```

---

### 11. Subject Mapping Resolution

**Objective**: Verify DynamoDB subject mapping resolution works correctly

**Test Steps**:
1. Create subjects in DynamoDB with specific project mappings
2. Create CSV referencing those subjects by external IDs
3. Start execution
4. Verify subject_mapping.json correctly maps external IDs to subject_ids

**Expected Results**:
- ResolveSubjectMappings EMR job queries DynamoDB
- subject_mapping.json contains correct mappings
- ProcessWithEMR uses correct subject_ids for evidence

**Verification**:
```python
# Setup test subjects in DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

table.put_item(Item={
    'PK': 'SUBJECT#test-subject-001',
    'SK': 'METADATA',
    'subject_id': 'test-subject-001',
    'external_id': 'EXT-001',
    'project_id': 'test-project'
})

# Run import with external IDs
response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps({
        "run_id": "test-mapping-001",
        "input_path": "s3://bucket/bulk-imports/with-external-ids.csv"
    })
)

waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify mapping file
s3_client = boto3.client('s3')
mapping_obj = s3_client.get_object(
    Bucket=bucket_name,
    Key=f'runs/test-mapping-001/subject_mapping.json'
)
mapping = json.loads(mapping_obj['Body'].read())
assert mapping['EXT-001'] == 'test-subject-001'
```

---

### 12. TTL Generation Verification

**Objective**: Verify TTL files are generated correctly for Neptune load

**Test Steps**:
1. Start execution with known evidence data
2. Wait for GenerateTTLWithEMR to complete
3. Inspect generated TTL files in S3

**Expected Results**:
- TTL files created in `s3://bucket/runs/{run_id}/neptune/projects/{project_id}/`
- TTL files created in `s3://bucket/runs/{run_id}/neptune/subjects/`
- TTL syntax is valid
- All term links represented

**Verification**:
```python
# After execution completes
s3_client = boto3.client('s3')

# List TTL files for projects
project_ttls = s3_client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=f'runs/test-run-001/neptune/projects/'
)
assert project_ttls['KeyCount'] > 0

# List TTL files for subjects
subject_ttls = s3_client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=f'runs/test-run-001/neptune/subjects/'
)
assert subject_ttls['KeyCount'] > 0

# Verify TTL content
ttl_obj = s3_client.get_object(
    Bucket=bucket_name,
    Key=project_ttls['Contents'][0]['Key']
)
ttl_content = ttl_obj['Body'].read().decode('utf-8')
assert '@prefix' in ttl_content
assert 'obo:RO_0002200' in ttl_content  # has_phenotype predicate
```

---

### 13. EventBridge Notifications

**Objective**: Verify success/failure events are fired to EventBridge

**Test Steps**:
1. Create EventBridge rule to capture PheBee events
2. Start successful execution
3. Verify BulkImportComplete event received
4. Start failed execution
5. Verify BulkImportFailed event received

**Expected Results**:
- Success event: DetailType="BulkImportComplete", Source="PheBee"
- Failure event: DetailType="BulkImportFailed", Source="PheBee"
- Event details include run_id and status

**Verification**:
```python
# Create test rule
events_client = boto3.client('events')
rule_name = f'test-bulk-import-events-{int(time.time())}'

events_client.put_rule(
    Name=rule_name,
    EventPattern=json.dumps({
        "source": ["PheBee"],
        "detail-type": ["BulkImportComplete", "BulkImportFailed"]
    }),
    State='ENABLED',
    EventBusName=phebee_bus_name
)

# Create CloudWatch log target to capture events
log_group_name = f'/aws/events/test-bulk-import-{int(time.time())}'
logs_client = boto3.client('logs')
logs_client.create_log_group(logGroupName=log_group_name)

events_client.put_targets(
    Rule=rule_name,
    EventBusName=phebee_bus_name,
    Targets=[{
        'Id': '1',
        'Arn': f'arn:aws:logs:{region}:{account_id}:log-group:{log_group_name}',
        'RoleArn': events_role_arn
    }]
)

# Run state machine
# ... start execution and wait for completion ...

# Check for events in CloudWatch Logs
time.sleep(10)  # Allow time for event delivery
log_events = logs_client.filter_log_events(logGroupName=log_group_name)
assert len(log_events['events']) > 0

# Verify event structure
event_data = json.loads(log_events['events'][0]['message'])
assert event_data['detail-type'] == 'BulkImportComplete'
assert event_data['detail']['run_id'] == 'test-run-001'
```

---

### 14. Error Propagation

**Objective**: Verify errors at each stage are properly caught and reported

**Test Steps**:
1. Test error at ValidateInput → NotifyFailure
2. Test error at ResolveSubjectMappings → NotifyFailure
3. Test error at ProcessWithEMR → NotifyFailure
4. Test error at GenerateTTLWithEMR → NotifyFailure
5. Test error at StartProjectLoad → NotifyFailure
6. Test error at StartSubjectsLoad → NotifyFailure

**Expected Results**:
- All errors caught by Catch clauses
- ResultPath="$.error" preserves error details
- NotifyFailure receives error information
- FailExecution state reached

**Verification**:
```python
# For each error type, verify error handling
test_cases = [
    ("invalid-input", "ValidateInput"),
    ("mapping-error", "ResolveSubjectMappings"),
    ("process-error", "ProcessWithEMR"),
    # ... etc
]

for test_id, expected_failure_state in test_cases:
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"{test_id}-{int(time.time())}",
        input=json.dumps({
            "run_id": test_id,
            "input_path": f"s3://bucket/error-tests/{test_id}.csv"
        })
    )

    waiter = sfn_client.get_waiter('execution_failed')
    waiter.wait(executionArn=response['executionArn'])

    # Verify error captured
    execution = sfn_client.describe_execution(executionArn=response['executionArn'])
    assert 'error' in execution.get('cause', '').lower()
```

---

### 15. Execution History Analysis

**Objective**: Verify complete state transition sequence

**Test Steps**:
1. Run successful execution
2. Get execution history
3. Analyze state transitions

**Expected Results**:
- All expected states visited in correct order
- No unexpected state transitions
- Timing between states reasonable

**Verification**:
```python
# Get full execution history
history = sfn_client.get_execution_history(
    executionArn=execution_arn,
    maxResults=1000
)

# Extract state transitions
state_entries = [e for e in history['events'] if e['type'] == 'TaskStateEntered']
state_names = [e['stateEnteredEventDetails']['name'] for e in state_entries]

# Verify expected sequence
expected_sequence = [
    'ValidateInput',
    'CheckValidation',
    'ResolveSubjectMappings',
    'ProcessWithEMR',
    'GenerateTTLWithEMR',
    'StartProjectLoad',
    # ... etc
]

for expected, actual in zip(expected_sequence, state_names):
    assert expected in actual
```

---

## Test Data Requirements

### Valid CSV Files
- Small dataset: 10 subjects, 50 evidence records
- Medium dataset: 100 subjects, 1000 evidence records
- Large dataset: 10,000 subjects, 50,000 evidence records

### CSV Format
```csv
subject_external_id,project_id,term_id,evidence_type,qualifiers,date_observed,notes
EXT-001,project-1,HP:0001250,ECO:0000033,[],2024-01-01,Test evidence
```

### Error Test Files
- invalid-input.csv: Non-existent S3 path
- malformed-csv.csv: Invalid CSV structure
- invalid-subjects.csv: Subject IDs not in DynamoDB
- malformed-ttl-test.csv: Data that produces invalid TTL

---

## Performance Benchmarks

| Dataset Size | Subjects | Evidence | Expected Duration | EMR Jobs | Neptune Loads |
|--------------|----------|----------|-------------------|----------|---------------|
| Small        | 10       | 50       | 5-10 min          | 3        | 1-2           |
| Medium       | 100      | 1,000    | 15-30 min         | 3        | 2-5           |
| Large        | 10,000   | 50,000   | 60-120 min        | 3        | 10-20         |

---

## Error Scenarios

| Error Type | Stage | Expected Behavior |
|------------|-------|-------------------|
| Missing S3 file | ValidateInput | Fail immediately, NotifyFailure |
| Invalid CSV format | ValidateInput | Fail immediately, NotifyFailure |
| Subject not in DynamoDB | ResolveSubjectMappings EMR | EMR job fails, NotifyFailure |
| Iceberg write error | ProcessWithEMR | EMR job fails, NotifyFailure |
| TTL generation error | GenerateTTLWithEMR | EMR job fails, NotifyFailure |
| Neptune load failure | MonitorProjectLoad/MonitorSubjectsLoad | Detect LOAD_FAILED, NotifyFailure |
| Athena timeout | MaterializeProjectSubjectTerms | Continue anyway (optional optimization) |

---

## Notes

- EMR Serverless applications must be in STARTED state before execution
- State machine uses distributed map for parallel processing
- Neptune bulk loads use queueRequest=TRUE for automatic queueing
- Materialization is optional - failures don't block overall success
- EventBridge events allow downstream workflows to react to completion

---

## Additional Test Scenarios from Existing Integration Tests

### Test: Multiple JSONL Files Processed
**Test Name**: `test_bulk_import_multiple_jsonl_files`

**Objective**: Verify that multiple JSONL input files in a directory are all processed correctly

**Setup**:
- Create test project
- Prepare 2 JSONL files with different evidence data:
  - File 1: subject-001 with HP_0001627
  - File 2: subject-001 with HP_0001627 (different evidence) + subject-002 with HP_0002664
- Upload both files to S3 under same prefix: `s3://bucket/test-data/{run_id}/jsonl/`

**Test Steps**:
1. Start Step Function with input_path pointing to jsonl directory
2. Wait for successful completion
3. Verify TTL files generated
4. Query Iceberg evidence table

**Expected Results**:
- Execution status: SUCCEEDED
- TTL files created for all input files
- Iceberg evidence table contains evidence from BOTH files:
  - 2 subjects total (subject-001, subject-002)
  - 2 distinct terms (HP_0001627, HP_0002664)
  - All evidence records from both files present
- No evidence lost between files

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py` (lines 142-165) creates multiple JSONL files and verifies they are all processed. This ensures the bulk import handles directory-based input correctly.

---

### Test: TTL File Generation and Structure Validation
**Test Name**: `test_bulk_import_ttl_generation_structure`

**Objective**: Verify TTL file generation from Iceberg data with correct structure

**Setup**:
- Create test data with subjects, terms, and qualifiers
- Upload to S3

**Test Steps**:
1. Run bulk import Step Function
2. After completion, list TTL files in output location: `runs/{run_id}/neptune/`
3. Download and parse TTL files
4. Validate TTL content structure

**Expected Results**:
- TTL files exist at expected S3 location
- TTL content includes:
  - Subject assertions: `phebee:Subject` declarations
  - TermLink assertions: `phebee:TermLink` declarations
  - Term assertions: term IRI references (HP_0001627, etc.)
  - Qualifier assertions: `hasQualifyingTerm` predicates for qualified evidence
  - hasTermLink predicates connecting subjects to termlinks
  - hasTerm predicates connecting termlinks to terms
- Required URI patterns present:
  - `http://ods.nationwidechildrens.org/phebee/`
  - `http://www.w3.org/1999/02/22-rdf-syntax-ns#`
  - `http://purl.obolibrary.org/obo/HP_`
- Basic RDF structure integrity validated

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py::test_bulk_import_stepfunction` (lines 264-304) validates TTL file generation and structure. This ensures Neptune can successfully load the generated RDF data.

---

### Test: Subject, TermLink, Term, and Qualifier Assertions in TTL
**Test Name**: `test_bulk_import_ttl_comprehensive_validation`

**Objective**: Verify comprehensive TTL generation including all RDF entity types

**Setup**:
- Create test data with:
  - 4 subjects (ttl-subject-001 through ttl-subject-004)
  - Multiple evidence records with various qualifier combinations
  - Some evidence with qualifiers (negated, family, hypothetical)
  - Some evidence without qualifiers

**Test Steps**:
1. Run bulk import
2. Parse all generated TTL files
3. Extract and count RDF entities

**Expected Results**:
- Subject count: 4 subjects declared in TTL
- TermLink count: Matches expected evidence count (one termlink per evidence with unique subject+term+qualifier combination)
- Qualifier assertions: Correct count matching positive qualifiers in test data
  - Count positive qualifiers from contexts (negated=1.0, family=1.0, hypothetical=1.0)
  - Verify `hasQualifyingTerm` predicates match
- RDF structure integrity:
  - Each termlink connects exactly one subject to one term
  - Subjects with evidence have termlinks
  - Subjects without evidence declared but no termlinks
- Exact counts validated (not just "some data exists")

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py::test_ttl_generation_comprehensive` (lines 591-1002) performs deep validation of TTL structure including subject counts, termlink counts, qualifier assertions, and RDF relationship integrity. This is critical for ensuring Neptune data quality.

---

### Test: Iceberg Data Validation via Athena
**Test Name**: `test_bulk_import_iceberg_athena_validation`

**Objective**: Verify evidence data correctly written to Iceberg and queryable via Athena

**Setup**:
- Create test data
- Run bulk import

**Test Steps**:
1. Execute bulk import Step Function
2. Query Iceberg evidence table using Athena:
   ```sql
   SELECT COUNT(*) as record_count,
          COUNT(DISTINCT subject_id) as subject_count,
          COUNT(DISTINCT term_iri) as term_count
   FROM phebee.evidence
   WHERE run_id = '{run_id}'
   ```
3. Validate specific terms present:
   ```sql
   SELECT DISTINCT term_iri
   FROM phebee.evidence
   WHERE run_id = '{run_id}'
   ORDER BY term_iri
   ```

**Expected Results**:
- Athena queries succeed
- record_count > 0 (evidence written)
- subject_count matches expected unique subjects
- term_count matches expected unique terms
- Expected terms present in results:
  - http://purl.obolibrary.org/obo/HP_0001627
  - http://purl.obolibrary.org/obo/HP_0002664
  - (other expected terms from test data)
- All records have correct run_id

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py` (lines 307-441) validates Iceberg data via Athena queries. This ensures the EMR processing correctly writes evidence to the Iceberg table with proper partitioning and schema.

---

### Test: Validation Failure Handling
**Test Name**: `test_bulk_import_validation_failure`

**Objective**: Verify Step Function properly handles validation failures for invalid input

**Setup**:
- Prepare invalid input (non-existent S3 bucket/path)

**Test Steps**:
1. Start Step Function with invalid S3 path:
   ```json
   {
     "run_id": "test-validation-failure",
     "input_path": "s3://nonexistent-bucket/nonexistent-prefix/"
   }
   ```
2. Monitor execution status

**Expected Results**:
- Execution status: FAILED
- Failure occurs during validation stage
- No downstream processing occurs
- No EMR jobs launched
- No Neptune loads attempted
- Clean failure with descriptive error message

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py::test_bulk_import_validation_failure` (lines 456-500) verifies proper validation and early failure for invalid inputs. This prevents wasted resources on invalid data.

---

### Test: term_source Field Handling (Optional Struct)
**Test Name**: `test_bulk_import_term_source_handling`

**Objective**: Verify optional term_source field correctly processed and stored in Iceberg

**Setup**:
- Create test data with two subjects:
  - Subject 1: Evidence WITH term_source: `{source: "hpo", version: "2024-01-01", iri: "http://purl.obolibrary.org/obo/hp.owl"}`
  - Subject 2: Evidence WITHOUT term_source (optional field)

**Test Steps**:
1. Run bulk import
2. Query Iceberg evidence table for subject 1 (with term_source)
3. Query Iceberg evidence table for subject 2 (without term_source)

**Expected Results**:
- Subject 1 evidence record:
  - term_source.source = "hpo"
  - term_source.version = "2024-01-01"
  - term_source.iri = "http://purl.obolibrary.org/obo/hp.owl"
- Subject 2 evidence record:
  - term_source.source is null/empty (field not provided)
  - No errors processing evidence without term_source
- Optional struct field handled correctly in both cases

**Why this test is important**: The existing integration test `test_bulk_import_stepfunction.py::test_bulk_import_with_term_source` (lines 1006-1275) and `test_evidence.py::test_evidence_with_term_source` (lines 512-551) verify handling of the optional term_source struct field. This ensures the Iceberg schema supports optional nested fields correctly.

---

### Test: EMR Duplicate Subject Handling
**Test Name**: `test_bulk_import_emr_duplicate_subject_handling`

**Objective**: Verify EMR correctly handles duplicate project_subject_ids by reusing subject IRIs

**Setup**:
- Create test data where same project_subject_id appears multiple times:
  - Row 1: project-1, subject-001, HP_0001249
  - Row 2: project-1, subject-001, HP_0002297 (same subject, different term)

**Test Steps**:
1. Run bulk import
2. Query evidence table for subject
3. Verify subject_id consistency

**Expected Results**:
- Both evidence records reference SAME subject_id
- Subject IRI reused for duplicate project_subject_id
- No duplicate subject nodes created
- Evidence properly aggregated under single subject

**Why this test is important**: The existing integration test `test_evidence.py::test_subject_reuse_across_runs` (lines 461-508) and `test_emr_subject_mapping.py` verify that duplicate subjects are correctly resolved to the same subject IRI, preventing duplicate subject creation.

---

### Test: EventBridge Event Firing for Success
**Test Name**: `test_bulk_import_eventbridge_success_event`

**Objective**: Verify BULK_IMPORT_SUCCESS event fired when Step Function completes successfully

**Setup**:
- Create test SQS queue
- Create EventBridge rule to capture bulk_import_success events and send to SQS queue

**Test Steps**:
1. Run bulk import Step Function to completion
2. Wait for Step Function status: SUCCEEDED
3. Poll SQS queue for event

**Expected Results**:
- Step Function execution: SUCCEEDED
- EventBridge event received in SQS queue
- Event detail-type: "bulk_import_success"
- Event payload includes:
  - run_id
  - domain_load_id
  - prov_load_id (if applicable)
- Event timestamp matches completion time

**Why this test is important**: The existing integration test `test_bulk_import_events.py::test_bulk_import_success_event` (lines 83-197) verifies EventBridge integration for success events. This enables downstream workflows to react to bulk import completion.

---

### Test: EventBridge Event Firing for Failure
**Test Name**: `test_bulk_import_eventbridge_failure_event`

**Objective**: Verify BULK_IMPORT_FAILURE event fired when Step Function fails

**Setup**:
- Create test SQS queue
- Create EventBridge rule to capture bulk_import_failure events

**Test Steps**:
1. Run bulk import Step Function with invalid data (will fail)
2. Wait for Step Function status: FAILED
3. Poll SQS queue for event

**Expected Results**:
- Step Function execution: FAILED
- EventBridge event received in SQS queue
- Event detail-type: "bulk_import_failure"
- Event payload includes:
  - run_id
  - error message/details
- Failure event properly triggered

**Why this test is important**: The existing integration test `test_bulk_import_events.py::test_bulk_import_failure_event` (lines 223-242) verifies EventBridge integration for failure events. This enables alerting and failure recovery workflows.

---
- Run IDs must be unique to avoid S3 path conflicts
