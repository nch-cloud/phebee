# UpdateHPOSFN State Machine Integration Tests

## Overview

The UpdateHPOSFN state machine downloads the latest HPO (Human Phenotype Ontology) release from GitHub, installs it to Neptune, materializes the ontology hierarchy to Iceberg tables, and fires an update event to EventBridge. It includes idempotency checks to avoid re-downloading unchanged versions.

**State Machine**: `UpdateHPOSFN`
**Type**: Scheduled maintenance workflow (can also be triggered manually)
**Execution Mode**: Asynchronous
**Max Duration**: 30-60 minutes

## Architecture

```
Download HPO
    ↓
Was New HPO Version Downloaded? (Choice)
    ├─ YES → Get HPO OWL Info
    └─ NO  → Success (skip update)
       ↓
Install HPO (calls InstallRDFXMLSFN.sync)
       ↓
Update Install Timestamp
       ↓
Delete Old HPO Hierarchy Data
       ↓
Prepare HPO Hierarchy Batches
       ↓
Insert HPO Hierarchy Batches (Distributed Map)
       ↓
Cleanup Batch Files
       ↓
Fire Update Event
       ↓
    Success
```

## Test Scenarios

### 1. Happy Path - New HPO Version

**Objective**: Verify complete HPO update workflow when new version is available

**Test Steps**:
1. Start execution with test mode disabled
   ```json
   {
     "test": false
   }
   ```
2. Monitor through all stages
3. Verify HPO ontology loaded to Neptune
4. Verify ontology hierarchy materialized to Iceberg
5. Verify update event fired

**Expected Results**:
- DownloadGithubReleaseFunction fetches latest HPO release
- New version detected (downloaded=true)
- Assets downloaded: hp.owl, phenotype_to_genes.txt, phenotype.hpoa, hp.obo
- Install HPO synchronously calls InstallRDFXMLSFN
- Graph name format: `hpo~v2024-04-26` (version from GitHub release tag)
- DynamoDB updated with installation timestamp
- Old hierarchy data deleted from Iceberg
- New hierarchy batches prepared from hp.obo
- Distributed map processes hierarchy batches in parallel (max concurrency: 10)
- Batch files cleaned up from S3
- EventBridge receives `source_updated` event with source="hpo"
- Execution status: SUCCEEDED

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-update-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for completion
execution_arn = response['executionArn']
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=execution_arn,
    WaiterConfig={'Delay': 60, 'MaxAttempts': 60}  # Up to 1 hour
)

# Verify DynamoDB timestamp
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#hpo', 'SK': 'VERSION'})
assert 'last_install_timestamp' in response['Item']

# Verify ontology hierarchy in Iceberg
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{ontology_hierarchy_table}
    WHERE ontology_source = 'hpo'
"""
result = execute_athena_query(query)
assert result[0]['count'] > 10000  # HPO has thousands of terms

# Verify EventBridge event
# (Requires event capture rule configured - see test setup)
```

---

### 2. No Update - Same Version

**Objective**: Verify idempotency when HPO version unchanged

**Test Steps**:
1. Run UpdateHPOSFN once to install a version
2. Immediately run again
3. Verify second execution skips download and installation

**Expected Results**:
- DownloadGithubReleaseFunction checks DynamoDB for last installed version
- Finds version matches latest GitHub release
- Returns downloaded=false
- Choice state evaluates to Success immediately
- No InstallRDFXMLSFN invocation
- No hierarchy materialization
- Execution duration: < 1 minute

**Verification**:
```python
# First execution
response1 = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-first-{int(time.time())}",
    input=json.dumps({"test": false})
)
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response1['executionArn'])

# Second execution immediately after
time.sleep(5)
response2 = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-second-{int(time.time())}",
    input=json.dumps({"test": false})
)
start_time = time.time()
waiter.wait(executionArn=response2['executionArn'])
duration = time.time() - start_time

# Verify quick completion (< 60 seconds)
assert duration < 60

# Verify no install step occurred
history = sfn_client.get_execution_history(executionArn=response2['executionArn'])
install_states = [e for e in history['events']
                  if e.get('stateEnteredEventDetails', {}).get('name') == 'Install HPO']
assert len(install_states) == 0
```

---

### 3. Test Mode - Dry Run

**Objective**: Verify test mode behavior

**Test Steps**:
1. Start execution with test mode enabled
   ```json
   {
     "test": true
   }
   ```
2. Verify DownloadGithubReleaseFunction operates in test mode
3. Verify workflow completes without actual installation

**Expected Results**:
- DownloadGithubReleaseFunction may use cached/test data
- Workflow proceeds through all steps
- No actual changes to production Neptune/Iceberg
- Execution succeeds

**Verification**:
```python
response = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-test-mode-{int(time.time())}",
    input=json.dumps({"test": true})
)

waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify test mode was passed through
history = sfn_client.get_execution_history(executionArn=response['executionArn'])
download_event = [e for e in history['events']
                  if e['type'] == 'LambdaFunctionScheduled'][0]
payload = json.loads(download_event['lambdaFunctionScheduledEventDetails']['input'])
assert payload['test'] == true
```

---

### 4. Asset Filtering - hp.owl and hp.obo

**Objective**: Verify correct assets are downloaded and used

**Test Steps**:
1. Start execution
2. Verify Download HPO requests specific assets
3. Verify Get HPO OWL Info filters for hp.owl
4. Verify Prepare HPO Hierarchy Batches uses hp.obo

**Expected Results**:
- Download HPO requests: hp.owl, phenotype_to_genes.txt, phenotype.hpoa, hp.obo
- Get HPO OWL Info JSONPath filter: `$.hpo.Payload.assets.[?(@.asset_name == 'hp.owl')]`
- Get HPO OWL Info also filters obo: `$.hpo.Payload.assets.[?(@.asset_name == 'hp.obo')]`
- Install HPO uses filtered_assets.[0].asset_path (hp.owl)
- Prepare HPO Hierarchy Batches uses obo_assets[0].asset_path (hp.obo)

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(stateMachineArn=update_hpo_sfn_arn)
definition = json.loads(sm_def['definition'])

# Verify Download HPO asset_names
download_params = definition['States']['Download HPO']['Parameters']['Payload']
assert 'hp.owl' in download_params['asset_names']
assert 'hp.obo' in download_params['asset_names']

# Verify Get HPO OWL Info filters
get_owl_params = definition['States']['Get HPO OWL Info']['Parameters']
assert 'hp.owl' in get_owl_params['filtered_assets.$']
assert 'hp.obo' in get_owl_params['obo_assets.$']
```

---

### 5. InstallRDFXMLSFN Synchronous Call

**Objective**: Verify synchronous nested state machine execution

**Test Steps**:
1. Start execution
2. Monitor Install HPO step
3. Verify InstallRDFXMLSFN is called with startExecution.sync:2
4. Verify UpdateHPOSFN waits for InstallRDFXMLSFN to complete

**Expected Results**:
- Install HPO uses Resource: `arn:aws:states:::states:startExecution.sync:2`
- Input includes source (hp.owl S3 path) and graph_name (hpo~version)
- UpdateHPOSFN blocks until InstallRDFXMLSFN completes
- InstallRDFXMLSFN output available in ResultPath: $.install

**Verification**:
```python
# Monitor execution
execution = sfn_client.describe_execution(executionArn=execution_arn)

# Get execution history
history = sfn_client.get_execution_history(executionArn=execution_arn)

# Find nested execution
nested_execution_events = [e for e in history['events']
                           if e['type'] == 'TaskScheduled' and
                           'InstallRDFXML' in str(e)]

assert len(nested_execution_events) > 0

# Verify synchronous behavior (parent waits for child)
install_start = None
install_end = None
for e in history['events']:
    if e.get('stateEnteredEventDetails', {}).get('name') == 'Install HPO':
        install_start = e['timestamp']
    if e.get('stateExitedEventDetails', {}).get('name') == 'Install HPO':
        install_end = e['timestamp']

assert install_end > install_start
assert (install_end - install_start).seconds > 30  # At least one load cycle
```

---

### 6. Hierarchy Materialization - Distributed Map

**Objective**: Verify distributed map processes batches in parallel

**Test Steps**:
1. Start execution
2. Monitor Insert HPO Hierarchy Batches step
3. Verify distributed map configuration
4. Verify parallel execution

**Expected Results**:
- ItemProcessor.ProcessorConfig.Mode: DISTRIBUTED
- ItemProcessor.ProcessorConfig.ExecutionType: STANDARD
- MaxConcurrency: 10
- ItemReader uses S3 listObjectsV2 to find batch files
- Insert Batch Lambda invoked for each batch file
- Retries configured: IntervalSeconds=2, MaxAttempts=5, BackoffRate=2.5

**Verification**:
```python
# Check state machine definition
definition = json.loads(sfn_client.describe_state_machine(
    stateMachineArn=update_hpo_sfn_arn
)['definition'])

map_state = definition['States']['Insert HPO Hierarchy Batches']
assert map_state['ItemProcessor']['ProcessorConfig']['Mode'] == 'DISTRIBUTED'
assert map_state['MaxConcurrency'] == 10

# During execution, count child executions
# (Distributed map creates child executions for each item)
executions = sfn_client.list_executions(
    stateMachineArn=update_hpo_sfn_arn,
    statusFilter='RUNNING'
)
# Multiple child executions indicate parallel processing
```

---

### 7. Delete Old Hierarchy Data

**Objective**: Verify old hierarchy data is deleted before new data insertion

**Test Steps**:
1. Install HPO version 1
2. Verify hierarchy data exists in Iceberg
3. Install HPO version 2
4. Verify old data deleted, new data present

**Expected Results**:
- Delete Old HPO Hierarchy Data invokes DeleteOntologyHierarchyPartitionFunction
- Function deletes partition: ontology_source='hpo', version=previous_version
- New hierarchy data has current version

**Verification**:
```python
# Install version 1 (simulated)
# ... run UpdateHPOSFN with older release ...

# Query hierarchy data
query1 = f"""
    SELECT DISTINCT version
    FROM {database}.{ontology_hierarchy_table}
    WHERE ontology_source = 'hpo'
"""
result1 = execute_athena_query(query1)
old_version = result1[0]['version']

# Install version 2
# ... run UpdateHPOSFN with newer release ...

# Verify version changed
query2 = f"""
    SELECT DISTINCT version
    FROM {database}.{ontology_hierarchy_table}
    WHERE ontology_source = 'hpo'
"""
result2 = execute_athena_query(query2)
new_version = result2[0]['version']

assert old_version != new_version
assert len(result2) == 1  # Only one version present
```

---

### 8. Batch File Cleanup

**Objective**: Verify temporary batch files are cleaned up after processing

**Test Steps**:
1. Start execution
2. After Prepare HPO Hierarchy Batches, list S3 files
3. After Insert HPO Hierarchy Batches, verify files still exist
4. After Cleanup Batch Files, verify files deleted

**Expected Results**:
- Prepare step creates batches in S3 prefix
- Cleanup Batch Files invokes CleanupS3PrefixFunction
- All batch files deleted from S3
- ResultPath: $.cleanup_result captures cleanup status

**Verification**:
```python
s3_client = boto3.client('s3')

# Monitor execution state
while True:
    execution = sfn_client.describe_execution(executionArn=execution_arn)
    if execution['status'] != 'RUNNING':
        break

    # Check if we're past batch preparation
    history = sfn_client.get_execution_history(executionArn=execution_arn)
    prepare_complete = any(e.get('stateExitedEventDetails', {}).get('name') == 'Prepare HPO Hierarchy Batches'
                           for e in history['events'])

    if prepare_complete:
        # List batch files
        # (Need to extract S3 prefix from execution output)
        break

    time.sleep(10)

# After execution completes, verify cleanup
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

s3_prefix = output['batch_preparation']['Payload']['s3_prefix']
s3_bucket = output['batch_preparation']['Payload']['s3_bucket']

# Verify no batch files remain
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
assert response.get('KeyCount', 0) == 0
```

---

### 9. Cleanup Failure - Continue Anyway

**Objective**: Verify workflow continues despite cleanup failures

**Test Steps**:
1. Force CleanupS3PrefixFunction to fail (e.g., missing permissions)
2. Verify Catch clause captures error
3. Verify workflow proceeds to Fire Update Event anyway

**Expected Results**:
- Cleanup Batch Files catches error
- ResultPath: $.cleanup_error captures exception
- Next state: Fire Update Event (not failure)
- Execution succeeds despite cleanup failure

**Verification**:
```python
# Temporarily remove S3 delete permissions to force cleanup failure
# (Implementation-specific to test environment)

response = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-cleanup-fail-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for completion
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify cleanup was attempted but didn't block success
history = sfn_client.get_execution_history(executionArn=response['executionArn'])
cleanup_events = [e for e in history['events']
                  if 'Cleanup' in str(e.get('stateEnteredEventDetails', {}).get('name', ''))]
assert len(cleanup_events) > 0

# Restore permissions
```

---

### 10. EventBridge Update Event

**Objective**: Verify source_updated event is fired to EventBridge

**Test Steps**:
1. Create EventBridge rule to capture PheBee events
2. Start execution
3. Verify event received with correct details

**Expected Results**:
- Event fired to PheBeeBus
- Source: "PheBee"
- DetailType: "source_updated"
- Detail contains: {"source": "hpo", "version": "v2024-04-26"}

**Verification**:
```python
# Create test rule (see bulk_import tests for full EventBridge test setup)
events_client = boto3.client('events')
rule_name = f'test-hpo-update-event-{int(time.time())}'

events_client.put_rule(
    Name=rule_name,
    EventPattern=json.dumps({
        "source": ["PheBee"],
        "detail-type": ["source_updated"],
        "detail": {
            "source": ["hpo"]
        }
    }),
    State='ENABLED',
    EventBusName=phebee_bus_name
)

# ... run execution ...

# Verify event details
# (Requires log group target configured)
assert event['detail']['source'] == 'hpo'
assert 'version' in event['detail']
```

---

### 11. GitHub API Rate Limiting

**Objective**: Verify handling of GitHub API rate limits

**Test Steps**:
1. Trigger multiple rapid executions to hit rate limit
2. Verify DownloadGithubReleaseFunction handles rate limiting gracefully

**Expected Results**:
- Function retries with exponential backoff
- Eventually succeeds or fails gracefully
- Error message indicates rate limiting

**Verification**:
```python
# Start multiple executions rapidly
execution_arns = []
for i in range(10):
    try:
        response = sfn_client.start_execution(
            stateMachineArn=update_hpo_sfn_arn,
            name=f"test-rate-limit-{i}-{int(time.time())}",
            input=json.dumps({"test": false})
        )
        execution_arns.append(response['executionArn'])
    except Exception as e:
        print(f"Execution {i} failed: {e}")

# Monitor for rate limiting errors
time.sleep(60)
for arn in execution_arns:
    execution = sfn_client.describe_execution(executionArn=arn)
    if execution['status'] == 'FAILED':
        assert 'rate limit' in execution.get('cause', '').lower() or \
               'retry' in execution.get('cause', '').lower()
```

---

### 12. Graph Name Formatting

**Objective**: Verify correct graph name format with version

**Test Steps**:
1. Start execution
2. Extract version from GitHub release
3. Verify graph name uses format `hpo~{version}`

**Expected Results**:
- Get HPO OWL Info extracts hpo_version from download response
- Install HPO uses States.Format('hpo~{}', $.hpo_version)
- Graph name examples: `hpo~v2024-04-26`, `hpo~v2024-03-06`

**Verification**:
```python
# Check execution output
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

hpo_version = output['hpo_version']
expected_graph_name = f'hpo~{hpo_version}'

# Verify in DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#hpo', 'SK': 'VERSION'})
assert expected_graph_name in str(response['Item'])
```

---

### 13. Concurrent Update Attempts

**Objective**: Verify behavior when multiple updates run concurrently

**Test Steps**:
1. Start two executions simultaneously
2. Monitor both executions
3. Verify behavior (one may detect no update needed, or both process same version)

**Expected Results**:
- First execution downloads and installs
- Second execution may detect same version and skip
- Or both process concurrently without conflicts
- Neptune handles concurrent loads via queueing

**Verification**:
```python
# Start two executions
response1 = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-concurrent-1-{int(time.time())}",
    input=json.dumps({"test": false})
)

time.sleep(2)  # Small delay

response2 = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-concurrent-2-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for both
for arn in [response1['executionArn'], response2['executionArn']]:
    waiter = sfn_client.get_waiter('execution_succeeded')
    waiter.wait(executionArn=arn)

# Both should succeed (one may be fast no-op)
```

---

### 14. Scheduled Execution

**Objective**: Verify scheduled EventBridge rule triggers execution

**Test Steps**:
1. Verify EventBridge rule exists
2. Check rule configuration (schedule expression, target)
3. Wait for scheduled execution or manually trigger
4. Verify execution starts with correct input

**Expected Results**:
- Rule: UpdateHPO-OnSchedule
- Schedule: cron(0 9 * * ? *) - 9:00 UTC daily (4:00 AM EST)
- Target: UpdateHPOSFN
- Input: {"test": false}

**Verification**:
```python
events_client = boto3.client('events')

# Find the schedule rule
rules = events_client.list_rules(NamePrefix='UpdateHPO-OnSchedule')
assert len(rules['Rules']) > 0

rule = rules['Rules'][0]
assert rule['ScheduleExpression'] == 'cron(0 9 * * ? *)'
assert rule['State'] == 'ENABLED'

# Check targets
targets = events_client.list_targets_by_rule(Rule=rule['Name'])
assert len(targets['Targets']) > 0

target = targets['Targets'][0]
assert 'UpdateHPOSFN' in target['Arn']
assert json.loads(target['Input']) == {"test": false}
```

---

### 15. Version Timestamp Tracking

**Objective**: Verify installation timestamp is correctly recorded in DynamoDB

**Test Steps**:
1. Start execution
2. After completion, query DynamoDB for HPO source record
3. Verify timestamp and version

**Expected Results**:
- Update Install Timestamp invokes UpdateInstallTimestampFunction
- DynamoDB record: PK='SOURCE#hpo', SK='VERSION'
- Fields: version, last_install_timestamp, source

**Verification**:
```python
# Run execution
# ... wait for completion ...

# Query DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

response = table.get_item(Key={'PK': 'SOURCE#hpo', 'SK': 'VERSION'})
item = response['Item']

assert item['source'] == 'hpo'
assert 'version' in item
assert 'last_install_timestamp' in item

# Verify timestamp is recent
import datetime
timestamp = datetime.datetime.fromisoformat(item['last_install_timestamp'])
now = datetime.datetime.now(datetime.timezone.utc)
assert (now - timestamp).total_seconds() < 3600  # Within last hour
```

---

## Test Data Requirements

### GitHub Release Mock Data
- For test mode, may need mock GitHub API responses
- Test with various HPO versions: v2024-04-26, v2024-03-06, v2023-10-09

### Expected Assets
- hp.owl (OWL format, 50-100MB)
- hp.obo (OBO format, 20-30MB)
- phenotype_to_genes.txt (Tab-delimited)
- phenotype.hpoa (Annotation file)

---

## Performance Benchmarks

| Stage | Expected Duration | Notes |
|-------|-------------------|-------|
| Download HPO | 2-5 min | Depends on GitHub API and file size |
| Install HPO | 5-15 min | Neptune load time |
| Prepare Hierarchy | 3-10 min | Parse OBO file |
| Insert Batches | 5-15 min | Distributed map with 10 concurrent workers |
| Cleanup | 30s-2 min | Delete batch files |
| **Total** | **15-45 min** | Full update cycle |

---

## Error Scenarios

| Error Type | Stage | Expected Behavior |
|------------|-------|-------------------|
| GitHub API unavailable | Download HPO | Lambda retries, eventually fails |
| Asset not found | Get HPO OWL Info | Empty array, fails at Install |
| Neptune load failure | Install HPO | InstallRDFXMLSFN fails, propagates to parent |
| OBO parse error | Prepare Hierarchy | MaterializeOntologyHierarchyFunction fails |
| Athena timeout | Insert Batch | Retry up to 5 times with backoff |
| S3 cleanup failure | Cleanup Batch Files | Caught, workflow continues |
| EventBridge unavailable | Fire Update Event | State machine fails |

---

## Notes

- UpdateHPOSFN is typically triggered by EventBridge schedule (daily at 4 AM EST)
- Can also be triggered manually for immediate updates
- Idempotency check prevents redundant updates
- Hierarchy materialization enables ancestor/descendant queries in Iceberg
- Distributed map allows parallel processing of hierarchy batches
- Synchronous nested state machine call ensures Neptune load completes before hierarchy processing

---

## Additional Test Scenarios from Existing Integration Tests

### Test: Hierarchy Table Populated with Correct Count
**Test Name**: `test_hpo_hierarchy_table_populated`

**Objective**: Verify ontology_hierarchy table is populated after HPO update with expected term count

**Setup**:
- Trigger HPO update state machine or use existing fixture
- Wait for completion

**Test Steps**:
1. Query Iceberg ontology_hierarchy table:
   ```sql
   SELECT COUNT(*) as term_count
   FROM {database}.{ontology_hierarchy_table}
   WHERE ontology_source = 'hpo'
   ```

**Expected Results**:
- Query returns results (table exists and populated)
- term_count > 10,000 (HPO has ~17,000 terms as of 2024)
- Hierarchy data successfully materialized
- Table structure correct with required columns:
  - ontology_source
  - term_id
  - term_label
  - depth
  - parent terms

**Why this test is important**: The existing integration test `test_update_hpo.py::test_hpo_hierarchy_table_populated` (lines 18-37) verifies the hierarchy table is successfully populated. This is critical for term hierarchy queries and the `include_descendants` parameter in get_subjects_pheno.

---

### Test: query_term_ancestors Returns Correct Lineage
**Test Name**: `test_query_term_ancestors_lineage`

**Objective**: Verify query_term_ancestors returns complete ancestor path from term to root

**Setup**:
- HPO hierarchy data loaded
- Test with known HPO term: HP:0000152 (Abnormality of head or neck)

**Test Steps**:
1. Call query_term_ancestors("HP:0000152", ontology_source="hpo")
2. Verify results

**Expected Results**:
- Ancestors list includes:
  - HP:0000152 (self - term should be in its own ancestor list)
  - HP:0000118 (Phenotypic abnormality - direct parent)
  - HP:0000001 (All - root term)
- Ancestor list length > 0
- Root term HP:0000001 present in results
- Complete lineage from term to root

**Why this test is important**: The existing integration test `test_update_hpo.py::test_query_term_ancestors` (lines 41-60) verifies the query_term_ancestors function works correctly. This is essential for computing term subsumption relationships.

---

### Test: query_term_descendants Returns Complete Subtree
**Test Name**: `test_query_term_descendants_subtree`

**Objective**: Verify query_term_descendants returns all descendant terms in the subtree

**Setup**:
- HPO hierarchy data loaded
- Test with parent term: HP:0000118 (Phenotypic abnormality)

**Test Steps**:
1. Call query_term_descendants("HP:0000118", ontology_source="hpo")
2. Verify results structure and content

**Expected Results**:
- Descendants list length > 5,000 (Phenotypic abnormality has thousands of descendants)
- Each descendant includes:
  - term_id (e.g., "HP:0000152")
  - term_label (human-readable label)
  - depth (distance from root)
- HP:0000152 (Abnormality of head or neck) present in descendants
- Complete subtree returned

**Why this test is important**: The existing integration test `test_update_hpo.py::test_query_term_descendants` (lines 64-86) verifies descendant queries work correctly. This is essential for the `include_child_terms=true` parameter in phenotype queries.

---

### Test: Ancestors Ordered by Depth (Root to Leaf)
**Test Name**: `test_ancestors_ordered_by_depth`

**Objective**: Verify ancestors are returned in correct depth order (root → intermediate → leaf)

**Setup**:
- HPO hierarchy data loaded
- Test with HP:0000152 (Abnormality of head or neck)

**Test Steps**:
1. Query ancestors for HP:0000152
2. Query depth values from hierarchy table
3. Verify ordering

**Expected Results**:
- Ancestors ordered by depth ASC (shallow to deep)
- First ancestor: HP:0000001 (depth = 0, root)
- Second ancestor: HP:0000118 (depth = 1, direct child of root)
- Last ancestor: HP:0000152 (the term itself, deepest)
- Depth values non-decreasing in sequence
- Ordering: root → parent → ... → term

**Why this test is important**: The existing integration test `test_update_hpo.py::test_ancestors_ordered_by_depth` (lines 90-133) verifies correct depth ordering. This ensures lineage paths are traversed in the correct direction.

---

### Test: Descendants Ordered by Depth (Shallow to Deep)
**Test Name**: `test_descendants_ordered_by_depth`

**Objective**: Verify descendants are returned in breadth-first order (shallow to deep)

**Setup**:
- HPO hierarchy data loaded
- Test with HP:0000118 (Phenotypic abnormality)

**Test Steps**:
1. Query descendants for HP:0000118
2. Check depth values in results
3. Verify ordering

**Expected Results**:
- Descendants ordered by depth ASC (shallow to deep)
- First descendants at depth 2 (direct children of HP:0000118)
- Depth values non-decreasing throughout list
- Ordering enables breadth-first traversal
- No depth inversions (later term cannot have lower depth)

**Why this test is important**: The existing integration test `test_update_hpo.py::test_descendants_ordered_by_depth` (lines 167-188) verifies breadth-first ordering of descendants. This is important for hierarchical query performance and correct subtree expansion.

---

### Test: Depth Values Correct (Root=0, Children Increment)
**Test Name**: `test_depth_values_correct`

**Objective**: Verify depth values are accurately computed from root

**Setup**:
- HPO hierarchy data loaded

**Test Steps**:
1. Query depth for known terms:
   - HP:0000001 (All - root)
   - HP:0000118 (Phenotypic abnormality - child of root)
   - HP:0000152 (Abnormality of head or neck - grandchild of root)

**Expected Results**:
- HP:0000001 depth = 0 (root)
- HP:0000118 depth = 1 (child of root)
- HP:0000152 depth > 1 (grandchild or deeper)
- Depth values consistent with hierarchy structure
- Parent depth always < child depth

**Why this test is important**: The existing integration test `test_update_hpo.py::test_depth_values_correct` (lines 137-163) verifies depth computation accuracy. Correct depth values are essential for hierarchy-based queries and term distance calculations.

---
