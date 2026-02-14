# UpdateMondoSFN State Machine Integration Tests

## Overview

The UpdateMondoSFN state machine downloads the latest MONDO (Monarch Disease Ontology) release from GitHub, installs it to Neptune, materializes the ontology hierarchy to Iceberg tables, and fires an update event to EventBridge. The workflow is nearly identical to UpdateHPOSFN.

**State Machine**: `UpdateMondoSFN`
**Type**: Scheduled maintenance workflow (can also be triggered manually)
**Execution Mode**: Asynchronous
**Max Duration**: 20-40 minutes

## Architecture

```
Download MONDO
    ↓
Was New MONDO Version Downloaded? (Choice)
    ├─ YES → Get MONDO OWL Info
    └─ NO  → Success (skip update)
       ↓
Install MONDO (calls InstallRDFXMLSFN.sync)
       ↓
Update Install Timestamp
       ↓
Delete Old MONDO Hierarchy Data
       ↓
Prepare MONDO Hierarchy Batches
       ↓
Insert MONDO Hierarchy Batches (Distributed Map)
       ↓
Cleanup Batch Files
       ↓
Fire Update Event
       ↓
    Success
```

## Key Differences from HPO

| Aspect | HPO | MONDO |
|--------|-----|-------|
| GitHub Repo | obophenotype/human-phenotype-ontology | monarch-initiative/mondo |
| Source Name | hpo | mondo |
| Assets | hp.owl, hp.obo, phenotype_to_genes.txt, phenotype.hpoa | mondo.owl, mondo.obo |
| Graph Name Format | hpo~{version} | mondo~{version} |
| Schedule | cron(0 9 * * ? *) - 9:00 UTC | cron(10 9 * * ? *) - 9:10 UTC |
| File Size | 50-100MB (OWL) | 30-60MB (OWL) |
| Term Count | ~15,000 | ~20,000 |

## Test Scenarios

*(Most scenarios identical to UpdateHPOSFN - see update_hpo_statemachine_tests.md)*

### 1. Happy Path - New MONDO Version

**Objective**: Verify complete MONDO update workflow

**Test Steps**:
1. Start execution with test mode disabled
2. Monitor through all stages
3. Verify MONDO ontology loaded to Neptune
4. Verify hierarchy materialized to Iceberg

**Expected Results**:
- DownloadGithubReleaseFunction fetches from monarch-initiative/mondo
- Assets downloaded: mondo.owl, mondo.obo
- Graph name format: `mondo~v2024-01-01`
- EventBridge event: source="mondo"
- Execution status: SUCCEEDED

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_execution(
    stateMachineArn=update_mondo_sfn_arn,
    name=f"test-mondo-update-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for completion
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=response['executionArn'],
    WaiterConfig={'Delay': 60, 'MaxAttempts': 40}
)

# Verify DynamoDB timestamp
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#mondo', 'SK': 'VERSION'})
assert 'last_install_timestamp' in response['Item']

# Verify hierarchy in Iceberg
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{ontology_hierarchy_table}
    WHERE ontology_source = 'mondo'
"""
result = execute_athena_query(query)
assert result[0]['count'] > 15000  # MONDO has thousands of disease terms
```

---

### 2. Asset Filtering - mondo.owl and mondo.obo

**Objective**: Verify correct MONDO-specific assets are used

**Test Steps**:
1. Verify Download MONDO requests mondo.owl and mondo.obo
2. Verify Install MONDO uses mondo.owl
3. Verify Prepare uses mondo.obo

**Expected Results**:
- Download MONDO asset_names: ["mondo.owl", "mondo.obo"]
- Filtered for mondo.owl in Install step
- Filtered for mondo.obo in Prepare Hierarchy step

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(stateMachineArn=update_mondo_sfn_arn)
definition = json.loads(sm_def['definition'])

download_params = definition['States']['Download MONDO']['Parameters']['Payload']
assert download_params['source_name'] == 'mondo'
assert download_params['user'] == 'monarch-initiative'
assert download_params['repo'] == 'mondo'
assert 'mondo.owl' in download_params['asset_names']
assert 'mondo.obo' in download_params['asset_names']
```

---

### 3. GitHub Repository Configuration

**Objective**: Verify correct GitHub repository is queried

**Test Steps**:
1. Examine state machine definition
2. Verify DownloadGithubReleaseFunction parameters
3. Confirm user=monarch-initiative, repo=mondo

**Expected Results**:
- Parameters.Payload.user: "monarch-initiative"
- Parameters.Payload.repo: "mondo"
- Parameters.Payload.source_name: "mondo"

**Verification**:
```python
definition = json.loads(sfn_client.describe_state_machine(
    stateMachineArn=update_mondo_sfn_arn
)['definition'])

download_state = definition['States']['Download MONDO']
payload = download_state['Parameters']['Payload']

assert payload['user'] == 'monarch-initiative'
assert payload['repo'] == 'mondo'
assert payload['source_name'] == 'mondo'
```

---

### 4. Scheduled Execution Time

**Objective**: Verify MONDO update is scheduled 10 minutes after HPO

**Test Steps**:
1. Query EventBridge for UpdateMondo-OnSchedule rule
2. Verify schedule expression
3. Confirm it's 10 minutes after HPO (avoid concurrent loads)

**Expected Results**:
- Rule name: UpdateMondo-OnSchedule
- Schedule: cron(10 9 * * ? *) - 9:10 UTC (4:10 AM EST)
- HPO schedule: cron(0 9 * * ? *) - 9:00 UTC (4:00 AM EST)
- 10-minute offset prevents concurrent Neptune loads

**Verification**:
```python
events_client = boto3.client('events')

# Get MONDO schedule rule
mondo_rules = events_client.list_rules(NamePrefix='UpdateMondo-OnSchedule')
mondo_rule = mondo_rules['Rules'][0]
assert mondo_rule['ScheduleExpression'] == 'cron(10 9 * * ? *)'

# Compare to HPO schedule
hpo_rules = events_client.list_rules(NamePrefix='UpdateHPO-OnSchedule')
hpo_rule = hpo_rules['Rules'][0]
assert hpo_rule['ScheduleExpression'] == 'cron(0 9 * * ? *)'

# Verify 10-minute offset
```

---

### 5. Event Bus Source Field

**Objective**: Verify EventBridge event correctly identifies MONDO as source

**Test Steps**:
1. Set up event capture
2. Run UpdateMondoSFN
3. Verify event detail.source = "mondo"

**Expected Results**:
- Event Detail: {"source": "mondo", "version": "v2024-01-01"}
- DetailType: "source_updated"
- Source: "PheBee"
- EventBusName: PheBeeBus

**Verification**:
```python
# Set up event rule (see HPO tests for full setup)
# Run execution
# Capture event

assert event['detail']['source'] == 'mondo'
assert event['detail-type'] == 'source_updated'
assert 'version' in event['detail']
```

---

### 6. No Update - Same MONDO Version

**Objective**: Verify idempotency specific to MONDO

**Test Steps**:
1. Run UpdateMondoSFN to install current version
2. Run again immediately
3. Verify second execution skips

**Expected Results**:
- Second execution: downloaded=false
- Quick completion (< 60s)
- No InstallRDFXMLSFN call

**Verification**:
```python
# First run
response1 = sfn_client.start_execution(
    stateMachineArn=update_mondo_sfn_arn,
    name=f"test-mondo-first-{int(time.time())}",
    input=json.dumps({"test": false})
)
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response1['executionArn'])

# Second run (no-op expected)
time.sleep(5)
response2 = sfn_client.start_execution(
    stateMachineArn=update_mondo_sfn_arn,
    name=f"test-mondo-second-{int(time.time())}",
    input=json.dumps({"test": false})
)
start_time = time.time()
waiter.wait(executionArn=response2['executionArn'])
duration = time.time() - start_time

assert duration < 60  # Quick no-op
```

---

### 7. DynamoDB Source Record

**Objective**: Verify MONDO-specific DynamoDB record structure

**Test Steps**:
1. Run UpdateMondoSFN
2. Query DynamoDB for SOURCE#mondo record
3. Verify fields

**Expected Results**:
- PK: 'SOURCE#mondo'
- SK: 'VERSION'
- source: 'mondo'
- version: latest MONDO version
- last_install_timestamp: ISO timestamp

**Verification**:
```python
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#mondo', 'SK': 'VERSION'})
item = response['Item']

assert item['source'] == 'mondo'
assert 'version' in item
assert 'last_install_timestamp' in item
assert item['version'].startswith('v')  # e.g., v2024-01-01
```

---

### 8. Hierarchy Data Isolation

**Objective**: Verify MONDO hierarchy is isolated from HPO hierarchy in Iceberg

**Test Steps**:
1. Install both HPO and MONDO
2. Query ontology_hierarchy table
3. Verify both ontologies coexist without conflicts

**Expected Results**:
- Records with ontology_source='hpo'
- Records with ontology_source='mondo'
- No cross-contamination
- Each ontology has distinct versions

**Verification**:
```python
# Run both updates
# ... execute UpdateHPOSFN and UpdateMondoSFN ...

# Query hierarchy table
query = f"""
    SELECT ontology_source, version, COUNT(*) as count
    FROM {database}.{ontology_hierarchy_table}
    GROUP BY ontology_source, version
"""
result = execute_athena_query(query)

# Verify both present
sources = {r['ontology_source'] for r in result}
assert 'hpo' in sources
assert 'mondo' in sources

# Verify distinct counts
hpo_count = [r['count'] for r in result if r['ontology_source'] == 'hpo'][0]
mondo_count = [r['count'] for r in result if r['ontology_source'] == 'mondo'][0]

assert hpo_count > 10000  # HPO has ~15K terms
assert mondo_count > 15000  # MONDO has ~20K terms
```

---

### 9. Concurrent HPO and MONDO Updates

**Objective**: Verify HPO and MONDO updates can run concurrently

**Test Steps**:
1. Start UpdateHPOSFN
2. Start UpdateMondoSFN (10 seconds later to simulate schedule)
3. Monitor both executions
4. Verify both succeed

**Expected Results**:
- Both state machines execute concurrently
- Neptune queues loads appropriately
- No conflicts or deadlocks
- Both complete successfully

**Verification**:
```python
# Start HPO update
hpo_response = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-concurrent-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait 10 seconds (simulate schedule offset)
time.sleep(10)

# Start MONDO update
mondo_response = sfn_client.start_execution(
    stateMachineArn=update_mondo_sfn_arn,
    name=f"test-mondo-concurrent-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for both
for arn in [hpo_response['executionArn'], mondo_response['executionArn']]:
    waiter = sfn_client.get_waiter('execution_succeeded')
    waiter.wait(executionArn=arn, WaiterConfig={'Delay': 60, 'MaxAttempts': 60})

# Verify both completed
```

---

### 10. MONDO Version Format

**Objective**: Verify MONDO version follows expected format

**Test Steps**:
1. Run UpdateMondoSFN
2. Extract version from output
3. Verify format matches GitHub release tags

**Expected Results**:
- Version format: v{YYYY}-{MM}-{DD} or v{YYYY}-{MM}
- Examples: v2024-01-01, v2024-02-05, v2023-12-01
- Graph name: mondo~v2024-01-01

**Verification**:
```python
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

mondo_version = output['mondo_version']

# Verify format
import re
assert re.match(r'v\d{4}-\d{2}-\d{2}', mondo_version) or \
       re.match(r'v\d{4}-\d{2}', mondo_version)

# Verify graph name
expected_graph_name = f'mondo~{mondo_version}'
# Check in DynamoDB or Neptune
```

---

## Performance Benchmarks

| Stage | Expected Duration | Notes |
|-------|-------------------|-------|
| Download MONDO | 1-3 min | Smaller than HPO |
| Install MONDO | 3-10 min | Neptune load |
| Prepare Hierarchy | 2-8 min | Parse OBO |
| Insert Batches | 4-12 min | Distributed map |
| Cleanup | 30s-2 min | S3 cleanup |
| **Total** | **10-35 min** | Typically faster than HPO |

---

## Error Scenarios

*(Same as UpdateHPOSFN, with MONDO-specific resources)*

| Error Type | Stage | Expected Behavior |
|------------|-------|-------------------|
| GitHub API unavailable | Download MONDO | Retry, eventual failure |
| mondo.owl not found | Get MONDO OWL Info | Empty array, Install fails |
| Neptune load failure | Install MONDO | Propagates from nested SFN |
| OBO parse error | Prepare Hierarchy | Lambda failure |

---

## Additional Test Coverage

### Test Scenarios 11-15
- Test mode behavior (identical to HPO)
- Lambda retry logic (identical to HPO)
- Distributed map configuration (identical to HPO)
- Delete old hierarchy data (MONDO-specific partition)
- Batch file cleanup (identical to HPO)

---

## Notes

- UpdateMondoSFN scheduled 10 minutes after UpdateHPOSFN to prevent concurrent Neptune loads
- MONDO focuses on disease terms while HPO focuses on phenotype terms
- Both use same Iceberg table (ontology_hierarchy) with ontology_source partitioning
- MONDO has more terms (~20K) than HPO (~15K)
- MONDO OBO file is typically smaller and faster to process
- All other behaviors identical to UpdateHPOSFN
