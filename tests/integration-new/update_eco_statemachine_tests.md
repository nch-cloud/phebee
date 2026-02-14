# UpdateECOSFN State Machine Integration Tests

## Overview

The UpdateECOSFN state machine downloads the latest ECO (Evidence & Conclusion Ontology) release from GitHub, installs it to Neptune, and fires an update event to EventBridge. Unlike HPO and MONDO, ECO does **not** materialize hierarchy to Iceberg tables (simpler workflow).

**State Machine**: `UpdateECOSFN`
**Type**: Scheduled maintenance workflow (can also be triggered manually)
**Execution Mode**: Asynchronous
**Max Duration**: 10-20 minutes

## Architecture

```
Download ECO
    ↓
Was New ECO Version Downloaded? (Choice)
    ├─ YES → Get ECO OWL Info
    └─ NO  → Success (skip update)
       ↓
Install ECO (calls InstallRDFXMLSFN.sync)
       ↓
Update Install Timestamp
       ↓
Fire Update Event
       ↓
    Success
```

## Key Differences from HPO/MONDO

| Aspect | HPO/MONDO | ECO |
|--------|-----------|-----|
| Hierarchy Materialization | YES (Delete → Prepare → Insert → Cleanup) | NO (skipped entirely) |
| Assets | OWL + OBO + others | OWL only (eco.owl) |
| GitHub Repo | obophenotype/human-phenotype-ontology, monarch-initiative/mondo | evidenceontology/evidenceontology |
| Extract ZIP | No | Yes |
| Schedule | cron(0 9...), cron(10 9...) | cron(20 9 * * ? *) - 9:20 UTC |
| Workflow Steps | 10 | 5 |
| Duration | 15-45 min | 5-15 min |

## Test Scenarios

### 1. Happy Path - New ECO Version

**Objective**: Verify complete ECO update workflow (simplified)

**Test Steps**:
1. Start execution with test mode disabled
2. Monitor through all stages
3. Verify ECO ontology loaded to Neptune
4. Verify NO hierarchy materialization occurs

**Expected Results**:
- DownloadGithubReleaseFunction fetches from evidenceontology/evidenceontology
- Asset downloaded: eco.owl (extracted from ZIP)
- extract_zip parameter: "true"
- Graph name format: `eco~v2023-09-14`
- Install ECO completes successfully
- DynamoDB timestamp updated
- EventBridge event fired with source="eco"
- NO hierarchy materialization steps
- Execution status: SUCCEEDED

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-update-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for completion
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(
    executionArn=response['executionArn'],
    WaiterConfig={'Delay': 30, 'MaxAttempts': 40}  # Shorter than HPO/MONDO
)

# Verify DynamoDB timestamp
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#eco', 'SK': 'VERSION'})
assert 'last_install_timestamp' in response['Item']
assert response['Item']['source'] == 'eco'

# Verify NO hierarchy data in Iceberg
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{ontology_hierarchy_table}
    WHERE ontology_source = 'eco'
"""
result = execute_athena_query(query)
assert result[0]['count'] == 0  # ECO does not materialize hierarchy
```

---

### 2. ZIP Extraction

**Objective**: Verify ECO release ZIP file is extracted correctly

**Test Steps**:
1. Start execution
2. Verify Download ECO includes extract_zip="true"
3. Verify eco.owl is extracted from ZIP
4. Verify Install ECO receives extracted file path

**Expected Results**:
- Download ECO Parameters.Payload.extract_zip: "true"
- DownloadGithubReleaseFunction extracts eco.owl from release ZIP
- Get ECO OWL Info filters for eco.owl
- Install ECO uses extracted eco.owl S3 path

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(stateMachineArn=update_eco_sfn_arn)
definition = json.loads(sm_def['definition'])

download_params = definition['States']['Download ECO']['Parameters']['Payload']
assert download_params['extract_zip'] == 'true'
assert 'eco.owl' in download_params['asset_names']

# Verify extraction during execution
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

# eco.owl should be in assets
eco_assets = [a for a in output['eco']['Payload']['assets']
              if a['asset_name'] == 'eco.owl']
assert len(eco_assets) == 1
assert 's3://' in eco_assets[0]['asset_path']
```

---

### 3. No Hierarchy Materialization

**Objective**: Verify ECO workflow skips all hierarchy steps

**Test Steps**:
1. Start execution
2. Monitor execution history
3. Confirm no hierarchy-related states are entered

**Expected Results**:
- States NOT executed:
  - Delete Old ECO Hierarchy Data
  - Prepare ECO Hierarchy Batches
  - Insert ECO Hierarchy Batches
  - Cleanup Batch Files
- Workflow goes directly from Update Install Timestamp → Fire Update Event
- Execution faster than HPO/MONDO

**Verification**:
```python
# Get execution history
history = sfn_client.get_execution_history(executionArn=execution_arn)

# Extract all state names
state_names = [e.get('stateEnteredEventDetails', {}).get('name', '')
               for e in history['events']
               if e['type'] == 'TaskStateEntered']

# Verify hierarchy states NOT present
hierarchy_states = [
    'Delete Old ECO Hierarchy Data',
    'Prepare ECO Hierarchy Batches',
    'Insert ECO Hierarchy Batches',
    'Cleanup Batch Files'
]

for state in hierarchy_states:
    assert state not in state_names

# Verify simplified workflow
expected_states = [
    'Download ECO',
    'Get ECO OWL Info',
    'Install ECO',
    'Update Install Timestamp',
    'Fire Update Event'
]

for state in expected_states:
    assert state in state_names
```

---

### 4. GitHub Repository Configuration

**Objective**: Verify correct ECO GitHub repository

**Test Steps**:
1. Examine state machine definition
2. Verify DownloadGithubReleaseFunction parameters

**Expected Results**:
- Parameters.Payload.source_name: "eco"
- Parameters.Payload.user: "evidenceontology"
- Parameters.Payload.repo: "evidenceontology"

**Verification**:
```python
definition = json.loads(sfn_client.describe_state_machine(
    stateMachineArn=update_eco_sfn_arn
)['definition'])

download_state = definition['States']['Download ECO']
payload = download_state['Parameters']['Payload']

assert payload['source_name'] == 'eco'
assert payload['user'] == 'evidenceontology'
assert payload['repo'] == 'evidenceontology'
assert payload['extract_zip'] == 'true'
```

---

### 5. Scheduled Execution Time

**Objective**: Verify ECO update is scheduled 20 minutes after HPO

**Test Steps**:
1. Query EventBridge for UpdateECO-OnSchedule rule
2. Verify schedule expression
3. Confirm staggered scheduling

**Expected Results**:
- Rule name: UpdateECO-OnSchedule
- Schedule: cron(20 9 * * ? *) - 9:20 UTC (4:20 AM EST)
- HPO: 9:00 UTC, MONDO: 9:10 UTC, ECO: 9:20 UTC
- 10-minute intervals prevent concurrent Neptune loads

**Verification**:
```python
events_client = boto3.client('events')

# Get all ontology update schedules
hpo_rule = events_client.list_rules(NamePrefix='UpdateHPO-OnSchedule')['Rules'][0]
mondo_rule = events_client.list_rules(NamePrefix='UpdateMondo-OnSchedule')['Rules'][0]
eco_rule = events_client.list_rules(NamePrefix='UpdateECO-OnSchedule')['Rules'][0]

assert hpo_rule['ScheduleExpression'] == 'cron(0 9 * * ? *)'
assert mondo_rule['ScheduleExpression'] == 'cron(10 9 * * ? *)'
assert eco_rule['ScheduleExpression'] == 'cron(20 9 * * ? *)'

# Verify 10-minute intervals
```

---

### 6. Asset Filtering - eco.owl Only

**Objective**: Verify only eco.owl is requested and used

**Test Steps**:
1. Verify Download ECO requests only eco.owl
2. Verify no OBO file requested (unlike HPO/MONDO)

**Expected Results**:
- asset_names: ["eco.owl"]
- No OBO file in assets (hierarchy not materialized)
- Get ECO OWL Info filters for eco.owl

**Verification**:
```python
definition = json.loads(sfn_client.describe_state_machine(
    stateMachineArn=update_eco_sfn_arn
)['definition'])

download_params = definition['States']['Download ECO']['Parameters']['Payload']
assert download_params['asset_names'] == ['eco.owl']
assert 'eco.obo' not in download_params['asset_names']

# Verify Get ECO OWL Info filter
get_owl_params = definition['States']['Get ECO OWL Info']['Parameters']
assert 'eco.owl' in get_owl_params['filtered_assets.$']
assert 'obo_assets' not in get_owl_params  # No OBO handling
```

---

### 7. Faster Execution Time

**Objective**: Verify ECO updates complete faster than HPO/MONDO

**Test Steps**:
1. Run UpdateECOSFN
2. Measure total execution time
3. Compare to HPO/MONDO benchmarks

**Expected Results**:
- ECO execution: 5-15 minutes
- HPO execution: 15-45 minutes
- MONDO execution: 10-35 minutes
- ECO ~2-3x faster due to no hierarchy materialization

**Verification**:
```python
start_time = time.time()

response = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-timing-{int(time.time())}",
    input=json.dumps({"test": false})
)

waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

duration = time.time() - start_time
print(f"ECO update completed in {duration/60:.1f} minutes")

assert duration < 900  # Less than 15 minutes
```

---

### 8. No Update - Same ECO Version

**Objective**: Verify idempotency for ECO

**Test Steps**:
1. Run UpdateECOSFN to install current version
2. Run again immediately
3. Verify second execution skips

**Expected Results**:
- Second execution: downloaded=false
- Quick completion (< 30s)
- No InstallRDFXMLSFN call

**Verification**:
```python
# First run
response1 = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-first-{int(time.time())}",
    input=json.dumps({"test": false})
)
waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response1['executionArn'])

# Second run (no-op expected)
time.sleep(5)
response2 = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-second-{int(time.time())}",
    input=json.dumps({"test": false})
)
start_time = time.time()
waiter.wait(executionArn=response2['executionArn'])
duration = time.time() - start_time

assert duration < 30  # Very quick no-op
```

---

### 9. Event Bus Source Field

**Objective**: Verify EventBridge event correctly identifies ECO

**Test Steps**:
1. Set up event capture
2. Run UpdateECOSFN
3. Verify event detail.source = "eco"

**Expected Results**:
- Event Detail: {"source": "eco", "version": "v2023-09-14"}
- DetailType: "source_updated"
- Source: "PheBee"

**Verification**:
```python
# Set up event rule to capture ECO updates
events_client = boto3.client('events')
rule_name = f'test-eco-update-event-{int(time.time())}'

events_client.put_rule(
    Name=rule_name,
    EventPattern=json.dumps({
        "source": ["PheBee"],
        "detail-type": ["source_updated"],
        "detail": {
            "source": ["eco"]
        }
    }),
    State='ENABLED',
    EventBusName=phebee_bus_name
)

# ... run execution and capture event ...

assert event['detail']['source'] == 'eco'
assert event['detail-type'] == 'source_updated'
```

---

### 10. DynamoDB Source Record

**Objective**: Verify ECO-specific DynamoDB record

**Test Steps**:
1. Run UpdateECOSFN
2. Query DynamoDB for SOURCE#eco
3. Verify structure

**Expected Results**:
- PK: 'SOURCE#eco'
- SK: 'VERSION'
- source: 'eco'
- version: latest ECO version
- last_install_timestamp: ISO timestamp

**Verification**:
```python
table = dynamodb.Table(dynamodb_table_name)
response = table.get_item(Key={'PK': 'SOURCE#eco', 'SK': 'VERSION'})
item = response['Item']

assert item['source'] == 'eco'
assert 'version' in item
assert 'last_install_timestamp' in item
assert item['version'].startswith('v')  # e.g., v2023-09-14
```

---

### 11. Concurrent with HPO/MONDO

**Objective**: Verify all three ontologies can update in sequence

**Test Steps**:
1. Start UpdateHPOSFN at T+0
2. Start UpdateMondoSFN at T+10min
3. Start UpdateECOSFN at T+20min
4. Monitor all three
5. Verify all succeed

**Expected Results**:
- All three complete successfully
- Neptune handles loads sequentially via queue
- No conflicts or interference
- Staggered schedule prevents concurrency issues

**Verification**:
```python
# Start HPO
hpo_response = sfn_client.start_execution(
    stateMachineArn=update_hpo_sfn_arn,
    name=f"test-hpo-seq-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait 10 minutes (simulate schedule)
time.sleep(600)

# Start MONDO
mondo_response = sfn_client.start_execution(
    stateMachineArn=update_mondo_sfn_arn,
    name=f"test-mondo-seq-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait 10 minutes
time.sleep(600)

# Start ECO
eco_response = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-seq-{int(time.time())}",
    input=json.dumps({"test": false})
)

# Wait for all to complete
for arn in [hpo_response['executionArn'],
            mondo_response['executionArn'],
            eco_response['executionArn']]:
    waiter = sfn_client.get_waiter('execution_succeeded')
    waiter.wait(executionArn=arn, WaiterConfig={'Delay': 60, 'MaxAttempts': 60})
```

---

### 12. ECO Version Format

**Objective**: Verify ECO version format

**Test Steps**:
1. Run UpdateECOSFN
2. Extract version from output
3. Verify format

**Expected Results**:
- Version format: v{YYYY}-{MM}-{DD}
- Example: v2023-09-14, v2024-01-15
- Graph name: eco~v2023-09-14

**Verification**:
```python
execution = sfn_client.describe_execution(executionArn=execution_arn)
output = json.loads(execution['output'])

eco_version = output['eco_version']

# Verify format
import re
assert re.match(r'v\d{4}-\d{2}-\d{2}', eco_version)

# Verify graph name
expected_graph_name = f'eco~{eco_version}'
```

---

### 13. ZIP Extraction Failure

**Objective**: Verify handling when ZIP extraction fails

**Test Steps**:
1. Force ZIP extraction failure (corrupted ZIP)
2. Verify DownloadGithubReleaseFunction fails
3. Verify workflow fails gracefully

**Expected Results**:
- DownloadGithubReleaseFunction raises exception
- Execution status: FAILED
- Error indicates extraction failure

**Verification**:
```python
# This may require simulating a corrupted ZIP in test environment
# Or removing extract permissions

# Monitor for extraction error in execution history
execution = sfn_client.describe_execution(executionArn=execution_arn)
assert execution['status'] == 'FAILED'
assert 'extract' in execution.get('cause', '').lower() or \
       'zip' in execution.get('cause', '').lower()
```

---

### 14. Test Mode Behavior

**Objective**: Verify test mode for ECO

**Test Steps**:
1. Start execution with test=true
2. Verify test mode propagated
3. Verify workflow completes

**Expected Results**:
- DownloadGithubReleaseFunction receives test: true
- May use cached/test data
- Workflow completes without production changes

**Verification**:
```python
response = sfn_client.start_execution(
    stateMachineArn=update_eco_sfn_arn,
    name=f"test-eco-test-mode-{int(time.time())}",
    input=json.dumps({"test": true})
)

waiter = sfn_client.get_waiter('execution_succeeded')
waiter.wait(executionArn=response['executionArn'])

# Verify test mode in execution history
history = sfn_client.get_execution_history(executionArn=response['executionArn'])
# ... check test parameter propagation ...
```

---

### 15. Execution State Sequence

**Objective**: Verify correct state sequence for simplified ECO workflow

**Test Steps**:
1. Run execution
2. Get execution history
3. Analyze state transitions

**Expected Results**:
- State sequence:
  1. Download ECO
  2. Was New ECO Version Downloaded?
  3. Get ECO OWL Info (if new)
  4. Install ECO
  5. Update Install Timestamp
  6. Fire Update Event
  7. Success
- No hierarchy states

**Verification**:
```python
history = sfn_client.get_execution_history(executionArn=execution_arn)

state_entries = [e for e in history['events'] if e['type'] == 'TaskStateEntered']
state_names = [e['stateEnteredEventDetails']['name'] for e in state_entries]

expected_sequence = [
    'Download ECO',
    'Get ECO OWL Info',
    'Install ECO',
    'Update Install Timestamp',
    'Fire Update Event'
]

for expected, actual in zip(expected_sequence, state_names):
    assert expected in actual
```

---

## Performance Benchmarks

| Stage | Expected Duration | Notes |
|-------|-------------------|-------|
| Download ECO | 1-2 min | Small ZIP file |
| Install ECO | 2-8 min | Neptune load (ECO is smaller) |
| Update Timestamp | < 30s | DynamoDB write |
| Fire Event | < 10s | EventBridge |
| **Total** | **5-15 min** | Much faster than HPO/MONDO |

---

## Error Scenarios

| Error Type | Stage | Expected Behavior |
|------------|-------|-------------------|
| GitHub API unavailable | Download ECO | Retry, eventual failure |
| ZIP extraction failure | Download ECO | Immediate failure |
| eco.owl not found | Get ECO OWL Info | Empty array, Install fails |
| Neptune load failure | Install ECO | Propagates from nested SFN |
| EventBridge unavailable | Fire Update Event | State machine fails |

---

## Notes

- **Simplest ontology workflow** - no hierarchy materialization
- ECO scheduled last (9:20 UTC) after HPO (9:00) and MONDO (9:10)
- ECO is smallest ontology (~600 terms for evidence types)
- ECO updated less frequently than HPO/MONDO (releases every few months vs monthly)
- **extract_zip: "true"** is unique to ECO - releases come as ZIP archives
- No OBO file needed since hierarchy not materialized
- ECO provides evidence codes used in evidence records (ECO:0000033, etc.)
- Workflow duration typically 50-70% faster than HPO/MONDO
