# ImportPhenopacketsSFN State Machine Integration Tests

## Overview

The ImportPhenopacketsSFN state machine processes ZIP files containing GA4GH Phenopackets. It parses the ZIP, processes each phenopacket to extract subject and evidence data, creates subjects in PheBee with DynamoDB/Neptune, and creates evidence records in Iceberg. Uses nested distributed maps for parallel processing.

**State Machine**: `ImportPhenopacketsSFN`
**Type**: User-triggered workflow (via API Gateway)
**Execution Mode**: Synchronous (startSyncExecution)
**Max Duration**: 5 minutes (API Gateway timeout)

## Architecture

```
ParseZipFile
    ↓
ImportPhenopacketsMap (Distributed Map - S3 ItemReader)
    ├─ RenameInput
    ├─ ProcessPhenopacket
    ├─ CreateSubject
    ├─ ParseSubjectBody
    ├─ PrepareEvidencePayload
    └─ CreateEvidenceMap (Nested Map - MaxConcurrency: 32)
         └─ CreateEvidence
    ↓
  Success
```

## Key Features

- **Distributed Map**: Processes multiple phenopackets in parallel (up to 32 concurrent)
- **S3 ItemReader**: Reads phenopackets from JSON lines file
- **Nested Map**: Each phenopacket's evidence records processed in parallel
- **Synchronous Execution**: Called via API Gateway with startSyncExecution
- **Input/Output Transformation**: Complex JSONPath and States.StringToJson operations

## Test Scenarios

### 1. Happy Path - Small Collection (10 Phenopackets)

**Objective**: Verify complete import workflow for small phenopacket collection

**Test Steps**:
1. Create ZIP file with 10 GA4GH phenopackets
2. Upload to S3: `s3://bucket/phenopackets/test-collection.zip`
3. Invoke state machine via API Gateway or directly:
   ```json
   {
     "project_id": "test-project-001",
     "s3_path": "s3://bucket/phenopackets/test-collection.zip",
     "output_s3_path": "s3://bucket/phenopackets/processed/test-collection.jsonl"
   }
   ```
4. Monitor execution
5. Verify all subjects and evidence created

**Expected Results**:
- ParseZipFile extracts phenopackets from ZIP
- Creates JSONL file at output_s3_path
- ImportPhenopacketsMap processes each phenopacket:
  - ProcessPhenopacket extracts subject_payload and evidence_payload
  - CreateSubject creates/updates subject in DynamoDB and Neptune
  - PrepareEvidencePayload replaces placeholder subject_id with actual subject_id
  - CreateEvidenceMap creates evidence records in Iceberg
- Execution status: SUCCEEDED
- All 10 subjects exist in DynamoDB
- All evidence records in Iceberg
- Return value: Summary with subject_ids array

**Verification**:
```python
# Start execution
sfn_client = boto3.client('stepfunctions')
response = sfn_client.start_sync_execution(
    stateMachineArn=import_phenopackets_sfn_arn,
    name=f"test-import-{int(time.time())}",
    input=json.dumps({
        "project_id": "test-project-001",
        "s3_path": "s3://bucket/phenopackets/test-collection.zip",
        "output_s3_path": "s3://bucket/phenopackets/processed/test-collection.jsonl"
    })
)

# Synchronous call waits for completion
assert response['status'] == 'SUCCEEDED'
output = json.loads(response['output'])

# Verify summary
assert 'Summary' in output
assert len(output['Summary']['subject_ids']) == 10

# Verify subjects in DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

for subject_id in output['Summary']['subject_ids']:
    response = table.get_item(
        Key={'PK': f'SUBJECT#{subject_id}', 'SK': 'METADATA'}
    )
    assert 'Item' in response

# Verify evidence in Iceberg
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{evidence_table}
    WHERE subject_id IN ({','.join(["'" + s + "'" for s in output['Summary']['subject_ids']])})
"""
result = execute_athena_query(query)
assert result[0]['count'] > 0
```

---

### 2. Distributed Map Parallelism

**Objective**: Verify phenopackets are processed in parallel

**Test Steps**:
1. Create ZIP with 32 phenopackets (matches MaxConcurrency)
2. Start execution
3. Monitor execution events to verify parallel processing

**Expected Results**:
- ImportPhenopacketsMap Mode: DISTRIBUTED
- ExecutionType: STANDARD
- MaxConcurrency: 32
- Multiple child executions created simultaneously
- Processing time ~1/N of sequential processing

**Verification**:
```python
# Check state machine definition
sm_def = sfn_client.describe_state_machine(
    stateMachineArn=import_phenopackets_sfn_arn
)
definition = json.loads(sm_def['definition'])

map_state = definition['States']['ImportPhenopacketsMap']
assert map_state['ItemProcessor']['ProcessorConfig']['Mode'] == 'DISTRIBUTED'
assert map_state['MaxConcurrency'] == 32

# Monitor execution - distributed map creates child executions
# (Child executions run in parallel)
```

---

### 3. S3 ItemReader - JSONL Format

**Objective**: Verify S3 ItemReader correctly reads phenopackets from JSONL

**Test Steps**:
1. Verify ParseZipFile creates JSONL file
2. Verify ItemReader configuration
3. Verify each line becomes a map item

**Expected Results**:
- ParseZipFile output includes bucket and key
- ItemReader.Resource: `arn:aws:states:::s3:getObject`
- ItemReader.ReaderConfig.InputType: JSON
- ItemReader.Parameters uses $.Payload.bucket and $.Payload.key
- Each JSONL line processed as separate map item

**Verification**:
```python
# Verify JSONL created by ParseZipFile
s3_client = boto3.client('s3')
output_path = "s3://bucket/phenopackets/processed/test-collection.jsonl"
bucket, key = output_path.replace('s3://', '').split('/', 1)

obj = s3_client.get_object(Bucket=bucket, Key=key)
jsonl_content = obj['Body'].read().decode('utf-8')

lines = jsonl_content.strip().split('\n')
assert len(lines) == 10  # One per phenopacket

# Verify each line is valid JSON
for line in lines:
    phenopacket = json.loads(line)
    assert 'id' in phenopacket or 'subject' in phenopacket
```

---

### 4. Subject ID Replacement

**Objective**: Verify PrepareEvidencePayload correctly replaces placeholder subject_id

**Test Steps**:
1. Phenopacket with subject.id = "patient-001"
2. CreateSubject assigns subject_id = "generated-uuid"
3. PrepareEvidencePayload replaces "patient-001" with "generated-uuid" in evidence
4. CreateEvidence receives correct subject_id

**Expected Results**:
- ProcessPhenopacket creates evidence_payload with phenopacket's subject.id
- CreateSubject returns actual subject_id (UUID or project-specific ID)
- ParseSubjectBody extracts subject_id from CreateSubject response
- PrepareEvidencePayload receives both evidence_payload and subject_id
- PrepareEvidencePayload replaces all occurrences of old ID with new ID
- CreateEvidence uses corrected subject_id

**Verification**:
```python
# Create phenopacket with known subject ID
phenopacket = {
    "id": "phenopacket-001",
    "subject": {
        "id": "patient-external-001",
        "sex": "MALE"
    },
    "phenotypicFeatures": [
        {
            "type": {"id": "HP:0001250", "label": "Seizure"}
        }
    ]
}

# Import via state machine
# ... execute and wait ...

# Query evidence to verify subject_id was replaced
query = f"""
    SELECT subject_id
    FROM {database}.{evidence_table}
    WHERE term_id = 'HP:0001250'
    LIMIT 1
"""
result = execute_athena_query(query)
actual_subject_id = result[0]['subject_id']

# Verify it's NOT the external ID
assert actual_subject_id != 'patient-external-001'

# Verify it's a valid PheBee subject ID
response = table.get_item(
    Key={'PK': f'SUBJECT#{actual_subject_id}', 'SK': 'METADATA'}
)
assert 'Item' in response
```

---

### 5. Nested Evidence Map

**Objective**: Verify evidence records within each phenopacket are processed in parallel

**Test Steps**:
1. Create phenopacket with 50 phenotypic features
2. Import phenopacket
3. Verify CreateEvidenceMap processes all 50 in parallel

**Expected Results**:
- CreateEvidenceMap ItemsPath: $.PreparedEvidence.Payload.fixed_evidence_payload
- MaxConcurrency: 32 (up to 32 evidence records in parallel per phenopacket)
- ItemProcessor.StartAt: CreateEvidence
- All 50 evidence records created

**Verification**:
```python
# Create phenopacket with many features
phenopacket = {
    "id": "phenopacket-many-features",
    "subject": {"id": "patient-002", "sex": "FEMALE"},
    "phenotypicFeatures": [
        {"type": {"id": f"HP:{1000000 + i:07d}"}}
        for i in range(50)
    ]
}

# Import via ZIP
# ... execute ...

# Verify all 50 evidence records created
query = f"""
    SELECT COUNT(*) as count
    FROM {database}.{evidence_table}
    WHERE subject_id = (
        SELECT subject_id FROM {database}.{evidence_table}
        WHERE term_id = 'HP:1000000' LIMIT 1
    )
"""
result = execute_athena_query(query)
assert result[0]['count'] == 50
```

---

### 6. API Gateway Integration

**Objective**: Verify state machine can be invoked via API Gateway

**Test Steps**:
1. Use API Gateway POST /phenopackets/import endpoint
2. Verify synchronous execution
3. Verify response returned within API timeout

**Expected Results**:
- API Gateway uses StartSyncExecution (not StartExecution)
- Response returned within 29 seconds (API Gateway limit)
- Response includes status and output

**Verification**:
```python
# Via API Gateway
import requests

api_url = "https://api-id.execute-api.us-east-1.amazonaws.com/phenopackets/import"
response = requests.post(
    api_url,
    headers={"Authorization": f"Bearer {auth_token}"},
    json={
        "project_id": "test-project-001",
        "s3_path": "s3://bucket/phenopackets/test-collection.zip",
        "output_s3_path": "s3://bucket/phenopackets/processed/test-collection.jsonl"
    },
    timeout=30
)

assert response.status_code == 200
result = response.json()

assert 'Summary' in result
assert 'subject_ids' in result['Summary']
```

---

### 7. Large Collection - Performance

**Objective**: Verify performance with larger phenopacket collections

**Test Steps**:
1. Create ZIP with 100 phenopackets
2. Each phenopacket has 10-20 features
3. Import and measure execution time

**Expected Results**:
- Distributed map processes up to 32 phenopackets concurrently
- Total execution time < 5 minutes (API Gateway limit)
- All subjects and evidence created successfully

**Verification**:
```python
start_time = time.time()

response = sfn_client.start_sync_execution(
    stateMachineArn=import_phenopackets_sfn_arn,
    name=f"test-large-import-{int(time.time())}",
    input=json.dumps({
        "project_id": "test-project-large",
        "s3_path": "s3://bucket/phenopackets/large-collection-100.zip",
        "output_s3_path": "s3://bucket/phenopackets/processed/large-100.jsonl"
    })
)

duration = time.time() - start_time

assert response['status'] == 'SUCCEEDED'
assert duration < 300  # < 5 minutes

# Verify count
output = json.loads(response['output'])
assert len(output['Summary']['subject_ids']) == 100
```

---

### 8. Duplicate Subject Handling

**Objective**: Verify CreateSubject handles duplicate subject IDs correctly

**Test Steps**:
1. Import phenopacket for subject "patient-001"
2. Import another phenopacket for same subject "patient-001"
3. Verify only one subject created, evidence added to existing

**Expected Results**:
- First import creates new subject
- Second import finds existing subject
- Both phenopackets' evidence linked to same subject
- DynamoDB has one subject record
- Neptune has one subject node

**Verification**:
```python
# First import
phenopacket1 = {
    "id": "pp-001",
    "subject": {"id": "patient-dup", "sex": "MALE"},
    "phenotypicFeatures": [{"type": {"id": "HP:0001250"}}]
}

# ... import phenopacket1 ...
output1 = json.loads(response1['output'])
subject_id1 = output1['Summary']['subject_ids'][0]

# Second import with same subject
phenopacket2 = {
    "id": "pp-002",
    "subject": {"id": "patient-dup", "sex": "MALE"},  # Same subject
    "phenotypicFeatures": [{"type": {"id": "HP:0002664"}}]  # Different feature
}

# ... import phenopacket2 ...
output2 = json.loads(response2['output'])
subject_id2 = output2['Summary']['subject_ids'][0]

# Verify same subject ID
assert subject_id1 == subject_id2

# Verify both evidence records exist
query = f"""
    SELECT term_id
    FROM {database}.{evidence_table}
    WHERE subject_id = '{subject_id1}'
"""
result = execute_athena_query(query)
term_ids = [r['term_id'] for r in result]
assert 'HP:0001250' in term_ids
assert 'HP:0002664' in term_ids
```

---

### 9. Error in Single Phenopacket

**Objective**: Verify failure in one phenopacket doesn't block others

**Test Steps**:
1. Create ZIP with 10 phenopackets
2. One phenopacket has invalid data (missing required field)
3. Import collection
4. Verify 9 succeed, 1 fails

**Expected Results**:
- Distributed map continues despite individual failures
- 9 phenopackets processed successfully
- 1 phenopacket fails (captured in execution history)
- Overall execution may succeed with partial results

**Verification**:
```python
# Create mix of valid and invalid phenopackets
# (Implementation depends on error handling strategy)

# ... import collection ...

# Check execution history for partial failures
history = sfn_client.get_execution_history(executionArn=execution_arn)

# Count successful vs failed child executions
# (Distributed map child executions tracked in history)
```

---

### 10. RenameInput Step

**Objective**: Verify RenameInput correctly restructures data for ProcessPhenopacket

**Test Steps**:
1. Examine state machine definition
2. Verify RenameInput Pass state
3. Verify output structure

**Expected Results**:
- RenameInput Type: Pass
- ResultPath: $.Data.Phenopacket
- OutputPath: $.Data
- Transforms S3 item reader output to expected format

**Verification**:
```python
# Check state machine definition
definition = json.loads(sfn_client.describe_state_machine(
    stateMachineArn=import_phenopackets_sfn_arn
)['definition'])

map_states = definition['States']['ImportPhenopacketsMap']['ItemProcessor']['States']
rename_state = map_states['RenameInput']

assert rename_state['Type'] == 'Pass'
assert rename_state['ResultPath'] == '$.Data.Phenopacket'
assert rename_state['OutputPath'] == '$.Data'
assert rename_state['Next'] == 'ProcessPhenopacket'
```

---

### 11. ParseSubjectBody - StringToJson

**Objective**: Verify States.StringToJson correctly parses CreateSubject response

**Test Steps**:
1. CreateSubject returns JSON string in Payload.body
2. ParseSubjectBody uses States.StringToJson
3. PrepareEvidencePayload receives parsed JSON

**Expected Results**:
- CreateSubject response format:
  ```json
  {
    "Payload": {
      "statusCode": 200,
      "body": "{\"subject\": {\"subject_id\": \"...\"}}"
    }
  }
  ```
- ParseSubjectBody parses body string to JSON
- Result in $.ParsedSubject.parsed_subject
- PrepareEvidencePayload can access $.ParsedSubject.parsed_subject.subject.subject_id

**Verification**:
```python
# Check state machine definition
parse_state = map_states['ParseSubjectBody']
assert parse_state['Type'] == 'Pass'
assert 'States.StringToJson' in parse_state['Parameters']['parsed_subject.$']

# Verify path reference
assert '$.CreateSubjectResult.Payload.body' in parse_state['Parameters']['parsed_subject.$']
```

---

### 12. ResultSelector - Subject IDs Summary

**Objective**: Verify ResultSelector extracts subject_ids for summary

**Test Steps**:
1. Complete import
2. Verify output contains Summary.subject_ids array
3. Verify array extraction

**Expected Results**:
- ImportPhenopacketsMap.ResultSelector:
  ```json
  {
    "subject_ids": "$[*].ProcessedData.subject_id"
  }
  ```
- Output includes array of all subject IDs created
- ResultPath: $.Summary

**Verification**:
```python
# Check state machine definition
map_state = definition['States']['ImportPhenopacketsMap']
assert 'ResultSelector' in map_state
assert map_state['ResultSelector']['subject_ids'] == '$[*].ProcessedData.subject_id'
assert map_state['ResultPath'] == '$.Summary'

# Verify actual output
output = json.loads(response['output'])
assert 'Summary' in output
assert 'subject_ids' in output['Summary']
assert isinstance(output['Summary']['subject_ids'], list)
```

---

### 13. Phenopacket v2 Format

**Objective**: Verify support for GA4GH Phenopacket v2 schema

**Test Steps**:
1. Create phenopackets following GA4GH Phenopacket v2 schema
2. Include required fields: id, subject, metaData
3. Import and verify correct parsing

**Expected Results**:
- ProcessPhenopacket handles v2 schema
- Extracts subject information correctly
- Extracts phenotypicFeatures with type.id
- Handles excluded flag for negated features
- Processes metaData.created timestamp

**Verification**:
```python
# Phenopacket v2 structure
phenopacket_v2 = {
    "id": "phenopacket-v2-test",
    "subject": {
        "id": "patient-v2-001",
        "sex": "FEMALE",
        "dateOfBirth": "2010-01-01T00:00:00Z"
    },
    "phenotypicFeatures": [
        {
            "type": {
                "id": "HP:0001250",
                "label": "Seizure"
            },
            "excluded": False
        },
        {
            "type": {
                "id": "HP:0002664",
                "label": "Neoplasm"
            },
            "excluded": True  # Negated feature
        }
    ],
    "metaData": {
        "created": "2024-01-01T00:00:00Z",
        "createdBy": "test-user",
        "resources": [
            {
                "id": "hp",
                "name": "Human Phenotype Ontology",
                "namespacePrefix": "HP",
                "url": "http://purl.obolibrary.org/obo/hp.owl",
                "version": "2024-04-26"
            }
        ]
    }
}

# Import and verify
# ... execute import ...

# Verify negated feature has qualifier
query = f"""
    SELECT qualifiers
    FROM {database}.{evidence_table}
    WHERE term_id = 'HP:0002664'
"""
result = execute_athena_query(query)
assert 'negated' in result[0]['qualifiers']
```

---

### 14. Empty Phenopacket Collection

**Objective**: Verify handling of empty ZIP file

**Test Steps**:
1. Create empty ZIP or ZIP with zero phenopackets
2. Attempt import
3. Verify appropriate handling

**Expected Results**:
- ParseZipFile may return empty array or fail
- ImportPhenopacketsMap processes zero items
- Summary.subject_ids is empty array
- Execution succeeds with no work done

**Verification**:
```python
# Import empty collection
response = sfn_client.start_sync_execution(
    stateMachineArn=import_phenopackets_sfn_arn,
    name=f"test-empty-{int(time.time())}",
    input=json.dumps({
        "project_id": "test-project-empty",
        "s3_path": "s3://bucket/phenopackets/empty.zip",
        "output_s3_path": "s3://bucket/phenopackets/processed/empty.jsonl"
    })
)

assert response['status'] == 'SUCCEEDED'
output = json.loads(response['output'])
assert output['Summary']['subject_ids'] == []
```

---

### 15. Timeout Handling

**Objective**: Verify behavior when approaching API Gateway timeout

**Test Steps**:
1. Create very large collection (200+ phenopackets)
2. Attempt import
3. Monitor execution time

**Expected Results**:
- If execution time > 5 minutes, API Gateway times out
- State machine continues executing (async from API perspective)
- Client receives timeout error but work completes
- Consider using async execution for large collections

**Verification**:
```python
# Large collection that may timeout
try:
    response = sfn_client.start_sync_execution(
        stateMachineArn=import_phenopackets_sfn_arn,
        name=f"test-timeout-{int(time.time())}",
        input=json.dumps({
            "project_id": "test-project-huge",
            "s3_path": "s3://bucket/phenopackets/huge-collection-200.zip",
            "output_s3_path": "s3://bucket/phenopackets/processed/huge.jsonl"
        }),
        timeout=300  # 5 minute timeout
    )
except sfn_client.exceptions.ExecutionTimedOut:
    # Expected for very large collections
    # Execution continues in background
    pass

# Alternatively, recommend using StartExecution (async) for large imports
```

---

## Test Data Requirements

### Valid Phenopacket Collections
- Small: 10 phenopackets, 5-10 features each
- Medium: 50 phenopackets, 10-20 features each
- Large: 100 phenopackets, 10-20 features each
- Huge: 200+ phenopackets (for timeout testing)

### Phenopacket Structure (GA4GH v2)
```json
{
  "id": "phenopacket-id",
  "subject": {
    "id": "subject-external-id",
    "sex": "MALE|FEMALE|OTHER_SEX|UNKNOWN_SEX"
  },
  "phenotypicFeatures": [
    {
      "type": {"id": "HP:0001250", "label": "Seizure"},
      "excluded": false
    }
  ],
  "metaData": {
    "created": "2024-01-01T00:00:00Z",
    "resources": [...]
  }
}
```

### Test Scenarios
- Duplicate subjects across phenopackets
- Negated features (excluded: true)
- Missing optional fields
- Invalid phenopackets (for error testing)

---

## Performance Benchmarks

| Collection Size | Phenopackets | Features | Expected Duration | Parallelism |
|-----------------|--------------|----------|-------------------|-------------|
| Small           | 10           | 50-100   | 30-60s            | 10          |
| Medium          | 50           | 500-1000 | 1-2 min           | 32          |
| Large           | 100          | 1000-2000| 2-4 min           | 32          |
| Huge            | 200+         | 2000+    | 4-8 min           | 32 (batched)|

---

## Error Scenarios

| Error Type | Stage | Expected Behavior |
|------------|-------|-------------------|
| Missing S3 file | ParseZipFile | Execution fails immediately |
| Invalid ZIP | ParseZipFile | Lambda error, execution fails |
| Invalid phenopacket JSON | ProcessPhenopacket | Individual item fails, others continue |
| CreateSubject failure | CreateSubject | Item fails, others continue |
| CreateEvidence failure | CreateEvidence | Individual evidence fails, others continue |
| Timeout (> 5 min) | Overall | API Gateway timeout, execution continues |

---

## Notes

- **Synchronous execution** required for API Gateway integration
- **5-minute timeout** is hard limit for API Gateway
- **Distributed map** enables parallel processing of phenopackets
- **Nested map** enables parallel evidence creation within each phenopacket
- **S3 ItemReader** efficiently streams phenopackets from JSONL
- **Subject ID replacement** ensures evidence links to PheBee subject IDs
- For very large collections (> 100 phenopackets), consider async execution or bulk import workflow instead
- Phenopacket format follows GA4GH Phenopacket v2 schema
- ImportPhenopacketsSFN is invoked via API Gateway POST /phenopackets/import

---

## Additional Test Scenarios from Existing Integration Tests

### Test: Phenopacket ZIP Import and Export Data Comparison
**Test Name**: `test_phenopackets_import_export_comparison`

**Objective**: Verify phenopackets can be imported, then exported, with subject IDs and phenotype term IDs matching

**Setup**:
- Prepare sample phenopackets ZIP file
- Upload to S3
- Create test project

**Test Steps**:
1. Import phenopackets via ImportPhenopacketsSFN
2. Wait for Step Function completion
3. Validate imported data in system
4. Export phenopackets via GetSubjectsPhenotypesFunction (output_type=phenopacket)
5. Compare imported vs exported phenopackets

**Expected Results**:
- Import Step Function: SUCCEEDED
- Subjects created for all phenopackets
- All phenotypicFeatures imported as evidence
- Export query returns all imported subjects
- Subject IDs match between import and export
- Phenotype term IDs match:
  - Imported terms: [HP:0001250, HP:0002664, ...]
  - Exported terms: same set
- Data round-trip successful

**Why this test is important**: The existing integration test `test_phenopackets.py::test_phenopackets` (lines 316-352) verifies round-trip import/export consistency. This ensures phenopacket data integrity and validates the import/export pipeline works correctly.

---

### Test: Phenopacket ZIP to S3 Export
**Test Name**: `test_phenopackets_zip_to_s3_export`

**Objective**: Verify phenopackets can be exported as ZIP file to S3

**Setup**:
- Import phenopackets
- Validate imported data

**Test Steps**:
1. Call GetSubjectsPhenotypesFunction with:
   ```json
   {
     "project_id": "test-project",
     "term_source": "hpo",
     "include_descendants": false,
     "include_phenotypes": true,
     "output_s3_path": "s3://bucket/export/phenopackets.zip",
     "output_type": "phenopacket_zip"
   }
   ```
2. Verify ZIP file created at S3 location
3. Download and extract ZIP
4. Compare with original import data

**Expected Results**:
- Lambda returns status 200
- ZIP file created at specified S3 path
- ZIP contains JSON files (one per subject)
- Extracted phenopackets match imported data
- Subject IDs consistent
- Phenotype terms consistent

**Why this test is important**: The existing integration test `test_phenopackets.py::test_phenopackets` (lines 339-345) verifies S3 export functionality. This enables bulk phenopacket export for data sharing and backup.

---

### Test: Term Labels Functionality in Phenopacket Export
**Test Name**: `test_phenopackets_term_labels`

**Objective**: Verify term labels are populated in exported phenopackets from ontology

**Setup**:
- Import phenopackets with HPO terms
- Ensure HPO ontology loaded

**Test Steps**:
1. Import phenopackets
2. Query subjects to get term labels:
   ```json
   {
     "project_id": "test-project"
   }
   ```
3. Verify term_links have labels

**Expected Results**:
- Response includes subjects array
- Each subject has term_links field
- term_links may be empty array (if no evidence)
- If term_links exist, each has:
  - term_iri (e.g., "http://purl.obolibrary.org/obo/HP_0001250")
  - term_label (e.g., "Seizure") populated from HPO ontology
- At least one term found with label
- Labels enriched from ontology hierarchy table

**Why this test is important**: The existing integration test `test_phenopackets.py::test_term_labels_functionality` (lines 356-392) verifies term label enrichment. This is critical for human-readable phenotype displays.

---

### Test: include_qualified Parameter with Phenopackets
**Test Name**: `test_phenopackets_include_qualified`

**Objective**: Verify include_qualified parameter correctly filters negated/family/hypothetical phenotypes

**Setup**:
- Import phenopackets with excluded features (negated phenotypes)

**Test Steps**:
1. Import phenopackets with both confirmed and excluded features
2. Query with default behavior (include_qualified not set):
   ```json
   {
     "project_id": "test-project",
     "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
     "term_source": "hpo",
     "include_phenotypes": true
   }
   ```
3. Query with include_qualified=true:
   ```json
   {
     "project_id": "test-project",
     "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
     "term_source": "hpo",
     "include_phenotypes": true,
     "include_qualified": true
   }
   ```

**Expected Results**:
- Default query (no include_qualified):
  - Returns subjects with confirmed phenotypes only
  - Excludes negated/family/hypothetical qualifiers
- include_qualified=true query:
  - Returns all subjects including those with qualified annotations
  - Subject count >= default query
- Qualifier filtering works correctly

**Why this test is important**: The existing integration test `test_phenopackets.py::test_include_qualified_parameter` (lines 396-442) verifies qualifier filtering with phenopackets. This ensures phenopacket excluded features are properly handled.

---
