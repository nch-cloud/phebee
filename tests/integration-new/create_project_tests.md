# Integration Test Plan: create_project.py

## Lambda: create_project

**Purpose**: Creates a new project in the PheBee system. Projects serve as top-level organizational units that group subjects and their associated phenotypic data.

**Dependencies**:
- Neptune (Graph Database) - stores project nodes and metadata
- No dependencies on DynamoDB, Iceberg, S3, or EventBridge

**Key Operations**:
- Creates project node in Neptune with IRI: `http://ods.nationwidechildrens.org/phebee/projects/{project_id}`
- Uses SPARQL INSERT to add project triples
- Idempotent operation - returns success if project already exists

---

## Integration Test Scenarios

### Test 1: Create New Project (Happy Path)
**Test Name**: `test_create_project_success`

**Setup**:
- Ensure project with test `project_id` does not exist in Neptune
- Clean state with no pre-existing project data

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "project_id": "test-project-001",
    "project_label": "Test Project 001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `project_created: true`
  - `project_id: "test-project-001"`
  - `project_iri: "http://ods.nationwidechildrens.org/phebee/projects/test-project-001"`
  - `message: "Project created."`
- Neptune verification:
  - Query Neptune for project IRI
  - Verify project node exists with correct label
  - Verify project has RDF type triple
  - Verify rdfs:label matches provided project_label

**Cleanup**:
- Delete project from Neptune using `remove_project` lambda
- Verify Neptune graph is clean

---

### Test 2: Create Duplicate Project (Idempotency)
**Test Name**: `test_create_project_already_exists`

**Setup**:
- Create project with specific `project_id` and label
- Verify project exists in Neptune

**Action**:
- Invoke lambda again with SAME `project_id` but DIFFERENT label:
  ```json
  {
    "project_id": "test-project-001",
    "project_label": "Different Label"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `project_created: false`
  - `message: "Project already exists."`
- Neptune verification:
  - Project node still exists
  - Label remains UNCHANGED (original label preserved)
  - No duplicate project nodes created
  - Single project IRI exists for the project_id

**Cleanup**:
- Delete project from Neptune
- Verify clean state

---

### Test 3: Missing Required Field - project_id
**Test Name**: `test_create_project_missing_project_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with incomplete payload:
  ```json
  {
    "project_label": "Test Project"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - Error message: "Missing required fields: project_id and project_label"
- Neptune verification:
  - No new project node created
  - Neptune state unchanged

**Cleanup**:
- None required

---

### Test 4: Missing Required Field - project_label
**Test Name**: `test_create_project_missing_project_label`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with incomplete payload:
  ```json
  {
    "project_id": "test-project-002"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - Error message: "Missing required fields: project_id and project_label"
- Neptune verification:
  - No new project node created
  - Neptune state unchanged

**Cleanup**:
- None required

---

### Test 5: Missing Both Required Fields
**Test Name**: `test_create_project_missing_both_fields`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with empty payload: `{}`

**Assertions**:
- Response status code: 400
- Response body contains:
  - Error message: "Missing required fields: project_id and project_label"
- Neptune verification:
  - No new project node created

**Cleanup**:
- None required

---

### Test 6: Special Characters in project_id
**Test Name**: `test_create_project_special_characters`

**Setup**:
- Clean Neptune state

**Action**:
- Invoke lambda with special characters in project_id:
  ```json
  {
    "project_id": "test-project_with.special-chars_123",
    "project_label": "Special Chars Project"
  }
  ```

**Assertions**:
- Response status code: 200
- Response indicates successful creation
- Neptune verification:
  - Project IRI correctly encodes special characters
  - Project node accessible via SPARQL
  - Label stored correctly

**Cleanup**:
- Delete project from Neptune

---

### Test 7: Unicode Characters in project_label
**Test Name**: `test_create_project_unicode_label`

**Setup**:
- Clean Neptune state

**Action**:
- Invoke lambda with Unicode characters:
  ```json
  {
    "project_id": "test-project-unicode",
    "project_label": "测试项目 Test Проект"
  }
  ```

**Assertions**:
- Response status code: 200
- Response indicates successful creation
- Neptune verification:
  - Project created successfully
  - Unicode label stored and retrievable
  - No encoding errors

**Cleanup**:
- Delete project from Neptune

---

### Test 8: Very Long project_id
**Test Name**: `test_create_project_long_id`

**Setup**:
- Clean Neptune state

**Action**:
- Invoke lambda with very long project_id (e.g., 256+ characters):
  ```json
  {
    "project_id": "project-" + ("a" * 250),
    "project_label": "Long ID Project"
  }
  ```

**Assertions**:
- Response status code: 200 or appropriate error code if length validation exists
- If successful:
  - Project created in Neptune
  - IRI properly formatted
- If validation exists:
  - Appropriate error message returned

**Cleanup**:
- Delete project if created

---

### Test 9: Empty String Values
**Test Name**: `test_create_project_empty_strings`

**Setup**:
- Clean Neptune state

**Action**:
- Invoke lambda with empty strings:
  ```json
  {
    "project_id": "",
    "project_label": ""
  }
  ```

**Assertions**:
- Response status code: 400
- Response indicates missing required fields (empty strings treated as missing)
- Neptune verification:
  - No project node created

**Cleanup**:
- None required

---

### Test 10: Concurrent Project Creation (Race Condition)
**Test Name**: `test_create_project_concurrent`

**Setup**:
- Clean Neptune state
- Prepare multiple lambda invocations with same project_id

**Action**:
- Invoke lambda 5 times concurrently with identical payload:
  ```json
  {
    "project_id": "race-test-project",
    "project_label": "Race Test"
  }
  ```

**Assertions**:
- All 5 invocations return status code 200
- At least one invocation returns `project_created: true`
- Remaining invocations return `project_created: false` with "already exists" message
- Neptune verification:
  - Exactly ONE project node exists (no duplicates)
  - Project IRI unique
  - No data corruption

**Cleanup**:
- Delete project from Neptune
- Verify single deletion succeeds

---

### Test 11: Neptune Connection Failure
**Test Name**: `test_create_project_neptune_unavailable`

**Setup**:
- Temporarily block Neptune connectivity (if test environment supports this)
- OR use Neptune cluster that is stopped/unavailable

**Action**:
- Invoke lambda with valid payload:
  ```json
  {
    "project_id": "test-unavailable",
    "project_label": "Unavailable Test"
  }
  ```

**Assertions**:
- Response status code: 500
- Response contains appropriate error message about Neptune connectivity
- Graceful error handling (no uncaught exceptions)
- Error logged to CloudWatch

**Cleanup**:
- Restore Neptune connectivity
- Verify no partial data written

---

### Test 12: Create Multiple Distinct Projects
**Test Name**: `test_create_multiple_projects`

**Setup**:
- Clean Neptune state

**Action**:
- Create 10 different projects sequentially with unique IDs

**Assertions**:
- All 10 creations return status 200 with `project_created: true`
- Neptune verification:
  - All 10 project nodes exist
  - Each has unique IRI
  - All labels stored correctly
  - No data mixing between projects
  - SPARQL query returns all 10 projects

**Cleanup**:
- Delete all 10 projects
- Verify Neptune clean state

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Null Values**: Test behavior when `project_id` or `project_label` is explicitly `null` rather than missing

2. **Whitespace Handling**: Test project IDs with leading/trailing whitespace

3. **SQL Injection / SPARQL Injection**: Test project_id with SPARQL query fragments to ensure proper escaping

4. **Maximum Payload Size**: Test with very large project_label (MB+ size) to verify payload limits

5. **Malformed JSON**: Test lambda's handling of invalid JSON in request body

6. **Case Sensitivity**: Create projects with IDs that differ only in case (e.g., "Project1" vs "project1")

7. **Performance**: Measure response time under load (e.g., 100 concurrent creates)

8. **State Consistency After Partial Failure**: If Neptune SPARQL fails mid-operation, verify no orphaned data

9. **Audit Trail**: Verify that project creation events are logged with appropriate detail for compliance

10. **Authorization**: If authorization is implemented, test that unauthorized users cannot create projects
