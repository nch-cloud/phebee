# Integration Test Plan: create_subject.py

## Lambda: create_subject

**Purpose**: Creates a new subject (patient/individual) in the PheBee system and links it to a project. Subjects can be created as new entities or linked to existing subjects across multiple projects. The lambda implements sophisticated race condition handling using conditional DynamoDB writes.

**Dependencies**:
- **Neptune (Graph Database)** - stores subject nodes, project linkages, and project_subject_iri nodes
- **DynamoDB** - stores bidirectional mappings between project subjects and internal subject IDs (source of truth for subject identity)
- **EventBridge** - fires `SUBJECT_CREATED` and `SUBJECT_LINKED` events for downstream processing
- **Iceberg/Athena** - analytical tables updated via events (indirect dependency)

**Key Operations**:
- Creates subject node in Neptune with UUID-based IRI: `http://ods.nationwidechildrens.org/phebee/subjects/{uuid}`
- Creates project-specific subject node: `http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}`
- Links subject to project via SPARQL triples
- Stores bidirectional mappings in DynamoDB using conditional writes for atomicity
- Fires EventBridge events for subject creation/linking
- Supports three modes: new subject creation, linking via known_subject_iri, linking via known_project_id + known_project_subject_id

---

## Integration Test Scenarios

### Test 1: Create New Subject (Happy Path)
**Test Name**: `test_create_subject_new`

**Setup**:
- Create test project in Neptune
- Verify project exists
- Ensure subject with test identifiers does not exist

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "project_id": "test-project-001",
    "project_subject_id": "patient-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_created: true`
  - `subject.iri` (UUID-based subject IRI)
  - `subject.subject_id` (UUID)
  - `subject.projects.test-project-001: "patient-001"`
- Neptune verification:
  - Subject IRI exists with UUID identifier
  - Project subject IRI exists: `.../projects/test-project-001/patient-001`
  - Triples link project_subject_iri to subject_iri
  - Triples link subject_iri to project_iri
- DynamoDB verification:
  - Forward mapping exists: `PK=PROJECT#test-project-001, SK=SUBJECT#patient-001` → `subject_id: {uuid}`
  - Reverse mapping exists: `PK=SUBJECT#{uuid}, SK=PROJECT#test-project-001#SUBJECT#patient-001`
- EventBridge verification:
  - `SUBJECT_CREATED` event fired with correct payload

**Cleanup**:
- Delete subject from all systems (Neptune, DynamoDB)
- Delete project
- Verify clean state

---

### Test 2: Create Subject - Project Does Not Exist
**Test Name**: `test_create_subject_project_not_found`

**Setup**:
- Ensure project "nonexistent-project" does NOT exist in Neptune

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "project_id": "nonexistent-project",
    "project_subject_id": "patient-001"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `subject_created: false`
  - `error: "No project found with ID: nonexistent-project"`
- Neptune verification:
  - No subject node created
  - No project_subject node created
- DynamoDB verification:
  - No mappings created

**Cleanup**:
- None required

---

### Test 3: Link Existing Subject via known_subject_iri
**Test Name**: `test_create_subject_link_via_known_iri`

**Setup**:
- Create test project-A
- Create subject already linked to project-A (subject_iri obtained)
- Create test project-B (separate project)
- Verify subject exists in DynamoDB with project-A mapping

**Action**:
- Link same subject to project-B using known_subject_iri:
  ```json
  {
    "project_id": "test-project-B",
    "project_subject_id": "patient-B-001",
    "known_subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/{uuid}"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_created: false` (subject already existed)
  - Same `subject.iri` as original subject
  - `subject.projects.test-project-B: "patient-B-001"`
- Neptune verification:
  - Original subject_iri still exists (no new subject created)
  - New project_subject_iri created for project-B
  - Triples link new project_subject_iri to same subject_iri
  - Subject linked to both project-A and project-B
- DynamoDB verification:
  - Original project-A mapping still exists
  - New forward mapping: `PK=PROJECT#test-project-B, SK=SUBJECT#patient-B-001` → same `subject_id`
  - New reverse mapping: `PK=SUBJECT#{uuid}, SK=PROJECT#test-project-B#SUBJECT#patient-B-001`
  - Single SUBJECT# partition has TWO SK entries (one per project)
- EventBridge verification:
  - `SUBJECT_LINKED` event fired (not SUBJECT_CREATED)

**Cleanup**:
- Remove subject from both projects
- Delete both projects
- Verify DynamoDB has no remaining mappings

---

### Test 4: Link Existing Subject via known_project_id + known_project_subject_id
**Test Name**: `test_create_subject_link_via_known_project`

**Setup**:
- Create test project-A
- Create subject linked to project-A with project_subject_id "patient-A-001"
- Create test project-B

**Action**:
- Link same subject to project-B using project-A identifiers:
  ```json
  {
    "project_id": "test-project-B",
    "project_subject_id": "patient-B-999",
    "known_project_id": "test-project-A",
    "known_project_subject_id": "patient-A-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_created: false`
  - Same `subject.iri` as project-A subject
  - `subject.projects.test-project-B: "patient-B-999"`
- DynamoDB verification:
  - DynamoDB query for project-A/patient-A-001 → subject_id
  - Same subject_id now mapped to project-B/patient-B-999
  - Three total mappings: 1 forward + 1 reverse for project-A, 1 forward + 1 reverse for project-B
- Neptune verification:
  - Same subject_iri linked to both projects
  - Two distinct project_subject_iri nodes exist
- EventBridge verification:
  - `SUBJECT_LINKED` event fired

**Cleanup**:
- Remove subject from both projects
- Delete both projects

---

### Test 5: Invalid known_project_id without known_project_subject_id
**Test Name**: `test_create_subject_invalid_known_project_missing_subject`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with incomplete known project reference:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001",
    "known_project_id": "some-project"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "If 'known_project_id' is provided, 'known_project_subject_id' is required."`
- No data created in any system

**Cleanup**:
- None required

---

### Test 6: Invalid - Both known_project_id and known_subject_iri Provided
**Test Name**: `test_create_subject_invalid_both_known_references`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with conflicting parameters:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001",
    "known_project_id": "project-A",
    "known_project_subject_id": "patient-A",
    "known_subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/uuid"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "'known_project_id' and 'known_subject_iri' cannot both be provided."`
- No data created

**Cleanup**:
- None required

---

### Test 7: Duplicate Subject Creation (Same project_id + project_subject_id)
**Test Name**: `test_create_subject_duplicate_idempotent`

**Setup**:
- Create test project
- Create subject with specific project_id and project_subject_id

**Action**:
- Invoke lambda again with SAME identifiers:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_created: false`
  - Same `subject.iri` as original
  - Message indicates already exists
- DynamoDB verification:
  - Only one mapping exists (no duplicates)
- Neptune verification:
  - Only one subject_iri exists
  - Only one project_subject_iri exists
  - No duplicate nodes or triples

**Cleanup**:
- Delete subject and project

---

### Test 8: Race Condition - Concurrent Subject Creation
**Test Name**: `test_create_subject_race_condition`

**Setup**:
- Create test project
- Prepare 5 concurrent lambda invocations with IDENTICAL payload

**Action**:
- Invoke lambda 5 times concurrently:
  ```json
  {
    "project_id": "race-test-project",
    "project_subject_id": "race-patient"
  }
  ```

**Assertions**:
- All 5 invocations return status 200
- Exactly ONE invocation returns `subject_created: true`
- Remaining 4 return `subject_created: false`
- All 5 return SAME subject_iri (winner's IRI)
- DynamoDB verification:
  - Exactly ONE forward mapping exists
  - Exactly ONE reverse mapping exists
  - No duplicate mappings (conditional write prevented races)
- Neptune verification:
  - Exactly ONE subject_iri node exists
  - Exactly ONE project_subject_iri node exists
  - Loser invocations cleaned up their Neptune data
  - No orphaned triples

**Cleanup**:
- Delete subject and project
- Verify single deletion succeeds

---

### Test 9: Missing Required Field - project_id
**Test Name**: `test_create_subject_missing_project_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with missing project_id:
  ```json
  {
    "project_subject_id": "patient-001"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "project_id and project_subject_id are required"`
- No data created

**Cleanup**:
- None required

---

### Test 10: Missing Required Field - project_subject_id
**Test Name**: `test_create_subject_missing_project_subject_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with missing project_subject_id:
  ```json
  {
    "project_id": "test-project"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "project_id and project_subject_id are required"`
- No data created

**Cleanup**:
- None required

---

### Test 11: known_subject_iri - Subject Does Not Exist
**Test Name**: `test_create_subject_known_iri_not_found`

**Setup**:
- Create test project
- Generate random UUID for non-existent subject

**Action**:
- Invoke lambda with non-existent subject IRI:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001",
    "known_subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/nonexistent-uuid"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "No subject found with IRI: ..."`
- DynamoDB verification:
  - Query for SUBJECT#nonexistent-uuid returns empty
- No new data created

**Cleanup**:
- Delete project

---

### Test 12: known_project_id - Subject Mapping Does Not Exist
**Test Name**: `test_create_subject_known_project_mapping_not_found`

**Setup**:
- Create test project-A
- Create test project-B
- Verify no subject exists with project-A/unknown-patient

**Action**:
- Invoke lambda with non-existent known project mapping:
  ```json
  {
    "project_id": "test-project-B",
    "project_subject_id": "patient-B",
    "known_project_id": "test-project-A",
    "known_project_subject_id": "unknown-patient"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "No subject found with Project ID: test-project-A and Project Subject ID: unknown-patient"`
- No new mappings created

**Cleanup**:
- Delete both projects

---

### Test 13: Invalid known_subject_iri Format
**Test Name**: `test_create_subject_invalid_iri_format`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with malformed IRI:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001",
    "known_subject_iri": "not-a-valid-iri"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains error about invalid IRI format
- No data created

**Cleanup**:
- Delete project

---

### Test 14: Link Subject to Same Project with Different project_subject_id
**Test Name**: `test_create_subject_link_different_project_subject_id_same_subject`

**Setup**:
- Create test project
- Create subject linked to project as "patient-001"

**Action**:
- Try to link SAME subject to SAME project with different project_subject_id:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-002",
    "known_subject_iri": "{existing_subject_iri}"
  }
  ```

**Assertions**:
- Response status code: 200
- Response indicates successful link
- DynamoDB verification:
  - Two forward mappings exist for same project:
    - `PK=PROJECT#test-project, SK=SUBJECT#patient-001` → subject_id
    - `PK=PROJECT#test-project, SK=SUBJECT#patient-002` → same subject_id
  - Two reverse mappings for same subject
- Neptune verification:
  - One subject_iri node
  - Two project_subject_iri nodes (one per project_subject_id)
  - Both link to same subject_iri

**Cleanup**:
- Remove subject (should clean up both linkages)
- Delete project

---

### Test 15: Conflict - Different Subject Already Linked to project_subject_id
**Test Name**: `test_create_subject_conflict_project_subject_id_taken`

**Setup**:
- Create test project
- Create subject-A linked to project as "patient-001"
- Create subject-B independently

**Action**:
- Try to link subject-B to project using SAME project_subject_id "patient-001":
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001",
    "known_subject_iri": "{subject_b_iri}"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `error: "Project subject ID 'patient-001' already linked to a different subject"`
- DynamoDB verification:
  - Original mapping unchanged
  - No new mapping created
- Neptune verification:
  - No new linkage created
  - subject-B remains unlinked to project

**Cleanup**:
- Delete subject-A and subject-B
- Delete project

---

### Test 16: DynamoDB Failure After Neptune Write (Cleanup Test)
**Test Name**: `test_create_subject_dynamodb_failure_cleanup`

**Setup**:
- Create test project
- Simulate DynamoDB failure (e.g., via IAM permission removal or table throttling)

**Action**:
- Invoke lambda to create new subject:
  ```json
  {
    "project_id": "test-project",
    "project_subject_id": "patient-001"
  }
  ```

**Assertions**:
- Response status code: 500
- Response indicates error
- Neptune verification:
  - No orphaned subject_iri nodes (cleanup executed)
  - No orphaned project_subject_iri nodes
  - No partial triples
- DynamoDB verification:
  - No mappings created (operation failed)
- System in consistent state (no partial writes)

**Cleanup**:
- Restore DynamoDB permissions
- Verify clean state

---

### Test 17: Neptune Link Failure After DynamoDB Write (Cleanup Test)
**Test Name**: `test_create_subject_neptune_link_failure_cleanup`

**Setup**:
- Create test project
- Simulate Neptune failure during link_subject_to_project operation

**Action**:
- Invoke lambda to create new subject

**Assertions**:
- Response status code: 500
- Response indicates error
- DynamoDB verification:
  - No mappings exist (cleanup executed)
  - Forward and reverse mappings both deleted
- Neptune verification:
  - No partial subject nodes
  - No orphaned triples
- System in consistent state (rollback successful)

**Cleanup**:
- Restore Neptune connectivity
- Verify clean state

---

### Test 18: EventBridge Event Verification - SUBJECT_CREATED
**Test Name**: `test_create_subject_event_subject_created`

**Setup**:
- Create test project
- Configure EventBridge rule to capture events in test SQS queue

**Action**:
- Create new subject

**Assertions**:
- Subject created successfully
- EventBridge verification:
  - `SUBJECT_CREATED` event fired
  - Event payload contains:
    - `iri`: subject IRI
    - `subject_id`: UUID
    - `projects`: {project_id: project_subject_id}
  - Event received in SQS queue
  - Event timestamp matches creation time

**Cleanup**:
- Delete subject
- Purge SQS queue

---

### Test 19: EventBridge Event Verification - SUBJECT_LINKED
**Test Name**: `test_create_subject_event_subject_linked`

**Setup**:
- Create two projects
- Create subject in project-A
- Configure EventBridge rule to capture events

**Action**:
- Link existing subject to project-B

**Assertions**:
- Subject linked successfully
- EventBridge verification:
  - `SUBJECT_LINKED` event fired (NOT SUBJECT_CREATED)
  - Event payload contains:
    - `subject_iri`: existing subject IRI
    - `projects`: {project_id_B: project_subject_id_B}
  - Event received in SQS queue

**Cleanup**:
- Delete subject from both projects
- Purge SQS queue

---

### Test 20: EventBridge Failure Does Not Block Subject Creation
**Test Name**: `test_create_subject_eventbridge_failure_non_blocking`

**Setup**:
- Create test project
- Simulate EventBridge failure (remove IAM permissions)

**Action**:
- Create new subject

**Assertions**:
- Response status code: 200
- Response indicates `subject_created: true`
- Subject creation succeeds despite EventBridge failure
- CloudWatch logs show EventBridge error (non-critical)
- DynamoDB verification:
  - Mappings created successfully
- Neptune verification:
  - Subject nodes and linkages created successfully
- EventBridge errors logged but do not propagate to user

**Cleanup**:
- Restore EventBridge permissions
- Delete subject
- Delete project

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Special Characters in project_subject_id**: Test with spaces, unicode, special chars

2. **Very Long project_subject_id**: Test with 1KB+ project_subject_id

3. **Reverse Mapping Failure During New Subject Creation**: Test cleanup when second DynamoDB put_item fails

4. **Concurrent Links to Different Projects**: Race condition where same subject linked to 2+ projects simultaneously

5. **DynamoDB Conditional Write Failure on Second Mapping**: Test reverse mapping failure cleanup

6. **Invalid Subject IRI in DynamoDB but Not in Neptune**: Orphaned DynamoDB data scenario

7. **Performance Under Load**: 100+ concurrent subject creations to same project

8. **Query DynamoDB for Subject with 10+ Project Mappings**: Verify pagination and complete retrieval

9. **Null vs Empty String for Optional Fields**: Test behavior with explicit null values

10. **Lambda Timeout During Neptune Write**: Test partial state recovery

11. **UUID Collision (Theoretical)**: Test if duplicate UUID handling exists

12. **Cross-Region Consistency**: If multi-region, test replication lag handling

13. **Batch Subject Creation**: Test creating 100+ subjects in rapid succession

14. **Subject Linking Chain**: Subject in Project-A → Project-B → Project-C verification

15. **Authorization**: Test that unauthorized users cannot create/link subjects
