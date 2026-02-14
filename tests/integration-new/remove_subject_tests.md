# Integration Test Plan: remove_subject.py

## Lambda: remove_subject

**Purpose**: Removes a subject from a specific project by deleting the project-specific subject linkage. If this is the last project mapping for the subject, performs a cascade deletion of all associated data including evidence, term links, and analytical table entries. The lambda maintains consistency across all dependent systems.

**Dependencies**:
- **DynamoDB** - stores and removes bidirectional project-subject mappings (source of truth for subject identity)
- **Neptune (Graph Database)** - deletes project_subject_iri nodes, and subject_iri + term links if last mapping
- **Iceberg/Athena** - deletes evidence records, subject_terms analytical table rows
- **PyIceberg** - Python library for deleting from Iceberg tables
- No direct EventBridge dependency

**Key Operations**:
- Accepts project_subject_iri parameter
- Parses IRI to extract project_id and project_subject_id
- Looks up internal subject_id from DynamoDB
- Deletes DynamoDB mappings (forward and reverse)
- Checks if this was the last project mapping for subject
- If NOT last: removes project-specific data only
- If last mapping: cascade deletes all evidence, term links, analytical data, and subject node
- Maintains atomicity using DynamoDB batch operations

---

## Integration Test Scenarios

### Test 1: Remove Subject from Project - NOT Last Mapping (Partial Removal)
**Test Name**: `test_remove_subject_not_last_mapping`

**Setup**:
- Create project-A and project-B
- Create subject linked to BOTH projects:
  - project-A: "patient-A-001"
  - project-B: "patient-B-001"
- Create evidence for subject via both project references

**Action**:
- Remove subject from project-A only:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/project-A/patient-A-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `message: "Subject unlinked from project project-A: {subject_iri}"`
- DynamoDB verification:
  - Forward mapping PROJECT#project-A / SUBJECT#patient-A-001 DELETED
  - Reverse mapping SUBJECT#{uuid} / PROJECT#project-A#SUBJECT#patient-A-001 DELETED
  - Mappings for project-B still exist
  - Query SUBJECT#{uuid} returns 1 remaining SK (project-B)
- Neptune verification:
  - project_subject_iri for project-A DELETED
  - subject_iri still exists (linked to project-B)
  - Term links still exist (subject still has evidence)
- Iceberg evidence verification:
  - Evidence records NOT deleted (subject still exists in project-B)
- Analytical tables verification:
  - by_project_term table: project-A rows DELETED
  - by_subject table: rows still exist (subject in project-B)
- Subject unlinked from one project but persists in other

**Cleanup**:
- Remove subject from project-B
- Delete both projects

---

### Test 2: Remove Subject - Last Mapping (Cascade Deletion)
**Test Name**: `test_remove_subject_last_mapping_cascade`

**Setup**:
- Create test project
- Create subject linked to project only
- Create 5 evidence records for subject
- Create term links in Neptune

**Action**:
- Remove subject from project:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project/patient-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response message: "Subject removed: {subject_iri}"
- DynamoDB verification:
  - Both forward and reverse mappings DELETED
  - Query SUBJECT#{uuid} returns empty (no remaining mappings)
- Iceberg evidence verification:
  - ALL 5 evidence records DELETED (cascade)
  - Query evidence by subject_id returns empty
- Neptune verification:
  - project_subject_iri DELETED
  - All term links DELETED
  - subject_iri DELETED
  - No orphaned nodes or triples
- Analytical tables verification:
  - by_subject table: all rows for subject DELETED
  - by_project_term table: all rows for subject in project DELETED
- Complete cascade deletion executed

**Cleanup**:
- Delete project (subject already removed)

---

### Test 3: Remove Subject - Subject Not Found
**Test Name**: `test_remove_subject_not_found`

**Setup**:
- Create test project
- Ensure subject with project_subject_id "nonexistent-patient" does NOT exist

**Action**:
- Invoke lambda with non-existent subject:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project/nonexistent-patient"
  }
  ```

**Assertions**:
- Response status code: 404
- Response body contains:
  - `message: "Subject not found"`
- DynamoDB verification:
  - No mappings found for this project + project_subject_id
- No data modifications in any system

**Cleanup**:
- Delete project

---

### Test 4: Missing Required Field - project_subject_iri
**Test Name**: `test_remove_subject_missing_iri`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with empty payload:
  ```json
  {}
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `message: "project_subject_iri is required"`
- No data deleted

**Cleanup**:
- None required

---

### Test 5: Invalid project_subject_iri Format
**Test Name**: `test_remove_subject_invalid_iri_format`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with malformed IRI:
  ```json
  {
    "project_subject_iri": "not-a-valid-iri"
  }
  ```

**Assertions**:
- Response status code: 400
- Response message indicates invalid IRI format
- No data deleted

**Cleanup**:
- Delete project

---

### Test 6: project_subject_iri with Missing Segments
**Test Name**: `test_remove_subject_incomplete_iri`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with incomplete IRI:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project"
  }
  ```

**Assertions**:
- Response status code: 400
- Response message indicates invalid IRI format (missing subject ID)
- No data deleted

**Cleanup**:
- Delete project

---

### Test 7: Remove Subject - DynamoDB Deletion Failure (Abort Operation)
**Test Name**: `test_remove_subject_dynamodb_failure_abort`

**Setup**:
- Create test project
- Create test subject
- Simulate DynamoDB deletion failure

**Action**:
- Remove subject

**Assertions**:
- Response status code: 500
- Response indicates failure to remove subject mapping
- DynamoDB verification:
  - Mappings may still exist (deletion failed)
- Neptune verification:
  - No data deleted (operation aborted before Neptune cleanup)
- Iceberg verification:
  - No evidence deleted (operation aborted)
- System consistency preserved (no partial deletions)

**Cleanup**:
- Restore DynamoDB permissions
- Delete subject manually
- Delete project

---

### Test 8: Remove Subject - Query Remaining Mappings Failure
**Test Name**: `test_remove_subject_query_failure_safe_default`

**Setup**:
- Create test project
- Create subject
- Simulate DynamoDB query failure for remaining mappings check

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200 or 500
- If 200:
  - Deletion proceeds
  - by_subject table NOT deleted (safe default assumes NOT last mapping)
  - by_project_term table deleted for this project
- CloudWatch logs show query failure
- Conservative approach: avoid accidental full deletion

**Cleanup**:
- Restore DynamoDB permissions
- Complete cleanup manually

---

### Test 9: Remove Subject - Evidence Cascade Deletion Failure
**Test Name**: `test_remove_subject_evidence_deletion_failure`

**Setup**:
- Create test project
- Create subject (last mapping)
- Create evidence
- Simulate Iceberg evidence deletion failure

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200
- Response indicates success (deletion proceeds)
- DynamoDB verification:
  - Mappings deleted
- Iceberg verification:
  - Evidence deletion attempted but failed
  - Orphaned evidence may exist
- Neptune verification:
  - Subject and term links deleted
- CloudWatch logs show evidence deletion error
- Partial cleanup may leave orphaned evidence

**Cleanup**:
- Restore Iceberg permissions
- Manually delete orphaned evidence
- Delete project

---

### Test 10: Remove Subject - Analytical Table Deletion Failure
**Test Name**: `test_remove_subject_analytical_table_failure`

**Setup**:
- Create test project
- Create subject
- Simulate analytical table deletion failure

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - Mappings deleted
- Analytical tables verification:
  - Deletion attempted but failed
  - Orphaned rows may exist
- CloudWatch logs show analytical table error
- Main subject removal proceeds

**Cleanup**:
- Restore permissions
- Manually clean up analytical tables
- Delete project

---

### Test 11: Remove Subject - Neptune Cleanup Failure
**Test Name**: `test_remove_subject_neptune_failure`

**Setup**:
- Create test project
- Create subject
- Simulate Neptune deletion failure

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - Mappings deleted
- Neptune verification:
  - Deletion attempted but failed
  - Orphaned nodes/triples may exist
- CloudWatch logs show Neptune error
- Main subject removal proceeds

**Cleanup**:
- Restore Neptune connectivity
- Manually clean up Neptune
- Delete project

---

### Test 12: Remove Subject with Large Evidence Set (Performance)
**Test Name**: `test_remove_subject_large_evidence_set`

**Setup**:
- Create test project
- Create subject (last mapping)
- Create 500 evidence records for subject

**Action**:
- Remove subject
- Measure response time

**Assertions**:
- Response status code: 200
- Response indicates success
- Iceberg verification:
  - All 500 evidence records deleted
  - No orphaned evidence
- Performance metrics:
  - Operation completes within lambda timeout (15 minutes)
  - Bulk deletion efficient
- All data cascaded correctly

**Cleanup**:
- Delete project

---

### Test 13: Remove Subject - Concurrent Removal from Multiple Projects
**Test Name**: `test_remove_subject_concurrent_multiple_projects`

**Setup**:
- Create project-A, project-B, project-C
- Create subject linked to all 3 projects
- Create evidence

**Action**:
- Invoke lambda 3 times concurrently to remove subject from all 3 projects simultaneously

**Assertions**:
- All 3 invocations complete
- Response codes: all 200
- DynamoDB verification:
  - All 6 mappings deleted (3 forward, 3 reverse)
- Last deletion triggers cascade:
  - All evidence deleted
  - All term links deleted
  - subject_iri deleted
- Race condition handled safely
- Exactly one invocation triggers cascade

**Cleanup**:
- Delete all 3 projects

---

### Test 14: Remove Subject - Concurrent Removal of Same Mapping
**Test Name**: `test_remove_subject_concurrent_same_mapping`

**Setup**:
- Create test project
- Create subject

**Action**:
- Invoke lambda 5 times concurrently to remove SAME project_subject_iri

**Assertions**:
- All 5 invocations complete
- Exactly ONE returns 200 (successful deletion)
- Remaining 4 return 404 (subject not found after first deletion)
- DynamoDB verification:
  - Mappings deleted exactly once
  - No duplicate deletions
- System consistency maintained

**Cleanup**:
- Delete project

---

### Test 14a: Remove Subject - Idempotency (Second Removal Returns 404)
**Test Name**: `test_remove_subject_idempotency`

**Setup**:
- Create test project
- Create test subject

**Action**:
- Remove subject from project (first time)
- Verify successful removal (status 200)
- Remove same subject again (second time) with identical payload

**Assertions**:
- First removal: status 200, message "Subject removed: {subject_iri}"
- Second removal: status 404, message "Subject not found" (or similar)
- DynamoDB verification:
  - After first removal: mappings deleted
  - After second removal: still no mappings (no errors)
- Neptune verification:
  - After first removal: subject nodes and linkages deleted
  - After second removal: still deleted (no errors)
- Idempotent operation - safe to call multiple times
- Second removal does not create errors or inconsistencies

**Cleanup**:
- Delete project

**Why this test is important**: The existing integration test `test_remove_subject.py` verifies this critical idempotency pattern at lines 64-71, testing that a second removal attempt returns 404. This ensures the API is safe to retry and prevents errors in distributed systems where duplicate removal requests may occur due to network issues or retry logic.

---

### Test 15: Remove Subject - Subject Has No Evidence
**Test Name**: `test_remove_subject_no_evidence`

**Setup**:
- Create test project
- Create subject (do NOT create any evidence)

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - Mappings deleted
- Iceberg verification:
  - No evidence to delete (cascade skipped)
- Neptune verification:
  - project_subject_iri deleted
  - subject_iri deleted if last mapping
  - No term links (none existed)
- Analytical tables verification:
  - No rows to delete (subject had no terms)

**Cleanup**:
- Delete project

---

### Test 16: Remove Subject - Verify termlink_data Extraction
**Test Name**: `test_remove_subject_termlink_data_extraction`

**Setup**:
- Create test project
- Create subject (last mapping)
- Create evidence (creates term links with specific termlink_ids)

**Action**:
- Remove subject

**Assertions**:
- Response status code: 200
- Iceberg verification:
  - Evidence deletion returns termlink_data with termlink_ids
- Neptune verification:
  - Term links with matching termlink_ids deleted
  - termlink_iri constructed correctly:
    - Format: `{subject_iri}/term-link/{termlink_id}`
- All term links cleaned up

**Cleanup**:
- Delete project

---

### Test 17: Remove Subject - Verify Atomic DynamoDB Batch Delete
**Test Name**: `test_remove_subject_atomic_batch_delete`

**Setup**:
- Create test project
- Create subject

**Action**:
- Remove subject
- Monitor DynamoDB operations

**Assertions**:
- Response status code: 200
- DynamoDB verification:
  - Both mappings deleted atomically via batch_writer
  - No partial state where only one mapping deleted
  - Batch operation ensures atomicity

**Cleanup**:
- Delete project

---

### Test 18: Remove Subject - Subject Linked to Different Projects with Same project_subject_id
**Test Name**: `test_remove_subject_same_project_subject_id_different_projects`

**Setup**:
- Create project-A and project-B
- Create subject-X linked to project-A as "patient-001"
- Create subject-Y linked to project-B as "patient-001" (same local ID, different subject)

**Action**:
- Remove subject-X from project-A

**Assertions**:
- Response status code: 200
- DynamoDB verification:
  - subject-X mappings deleted
  - subject-Y mappings for project-B untouched
- Different subjects isolated correctly
- No data mixing

**Cleanup**:
- Remove subject-Y from project-B
- Delete both projects

---

### Test 19: Remove Subject - Whitespace in project_subject_iri
**Test Name**: `test_remove_subject_iri_with_whitespace`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with IRI containing whitespace:
  ```json
  {
    "project_subject_iri": " http://ods.nationwidechildrens.org/phebee/projects/test-project/patient-001 "
  }
  ```

**Assertions**:
- Response status code: 400 or 404
- IRI parsing fails or subject not found
- Whitespace handling documented

**Cleanup**:
- Delete project

---

### Test 20: Remove Subject - Verify by_subject vs by_project_term Table Logic
**Test Name**: `test_remove_subject_analytical_tables_logic`

**Setup**:
- Create project-A and project-B
- Create subject linked to both
- Create evidence

**Action**:
- Remove subject from project-A (NOT last mapping)

**Assertions**:
- Response status code: 200
- Analytical tables verification:
  - by_project_term table: rows for project-A/subject deleted
  - by_subject table: rows still exist (subject in project-B)
  - delete_subject_terms called with include_by_subject=False
- Remove subject from project-B (last mapping):
- Analytical tables verification:
  - by_project_term table: rows for project-B/subject deleted
  - by_subject table: rows deleted (include_by_subject=True)
- Correct table cleanup logic for partial vs full removal

**Cleanup**:
- Delete both projects

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Remove Subject with Null project_subject_iri**: Test null parameter handling

2. **Remove Subject with Empty String IRI**: Test empty string parameter

3. **Remove Subject - Very Long IRI**: Test with 2KB+ IRI string

4. **Remove Subject - Special Characters in project_subject_id**: Test unicode, spaces, special chars

5. **Remove Subject - Case Sensitivity**: Test if project_id or project_subject_id case-sensitive

6. **Remove Subject - Lambda Timeout**: Test with very large evidence set (10K+ records)

7. **Remove Subject - Partial Evidence Deletion**: Test if only some evidence deleted

8. **Remove Subject - Term Links with 50+ Qualifiers**: Test deletion of complex term links

9. **Remove Subject - S3 Throttling During Iceberg Delete**: Test retry logic

10. **Remove Subject - Athena Concurrent Query Limit**: Test when many queries active

11. **Remove Subject - Neptune Transaction Failure**: Test SPARQL DELETE failure

12. **Remove Subject - Authorization**: Test unauthorized users cannot remove subjects

13. **Remove Subject - Audit Trail**: Verify deletion logged for compliance

14. **Remove Subject - Cross-Region Replication**: If multi-region, test deletion propagation

15. **Remove Subject - Verify No Orphaned Data**: Comprehensive check across all systems

16. **Remove Subject After Project Deletion**: Test removing subject from non-existent project

17. **Remove Subject - Evidence Created After Mapping Deletion**: Test race condition

18. **Remove Subject - Multiple Subjects with Same project_subject_id**: Test isolation

19. **Remove Subject - Analytical Table Partition Cleanup**: Test if empty partitions removed

20. **Remove Subject - Performance with 100+ Term Links**: Test Neptune cleanup performance
