# Integration Test Plan: remove_project.py

## Lambda: remove_project

**Purpose**: Completely removes a project and all associated data from the PheBee system. This is the most comprehensive deletion operation, cascading through all subjects linked to the project. For each subject, it determines if the subject is linked to other projects and performs appropriate cleanup (full or partial). The lambda ensures consistency across all dependent systems.

**Dependencies**:
- **DynamoDB** - queries all subjects in project, removes all project-subject mappings
- **Neptune (Graph Database)** - clears project named graph, deletes project_subject_iri nodes, subject_iri nodes, and term links
- **Iceberg/Athena** - deletes evidence records, analytical table rows for project
- **PyIceberg** - Python library for deleting from Iceberg tables
- **DynamoDB GSI1** - uses Global Secondary Index to efficiently query all subjects by project_id
- No direct EventBridge dependency

**Key Operations**:
- Accepts project_id parameter
- Queries DynamoDB GSI1 to find all subjects in project
- For each subject: performs remove_subject logic (unlink or cascade)
- Clears Neptune named graph for project
- Deletes all project data from by_project_term analytical table
- Maintains consistency across all systems

---

## Integration Test Scenarios

### Test 1: Remove Project with Single Subject (Subject Cascade)
**Test Name**: `test_remove_project_single_subject_cascade`

**Setup**:
- Create test project
- Create subject linked to project only
- Create evidence for subject

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "project_id": "test-project-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `message: "Project test-project-001 successfully removed."`
- DynamoDB verification:
  - Query GSI1 for PROJECT#test-project-001 returns empty
  - All subject mappings for project deleted
- Iceberg evidence verification:
  - All evidence for subject deleted (cascade)
- Neptune verification:
  - Project named graph cleared
  - project_subject_iri deleted
  - subject_iri deleted (was last mapping)
  - All term links deleted
- Analytical tables verification:
  - by_project_term table: all rows for project deleted
  - by_subject table: all rows for subject deleted

**Cleanup**:
- Project and all data already removed

---

### Test 2: Remove Project with Multiple Subjects (Multiple Cascades)
**Test Name**: `test_remove_project_multiple_subjects_cascade`

**Setup**:
- Create test project
- Create 5 subjects linked to project only
- Create evidence for each subject

**Action**:
- Remove project

**Assertions**:
- Response status code: 200
- Response indicates successful removal
- DynamoDB verification:
  - All 5 subjects unmapped from project
  - All 10 mappings deleted (5 forward, 5 reverse)
- Iceberg verification:
  - All evidence for all 5 subjects deleted
- Neptune verification:
  - Project named graph cleared
  - All 5 project_subject_iri nodes deleted
  - All 5 subject_iri nodes deleted (were last mappings)
  - All term links deleted
- Analytical tables verification:
  - by_project_term table: all rows for project deleted
  - by_subject table: all rows for 5 subjects deleted
- Complete project removal with multiple subject cascades

**Cleanup**:
- All data already removed

---

### Test 3: Remove Project with Shared Subjects (Partial Cleanup)
**Test Name**: `test_remove_project_shared_subjects`

**Setup**:
- Create project-A and project-B
- Create subject-1 linked to BOTH projects
- Create subject-2 linked to project-A only
- Create subject-3 linked to BOTH projects
- Create evidence for all subjects

**Action**:
- Remove project-A

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - subject-1 unlinked from project-A, still linked to project-B
  - subject-2 fully removed (was only in project-A)
  - subject-3 unlinked from project-A, still linked to project-B
- Iceberg verification:
  - Evidence for subject-2 deleted (cascade)
  - Evidence for subject-1 and subject-3 NOT deleted (still in project-B)
- Neptune verification:
  - Project-A named graph cleared
  - project_subject_iri for project-A deleted for all 3 subjects
  - subject_iri for subject-1 and subject-3 still exist (linked to project-B)
  - subject_iri for subject-2 deleted (was last mapping)
- Analytical tables verification:
  - by_project_term table: all project-A rows deleted
  - by_subject table: subject-2 rows deleted, subject-1 and subject-3 rows remain

**Cleanup**:
- Remove project-B
- Verify complete cleanup of remaining subjects

---

### Test 4: Remove Project - Project Not Found (Empty Project)
**Test Name**: `test_remove_project_not_found`

**Setup**:
- Ensure project "nonexistent-project" does NOT exist

**Action**:
- Invoke lambda with non-existent project:
  ```json
  {
    "project_id": "nonexistent-project"
  }
  ```

**Assertions**:
- Response status code: 200 (operation succeeds even if project empty)
- Response message indicates project removed
- DynamoDB verification:
  - GSI1 query returns empty (no subjects)
- Neptune verification:
  - CLEAR GRAPH executed (clears empty graph, no error)
- Analytical tables verification:
  - DELETE query executes (no rows match, no error)
- Idempotent operation (safe to call on non-existent project)

**Cleanup**:
- None required

---

### Test 5: Missing Required Field - project_id
**Test Name**: `test_remove_project_missing_project_id`

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
  - `error: "project_id is required"`
- No data deleted

**Cleanup**:
- None required

---

### Test 6: Remove Project with Null project_id
**Test Name**: `test_remove_project_null_project_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with null project_id:
  ```json
  {
    "project_id": null
  }
  ```

**Assertions**:
- Response status code: 400
- Response indicates project_id required
- Null treated as missing

**Cleanup**:
- None required

---

### Test 7: Remove Project - DynamoDB GSI Query Failure
**Test Name**: `test_remove_project_gsi_query_failure`

**Setup**:
- Create test project with subjects
- Simulate DynamoDB GSI query failure

**Action**:
- Remove project

**Assertions**:
- Response status code: 200 or 500
- If 500: Error indicates failure to query subjects
- CloudWatch logs show GSI query error
- If 200: Operation proceeds without subject cleanup (risky)
- Document behavior when subject list unavailable

**Cleanup**:
- Restore DynamoDB permissions
- Manually clean up subjects
- Delete project

---

### Test 8: Remove Project - Subject Unlink Failure (Partial Cleanup)
**Test Name**: `test_remove_project_subject_unlink_failure`

**Setup**:
- Create test project
- Create 3 subjects in project
- Simulate failure during second subject unlink

**Action**:
- Remove project

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - First subject unlinked successfully
  - Second subject unlink failed (logged)
  - Third subject may or may not be unlinked
- CloudWatch logs show subject unlink errors
- Partial cleanup scenario (some subjects remain linked)
- Operation continues despite individual subject failures

**Cleanup**:
- Manually unlink remaining subjects
- Complete project removal

---

### Test 9: Remove Project - Neptune Graph Clear Failure
**Test Name**: `test_remove_project_neptune_failure`

**Setup**:
- Create test project with subjects
- Simulate Neptune CLEAR GRAPH failure

**Action**:
- Remove project

**Assertions**:
- Response status code: 200 or 500
- If 200: Operation proceeds despite Neptune failure
- CloudWatch logs show Neptune error
- Neptune verification:
  - Project named graph may still contain data
  - Orphaned triples possible
- DynamoDB and Iceberg cleanup may succeed

**Cleanup**:
- Restore Neptune connectivity
- Manually clear Neptune graph
- Verify complete cleanup

---

### Test 10: Remove Project - Analytical Table Deletion Failure
**Test Name**: `test_remove_project_analytical_table_failure`

**Setup**:
- Create test project with subjects and evidence
- Simulate Iceberg analytical table deletion failure

**Action**:
- Remove project

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB and Neptune cleanup succeed
- Analytical tables verification:
  - Deletion attempted but failed
  - Orphaned rows for project may exist
- CloudWatch logs show Iceberg error
- Main project removal proceeds

**Cleanup**:
- Restore Iceberg permissions
- Manually clean up analytical table
- Verify complete cleanup

---

### Test 11: Remove Project with Large Number of Subjects (Performance)
**Test Name**: `test_remove_project_large_subject_count`

**Setup**:
- Create test project
- Create 100 subjects in project
- Create evidence for each subject

**Action**:
- Remove project
- Measure response time

**Assertions**:
- Response status code: 200
- Response indicates success
- DynamoDB verification:
  - All 100 subjects unlinked
  - 200 mappings deleted (100 forward, 100 reverse)
- Iceberg verification:
  - All evidence deleted (if subjects only in this project)
- Performance metrics:
  - Operation completes within lambda timeout (15 minutes)
  - Efficient bulk processing
- All data cleaned up correctly

**Cleanup**:
- Project already removed

---

### Test 12: Remove Project - Concurrent Removal Attempts
**Test Name**: `test_remove_project_concurrent_removal`

**Setup**:
- Create test project with subjects

**Action**:
- Invoke lambda 3 times concurrently to remove SAME project

**Assertions**:
- All 3 invocations complete
- All return status 200
- First invocation performs actual cleanup
- Subsequent invocations operate on already-removed project (idempotent)
- DynamoDB verification:
  - All subjects unlinked (no duplicate processing)
- Neptune verification:
  - Graph cleared once (CLEAR GRAPH idempotent)
- No duplicate deletions or errors

**Cleanup**:
- Project already removed

---

### Test 13: Remove Project - Subject Linked to Project Multiple Times
**Test Name**: `test_remove_project_subject_multiple_linkages`

**Setup**:
- Create test project
- Create subject-A linked to project as "patient-001"
- Link same subject-A to project again as "patient-002" (unusual but possible)

**Action**:
- Remove project

**Assertions**:
- Response status code: 200
- DynamoDB verification:
  - GSI1 query returns 2 items (both linkages found)
  - Both linkages unlinked
  - 4 mappings deleted total (2 forward, 2 reverse)
- Subject removed from project completely
- No partial linkages remain

**Cleanup**:
- Project already removed

---

### Test 14: Remove Project - Evidence Cascade for 1000+ Records
**Test Name**: `test_remove_project_large_evidence_cascade`

**Setup**:
- Create test project
- Create subject (only in this project)
- Create 1000 evidence records for subject

**Action**:
- Remove project
- Measure response time

**Assertions**:
- Response status code: 200
- Iceberg verification:
  - All 1000 evidence records deleted
  - Bulk deletion efficient
- Performance metrics:
  - Operation completes within timeout
  - No throttling errors
- Complete cascade executed

**Cleanup**:
- Project already removed

---

### Test 15: Remove Project - Verify GSI1 Index Usage
**Test Name**: `test_remove_project_gsi1_usage`

**Setup**:
- Create test project
- Create 10 subjects in project

**Action**:
- Remove project
- Monitor DynamoDB queries

**Assertions**:
- Response status code: 200
- DynamoDB verification:
  - GSI1 queried with GSI1PK = PROJECT#{project_id}
  - Single query returns all subjects in project
  - Efficient lookup using index
- All 10 subjects found and unlinked

**Cleanup**:
- Project already removed

---

### Test 16: Remove Project - Verify Analytical Table DELETE Query
**Test Name**: `test_remove_project_analytical_table_delete_query`

**Setup**:
- Create test project
- Create subjects with evidence

**Action**:
- Remove project

**Assertions**:
- Response status code: 200
- Analytical tables verification:
  - DELETE query executed on by_project_term table:
    - `DELETE FROM {database}.{table} WHERE project_id = '{project_id}'`
  - All rows for project removed
  - Athena query completes successfully
- Iceberg environment variables set correctly

**Cleanup**:
- Project already removed

---

### Test 17: Remove Project with Empty String project_id
**Test Name**: `test_remove_project_empty_string_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with empty string:
  ```json
  {
    "project_id": ""
  }
  ```

**Assertions**:
- Response status code: 400 or 200
- If 400: Empty string treated as missing parameter
- If 200: Queries return empty (no project with ID "")
- Document current behavior

**Cleanup**:
- None required

---

### Test 18: Remove Project - Special Characters in project_id
**Test Name**: `test_remove_project_special_characters`

**Setup**:
- Create project with special characters in ID: "test-project_with.special-chars"
- Create subjects in project

**Action**:
- Remove project with special character ID

**Assertions**:
- Response status code: 200
- DynamoDB verification:
  - GSI1 query handles special characters correctly
  - All subjects found and unlinked
- Neptune verification:
  - CLEAR GRAPH with special character IRI succeeds
- Analytical table verification:
  - DELETE query escapes special characters correctly
- Special characters handled properly

**Cleanup**:
- Project already removed

---

### Test 19: Remove Project - Very Long project_id
**Test Name**: `test_remove_project_long_id`

**Setup**:
- Create project with very long ID (250+ characters)

**Action**:
- Remove project with long ID

**Assertions**:
- Response status code: 200
- All systems handle long project_id correctly
- No truncation or errors

**Cleanup**:
- Project already removed

---

### Test 20: Remove Project - Verify Complete Multi-System Cleanup
**Test Name**: `test_remove_project_complete_cleanup_verification`

**Setup**:
- Create test project
- Create 5 subjects (3 only in this project, 2 shared with other project)
- Create evidence for all subjects
- Create term links in Neptune

**Action**:
- Remove project
- Perform comprehensive verification across all systems

**Assertions**:
- Response status code: 200
- **DynamoDB comprehensive check**:
  - GSI1 query returns empty for project
  - Forward mappings for project deleted
  - Reverse mappings for 3 exclusive subjects deleted
  - Reverse mappings for 2 shared subjects updated (project removed)
- **Iceberg evidence check**:
  - Evidence for 3 exclusive subjects deleted
  - Evidence for 2 shared subjects NOT deleted
- **Neptune comprehensive check**:
  - Project named graph empty
  - project_subject_iri nodes for all 5 subjects deleted
  - subject_iri for 3 exclusive subjects deleted
  - subject_iri for 2 shared subjects still exist
  - Term links for 3 exclusive subjects deleted
  - Term links for 2 shared subjects still exist
- **Analytical tables check**:
  - by_project_term table: all rows for project deleted
  - by_subject table: rows for 3 exclusive subjects deleted
  - by_subject table: rows for 2 shared subjects still exist
- **No orphaned data**:
  - No partial mappings
  - No orphaned triples
  - No inconsistent states
- Complete multi-system consistency

**Cleanup**:
- Delete other project with 2 shared subjects
- Verify final complete cleanup

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Remove Project Twice in Rapid Succession**: Test double-deletion race window

2. **Remove Project - Lambda Timeout**: Test with 1000+ subjects to verify timeout handling

3. **Remove Project - DynamoDB Batch Write Limit**: Test if subject count exceeds batch limits

4. **Remove Project - Athena Concurrent Query Limit**: Test when many queries active

5. **Remove Project - S3 Throttling During Iceberg Delete**: Test retry logic

6. **Remove Project - Neptune Transaction Size Limit**: Test if CLEAR GRAPH exceeds limits

7. **Remove Project - Iceberg Table Lock**: Test behavior when table locked

8. **Remove Project - Authorization**: Test unauthorized users cannot remove projects

9. **Remove Project - Audit Trail**: Verify complete deletion logged for compliance

10. **Remove Project - Cross-Region Replication**: If multi-region, test deletion propagation

11. **Remove Project - Partial Subject List**: Test if GSI pagination required for 100+ subjects

12. **Remove Project - Subject with 100+ Term Links**: Test Neptune cleanup performance

13. **Remove Project - Verify No Cascade to Other Projects**: Test isolation when subjects shared

14. **Remove Project - Evidence Created During Deletion**: Test race condition

15. **Remove Project - Subject Added During Deletion**: Test race condition

16. **Remove Project - Whitespace in project_id**: Test with leading/trailing whitespace

17. **Remove Project - Case Sensitivity**: Test if project_id case-sensitive

18. **Remove Project - Iceberg Partition Cleanup**: Test if empty partitions removed

19. **Remove Project - CloudWatch Metrics**: Verify deletion metrics published

20. **Remove Project - Rollback on Partial Failure**: Test if any rollback mechanism exists

---

## Critical Integration Considerations

### Cross-Lambda Dependencies:
- `remove_project` internally calls `remove_subject` logic for each subject
- Must maintain consistency with `remove_subject` behavior
- Should handle same edge cases as `remove_subject`

### System-Wide Consistency Checks:
1. **DynamoDB ↔ Neptune Consistency**: Verify mappings and graph nodes aligned
2. **Iceberg ↔ Neptune Consistency**: Verify evidence and term links aligned
3. **Analytical Tables ↔ Evidence Consistency**: Verify aggregations match evidence
4. **Project-Subject Linkage Integrity**: Verify no orphaned linkages

### Performance Considerations:
- Projects with 100+ subjects may approach lambda timeout
- Consider implementing asynchronous processing for very large projects
- Monitor DynamoDB throughput for bulk operations
- Monitor Athena query concurrency limits

### Failure Recovery:
- Document manual cleanup procedures for partial failures
- Implement monitoring/alerting for incomplete project removals
- Consider implementing compensating transactions for critical failures
