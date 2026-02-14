# Integration Test Plan: remove_evidence.py

## Lambda: remove_evidence

**Purpose**: Deletes a single evidence record and maintains consistency across all dependent systems. When the last evidence for a term link is deleted, the term link is also removed from Neptune. The lambda ensures that analytical tables are updated incrementally to reflect the evidence deletion.

**Dependencies**:
- **Iceberg/Athena** - deletes evidence record from evidence table (source of truth)
- **Neptune (Graph Database)** - deletes term links when evidence count reaches zero
- **Iceberg Analytical Tables** - `subject_terms_by_subject` and `subject_terms_by_project_term` tables updated incrementally
- **PyIceberg** - Python library for deleting from Iceberg tables
- No direct EventBridge dependency

**Key Operations**:
- Retrieves evidence record to get termlink_id before deletion
- Deletes evidence record from Iceberg evidence table
- Counts remaining evidence for the same termlink_id
- Updates analytical tables with decremented evidence_count
- If evidence_count reaches 0: deletes term link from Neptune
- Maintains consistency across all systems

---

## Integration Test Scenarios

### Test 1: Remove Evidence Record (Happy Path - Evidence Remains)
**Test Name**: `test_remove_evidence_with_remaining_evidence`

**Setup**:
- Create test subject
- Create 3 evidence records for same term link (HP_0001249, no qualifiers)
- Capture evidence_ids

**Action**:
- Invoke lambda to delete one evidence record:
  ```json
  {
    "evidence_id": "{evidence_id_1}"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `message: "Evidence deleted successfully"`
- Iceberg verification:
  - Evidence record with evidence_id_1 no longer exists
  - Other 2 evidence records still exist
  - Query by termlink_id returns 2 records
- Neptune verification:
  - Term link still exists (evidence_count > 0)
  - No term link deletion occurred
- Analytical tables verification:
  - Query `subject_terms_by_subject` for subject + term
  - evidence_count = 2 (decremented from 3)
  - Row still exists

**Cleanup**:
- Delete remaining evidence records
- Delete subject

---

### Test 2: Remove Last Evidence for Term Link (Cascade to Neptune)
**Test Name**: `test_remove_evidence_last_evidence_cascade`

**Setup**:
- Create test subject
- Create single evidence record for term HP_0001249

**Action**:
- Delete the evidence record

**Assertions**:
- Response status code: 200
- Response message: "Evidence deleted successfully"
- Iceberg verification:
  - Evidence record deleted
  - No evidence remains for this termlink_id
- Neptune verification:
  - Term link DELETED from Neptune
  - Query for term link returns nothing
  - Termlink IRI no longer exists
- Analytical tables verification:
  - Query `subject_terms_by_subject` for subject + term
  - Row REMOVED (evidence_count reached 0)
  - No entry for this term link

**Cleanup**:
- Delete subject (already clean)

---

### Test 3: Remove Evidence - Evidence Not Found
**Test Name**: `test_remove_evidence_not_found`

**Setup**:
- Generate random UUID that does not exist as evidence_id

**Action**:
- Invoke lambda with non-existent evidence_id:
  ```json
  {
    "evidence_id": "nonexistent-uuid-12345"
  }
  ```

**Assertions**:
- Response status code: 404
- Response body contains:
  - `message: "Evidence not found"`
- No data modifications in any system
- Safe idempotent operation

**Cleanup**:
- None required

---

### Test 4: Missing Required Field - evidence_id
**Test Name**: `test_remove_evidence_missing_evidence_id`

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
  - `message: "evidence_id is required"`
- No data deleted

**Cleanup**:
- None required

---

### Test 5: Remove Evidence with Null evidence_id
**Test Name**: `test_remove_evidence_null_evidence_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with null evidence_id:
  ```json
  {
    "evidence_id": null
  }
  ```

**Assertions**:
- Response status code: 400
- Response indicates evidence_id required
- Null treated as missing

**Cleanup**:
- None required

---

### Test 6: Remove Evidence - Multiple Term Links for Same Term
**Test Name**: `test_remove_evidence_distinct_termlinks`

**Setup**:
- Create test subject
- Create 2 evidence for HP_0001249 without qualifiers (termlink-A)
- Create 2 evidence for HP_0001249 with qualifiers ["HP:0012823"] (termlink-B)
- These create 2 distinct term links

**Action**:
- Delete both evidence records for termlink-A

**Assertions**:
- Both deletions return status 200
- Iceberg verification:
  - Evidence for termlink-A deleted
  - Evidence for termlink-B still exists
- Neptune verification:
  - Term link A deleted (evidence_count = 0)
  - Term link B still exists (evidence_count = 2)
- Analytical tables verification:
  - termlink-A row removed
  - termlink-B row still present with evidence_count = 2
- Distinct term links managed independently

**Cleanup**:
- Delete remaining evidence
- Delete subject

---

### Test 7: Remove Evidence - Verify Idempotency (Delete Twice)
**Test Name**: `test_remove_evidence_idempotent`

**Setup**:
- Create test subject
- Create evidence record

**Action**:
- Delete evidence (first time)
- Delete same evidence again (second time)

**Assertions**:
- First deletion: status 200, success message
- Second deletion: status 404, "Evidence not found"
- Iceberg verification:
  - Evidence deleted after first invocation
  - Second invocation finds nothing to delete
- No errors or inconsistencies
- Safe to call multiple times

**Cleanup**:
- Delete subject

---

### Test 8: Remove Evidence - Iceberg Deletion Failure
**Test Name**: `test_remove_evidence_iceberg_failure`

**Setup**:
- Create test subject
- Create evidence
- Simulate Iceberg deletion failure

**Action**:
- Invoke lambda to delete evidence

**Assertions**:
- Response status code: 500
- Response indicates deletion failed
- Iceberg verification:
  - Evidence record still exists (deletion failed)
- Neptune verification:
  - No term link deletion (only happens after successful Iceberg deletion)
- Analytical tables verification:
  - No updates (protected from inconsistency)
- System remains consistent (no partial deletions)

**Cleanup**:
- Restore Iceberg permissions
- Delete evidence
- Delete subject

---

### Test 9: Remove Evidence - Neptune Deletion Failure (Non-Blocking)
**Test Name**: `test_remove_evidence_neptune_failure_non_blocking`

**Setup**:
- Create test subject
- Create single evidence (will be last evidence for term link)
- Simulate Neptune failure

**Action**:
- Delete evidence

**Assertions**:
- Response status code: 200
- Response indicates success
- Iceberg verification:
  - Evidence deleted successfully
- Neptune verification:
  - Term link NOT deleted (Neptune failure logged)
- CloudWatch logs show Neptune error as non-critical
- Evidence deletion proceeds despite Neptune failure
- Analytical tables verification:
  - Row removed (analytical tables updated successfully)

**Cleanup**:
- Restore Neptune connectivity
- Manually clean up orphaned term link if needed
- Delete subject

---

### Test 10: Remove Evidence - Analytical Table Update Failure (Non-Blocking)
**Test Name**: `test_remove_evidence_analytical_table_failure_non_blocking`

**Setup**:
- Create test subject
- Create evidence
- Simulate analytical table update failure

**Action**:
- Delete evidence

**Assertions**:
- Response status code: 200
- Response indicates success
- Iceberg verification:
  - Evidence deleted successfully
- Analytical tables verification:
  - Update may have failed (logged as non-critical)
  - Out-of-sync state possible
- CloudWatch logs show analytical table error
- Main evidence deletion not blocked

**Cleanup**:
- Fix analytical table permissions
- Re-run materialization to sync tables
- Delete subject

---

### Test 11: Remove Evidence - Concurrent Deletion of Same Evidence
**Test Name**: `test_remove_evidence_concurrent_same_evidence`

**Setup**:
- Create test subject
- Create evidence record

**Action**:
- Invoke lambda 5 times concurrently to delete SAME evidence

**Assertions**:
- All 5 invocations complete (no crashes)
- Exactly ONE returns status 200 (successful deletion)
- Remaining 4 return status 404 ("Evidence not found")
- Iceberg verification:
  - Evidence deleted exactly once
  - No duplicate deletions
- Neptune verification:
  - Term link deleted once (if last evidence)
  - No duplicate Neptune operations
- Race condition handled safely

**Cleanup**:
- Delete subject

---

### Test 12: Remove Evidence - Concurrent Deletion of Different Evidence (Same Term Link)
**Test Name**: `test_remove_evidence_concurrent_different_evidence`

**Setup**:
- Create test subject
- Create 5 evidence records for same term link
- Capture all evidence_ids

**Action**:
- Invoke lambda 5 times concurrently to delete ALL 5 evidence records simultaneously

**Assertions**:
- All 5 invocations return status 200
- Iceberg verification:
  - All 5 evidence records deleted
  - No evidence remains for termlink_id
- Neptune verification:
  - Term link deleted exactly once
  - Last deletion triggers Neptune cleanup
- Analytical tables verification:
  - evidence_count decrements correctly
  - Row removed when count reaches 0
- Concurrent deletions handled correctly

**Cleanup**:
- Delete subject

---

### Test 13: Remove Evidence for Deleted Subject
**Test Name**: `test_remove_evidence_for_deleted_subject`

**Setup**:
- Create test subject
- Create evidence
- Delete subject (removes subject but evidence is immutable)

**Action**:
- Delete evidence record

**Assertions**:
- Response status code: 200
- Response indicates success
- Iceberg verification:
  - Evidence deleted (immutable records independent of subject)
- Evidence can be deleted even after subject removed
- Document: Evidence records are independent entities

**Cleanup**:
- Already clean

---

### Test 14: Remove Evidence - Verify termlink_id Extraction
**Test Name**: `test_remove_evidence_termlink_id_extraction`

**Setup**:
- Create test subject
- Create evidence with specific term and qualifiers

**Action**:
- Delete evidence
- Capture termlink_id from get_evidence before deletion

**Assertions**:
- Response status code: 200
- Iceberg verification:
  - Evidence deleted
  - Lambda correctly extracted termlink_id before deletion
- Neptune verification:
  - Correct term link deleted (matching termlink_id)
- termlink_id computation consistent

**Cleanup**:
- Delete subject

---

### Test 15: Remove Evidence with Empty String evidence_id
**Test Name**: `test_remove_evidence_empty_string_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with empty string:
  ```json
  {
    "evidence_id": ""
  }
  ```

**Assertions**:
- Response status code: 400 or 404
- If 400: Empty string treated as missing parameter
- If 404: Empty string treated as non-existent evidence
- Document current behavior

**Cleanup**:
- None required

---

### Test 16: Remove Evidence - Very Long evidence_id
**Test Name**: `test_remove_evidence_very_long_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with 1KB+ evidence_id string

**Assertions**:
- Response status code: 404 or 400
- Graceful handling of invalid input
- No server crashes or timeouts

**Cleanup**:
- None required

---

### Test 17: Remove Evidence - Malformed evidence_id
**Test Name**: `test_remove_evidence_malformed_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with malformed UUID:
  ```json
  {
    "evidence_id": "not-a-valid-uuid-format"
  }
  ```

**Assertions**:
- Response status code: 404
- Response message: "Evidence not found"
- Malformed ID treated as non-existent
- No server errors

**Cleanup**:
- None required

---

### Test 18: Remove Evidence - Verify subject_iri Construction
**Test Name**: `test_remove_evidence_subject_iri_construction`

**Setup**:
- Create test subject
- Create evidence

**Action**:
- Delete evidence

**Assertions**:
- Response status code: 200
- Neptune verification:
  - Term link IRI constructed correctly:
    - Format: `http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{termlink_id}`
  - Correct term link deleted
- subject_iri and termlink_iri construction logic verified

**Cleanup**:
- Delete subject

---

### Test 19: Remove Evidence - Performance Test
**Test Name**: `test_remove_evidence_performance`

**Setup**:
- Create test subject
- Create 100 evidence records

**Action**:
- Delete all 100 evidence records sequentially
- Measure total time

**Assertions**:
- All 100 deletions return status 200
- Performance metrics:
  - Average deletion time < 2 seconds per evidence
  - No timeouts
  - No throttling errors
- Iceberg verification:
  - All evidence deleted
- Analytical tables verification:
  - All updates processed correctly

**Cleanup**:
- Delete subject

---

### Test 20: Remove Evidence - Bulk Deletion Consistency
**Test Name**: `test_remove_evidence_bulk_consistency`

**Setup**:
- Create 10 test subjects
- Create 10 evidence records per subject (100 total)
- Create term links in Neptune

**Action**:
- Delete all 100 evidence records

**Assertions**:
- All deletions successful
- Iceberg verification:
  - All evidence deleted
  - No orphaned evidence records
- Neptune verification:
  - All term links deleted
  - No orphaned term links
- Analytical tables verification:
  - All rows removed
  - evidence_counts accurate throughout
- Cross-system consistency maintained

**Cleanup**:
- Delete all subjects

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Remove Evidence During Evidence Creation**: Test race condition where evidence deleted while being created

2. **Remove Evidence with Special Characters in evidence_id**: Test unicode or special chars

3. **Remove Evidence - Lambda Timeout**: Test deletion of evidence with complex dependencies

4. **Remove Evidence - Partial Neptune Cleanup**: Test when some Neptune triples deleted but not all

5. **Remove Evidence Twice in Rapid Succession**: Test double-deletion race window

6. **Remove Evidence - S3 Throttling During Iceberg Write**: Test retry logic

7. **Remove Evidence - Athena Query Limit**: Test deletion when many concurrent Athena queries

8. **Remove Evidence for Subject with 100+ Terms**: Test analytical table update performance

9. **Remove Evidence - Iceberg Table Lock**: Test behavior when table locked by another operation

10. **Remove Evidence - Verify Delete is Hard Delete**: Confirm evidence permanently removed, not soft-deleted

11. **Remove Evidence - Audit Trail**: Verify deletion logged for compliance

12. **Remove Evidence - Authorization**: Test unauthorized users cannot delete evidence

13. **Remove Evidence - Cross-Region Replication**: If multi-region, test deletion propagation

14. **Remove Evidence After Term Link Manual Deletion**: Test if Neptune term link already deleted

15. **Remove Evidence - Whitespace in evidence_id**: Test with leading/trailing whitespace

16. **Remove Evidence - Null Fields in Evidence Record**: Test deletion when evidence has null fields

17. **Remove Evidence - Large Qualifiers Array**: Test deletion of evidence with 50+ qualifiers

18. **Remove Evidence - Evidence Created in Batch**: Test deleting evidence from bulk import run

19. **Remove Evidence - Analytical Table Partition Management**: Test if deletion creates empty partitions

20. **Remove Evidence - Count Remaining Evidence Failure**: Test if evidence count query fails
