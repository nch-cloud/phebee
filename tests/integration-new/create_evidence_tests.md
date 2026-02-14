# Integration Test Plan: create_evidence.py

## Lambda: create_evidence

**Purpose**: Creates evidence records that document the provenance of phenotypic term annotations for subjects. Evidence can include manual annotations, clinical notes, automated extractions, and other supporting documentation. The lambda maintains consistency across the transactional evidence store (Iceberg) and the graph database (Neptune).

**Dependencies**:
- **Iceberg/Athena** - stores immutable evidence records in the evidence table (source of truth)
- **Neptune (Graph Database)** - stores term links (subject-term-qualifier relationships) and maintains evidence counts
- **Iceberg Analytical Tables** - `subject_terms_by_subject` and `subject_terms_by_project_term` tables updated incrementally
- **PyIceberg** - Python library for writing to Iceberg tables
- No direct EventBridge dependency (events fired elsewhere)

**Key Operations**:
- Writes evidence record to Iceberg evidence table with UUID-based evidence_id
- Computes termlink_id hash from (subject_iri, term_iri, qualifiers)
- Creates or updates term link in Neptune (idempotent)
- Incrementally updates analytical tables for subject-term aggregations
- Supports various evidence types: manual_annotation, clinical_note, automated extraction, etc.

---

## Integration Test Scenarios

### Test 1: Create Evidence Record (Happy Path - Minimal Fields)
**Test Name**: `test_create_evidence_minimal`

**Setup**:
- Create test project
- Create test subject
- Ensure clean evidence state

**Action**:
- Invoke lambda with minimal required payload:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Response status code: 201
- Response body contains:
  - `evidence_id` (UUID)
  - `run_id` (null)
  - `batch_id` (null)
  - `evidence_type: "manual_annotation"` (default)
  - `subject_id: "test-subject-uuid"`
  - `term_iri: "http://purl.obolibrary.org/obo/HP_0001249"`
  - `creator.creator_id: "user-123"`
  - `creator.creator_type: "human"` (default)
  - `created_timestamp` (ISO format)
- Iceberg verification:
  - Query evidence table for evidence_id
  - Record exists with all fields matching
  - termlink_id computed correctly (hash of subject+term+qualifiers)
  - created_date matches timestamp date
- Neptune verification:
  - Term link exists with computed termlink_id
  - Term link connects subject_iri to term_iri
  - No qualifiers on term link
  - Creator IRI referenced in term link
- Analytical tables verification:
  - Query `subject_terms_by_subject` for subject_id
  - Row exists with term_iri and evidence_count = 1
  - Query `subject_terms_by_project_term` for project_id + term_iri
  - Row exists with evidence_count = 1

**Cleanup**:
- Delete evidence record via remove_evidence lambda
- Verify cascading cleanup in Neptune and analytical tables
- Delete subject and project

---

### Test 2: Create Evidence with All Optional Fields
**Test Name**: `test_create_evidence_full_fields`

**Setup**:
- Create test project
- Create test subject

**Action**:
- Invoke lambda with all optional fields:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
    "creator_id": "system-nlp-v2",
    "creator_name": "NLP System v2.0",
    "creator_type": "automated",
    "evidence_type": "clinical_note",
    "run_id": "run-abc-123",
    "batch_id": "batch-5",
    "encounter_id": "encounter-789",
    "clinical_note_id": "note-456",
    "span_start": 125,
    "span_end": 150,
    "qualifiers": ["HP:0012823", "HP:0003577"],
    "term_source": {
      "source": "hpo",
      "version": "2024-01-01",
      "iri": "http://purl.obolibrary.org/obo/hp.owl"
    },
    "note_timestamp": "2024-01-15T10:30:00Z",
    "provider_type": "physician",
    "author_specialty": "neurology",
    "note_type": "progress_note"
  }
  ```

**Assertions**:
- Response status code: 201
- Response body contains all provided fields
- Iceberg verification:
  - Evidence record has all fields populated
  - qualifiers stored as array
  - term_source stored as struct
  - Clinical note context fields present
- Neptune verification:
  - Term link created with qualifiers
  - Qualifiers are IRI-formatted
  - Creator IRI includes "system-nlp-v2"
- Analytical tables verification:
  - termlink_id includes qualifiers in hash
  - Separate rows for qualified vs unqualified terms

**Cleanup**:
- Delete evidence
- Verify all fields cleaned up

---

### Test 3: Multiple Evidence Records for Same Term Link
**Test Name**: `test_create_evidence_multiple_same_termlink`

**Setup**:
- Create test project
- Create test subject

**Action**:
- Create 3 evidence records with identical (subject, term, qualifiers):
  ```json
  [
    {
      "subject_id": "subject-uuid",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
      "creator_id": "creator-1",
      "evidence_type": "clinical_note",
      "clinical_note_id": "note-1"
    },
    {
      "subject_id": "subject-uuid",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
      "creator_id": "creator-2",
      "evidence_type": "manual_annotation"
    },
    {
      "subject_id": "subject-uuid",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
      "creator_id": "creator-3",
      "evidence_type": "clinical_note",
      "clinical_note_id": "note-2"
    }
  ]
  ```

**Assertions**:
- All 3 invocations return status 201
- Each returns unique evidence_id
- All return same termlink_id (computed from subject+term+qualifiers)
- Iceberg verification:
  - 3 distinct evidence records exist
  - All have same termlink_id
  - Different evidence_ids
  - Different clinical_note_ids
- Neptune verification:
  - Only ONE term link exists (idempotent creation)
  - Term link connects same subject to same term
  - Neptune logs indicate "already exists" for 2nd and 3rd
- Analytical tables verification:
  - Query subject_terms_by_subject for subject
  - evidence_count = 3 for this term_iri
  - Single row, incremented count

**Cleanup**:
- Delete all 3 evidence records
- Verify evidence_count decrements to 0
- Verify term link deleted from Neptune after last evidence removed

---

### Test 4: Evidence with Qualifiers vs Without Qualifiers (Different Term Links)
**Test Name**: `test_create_evidence_qualified_vs_unqualified`

**Setup**:
- Create test project
- Create test subject

**Action**:
- Create evidence WITHOUT qualifiers:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "creator-1"
  }
  ```
- Create evidence WITH qualifiers for same subject+term:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "creator-2",
    "qualifiers": ["HP:0012823"]
  }
  ```

**Assertions**:
- Both return status 201
- Different evidence_ids
- Different termlink_ids (qualifiers change hash)
- Iceberg verification:
  - 2 distinct evidence records
  - First has empty qualifiers array
  - Second has qualifiers array with one element
  - Different termlink_ids
- Neptune verification:
  - TWO term links exist for same subject+term
  - One term link has no qualifiers
  - Other term link has qualifier predicate(s)
- Analytical tables verification:
  - TWO rows in subject_terms_by_subject for this subject+term combo
  - One row for unqualified termlink
  - One row for qualified termlink
  - Each has evidence_count = 1

**Cleanup**:
- Delete both evidence records
- Verify both term links cleaned up from Neptune
- Verify both analytical table rows removed

---

### Test 5: Missing Required Field - subject_id
**Test Name**: `test_create_evidence_missing_subject_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with missing subject_id:
  ```json
  {
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `message: "Missing required fields: subject_id, term_iri, and creator_id are required."`
- No data created in any system

**Cleanup**:
- None required

---

### Test 6: Missing Required Field - term_iri
**Test Name**: `test_create_evidence_missing_term_iri`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with missing term_iri:
  ```json
  {
    "subject_id": "subject-uuid",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains error about missing required fields
- No data created

**Cleanup**:
- None required

---

### Test 7: Missing Required Field - creator_id
**Test Name**: `test_create_evidence_missing_creator_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with missing creator_id:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains error about missing required fields
- No data created

**Cleanup**:
- None required

---

### Test 8: Invalid term_iri Format
**Test Name**: `test_create_evidence_invalid_term_iri`

**Setup**:
- Create test subject

**Action**:
- Invoke lambda with invalid term_iri:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "not-a-valid-iri",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Response status code: 201 or 400 (depending on validation)
- If accepted:
  - Evidence created with malformed IRI
  - May cause downstream issues
- If rejected:
  - Error message about invalid IRI format
- Test documents current behavior

**Cleanup**:
- Delete evidence if created
- Document expected behavior

---

### Test 9: Evidence with run_id and batch_id
**Test Name**: `test_create_evidence_with_run_batch`

**Setup**:
- Create test project
- Create test subject

**Action**:
- Create multiple evidence records with same run_id:
  ```json
  [
    {
      "subject_id": "subject-1",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
      "creator_id": "nlp-system",
      "run_id": "run-xyz-789",
      "batch_id": "batch-0"
    },
    {
      "subject_id": "subject-2",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
      "creator_id": "nlp-system",
      "run_id": "run-xyz-789",
      "batch_id": "batch-1"
    }
  ]
  ```

**Assertions**:
- Both return status 201
- Different evidence_ids but same run_id
- Different batch_ids
- Iceberg verification:
  - Query evidence table by run_id
  - Returns both evidence records
  - run_id field populated
  - batch_id distinguishes records
- Can query by run_id using query_evidence_by_run lambda

**Cleanup**:
- Delete both evidence records
- Verify cleanup

---

### Test 10: Evidence with Clinical Note Context Fields
**Test Name**: `test_create_evidence_clinical_note_context`

**Setup**:
- Create test subject

**Action**:
- Create evidence with clinical note metadata:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "note-extractor",
    "evidence_type": "clinical_note",
    "clinical_note_id": "note-12345",
    "encounter_id": "encounter-67890",
    "span_start": 42,
    "span_end": 67,
    "note_timestamp": "2024-01-15T14:22:00Z",
    "provider_type": "nurse_practitioner",
    "author_specialty": "family_medicine",
    "note_type": "admission_note"
  }
  ```

**Assertions**:
- Response status code: 201
- Iceberg verification:
  - Evidence record has all clinical note fields
  - span_start and span_end are integers
  - note_timestamp formatted correctly
  - provider_type, author_specialty, note_type strings stored
- Can query evidence and retrieve clinical context

**Cleanup**:
- Delete evidence

---

### Test 11: Qualifier Short Name vs IRI Format
**Test Name**: `test_create_evidence_qualifier_formats`

**Setup**:
- Create test subject

**Action**:
- Create evidence with short-name qualifiers:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "qualifiers": ["HP:0012823", "HP:0003577"]
  }
  ```

**Assertions**:
- Response status code: 201
- Iceberg verification:
  - Qualifiers stored as provided (short names)
- Neptune verification:
  - Qualifiers converted to IRIs in term link:
    - `http://ods.nationwidechildrens.org/phebee/qualifier/HP:0012823`
    - `http://ods.nationwidechildrens.org/phebee/qualifier/HP:0003577`
  - Term link has qualifier predicates

**Cleanup**:
- Delete evidence

---

### Test 12: Empty Qualifiers Array vs Missing Qualifiers
**Test Name**: `test_create_evidence_qualifiers_empty_vs_missing`

**Setup**:
- Create test subject

**Action**:
- Create evidence with empty qualifiers array:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "qualifiers": []
  }
  ```
- Create evidence with missing qualifiers field:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Both return status 201
- Both create evidence successfully
- Iceberg verification:
  - First has qualifiers = [] (empty array)
  - Second has qualifiers = [] or null (default)
- Neptune verification:
  - Both create term links without qualifiers
  - termlink_id computed identically for empty vs missing
- Functional equivalence verified

**Cleanup**:
- Delete both evidence records

---

### Test 13: Iceberg Write Failure Does Not Create Neptune Term Link
**Test Name**: `test_create_evidence_iceberg_failure_no_neptune`

**Setup**:
- Create test subject
- Simulate Iceberg write failure (e.g., S3 permissions issue)

**Action**:
- Invoke lambda to create evidence

**Assertions**:
- Response status code: 500
- Response indicates internal server error
- Iceberg verification:
  - No evidence record written
- Neptune verification:
  - No term link created (Neptune write skipped if Iceberg fails)
- System consistency: Iceberg is source of truth, Neptune secondary

**Cleanup**:
- Restore Iceberg permissions
- Verify clean state

---

### Test 14: Neptune Term Link Failure Does Not Block Evidence Creation
**Test Name**: `test_create_evidence_neptune_failure_non_blocking`

**Setup**:
- Create test subject
- Simulate Neptune failure

**Action**:
- Invoke lambda to create evidence

**Assertions**:
- Response status code: 201
- Response indicates evidence created successfully
- Iceberg verification:
  - Evidence record written successfully
- Neptune verification:
  - Term link NOT created (failure logged)
- CloudWatch logs show Neptune error as non-critical
- Evidence creation proceeds despite Neptune failure

**Cleanup**:
- Restore Neptune connectivity
- Manually create term link if needed
- Delete evidence

---

### Test 15: Analytical Table Update Failure Does Not Block Evidence Creation
**Test Name**: `test_create_evidence_analytical_table_failure_non_blocking`

**Setup**:
- Create test subject
- Simulate analytical table update failure

**Action**:
- Invoke lambda to create evidence

**Assertions**:
- Response status code: 201
- Evidence created successfully
- Iceberg verification:
  - Evidence record exists in evidence table
- Analytical tables verification:
  - Update may have failed (logged as non-critical)
- CloudWatch logs show analytical table error
- Main evidence creation not blocked

**Cleanup**:
- Fix analytical table permissions
- Re-run materialization to sync tables
- Delete evidence

---

### Test 16: Evidence for Non-Existent Subject
**Test Name**: `test_create_evidence_subject_not_exists`

**Setup**:
- Generate random UUID for non-existent subject
- Verify subject does not exist in DynamoDB

**Action**:
- Create evidence for non-existent subject:
  ```json
  {
    "subject_id": "nonexistent-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123"
  }
  ```

**Assertions**:
- Response status code: 201 (evidence creation does NOT validate subject existence)
- Evidence created successfully
- Iceberg verification:
  - Evidence record exists with nonexistent subject_id
- Neptune verification:
  - Term link created (subject_iri constructed from subject_id)
  - Subject node may not exist in Neptune
- Document: Evidence creation is independent of subject existence validation

**Cleanup**:
- Delete evidence
- Document implications for data integrity

---

### Test 17: Same Evidence Created Twice (Not Truly Idempotent)
**Test Name**: `test_create_evidence_duplicate_not_idempotent`

**Setup**:
- Create test subject

**Action**:
- Create evidence with specific parameters
- Immediately create evidence with IDENTICAL parameters

**Assertions**:
- Both invocations return status 201
- Different evidence_ids (evidence creation NOT idempotent)
- Same termlink_id
- Iceberg verification:
  - TWO evidence records exist
  - Different evidence_ids, identical other fields
  - Duplicate evidence not prevented
- Neptune verification:
  - Single term link (idempotent)
- Analytical tables verification:
  - evidence_count = 2
- Document: Evidence creation creates duplicate records if called multiple times

**Cleanup**:
- Delete both evidence records
- Verify cleanup

---

### Test 18: Evidence with Null creator_name
**Test Name**: `test_create_evidence_null_creator_name`

**Setup**:
- Create test subject

**Action**:
- Create evidence with explicit null creator_name:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "creator_name": null
  }
  ```

**Assertions**:
- Response status code: 201
- Response has creator without creator_name field (or null)
- Iceberg verification:
  - creator_name stored as null or omitted

**Cleanup**:
- Delete evidence

---

### Test 19: Evidence with Invalid creator_type
**Test Name**: `test_create_evidence_invalid_creator_type`

**Setup**:
- Create test subject

**Action**:
- Create evidence with invalid creator_type:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "creator_type": "invalid_type"
  }
  ```

**Assertions**:
- Response status code: 201 (no validation on creator_type)
- Evidence created with invalid type
- Document: No enumeration validation on creator_type

**Cleanup**:
- Delete evidence
- Consider adding validation

---

### Test 20: Bulk Evidence Creation Performance
**Test Name**: `test_create_evidence_bulk_performance`

**Setup**:
- Create test project
- Create 10 test subjects

**Action**:
- Create 100 evidence records rapidly:
  - 10 subjects × 10 terms each
  - Measure end-to-end time

**Assertions**:
- All 100 invocations return status 201
- All evidence_ids unique
- Iceberg verification:
  - 100 evidence records exist
  - All written successfully
- Neptune verification:
  - 100 term links created (assuming unique subject+term+qualifier combinations)
- Analytical tables verification:
  - All evidence_counts accurate
- Performance metrics:
  - Average latency per evidence record
  - P95 and P99 latencies
  - No throttling errors

**Cleanup**:
- Delete all 100 evidence records
- Delete subjects and project

---

### Test 21: Qualifier Standardization with true/false Format
**Test Name**: `test_create_evidence_qualifier_standardization`

**Setup**:
- Create test subject

**Action**:
- Create evidence with positive qualifiers:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
    "creator_id": "test-creator",
    "qualifiers": ["negated", "family"]
  }
  ```

**Assertions**:
- Response status code: 201
- Iceberg verification:
  - Evidence record has qualifiers stored as array: ["negated", "family"]
- Neptune verification:
  - Term link created with qualifier predicates
  - Qualifiers stored in standardized format
- GetEvidence verification:
  - Retrieve evidence and verify qualifiers returned as dict: {"negated": "true", "family": "true"}
  - Qualifier values are string "true", not boolean
  - All specified qualifiers present with "true" values

**Cleanup**:
- Delete evidence
- Delete subject

**Why this test is important**: The existing integration test `test_qualifiers.py` (lines 34-70) verifies that qualifiers are standardized to {"negated": "true"} format for API responses. This ensures consistent qualifier representation across the system and enables proper hash computation for termlink_id generation.

---

### Test 22: Qualifier Hash Consistency for termlink_id Generation
**Test Name**: `test_create_evidence_qualifier_hash_consistency`

**Setup**:
- Create test subject

**Action**:
- Create two evidence records with identical qualifiers in different order:
  ```json
  // Evidence 1
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
    "creator_id": "creator-1",
    "qualifiers": ["negated", "family"]
  }
  // Evidence 2
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
    "creator_id": "creator-2",
    "qualifiers": ["family", "negated"]  // Same qualifiers, different order
  }
  ```

**Assertions**:
- Both return status 201
- Both evidence records have SAME termlink_id
- Iceberg verification:
  - Query both evidence records
  - termlink_id computed identically regardless of qualifier order
  - Qualifiers are sorted/normalized before hashing
- Neptune verification:
  - Single term link created (idempotent due to same hash)
- Analytical tables verification:
  - Single row with evidence_count = 2

**Cleanup**:
- Delete both evidence records
- Delete subject

**Why this test is important**: The existing integration test `test_qualifiers.py` (lines 72-90) verifies hash consistency for identical qualifiers. This ensures that the same subject-term-qualifier combination always produces the same termlink_id, enabling proper evidence aggregation.

---

### Test 23: Different Qualifiers Produce Different termlink_ids
**Test Name**: `test_create_evidence_different_qualifiers_different_hash`

**Setup**:
- Create test subject

**Action**:
- Create evidence with "negated" qualifier
- Create evidence with "hypothetical" qualifier (same subject and term)

**Assertions**:
- Both return status 201
- Different evidence_ids
- Different termlink_ids
- Iceberg verification:
  - Two evidence records with different termlink_ids
  - First has qualifiers = ["negated"]
  - Second has qualifiers = ["hypothetical"]
- Neptune verification:
  - TWO term links created (different qualifier combinations)
- Analytical tables verification:
  - Two rows for this subject+term (different qualifier combinations)

**Cleanup**:
- Delete both evidence records
- Delete subject

**Why this test is important**: The existing integration test `test_qualifiers.py` (lines 107-125) verifies that different qualifier values produce different hashes. This ensures proper separation of qualified vs unqualified annotations.

---

### Test 24: Empty Qualifiers vs Missing Qualifiers (Same termlink_id)
**Test Name**: `test_create_evidence_empty_qualifiers_hash`

**Setup**:
- Create test subject

**Action**:
- Create evidence with empty qualifiers array: `"qualifiers": []`
- Create evidence with missing qualifiers field (omitted from payload)

**Assertions**:
- Both return status 201
- Both have SAME termlink_id (empty and missing treated identically)
- Iceberg verification:
  - Both evidence records have empty/null qualifiers
  - Same termlink_id computed
- Neptune verification:
  - Single term link (idempotent creation)
- Analytical tables verification:
  - Single row with evidence_count = 2

**Cleanup**:
- Delete both evidence records
- Delete subject

**Why this test is important**: The existing integration test `test_qualifiers.py` (lines 133-154) verifies that empty qualifiers array and missing qualifiers produce the same hash. This ensures consistent handling of unqualified annotations.

---

### Test 25: Qualifier Parsing Consistency Across Endpoints
**Test Name**: `test_create_evidence_qualifier_parsing_consistency`

**Setup**:
- Create test subject
- Create evidence with multiple qualifiers: ["negated", "hypothetical"]

**Action**:
- Create evidence
- Retrieve via GetEvidence
- Retrieve via GetSubject
- Retrieve via GetSubjectTermInfo

**Assertions**:
- All endpoints return qualifiers in consistent format
- GetEvidence returns qualifiers as dict: {"negated": "true", "hypothetical": "true"}
- GetSubject includes qualifiers in terms array
- GetSubjectTermInfo shows qualifiers for term link
- Qualifier parsing consistent across all read endpoints

**Cleanup**:
- Delete evidence
- Delete subject

**Why this test is important**: The existing integration test `test_qualifier_parsing_comprehensive.py` (lines 12-90) verifies that qualifier parsing works consistently across GetSubject, GetSubjectTermInfo, and other endpoints. This ensures a unified qualifier representation throughout the API.

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Very Long Clinical Note ID**: Test with 1KB+ clinical_note_id string

2. **Negative span_start/span_end**: Test with invalid span values

3. **span_start > span_end**: Test with inverted span values

4. **Future note_timestamp**: Test with timestamp in the future

5. **Invalid ISO Timestamp Format**: Test with malformed timestamp string

6. **Qualifiers with 50+ Elements**: Test with very large qualifiers array

7. **Duplicate Qualifiers in Array**: Test with ["HP:0012823", "HP:0012823"]

8. **Evidence with run_id but No batch_id**: Test optional field independence

9. **Special Characters in creator_id**: Test with unicode, spaces, special chars

10. **Lambda Timeout During Iceberg Write**: Test partial write scenarios

11. **S3 503 Throttling During Iceberg Write**: Test retry logic

12. **Iceberg Table Not Found**: Test error handling when table missing

13. **Athena Query Failure During Analytical Update**: Test resilience

14. **Concurrent Evidence Creation for Same Term Link**: Race conditions

15. **Evidence Record Size Limits**: Test with maximum payload size

16. **termlink_id Hash Collision (Theoretical)**: Test if collision handling exists

17. **Multiple Creators for Same Evidence**: Test if multiple creator metadata supported

18. **Evidence Versioning**: Test if evidence can be updated (or only created)

19. **Soft Delete vs Hard Delete**: Verify evidence deletion is hard delete

20. **Cross-Region Iceberg Replication**: Test eventual consistency if multi-region
