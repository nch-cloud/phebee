# Integration Test Plan: get_subject_term_info.py

## Lambda: get_subject_term_info

**Purpose**: Retrieves detailed information about a specific term link for a subject, including the term IRI, qualifiers, and evidence count. This provides a focused view of a single subject-term relationship.

**Dependencies**:
- **Iceberg/Athena** - queries `subject_terms_by_subject` analytical table filtered by subject_id, term_iri, and qualifiers
- **PyIceberg** - Python library for reading from Iceberg tables
- No dependencies on Neptune, DynamoDB, S3, or EventBridge

**Key Operations**:
- Accepts required parameters: subject_id, term_iri
- Accepts optional parameter: qualifiers (array)
- Queries Iceberg analytical table for specific term link
- Returns termlink_id, term_iri, qualifiers, evidence_count
- Returns 404 if term link not found for subject

---

## Integration Test Scenarios

### Test 1: Get Subject Term Info (Happy Path - No Qualifiers)
**Test Name**: `test_get_subject_term_info_no_qualifiers_success`

**Setup**:
- Create test subject
- Create 3 evidence records for subject with same term (HP_0001249) and no qualifiers

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `termlink_id` (hash of subject+term+qualifiers)
  - `term_iri: "http://purl.obolibrary.org/obo/HP_0001249"`
  - `qualifiers: []` (empty array)
  - `evidence_count: 3`
- Iceberg verification:
  - Query matches single row in `subject_terms_by_subject` table
  - Evidence count aggregated correctly

**Cleanup**:
- Delete all evidence
- Delete subject

---

### Test 2: Get Subject Term Info with Qualifiers (Happy Path)
**Test Name**: `test_get_subject_term_info_with_qualifiers_success`

**Setup**:
- Create test subject
- Create 2 evidence records with term HP_0001249 and qualifiers ["HP:0012823", "HP:0003577"]

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": ["HP:0012823", "HP:0003577"]
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `termlink_id` (hash includes qualifiers)
  - `term_iri: "http://purl.obolibrary.org/obo/HP_0001249"`
  - `qualifiers: ["HP:0012823", "HP:0003577"]`
  - `evidence_count: 2`
- Qualifiers array order preserved

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 3: Get Subject Term Info - Term Not Found
**Test Name**: `test_get_subject_term_info_term_not_found`

**Setup**:
- Create test subject
- Create evidence with different term (HP_0002297)

**Action**:
- Query for term that does NOT exist for subject:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
  }
  ```

**Assertions**:
- Response status code: 404
- Response body contains:
  - `message: "Term not found for subject"`
- No term info returned

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 4: Get Subject Term Info - Subject Does Not Exist
**Test Name**: `test_get_subject_term_info_subject_not_found`

**Setup**:
- Generate random UUID for non-existent subject

**Action**:
- Invoke lambda with non-existent subject_id:
  ```json
  {
    "subject_id": "nonexistent-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
  }
  ```

**Assertions**:
- Response status code: 404
- Response message: "Term not found for subject"
- Iceberg query returns no rows

**Cleanup**:
- None required

---

### Test 5: Missing Required Field - subject_id
**Test Name**: `test_get_subject_term_info_missing_subject_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda without subject_id:
  ```json
  {
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `message: "Missing required field: subject_id"`
- Clear error message

**Cleanup**:
- None required

---

### Test 6: Missing Required Field - term_iri
**Test Name**: `test_get_subject_term_info_missing_term_iri`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda without term_iri:
  ```json
  {
    "subject_id": "test-subject-uuid"
  }
  ```

**Assertions**:
- Response status code: 400
- Response body contains:
  - `message: "Missing required field: term_iri"`
- Clear error message

**Cleanup**:
- None required

---

### Test 7: Get Subject Term Info - Qualifiers Mismatch
**Test Name**: `test_get_subject_term_info_qualifiers_mismatch`

**Setup**:
- Create test subject
- Create evidence with term HP_0001249 and qualifiers ["HP:0012823"]

**Action**:
- Query with DIFFERENT qualifiers:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": ["HP:0003577"]
  }
  ```

**Assertions**:
- Response status code: 404
- Response message: "Term not found for subject"
- Qualifiers must match exactly for term link to be found
- Different qualifiers = different termlink_id = different term link

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 8: Get Subject Term Info - Empty Qualifiers Array
**Test Name**: `test_get_subject_term_info_empty_qualifiers`

**Setup**:
- Create test subject
- Create evidence with term HP_0001249 and no qualifiers

**Action**:
- Query with explicit empty qualifiers array:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": []
  }
  ```

**Assertions**:
- Response status code: 200
- Response contains term info with empty qualifiers
- Empty array equivalent to omitting qualifiers field

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 9: Get Subject Term Info - Qualifiers Order Sensitivity
**Test Name**: `test_get_subject_term_info_qualifiers_order`

**Setup**:
- Create test subject
- Create evidence with qualifiers in specific order: ["HP:0012823", "HP:0003577"]

**Action**:
- Query with qualifiers in DIFFERENT order:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": ["HP:0003577", "HP:0012823"]
  }
  ```

**Assertions**:
- Response status code: 200 or 404 (depending on hash implementation)
- If 200: Hash computation normalizes qualifier order (order-insensitive)
- If 404: Qualifier order matters for termlink_id hash (order-sensitive)
- Document current behavior

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 10: Get Subject Term Info - Multiple Term Links for Same Term
**Test Name**: `test_get_subject_term_info_multiple_termlinks_same_term`

**Setup**:
- Create test subject
- Create evidence with HP_0001249 and no qualifiers (termlink-A)
- Create evidence with HP_0001249 and qualifiers ["HP:0012823"] (termlink-B)

**Action**:
- Query for unqualified term link:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": []
  }
  ```
- Query for qualified term link:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": ["HP:0012823"]
  }
  ```

**Assertions**:
- Both queries return status 200
- First query returns:
  - termlink_id for unqualified term link
  - qualifiers: []
  - evidence_count from unqualified evidence
- Second query returns:
  - DIFFERENT termlink_id
  - qualifiers: ["HP:0012823"]
  - evidence_count from qualified evidence
- Two distinct term links correctly differentiated

**Cleanup**:
- Delete all evidence
- Delete subject

---

### Test 11: Get Subject Term Info - Verify Evidence Count Accuracy
**Test Name**: `test_get_subject_term_info_evidence_count_accuracy`

**Setup**:
- Create test subject
- Create 7 evidence records with same term and no qualifiers

**Action**:
- Get subject term info

**Assertions**:
- Response status code: 200
- Response contains:
  - `evidence_count: 7`
- Evidence count matches number of evidence records created
- Iceberg analytical table aggregation correct

**Cleanup**:
- Delete all evidence
- Delete subject

---

### Test 12: Get Subject Term Info - After Evidence Deletion
**Test Name**: `test_get_subject_term_info_after_evidence_deletion`

**Setup**:
- Create test subject
- Create 5 evidence records for term HP_0001249
- Get term info (verify evidence_count = 5)
- Delete 3 evidence records

**Action**:
- Get subject term info again

**Assertions**:
- Response status code: 200
- Response contains:
  - `evidence_count: 2` (decremented)
- Analytical table updated correctly after deletions
- Delete remaining 2 evidence records
- Get term info again:
  - Response status code: 404 (term link removed when evidence_count = 0)

**Cleanup**:
- Complete cleanup already done in test

---

### Test 13: Get Subject Term Info - Invalid term_iri Format
**Test Name**: `test_get_subject_term_info_invalid_term_iri`

**Setup**:
- Create test subject

**Action**:
- Invoke lambda with malformed term_iri:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "not-a-valid-iri"
  }
  ```

**Assertions**:
- Response status code: 404 (term not found)
- Malformed IRI treated as non-existent term
- No server errors

**Cleanup**:
- Delete subject

---

### Test 14: Get Subject Term Info - Null Qualifiers
**Test Name**: `test_get_subject_term_info_null_qualifiers`

**Setup**:
- Create test subject
- Create evidence without qualifiers

**Action**:
- Query with explicit null qualifiers:
  ```json
  {
    "subject_id": "test-subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "qualifiers": null
  }
  ```

**Assertions**:
- Response status code: 200
- null qualifiers treated as empty array
- Functional equivalence with omitting qualifiers field

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 15: Get Subject Term Info - Large Qualifiers Array
**Test Name**: `test_get_subject_term_info_large_qualifiers`

**Setup**:
- Create test subject
- Create evidence with 50-element qualifiers array

**Action**:
- Get subject term info with all 50 qualifiers

**Assertions**:
- Response status code: 200
- Response contains:
  - All 50 qualifiers in array
  - Correct termlink_id (hash computation handles large arrays)
- No truncation of qualifiers

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 16: Get Subject Term Info - Iceberg Query Failure
**Test Name**: `test_get_subject_term_info_iceberg_failure`

**Setup**:
- Create test subject
- Simulate Iceberg/Athena query failure

**Action**:
- Get subject term info

**Assertions**:
- Response status code: 500
- Response contains:
  - `message: "Internal server error: {error_details}"`
- Error logged to CloudWatch
- Graceful error handling

**Cleanup**:
- Restore Iceberg permissions
- Delete subject

---

### Test 17: Get Subject Term Info - Verify termlink_id Consistency
**Test Name**: `test_get_subject_term_info_termlink_id_consistency`

**Setup**:
- Create test subject
- Create evidence with specific term and qualifiers
- Manually compute expected termlink_id hash

**Action**:
- Get subject term info

**Assertions**:
- Response status code: 200
- Response termlink_id matches manually computed hash
- Hash algorithm consistent with:
  - Hash of (subject_iri, term_iri, qualifiers)
  - Same hash used in evidence creation and Neptune term links

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 18: Get Subject Term Info - Response Time Performance
**Test Name**: `test_get_subject_term_info_performance`

**Setup**:
- Create test subject
- Create evidence for term

**Action**:
- Get subject term info
- Measure response time

**Assertions**:
- Response status code: 200
- Performance metrics:
  - Response time < 3 seconds
  - Iceberg query optimized for single term link lookup
- No timeouts

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 19: Get Subject Term Info - Unicode in term_iri
**Test Name**: `test_get_subject_term_info_unicode_term_iri`

**Setup**:
- Create test subject
- Create evidence with term IRI containing unicode (if supported)

**Action**:
- Get subject term info with unicode term IRI

**Assertions**:
- Response status code: 200 or 404
- If supported:
  - Term info retrieved correctly
  - Unicode handled properly
- Document unicode support in term IRIs

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 20: Get Subject Term Info - Concurrent Queries
**Test Name**: `test_get_subject_term_info_concurrent`

**Setup**:
- Create test subject
- Create evidence for 5 different terms

**Action**:
- Query for all 5 term links concurrently (5 parallel lambda invocations)

**Assertions**:
- All 5 queries return status 200
- Each returns correct term info for its specific term
- No data mixing between concurrent queries
- Concurrent reads perform correctly

**Cleanup**:
- Delete all evidence
- Delete subject

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Empty String Parameters**: Test with empty strings for subject_id or term_iri

2. **Whitespace in Parameters**: Test with leading/trailing whitespace

3. **Very Long term_iri**: Test with 2KB+ term IRI string

4. **Malformed Qualifiers Array**: Test with qualifiers as string instead of array

5. **Qualifiers with Non-String Elements**: Test with numeric or object elements in qualifiers array

6. **Duplicate Qualifiers**: Test with ["HP:0012823", "HP:0012823"] in qualifiers

7. **Case Sensitivity**: Test if term_iri is case-sensitive

8. **Extra Fields in Request**: Test if extra parameters ignored gracefully

9. **Lambda Cold Start Performance**: Measure first invocation vs warm invocations

10. **Athena Query Timeout**: Test behavior if Iceberg query exceeds timeout

11. **Get After Subject Deletion**: Query term info for deleted subject

12. **Analytical Table Out of Sync**: Test when analytical table not updated after evidence changes

13. **Term Info for Term with Zero Evidence**: Test if term link exists but evidence_count = 0

14. **Cross-Region Read**: If multi-region, test reading from replicated Iceberg table

15. **Authorization**: Test that unauthorized users cannot read term info

16. **Audit Logging**: Verify term info reads logged for compliance

17. **Pagination**: Test if large numbers of qualifiers trigger pagination issues

18. **Qualifier IRI vs Short Name**: Test if qualifiers must match format used in evidence creation

19. **Get Term Info Immediately After Evidence Creation**: Test eventual consistency timing

20. **Subject with Multiple Projects**: Verify term info aggregates across all project linkages
