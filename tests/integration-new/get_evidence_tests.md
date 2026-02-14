# Integration Test Plan: get_evidence.py

## Lambda: get_evidence

**Purpose**: Retrieves a single evidence record by its unique evidence_id. This is a simple read operation that queries the Iceberg evidence table.

**Dependencies**:
- **Iceberg/Athena** - queries the evidence table for the specified evidence_id
- **PyIceberg** - Python library for reading from Iceberg tables
- No dependencies on Neptune, DynamoDB, S3, or EventBridge

**Key Operations**:
- Queries Iceberg evidence table by evidence_id (primary key)
- Returns complete evidence record including all fields
- Returns 404 if evidence not found

---

## Integration Test Scenarios

### Test 1: Get Evidence Record (Happy Path)
**Test Name**: `test_get_evidence_success`

**Setup**:
- Create test project
- Create test subject
- Create evidence record using create_evidence lambda
- Capture returned evidence_id

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "evidence_id": "{captured_evidence_id}"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains complete evidence record:
  - `evidence_id` matches requested ID
  - `subject_id` matches original
  - `term_iri` matches original
  - `creator` object with creator_id, creator_type, creator_name
  - `evidence_type` present
  - `created_timestamp` present
  - `termlink_id` present
- All fields from creation preserved
- Data integrity: matches original creation payload

**Cleanup**:
- Delete evidence record
- Delete subject and project

---

### Test 2: Get Evidence with All Optional Fields
**Test Name**: `test_get_evidence_full_fields`

**Setup**:
- Create test subject
- Create evidence with all optional fields populated:
  - run_id, batch_id, encounter_id, clinical_note_id
  - span_start, span_end, qualifiers
  - note_timestamp, provider_type, author_specialty, note_type
  - term_source

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains ALL fields:
  - `run_id` present and matches
  - `batch_id` present and matches
  - `encounter_id` present and matches
  - `clinical_note_id` present and matches
  - `span_start` and `span_end` are integers
  - `qualifiers` array populated
  - `note_timestamp` formatted correctly
  - `provider_type`, `author_specialty`, `note_type` strings present
  - `term_source` struct with source, version, iri
- Field types correct (strings, integers, arrays, structs)

**Cleanup**:
- Delete evidence

---

### Test 3: Get Evidence - Not Found
**Test Name**: `test_get_evidence_not_found`

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
- No data returned
- Error is user-friendly

**Cleanup**:
- None required

---

### Test 4: Missing Required Field - evidence_id
**Test Name**: `test_get_evidence_missing_evidence_id`

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
- Error clearly indicates missing parameter

**Cleanup**:
- None required

---

### Test 5: Get Evidence with Null evidence_id
**Test Name**: `test_get_evidence_null_evidence_id`

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

### Test 6: Get Evidence with Empty String evidence_id
**Test Name**: `test_get_evidence_empty_string_evidence_id`

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
- If 404: Empty string treated as non-existent evidence_id
- Document current behavior

**Cleanup**:
- None required

---

### Test 7: Get Evidence - Verify Data Immutability
**Test Name**: `test_get_evidence_immutability`

**Setup**:
- Create evidence record
- Get evidence (first retrieval)
- Wait 5 seconds
- Get evidence again (second retrieval)

**Action**:
- Compare both retrievals

**Assertions**:
- Both retrievals return status 200
- evidence_id identical in both
- created_timestamp identical in both (immutable)
- All fields identical across retrievals
- Evidence records are immutable (no updates)

**Cleanup**:
- Delete evidence

---

### Test 8: Get Evidence - Recent Creation (Eventual Consistency)
**Test Name**: `test_get_evidence_eventual_consistency`

**Setup**:
- Create evidence record

**Action**:
- Immediately get evidence (within 1 second of creation)

**Assertions**:
- Response status code: 200 (or 404 if eventual consistency delay)
- If 200: Evidence immediately readable
- If 404: Retry with exponential backoff up to 30 seconds
- Document read-after-write consistency behavior
- Iceberg/Athena typically consistent but may have slight delay

**Cleanup**:
- Delete evidence

---

### Test 9: Get Evidence with Qualifiers
**Test Name**: `test_get_evidence_with_qualifiers`

**Setup**:
- Create evidence with qualifiers array:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "qualifiers": ["HP:0012823", "HP:0003577"]
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - `qualifiers` array with 2 elements
  - Qualifiers match original ["HP:0012823", "HP:0003577"]
  - Array order preserved
- Qualifiers stored and retrieved correctly

**Cleanup**:
- Delete evidence

---

### Test 10: Get Evidence with Empty Qualifiers Array
**Test Name**: `test_get_evidence_empty_qualifiers`

**Setup**:
- Create evidence with empty qualifiers:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "qualifiers": []
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - `qualifiers` field as empty array [] or omitted
- Empty qualifiers handled correctly

**Cleanup**:
- Delete evidence

---

### Test 11: Get Evidence - Creator Object Structure
**Test Name**: `test_get_evidence_creator_structure`

**Setup**:
- Create evidence with full creator info:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-456",
    "creator_name": "Dr. Jane Smith",
    "creator_type": "human"
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - `creator` object (not flat fields)
  - `creator.creator_id: "user-456"`
  - `creator.creator_name: "Dr. Jane Smith"`
  - `creator.creator_type: "human"`
- Creator structured as nested object

**Cleanup**:
- Delete evidence

---

### Test 12: Get Evidence - term_source Structure
**Test Name**: `test_get_evidence_term_source_structure`

**Setup**:
- Create evidence with term_source:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "term_source": {
      "source": "hpo",
      "version": "2024-01-01",
      "iri": "http://purl.obolibrary.org/obo/hp.owl"
    }
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - `term_source` object
  - `term_source.source: "hpo"`
  - `term_source.version: "2024-01-01"`
  - `term_source.iri: "http://purl.obolibrary.org/obo/hp.owl"`
- term_source structured correctly as nested object

**Cleanup**:
- Delete evidence

---

### Test 13: Get Evidence Without term_source
**Test Name**: `test_get_evidence_no_term_source`

**Setup**:
- Create evidence WITHOUT term_source (optional field):
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123"
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response:
  - `term_source` field missing or null
  - Optional field correctly omitted
- No error when optional field absent

**Cleanup**:
- Delete evidence

---

### Test 14: Get Evidence - Malformed evidence_id
**Test Name**: `test_get_evidence_malformed_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with malformed evidence_id:
  ```json
  {
    "evidence_id": "not-a-valid-uuid-format"
  }
  ```

**Assertions**:
- Response status code: 404 (evidence not found)
- Response message: "Evidence not found"
- Malformed ID treated as non-existent
- No server errors (graceful handling)

**Cleanup**:
- None required

---

### Test 15: Get Evidence - Very Long evidence_id
**Test Name**: `test_get_evidence_very_long_id`

**Setup**:
- No prerequisites

**Action**:
- Invoke lambda with 1KB+ evidence_id string

**Assertions**:
- Response status code: 404 or 400
- Graceful handling of invalid input
- No server crashes or timeouts
- Lambda payload size limits respected

**Cleanup**:
- None required

---

### Test 16: Get Evidence - Verify termlink_id Computation
**Test Name**: `test_get_evidence_termlink_id_consistency`

**Setup**:
- Create evidence with specific subject_id, term_iri, qualifiers
- Manually compute expected termlink_id hash

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - `termlink_id` field
  - termlink_id matches manually computed hash
  - Hash consistent with (subject_iri, term_iri, qualifiers)
- Verify hash algorithm matches expectations

**Cleanup**:
- Delete evidence

---

### Test 17: Iceberg Query Failure
**Test Name**: `test_get_evidence_iceberg_failure`

**Setup**:
- Simulate Iceberg/Athena query failure (e.g., permissions issue)

**Action**:
- Invoke lambda to get evidence

**Assertions**:
- Response status code: 500
- Response contains:
  - `message: "Internal server error: {error_details}"`
- Error logged to CloudWatch
- Graceful error handling (no stack traces to user)

**Cleanup**:
- Restore Iceberg permissions

---

### Test 18: Get Evidence - Performance with Large Record
**Test Name**: `test_get_evidence_large_record_performance`

**Setup**:
- Create evidence with large payload:
  - 50-element qualifiers array
  - Long clinical_note_id (10KB)
  - All optional fields populated

**Action**:
- Get evidence by evidence_id
- Measure response time

**Assertions**:
- Response status code: 200
- Response contains all fields correctly
- Performance metrics:
  - Response time < 5 seconds (acceptable for large record)
  - No timeouts
  - Complete record returned

**Cleanup**:
- Delete evidence

---

### Test 19: Get Evidence - Unicode in Fields
**Test Name**: `test_get_evidence_unicode_fields`

**Setup**:
- Create evidence with Unicode characters:
  ```json
  {
    "subject_id": "subject-uuid",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
    "creator_id": "user-123",
    "creator_name": "医生 Доктор Dr. Smith"
  }
  ```

**Action**:
- Get evidence by evidence_id

**Assertions**:
- Response status code: 200
- Response contains:
  - creator_name with Unicode characters intact: "医生 Доктор Dr. Smith"
  - No encoding issues
  - UTF-8 handling correct

**Cleanup**:
- Delete evidence

---

### Test 20: Get Multiple Evidence Records Sequentially
**Test Name**: `test_get_evidence_multiple_sequential`

**Setup**:
- Create 10 different evidence records
- Capture all evidence_ids

**Action**:
- Get each evidence record sequentially

**Assertions**:
- All 10 gets return status 200
- Each returns correct evidence matching its evidence_id
- No data mixing between evidence records
- All evidence_ids unique
- All data integrity maintained

**Cleanup**:
- Delete all 10 evidence records

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Concurrent Gets of Same Evidence**: Test 100 concurrent reads of same evidence_id

2. **Get After Deletion**: Create evidence, delete it, try to get it (should be 404)

3. **Special Characters in evidence_id**: Test with URL-encoded characters

4. **Case Sensitivity**: Test if evidence_id is case-sensitive

5. **Whitespace in evidence_id**: Test with leading/trailing whitespace

6. **Extra Fields in Request**: Test if extra parameters ignored gracefully

7. **Lambda Cold Start Performance**: Measure first invocation vs warm invocations

8. **Athena Query Timeout**: Test behavior if Iceberg query exceeds timeout

9. **Partial Record Retrieval**: Verify all fields returned (no truncation)

10. **Read from Iceberg Snapshot**: Test read consistency with Iceberg table versions

11. **Cross-Region Read**: If multi-region, test reading from replicated Iceberg table

12. **Evidence Created in Different Account/Region**: Test isolation

13. **Payload Size Limits**: Test with evidence_id in oversized request body

14. **Malformed JSON Request**: Test lambda's handling of invalid JSON

15. **Authorization**: Test that unauthorized users cannot read evidence

16. **Rate Limiting**: Test behavior under high request rate (1000+ req/sec)

17. **Caching**: Document if any caching layer exists for evidence retrieval

18. **Audit Logging**: Verify evidence reads are logged for compliance

19. **Evidence Record Versioning**: If evidence has versions, test retrieval of specific version

20. **Deleted Subject's Evidence**: Test retrieving evidence for deleted subject (evidence should still exist as immutable record)
