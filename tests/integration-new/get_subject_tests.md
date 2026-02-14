# Integration Test Plan: get_subject.py

## Lambda: get_subject

**Purpose**: Retrieves subject information including all associated phenotypic terms and their evidence counts. Supports two query modes: by project-specific identifier (project_subject_iri) or by internal subject identifier (subject_id). The lambda aggregates data from the Iceberg analytical tables to provide a complete view of a subject's phenotypic profile.

**Dependencies**:
- **Iceberg/Athena** - queries `subject_terms_by_subject` analytical table for term aggregations
- **DynamoDB** - maps project_subject_iri to internal subject_id when querying by project identifier
- **PyIceberg** - Python library for reading from Iceberg tables
- No dependencies on Neptune, S3, or EventBridge

**Key Operations**:
- Accepts either `project_subject_iri` or `subject_id` parameter
- If project_subject_iri provided: looks up subject_id from DynamoDB mapping
- Queries Iceberg analytical table for subject's terms
- Returns subject with terms array containing term_iri, qualifiers, evidence_count for each term link
- Returns 404 if subject not found or has no terms

---

## Integration Test Scenarios

### Test 1: Get Subject by subject_id (Happy Path)
**Test Name**: `test_get_subject_by_id_success`

**Setup**:
- Create test project
- Create test subject (capture subject_id)
- Create 3 evidence records for subject with different terms:
  - HP_0001249 (2 evidence records)
  - HP_0002297 (1 evidence record)

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "subject_id": "{captured_subject_id}"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_iri: "http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"`
  - `subject_id` matches requested ID
  - `terms` array with 2 elements
- Terms array verification:
  - First term: `term_iri: "http://purl.obolibrary.org/obo/HP_0001249"`, `evidence_count: 2`
  - Second term: `term_iri: "http://purl.obolibrary.org/obo/HP_0002297"`, `evidence_count: 1`
  - Each term has `termlink_id` field
  - Qualifiers array present (empty or populated)
- Iceberg verification:
  - Query matches data in `subject_terms_by_subject` table
  - Aggregated evidence counts correct

**Cleanup**:
- Delete all evidence records
- Delete subject and project

---

### Test 2: Get Subject by project_subject_iri (Happy Path)
**Test Name**: `test_get_subject_by_project_iri_success`

**Setup**:
- Create test project with project_id "test-project-001"
- Create test subject with project_subject_id "patient-001"
- Create evidence for subject

**Action**:
- Invoke lambda with payload:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project-001/patient-001"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains:
  - `subject_iri` (internal subject IRI)
  - `subject_id` (UUID)
  - `project_subject_iri` (echoed back in response)
  - `terms` array with term data
- DynamoDB verification:
  - Lookup of PROJECT#test-project-001 / SUBJECT#patient-001 → subject_id succeeded
- Iceberg verification:
  - Terms queried using resolved subject_id

**Cleanup**:
- Delete evidence
- Delete subject and project

---

### Test 3: Get Subject with No Terms (Empty Subject)
**Test Name**: `test_get_subject_no_terms`

**Setup**:
- Create test project
- Create test subject
- Do NOT create any evidence for subject

**Action**:
- Get subject by subject_id

**Assertions**:
- Response status code: 404 or 200 (depending on implementation)
- If 404:
  - Response message: "Subject not found"
  - Subject with no terms treated as non-existent
- If 200:
  - Response contains:
    - `subject_iri` and `subject_id`
    - `terms: []` (empty array)
- Document current behavior

**Cleanup**:
- Delete subject and project

---

### Test 4: Get Subject - Subject Does Not Exist (by subject_id)
**Test Name**: `test_get_subject_by_id_not_found`

**Setup**:
- Generate random UUID that does not exist as subject_id

**Action**:
- Invoke lambda with non-existent subject_id:
  ```json
  {
    "subject_id": "nonexistent-uuid-12345"
  }
  ```

**Assertions**:
- Response status code: 404
- Response body contains:
  - `message: "Subject not found"`
- No subject data returned

**Cleanup**:
- None required

---

### Test 5: Get Subject - Subject Does Not Exist (by project_subject_iri)
**Test Name**: `test_get_subject_by_project_iri_not_found`

**Setup**:
- Create test project
- Ensure subject with project_subject_id "nonexistent-patient" does NOT exist

**Action**:
- Invoke lambda with non-existent project_subject_iri:
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project-001/nonexistent-patient"
  }
  ```

**Assertions**:
- Response status code: 404
- Response message: "Subject not found"
- DynamoDB verification:
  - Lookup returns no mapping for this project + project_subject_id

**Cleanup**:
- Delete project

---

### Test 6: Missing Both Parameters
**Test Name**: `test_get_subject_missing_both_parameters`

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
  - `message: "Must provide either project_subject_iri or subject_id"`
- Clear error message indicating required parameter

**Cleanup**:
- None required

---

### Test 7: Both Parameters Provided (Ambiguous)
**Test Name**: `test_get_subject_both_parameters_provided`

**Setup**:
- Create test subject

**Action**:
- Invoke lambda with BOTH parameters:
  ```json
  {
    "subject_id": "subject-uuid",
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project/patient"
  }
  ```

**Assertions**:
- Response status code: 200
- Response prioritizes one parameter over the other (document which)
- OR returns error indicating ambiguous request
- Document current behavior and precedence

**Cleanup**:
- Delete subject

---

### Test 8: Get Subject with Qualified Terms
**Test Name**: `test_get_subject_with_qualified_terms`

**Setup**:
- Create test subject
- Create evidence with qualifiers:
  - HP_0001249 with qualifiers ["HP:0012823"]
  - HP_0001249 without qualifiers
- This creates 2 distinct term links for same term

**Action**:
- Get subject by subject_id

**Assertions**:
- Response status code: 200
- Response terms array contains:
  - TWO entries for HP_0001249
  - One entry with empty qualifiers array, evidence_count = 1
  - One entry with qualifiers ["HP:0012823"], evidence_count = 1
  - Different termlink_ids for each
- Qualified and unqualified terms treated as distinct

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 9: Get Subject with Multiple Evidence per Term Link
**Test Name**: `test_get_subject_evidence_count_aggregation`

**Setup**:
- Create test subject
- Create 5 evidence records for same term (HP_0001249) without qualifiers

**Action**:
- Get subject by subject_id

**Assertions**:
- Response status code: 200
- Response terms array contains:
  - Single entry for HP_0001249
  - `evidence_count: 5`
- Evidence properly aggregated by term link

**Cleanup**:
- Delete all evidence
- Delete subject

---

### Test 10: Get Subject Linked to Multiple Projects
**Test Name**: `test_get_subject_multiple_projects`

**Setup**:
- Create project-A and project-B
- Create subject linked to both projects:
  - project-A: project_subject_id "patient-A-001"
  - project-B: project_subject_id "patient-B-001"
- Create evidence via each project reference

**Action**:
- Get subject by subject_id (internal ID)

**Assertions**:
- Response status code: 200
- Response contains:
  - Single subject_iri (same internal subject)
  - Terms aggregated across ALL projects
  - No duplication based on project linkages
- Get subject by project-A project_subject_iri:
  - Returns same subject_id
  - Response includes `project_subject_iri` for project-A
- Get subject by project-B project_subject_iri:
  - Returns same subject_id
  - Response includes `project_subject_iri` for project-B
- Both queries return identical terms (aggregated across projects)

**Cleanup**:
- Delete evidence
- Remove subject from both projects
- Delete both projects

---

### Test 11: Invalid project_subject_iri Format
**Test Name**: `test_get_subject_invalid_project_iri_format`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with malformed project_subject_iri:
  ```json
  {
    "project_subject_iri": "not-a-valid-iri-format"
  }
  ```

**Assertions**:
- Response status code: 404 or 400
- Response indicates error parsing IRI or subject not found
- Graceful error handling

**Cleanup**:
- Delete project

---

### Test 12: project_subject_iri with Missing Segments
**Test Name**: `test_get_subject_incomplete_project_iri`

**Setup**:
- Create test project

**Action**:
- Invoke lambda with incomplete IRI (missing project_subject_id):
  ```json
  {
    "project_subject_iri": "http://ods.nationwidechildrens.org/phebee/projects/test-project"
  }
  ```

**Assertions**:
- Response status code: 404 or 400
- Response indicates invalid IRI format or subject not found
- Error logged

**Cleanup**:
- Delete project

---

### Test 13: Get Subject - Project Exists but Subject Not in Project
**Test Name**: `test_get_subject_project_exists_subject_not_linked`

**Setup**:
- Create project-A
- Create subject linked to different project-B

**Action**:
- Try to get subject using project-A project_subject_iri

**Assertions**:
- Response status code: 404
- Response message: "Subject not found"
- DynamoDB lookup finds no mapping for project-A + project_subject_id

**Cleanup**:
- Delete subject and both projects

---

### Test 14: Get Subject - Verify Terms Ordering
**Test Name**: `test_get_subject_terms_ordering`

**Setup**:
- Create test subject
- Create evidence for 10 different terms in specific order

**Action**:
- Get subject by subject_id

**Assertions**:
- Response status code: 200
- Response terms array contains all 10 terms
- Document terms ordering:
  - Alphabetical by term_iri?
  - By evidence_count descending?
  - By creation timestamp?
- Consistent ordering across multiple retrievals

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 15: Get Subject - Subject in DynamoDB but No Terms in Iceberg
**Test Name**: `test_get_subject_dynamodb_only_no_iceberg`

**Setup**:
- Create test project
- Create test subject (DynamoDB mappings created)
- Do NOT create any evidence (no Iceberg data)
- Manually verify DynamoDB has mapping but Iceberg analytical table has no rows

**Action**:
- Get subject by project_subject_iri

**Assertions**:
- Response status code: 200 or 404
- If 200:
  - Response contains subject_iri and subject_id
  - `terms: []` (empty array)
  - DynamoDB lookup successful, Iceberg query returns empty
- If 404:
  - Subject without terms treated as non-existent
- Document behavior when subject exists but has no phenotypic data

**Cleanup**:
- Delete subject
- Delete project

---

### Test 16: Get Subject - Performance with Large Term List
**Test Name**: `test_get_subject_large_term_list_performance`

**Setup**:
- Create test subject
- Create evidence for 100 different terms (1 evidence per term)

**Action**:
- Get subject by subject_id
- Measure response time

**Assertions**:
- Response status code: 200
- Response contains:
  - All 100 terms in terms array
  - Correct evidence_count for each
- Performance metrics:
  - Response time < 10 seconds
  - No pagination issues
  - Complete data returned

**Cleanup**:
- Delete all evidence
- Delete subject

---

### Test 17: Get Subject - Iceberg Query Failure
**Test Name**: `test_get_subject_iceberg_failure`

**Setup**:
- Create test subject
- Simulate Iceberg/Athena query failure

**Action**:
- Get subject by subject_id

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

### Test 18: Get Subject - DynamoDB Lookup Failure
**Test Name**: `test_get_subject_dynamodb_failure`

**Setup**:
- Create test subject
- Simulate DynamoDB query failure

**Action**:
- Get subject by project_subject_iri

**Assertions**:
- Response status code: 500
- Response indicates internal error
- Error logged
- DynamoDB failure prevents subject_id resolution

**Cleanup**:
- Restore DynamoDB permissions
- Delete subject

---

### Test 19: Get Subject - Verify termlink_id Consistency
**Test Name**: `test_get_subject_termlink_id_consistency`

**Setup**:
- Create test subject
- Create evidence with specific term and qualifiers
- Manually compute expected termlink_id hash

**Action**:
- Get subject by subject_id

**Assertions**:
- Response status code: 200
- Response terms array:
  - Each term has `termlink_id` field
  - termlink_id matches hash of (subject_iri, term_iri, qualifiers)
  - Hash computation consistent with evidence creation

**Cleanup**:
- Delete evidence
- Delete subject

---

### Test 20: Get Subject After Evidence Deletion (Consistency Check)
**Test Name**: `test_get_subject_after_evidence_deletion`

**Setup**:
- Create test subject
- Create 3 evidence records for HP_0001249
- Get subject (verify evidence_count = 3)
- Delete 2 evidence records

**Action**:
- Get subject again after deletion

**Assertions**:
- Response status code: 200
- Response terms array:
  - HP_0001249 entry has `evidence_count: 1` (decremented)
  - Analytical table updated correctly
- After deleting last evidence record:
  - Get subject again
  - HP_0001249 no longer in terms array (removed when evidence_count reaches 0)

**Cleanup**:
- Delete remaining evidence
- Delete subject

---

## Edge Cases and Production Considerations

### Additional Scenarios to Consider:

1. **Null vs Empty String Parameters**: Test with explicit null or empty string values

2. **Special Characters in project_subject_id**: Test with unicode, spaces, special chars in IRI

3. **Very Long project_subject_iri**: Test with 2KB+ IRI string

4. **Case Sensitivity of subject_id**: Test if UUIDs case-sensitive

5. **Whitespace in Parameters**: Test with leading/trailing whitespace

6. **Subject with 1000+ Terms**: Test pagination or limits on terms array size

7. **Concurrent Gets of Same Subject**: Test 100 concurrent reads

8. **Get Subject Immediately After Creation**: Test eventual consistency timing

9. **Get Subject via Deleted Project**: Subject exists but project deleted

10. **Cross-Region Read**: If multi-region, test reading from replicated Iceberg table

11. **Iceberg Table Version Consistency**: Test reading from specific Iceberg snapshot

12. **Analytical Table Out of Sync**: Test when analytical table not updated after evidence creation

13. **Subject with Terms but No Evidence Records**: Test analytical table integrity

14. **Lambda Timeout with Large Query**: Test behavior with very large subject data

15. **Authorization**: Test unauthorized users cannot read subject data

16. **Audit Logging**: Verify subject reads logged for compliance

17. **Subject Deleted but Evidence Remains**: Test querying subject after remove_subject

18. **Multiple Qualifiers per Term**: Test terms with large qualifiers arrays

19. **Terms with Same term_iri but Different Qualifiers**: Verify distinct termlink_ids

20. **Project-Specific vs Global Subject View**: Document if any project-level filtering exists
