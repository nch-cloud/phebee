# Integration Test Plan: get_subjects_pheno.py

## Lambda Metadata

**Purpose**: Query subjects by project with optional term filtering, return as JSON or phenopackets, with compression.

**Dependencies**:
- Iceberg: Query subject_terms analytical tables
- DynamoDB: Get term source versions
- Output formats: JSON, phenopacket, phenopacket_zip

**Key Operations**:
- Queries subjects by project_id
- Optional filtering by term_iri with hierarchy (include_child_terms)
- Optional qualifier filtering (include_qualified)
- Converts to phenopacket format if requested
- Compresses response with gzip
- Supports S3 output or direct return

---

## Integration Test Scenarios

### Test 1: test_get_subjects_all_in_project (project_id only, returns all subjects)
### Test 2: test_get_subjects_filtered_by_term (term_iri + project_id, returns subjects with that term)
### Test 3: test_get_subjects_include_child_terms (term_iri with hierarchy, includes descendant terms)
### Test 4: test_get_subjects_exclude_child_terms (include_child_terms=false, exact term only)
### Test 5: test_get_subjects_exclude_qualified (include_qualified=false, filters out negated terms)
### Test 6: test_get_subjects_include_qualified (include_qualified=true, includes negated)
### Test 7: test_get_subjects_pagination (limit=100, cursor for next page)
### Test 8: test_get_subjects_json_output (output_type=json, returns JSON)
### Test 9: test_get_subjects_phenopacket_output (output_type=phenopacket, converts to phenopacket format)
### Test 10: test_get_subjects_phenopacket_zip_output (output_type=phenopacket_zip, returns zip file)
### Test 11: test_get_subjects_gzip_compression (Response has Content-Encoding: gzip, compressed)
### Test 12: test_get_subjects_s3_output (output_s3_path provided, writes to S3, returns S3 path)
### Test 13: test_get_subjects_direct_output (No S3 path, returns base64-encoded gzipped body)
### Test 14: test_get_subjects_missing_project_id (project_id not provided, error)
### Test 15: test_get_subjects_root_term_rejected (HP:0000001 with include_child_terms=true, error to prevent full scan)
### Test 16: test_get_subjects_term_id_conversion (term_iri converted to term_id for Iceberg query)
### Test 17: test_get_subjects_project_subject_ids_filter (Filter by list of project_subject_ids)
### Test 18: test_get_subjects_term_source_version (Uses current HPO/MONDO versions from DynamoDB)
### Test 19: test_get_subjects_pagination_info (Response includes pagination: total, has_more, next_cursor)
### Test 20: test_get_subjects_performance (Query 1000 subjects in <3s)

---

## Additional Test Scenarios from Existing Integration Tests

### Test 21: Project Query Returns All Subjects
**Test Name**: `test_get_subjects_project_query_all_subjects`

**Setup**:
- Create test project
- Create 4 test subjects using CreateSubjectFunction:
  - subject_a, subject_b, subject_c, subject_d

**Action**:
- Query with only project_id (no filters):
  ```json
  {
    "project_id": "test-project"
  }
  ```

**Assertions**:
- Response status code: 200
- Response body contains all 4 subjects
- Each subject has project_subject_iri field
- Extracted project_subject_iris match expected set
- No filtering applied - returns complete project cohort

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_project_query` (lines 56-101) verifies basic project-level querying without filters. This is the most common query pattern and must work reliably.

---

### Test 22: Query with Specific project_subject_ids Filter
**Test Name**: `test_get_subjects_project_subject_ids_filter`

**Setup**:
- Create test project
- Create subjects: subject_a, subject_b, subject_c, subject_d

**Action**:
- Query with project_subject_ids filter:
  ```json
  {
    "project_id": "test-project",
    "project_subject_ids": ["subject_b", "subject_c"]
  }
  ```

**Assertions**:
- Response status code: 200
- Response returns exactly 2 subjects
- Subject IRIs end with "subject_b" and "subject_c"
- Other subjects (subject_a, subject_d) NOT returned
- Filter correctly restricts to specified subjects

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_subject_specific_query` (lines 108-158) and `test_query_subjects.py::test_project_subject_ids_filter` (lines 521-563) verify the project_subject_ids filter works correctly. This is critical for targeted queries of specific patient cohorts.

---

### Test 23: Term Filtering with term_iri Parameter
**Test Name**: `test_get_subjects_term_filtering_basic`

**Setup**:
- Create test project
- Create test subjects

**Action**:
- Query with specific term_iri:
  ```json
  {
    "project_id": "test-project",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
    "term_source": "hpo",
    "include_phenotypes": true
  }
  ```

**Assertions**:
- Response status code: 200
- Response contains pagination metadata
- Response handles case when no subjects match term (returns empty results, not error)
- Term filtering mechanism works correctly

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_term_filtering_basic` (lines 189-253) verifies term_iri parameter handling and graceful handling of empty result sets.

---

### Test 24: Term Filtering with include_descendants Parameter
**Test Name**: `test_get_subjects_include_descendants`

**Setup**:
- Create test project
- Create subjects
- Requires HPO hierarchy data loaded

**Action**:
- Query parent term with include_descendants=True:
  ```json
  {
    "project_id": "test-project",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001332",
    "term_source": "hpo",
    "include_descendants": true,
    "include_phenotypes": true
  }
  ```

**Assertions**:
- Response status code: 200
- Response body is list format
- Query executes successfully (uses hierarchy table)
- Returns subjects with parent term OR any descendant term

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_term_filtering_basic` (lines 218-236) verifies hierarchy-based querying. This is critical for semantic search across the ontology.

---

### Test 25: Pagination with limit Parameter
**Test Name**: `test_get_subjects_pagination_basic`

**Setup**:
- Create test project
- Create 4 test subjects

**Action**:
- Query with limit=2:
  ```json
  {
    "project_id": "test-project",
    "limit": 2
  }
  ```

**Assertions**:
- Response status code: 200
- Response contains exactly 2 subjects (respects limit)
- Pagination metadata includes:
  - `limit: 2`
  - `has_more: true` (indicates more results available)
- Can fetch more pages using cursor

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_pagination_basic` (lines 257-288) verifies basic pagination controls. This is essential for handling large result sets.

---

### Test 26: Pagination with Cursor (Multi-Page)
**Test Name**: `test_get_subjects_pagination_cursor`

**Setup**:
- Create test project
- Create 4 test subjects

**Action**:
- First page: query with limit=2
- Second page: query with limit=2 and cursor from first page
- Continue until has_more=false

**Assertions**:
- Response status code: 200 for all pages
- Each page returns up to 2 subjects
- Pagination metadata correct:
  - `has_more: true` when more pages exist
  - `next_cursor` provided when has_more=true
  - `has_more: false` on final page
- All subjects retrieved across pages (no duplicates, no missing)
- Total subjects match expected count

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_pagination_cursor` (lines 292-360) verifies cursor-based pagination across multiple pages. This ensures complete and consistent result set retrieval.

---

### Test 27: Empty Cursor Handling
**Test Name**: `test_get_subjects_pagination_empty_cursor`

**Setup**:
- Create test project
- Create test subjects

**Action**:
- Query with empty cursor string:
  ```json
  {
    "project_id": "test-project",
    "limit": 2,
    "cursor": ""
  }
  ```

**Assertions**:
- Response status code: 200
- Handles gracefully (treats as first page)
- Returns results normally
- Pagination metadata present

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_pagination_empty_cursor` (lines 364-387) verifies graceful handling of empty/invalid cursor values.

---

### Test 28: Term Labels in Response
**Test Name**: `test_get_subjects_term_labels_functionality`

**Setup**:
- Create test project
- Create subjects with evidence
- Requires ontology data loaded (HPO)

**Action**:
- Query with include_phenotypes=True:
  ```json
  {
    "project_id": "test-project",
    "include_phenotypes": true
  }
  ```

**Assertions**:
- Response status code: 200
- Response includes subjects with term_links field
- term_links may be empty list if no terms
- If terms exist, each has term_label field populated from ontology

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_term_labels_functionality` (lines 391-417) verifies term label enrichment from ontology. This provides human-readable labels for phenotype terms.

---

### Test 29: include_qualified Parameter (Default Excludes Negated/Family/Hypothetical)
**Test Name**: `test_get_subjects_include_qualified_default`

**Setup**:
- Create test project
- Create subjects with qualified and unqualified evidence

**Action**:
- Query without include_qualified parameter (default behavior):
  ```json
  {
    "project_id": "test-project",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
    "term_source": "hpo",
    "include_phenotypes": true
  }
  ```

**Assertions**:
- Response status code: 200
- By default, excludes subjects with:
  - negated qualifiers
  - family qualifiers
  - hypothetical qualifiers
- Only returns subjects with confirmed/unqualified phenotypes

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_include_qualified_parameter` (lines 421-469) verifies that the default behavior excludes qualified annotations, which is the expected clinical use case (confirmed phenotypes only).

---

### Test 30: include_qualified=True Includes All Qualifiers
**Test Name**: `test_get_subjects_include_qualified_true`

**Setup**:
- Create test project
- Create subjects with various qualifiers

**Action**:
- Query with include_qualified=True:
  ```json
  {
    "project_id": "test-project",
    "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
    "term_source": "hpo",
    "include_phenotypes": true,
    "include_qualified": true
  }
  ```

**Assertions**:
- Response status code: 200
- Returns subjects with ANY qualifier type (negated, family, hypothetical)
- Result count >= default query (includes qualified subjects)
- Allows research use cases that need all annotations

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_include_qualified_parameter` (lines 447-469) and `test_include_qualified_with_direct_subjects` (lines 473-517) verify that include_qualified=true returns the complete dataset including qualified annotations.

---

### Test 31: Empty Result Sets (Non-Existent Term)
**Test Name**: `test_get_subjects_empty_result_set`

**Setup**:
- Create test project

**Action**:
- Query with non-existent term:
  ```json
  {
    "project_id": "test-project",
    "term_iri": "http://purl.obolibrary.org/obo/HP_9999999",
    "term_source": "hpo"
  }
  ```

**Assertions**:
- Response status code: 200 (not 404)
- Response body is empty list: []
- n_subjects: 0
- Graceful handling of no matches

**Cleanup**:
- Delete project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_empty_result_sets` (lines 567-605) verifies graceful handling when queries return no results, preventing errors in downstream consumers.

---

### Test 32: Empty Result Set (Non-Existent project_subject_ids)
**Test Name**: `test_get_subjects_empty_result_project_subjects`

**Setup**:
- Create test project

**Action**:
- Query with non-existent project_subject_ids:
  ```json
  {
    "project_id": "test-project",
    "project_subject_ids": ["non_existent_1", "non_existent_2"]
  }
  ```

**Assertions**:
- Response status code: 200
- Response body is empty list: []
- n_subjects: 0
- No errors thrown

**Cleanup**:
- Delete project

**Why this test is important**: The existing integration test `test_query_subjects.py::test_empty_result_sets` (lines 590-605) verifies graceful handling when filtered subject lists return no matches.

---

### Test 33: Cache vs Neptune Performance Comparison
**Test Name**: `test_get_subjects_cache_vs_neptune`

**Setup**:
- Create test project with subjects and evidence
- Ensure analytical tables populated (cache)

**Action**:
- Query with _use_cache=false (Neptune path):
  ```json
  {
    "project_id": "test-project",
    "_use_cache": false
  }
  ```
- Query with _use_cache=true (default, Iceberg cache path)

**Assertions**:
- Both queries return status 200
- Results are consistent between Neptune and cache
- Cache query significantly faster than Neptune
- Correctness verified across both paths

**Cleanup**:
- Delete subjects and project

**Why this test is important**: The existing integration test `test_cache_vs_neptune.py` verifies that the Iceberg analytical table cache produces identical results to Neptune queries while providing better performance. This validates the cache materialization correctness.

---
