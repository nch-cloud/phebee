# Integration Test Implementation Status

## Overview

This document tracks the implementation status of the comprehensive integration test suite based on the test plans in this directory.

**Test Suite Location**: `/tests/integration-new/`
**Test Plans Total**: 43 (37 lambdas + 6 state machines)

---

## Core Infrastructure ✅ COMPLETE

### Fixtures ([conftest.py](conftest.py))

Four core fixtures have been implemented to standardize test patterns:

1. **`test_subject`** - Creates a test subject with automatic cleanup
   - Returns: `(subject_uuid: str, project_subject_iri: str)`
   - Usage: Most tests that need subjects for evidence/term links

2. **`create_evidence_helper`** - Standardizes evidence creation
   - Returns: Callable that creates evidence with sensible defaults
   - Usage: Tests that create evidence records

3. **`standard_hpo_terms`** (session-scoped) - Standard HPO terms for testing
   - Returns: Dict mapping term names to IRIs
   - Usage: Tests that need known ontology terms (hierarchy, queries)

4. **`query_athena`** - Execute Athena queries for verification
   - Returns: Callable that executes SQL and returns results
   - **Critical**: Primary verification method since direct Neptune access is unavailable

5. **`wait_for_subject_terms`** - Poll subject_terms tables until termlink appears
   - Returns: Callable that waits for termlink in both subject_terms tables
   - Usage: Tests that verify subject_terms table synchronization after evidence creation

### Inherited Fixtures (from `/tests/integration/conftest.py`)

- **`test_project_id`** (function-scoped) - Unique project per test
- **`physical_resources`** (session-scoped) - CloudFormation outputs
- **`update_hpo`** (session-scoped) - Ensures HPO loaded
- **`update_mondo`**, **`update_eco`** - Other ontologies
- **`cloudformation_stack`** - Stack name

---

## Implemented Test Files

### ✅ 1. create_project ([test_create_project.py](test_create_project.py))

**Status**: Complete - 11 tests implemented
**Test Plan**: [create_project_tests.md](create_project_tests.md)

**Tests Implemented**:
1. ✅ `test_create_project_success` - Happy path
2. ✅ `test_create_project_already_exists` - Idempotency
3. ✅ `test_create_project_missing_project_id` - Validation
4. ✅ `test_create_project_missing_project_label` - Validation
5. ✅ `test_create_project_missing_both_fields` - Validation
6. ✅ `test_create_project_special_characters` - Edge case
7. ✅ `test_create_project_unicode_label` - Unicode support
8. ✅ `test_create_project_long_id` - Boundary testing
9. ✅ `test_create_project_empty_strings` - Validation
10. ✅ `test_create_project_concurrent` - Race conditions
11. ✅ `test_create_multiple_projects` - Bulk operations

**Tests Not Implemented**:
- Neptune unavailability test - Not feasible in integration tests (see "Infrastructure Failure Testing" below)

**Key Implementation Decisions**:
- **Neptune-free verification**: Uses `verify_project_exists()` helper that attempts to create a subject in the project as indirect verification
- **Concurrent testing**: Uses `ThreadPoolExecutor` to simulate race conditions
- **Cleanup**: All tests clean up via `remove_project()` in finally blocks

**Verification Strategy**:
```python
# Since Neptune direct access unavailable, verify indirectly:
def verify_project_exists(project_id, physical_resources):
    # Try to create a subject in the project
    # If project exists → subject creation succeeds
    # If project missing → subject creation fails
    return subject_creation_succeeded
```

---

### ✅ 2. create_subject ([test_create_subject.py](test_create_subject.py))

**Status**: Complete - 14 tests implemented
**Test Plan**: [create_subject_tests.md](create_subject_tests.md)

**Tests Implemented**:
1. ✅ `test_create_subject_new` - Happy path new subject creation
2. ✅ `test_create_subject_project_not_found` - Validation error
3. ✅ `test_create_subject_link_via_known_iri` - Link via known_subject_iri
4. ✅ `test_create_subject_link_via_known_project` - Link via known_project_id + known_project_subject_id
5. ✅ `test_create_subject_invalid_known_project_missing_subject` - Validation
6. ✅ `test_create_subject_invalid_both_known_references` - Validation
7. ✅ `test_create_subject_duplicate_idempotent` - Idempotency
8. ✅ `test_create_subject_race_condition` - Race condition with concurrent creates
9. ✅ `test_create_subject_missing_project_id` - Validation
10. ✅ `test_create_subject_missing_project_subject_id` - Validation
11. ✅ `test_create_subject_known_iri_not_found` - Error handling
12. ✅ `test_create_subject_known_project_mapping_not_found` - Error handling
13. ✅ `test_create_subject_invalid_iri_format` - Validation
14. ✅ `test_create_subject_conflict_project_subject_id_taken` - Conflict detection
15. ⏭️ Test 14 from plan - Link to same project with different project_subject_id (edge case)
16. ⏭️ Tests 16-17 from plan - Infrastructure failure tests (can't reliably simulate)
17. ⏭️ Tests 18-20 from plan - EventBridge event verification (requires SQS setup)

**Key Implementation Decisions**:
- **DynamoDB verification**: Uses helper functions to query DynamoDB mappings directly
  - `get_subject_mapping_from_dynamodb()` - Query forward mapping (PROJECT# → subject_id)
  - `verify_subject_has_project_mapping()` - Query reverse mapping (SUBJECT# → PROJECT#)
  - `count_subject_project_mappings()` - Count total project linkages for a subject
- **Race condition testing**: Uses `ThreadPoolExecutor` to test conditional DynamoDB writes
- **Environment setup**: Added `setup_environment` autouse fixture to set `PheBeeDynamoTable` env var
- **Multi-project verification**: Tests verify subjects can be linked to multiple projects correctly

**Verification Strategy**:
```python
# Verify DynamoDB bidirectional mappings
forward_mapping = get_subject_mapping_from_dynamodb(project_id, project_subject_id)
assert forward_mapping == subject_uuid

reverse_mapping_exists = verify_subject_has_project_mapping(
    subject_uuid, project_id, project_subject_id
)
assert reverse_mapping_exists

# Verify count for multi-project scenarios
mapping_count = count_subject_project_mappings(subject_uuid)
assert mapping_count == 2  # Subject linked to 2 projects
```

---

### ✅ 3. create_evidence ([test_create_evidence.py](test_create_evidence.py))

**Status**: Complete - 10 tests implemented, all passing
**Test Plan**: [create_evidence_tests.md](create_evidence_tests.md)

**Tests Implemented**:
1. ✅ `test_create_evidence_minimal` - Happy path with minimal fields + subject_terms verification
2. ✅ `test_create_evidence_full_fields` - All optional fields (note_context, text_annotation)
3. ✅ `test_create_evidence_multiple_same_termlink` - Multiple evidence for same termlink + subject_terms count
4. ✅ `test_create_evidence_qualifier_standardization` - **CRITICAL** Test 21 + subject_terms qualifiers
5. ✅ `test_create_evidence_qualifier_hash_consistency` - **CRITICAL** Test 22
6. ✅ `test_create_evidence_different_qualifiers_different_hash` - **CRITICAL** Test 23 + subject_terms
7. ✅ `test_create_evidence_empty_qualifiers_hash` - **CRITICAL** Test 24
8. ✅ `test_create_evidence_multi_project_update` - **NEW** Multi-project subject_terms synchronization
9. ✅ `test_create_evidence_missing_required_fields` - Validation
10. ✅ `test_create_evidence_subject_not_exists` - Edge case (documented behavior)

**Key Implementation Decisions**:
- **Athena verification as primary method**: Uses `wait_for_iceberg_evidence()` helper that polls Athena until evidence appears
- **Subject_terms verification**: Added `wait_for_subject_terms()` fixture to verify synchronous subject_terms table updates
  - 5 tests verify subject_terms_by_subject and subject_terms_by_project_term tables
  - Verifies evidence_count aggregation, qualifier storage, and multi-project synchronization
- **Struct field querying**: Queries nested Iceberg structs with dot notation (`creator.creator_id`, `text_annotation.span_start`)
- **All 4 critical qualifier tests passing**: Tests 21-24 verify hash consistency, standardization, and proper handling
- **First heavy use of query_athena fixture**: This test suite validates the Athena-based verification strategy

**Verification Strategy**:
```python
# Poll Athena until evidence appears (Iceberg writes have small delay)
evidence_record = wait_for_iceberg_evidence(query_athena, evidence_id)

# Query nested struct fields with dot notation
results = query_athena(f"""
    SELECT
        evidence_id,
        creator.creator_id as creator_id,
        text_annotation.span_start as span_start
    FROM phebee.evidence
    WHERE evidence_id = '{evidence_id}'
""")

# Verify termlink_id consistency across evidence records
count = count_evidence_for_termlink(query_athena, termlink_id)

# Verify subject_terms tables updated synchronously
by_subject, by_project = wait_for_subject_terms(
    subject_id=subject_uuid,
    termlink_id=termlink_id,
    project_id=test_project_id
)
assert int(by_subject["evidence_count"]) == 1
assert by_subject["qualifiers"] == expected_qualifiers_json
```

**Critical Qualifier Tests Results**:
- ✅ Test 21: Qualifier standardization - PASSED
- ✅ Test 22: Hash consistency (different order = same hash) - PASSED
- ✅ Test 23: Different qualifiers = different hashes - PASSED
- ✅ Test 24: Empty vs missing qualifiers = same hash - PASSED
- ⏭️ Test 25: Qualifier parsing consistency - Not implemented (requires GetEvidence, GetSubject endpoints)

**Bugs Fixed During Implementation**:
- Fixed iceberg.py note_context ROW construction to match table schema (note_type, note_date, provider_type, author_specialty)
- Fixed ISO8601 timestamp conversion to Athena TIMESTAMP format (YYYY-MM-DD HH:MM:SS)
- **CRITICAL**: Fixed missing DynamoDB permissions for CreateEvidenceFunction in template.yaml
  - Added `DynamoDBCrudPolicy` to allow `get_projects_for_subject()` queries
  - Without this, subject_terms updates silently failed with empty project list
- Added `wait_for_subject_terms()` fixture to conftest.py for polling subject_terms tables

---

### ✅ 4. get_subject ([test_get_subject.py](test_get_subject.py))

**Status**: Complete - 12 tests implemented, all passing
**Test Plan**: [get_subject_tests.md](get_subject_tests.md)

**Tests Implemented**:
1. ✅ `test_get_subject_by_id_success` - Get by subject_id
2. ✅ `test_get_subject_by_project_iri_success` - Get by project_subject_iri
3. ✅ `test_get_subject_with_multiple_terms` - Subject with 2+ terms
4. ✅ `test_get_subject_with_qualifiers` - Qualifiers array populated
5. ✅ `test_get_subject_not_found` - 404 for non-existent subject
6. ✅ `test_get_subject_missing_id_parameter` - Validation error
7. ✅ `test_get_subject_null_id` - Null parameter handling
8. ✅ `test_get_subject_empty_string_id` - Empty string handling
9. ✅ `test_get_subject_malformed_project_iri` - Invalid IRI format
10. ✅ `test_get_subject_no_terms` - Subject without evidence
11. ✅ `test_get_subject_response_structure` - Full response validation
12. ✅ `test_get_subject_term_labels` - Term label enrichment

**Key Implementation Notes**:
- Uses both query modes (subject_id direct lookup, project_subject_iri with DynamoDB resolution)
- Verifies term aggregation from evidence via GetSubject API
- Tests qualifier parsing and label enrichment from ontology

---

### ✅ 5. get_subjects_pheno ([test_get_subjects_pheno.py](test_get_subjects_pheno.py))

**Status**: Complete - 18 tests implemented, all passing
**Test Plan**: [get_subjects_pheno_tests.md](get_subjects_pheno_tests.md)

**Tests Implemented**:
1. ✅ `test_get_subjects_all_in_project` - Basic project query
2. ✅ `test_get_subjects_missing_project_id` - Validation
3. ✅ `test_get_subjects_filtered_by_term` - Term IRI filtering
4. ✅ `test_get_subjects_exclude_child_terms` - Hierarchy control
5. ✅ `test_get_subjects_exclude_qualified` - Qualifier filtering
6. ✅ `test_get_subjects_include_qualified` - Include qualified terms
7. ✅ `test_get_subjects_pagination_basic` - Basic pagination
8. ✅ `test_get_subjects_pagination_cursor` - Multi-page cursor navigation
9. ✅ `test_get_subjects_pagination_empty_cursor` - Empty cursor handling
10. ✅ `test_get_subjects_project_subject_ids_filter` - Subject ID filtering
11. ✅ `test_get_subjects_json_output` - JSON output format
12. ✅ `test_get_subjects_gzip_compression` - Gzip compression
13. ✅ `test_get_subjects_empty_result_set` - Empty results handling
14. ✅ `test_get_subjects_empty_result_project_subjects` - Empty filtered results
15. ✅ `test_get_subjects_term_id_conversion` - IRI to ID conversion
16. ✅ `test_get_subjects_root_term_rejected` - Root term validation
17. ✅ `test_get_subjects_response_structure` - Response structure validation
18. ✅ `test_get_subjects_pagination_info` - Pagination metadata

**Key Implementation Notes**:
- Queries Iceberg subject_terms analytical tables via Athena
- Implements limit+1 pattern for pagination (no expensive COUNT query)
- Reduced Lambda execution time from 4-6s to ~2.4s (50% improvement)
- Tests run in 986 seconds (16:26 minutes)

**Performance Optimizations**:
- Eliminated COUNT query using limit+1 pattern to detect more pages
- Removed total_count from pagination response (breaking change)
- Added ORDER BY subject_id for consistent pagination ordering

---

### ✅ 6. remove_evidence ([test_remove_evidence.py](test_remove_evidence.py))

**Status**: Complete - 11 tests implemented, all passing
**Test Plan**: [remove_evidence_tests.md](remove_evidence_tests.md)

**Tests Implemented**:
1. ✅ `test_remove_evidence_with_remaining_evidence` - Delete with remaining evidence
2. ✅ `test_remove_evidence_last_evidence_cascade` - Cascade delete to Neptune
3. ✅ `test_remove_evidence_not_found` - 404 for non-existent evidence
4. ✅ `test_remove_evidence_missing_evidence_id` - Validation error
5. ✅ `test_remove_evidence_null_evidence_id` - Null parameter handling
6. ✅ `test_remove_evidence_distinct_termlinks` - Multiple termlinks per term
7. ✅ `test_remove_evidence_idempotent` - Delete twice (idempotency)
8. ✅ `test_remove_evidence_empty_string_id` - Empty string handling
9. ✅ `test_remove_evidence_malformed_id` - Malformed UUID
10. ✅ `test_remove_evidence_for_deleted_subject` - Evidence independence
11. ✅ `test_remove_evidence_termlink_id_extraction` - Termlink ID extraction

**Key Implementation Notes**:
- Deletes evidence from Iceberg evidence table
- Decrements evidence_count in subject_terms analytical tables
- Cascades deletion to Neptune when last evidence for termlink removed
- Verifies termlink_id extraction and correct term link cleanup
- Tests run in 552 seconds (9:12 minutes)

**Bugs Fixed During Implementation**:
- **CreateEvidence response**: Added termlink_id to response body for test verification
- **RemoveEvidence environment variables**: Added ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE and ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE
- **RemoveEvidence permissions**: Added DynamoDBCrudPolicy for get_projects_for_subject() queries
- **Test fixtures**: Fixed physical_resources lookups for function names

**Verification Strategy**:
```python
# Verify evidence deleted from Iceberg
deleted = wait_for_evidence_deletion(query_athena, evidence_id)
assert deleted

# Verify analytical tables updated
subject_terms = get_subject_terms_by_termlink(query_athena, subject_uuid, termlink_id)
assert int(subject_terms["evidence_count"]) == 2  # Decremented from 3

# When last evidence deleted, row removed
assert subject_terms is None  # Row deleted when count reaches 0
```

---

### ✅ 7. remove_subject ([test_remove_subject.py](test_remove_subject.py))

**Status**: Complete - 7 tests implemented, all passing
**Test Plan**: [remove_subject_tests.md](remove_subject_tests.md)

**Tests Implemented**:
1. ✅ `test_remove_subject_not_last_mapping` - Partial removal (unlink from one project)
2. ✅ `test_remove_subject_last_mapping_cascade` - Full cascade deletion
3. ✅ `test_remove_subject_not_found` - 404 for non-existent subject
4. ✅ `test_remove_subject_missing_iri` - Validation error (missing parameter)
5. ✅ `test_remove_subject_invalid_iri_format` - Invalid IRI format
6. ✅ `test_remove_subject_incomplete_iri` - Incomplete IRI (< 6 parts)
7. ✅ `test_remove_subject_idempotent` - Delete twice (idempotency)

**Key Implementation Notes**:
- Handles two scenarios: partial removal (NOT last mapping) vs full cascade (last mapping)
- Partial removal: Deletes DynamoDB mappings for one project, removes by_project_term analytical data only
- Full cascade: Deletes ALL evidence, ALL analytical tables, ALL Neptune term links, and subject_iri
- Queries DynamoDB reverse mappings to determine if last mapping
- Uses atomic batch_writer for DynamoDB deletion
- Tests run in 198 seconds (3:17 minutes)

**Bugs Fixed During Implementation**:
- **RemoveSubjectFunction environment variables**: Added missing environment variables:
  - `ICEBERG_DATABASE`, `ICEBERG_EVIDENCE_TABLE`
  - `ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE`, `ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE`
  - `PHEBEE_BUCKET_NAME`, `PheBeeDynamoTable`, `EVIDENCE_TABLE`
- **RemoveSubjectFunction permissions**: Added missing policies:
  - `PheBeeIcebergPolicy` for Athena/Iceberg access
  - `DynamoDBCrudPolicy` for DynamoDB table access
- **CreateSubject response parsing**: Tests construct `project_subject_iri` rather than extracting from API response

**Verification Strategy**:
```python
# Partial removal verification
mapping_a = get_dynamodb_forward_mapping(table_name, project_a_id, project_subject_id_a)
assert mapping_a is None  # Removed project's mapping deleted

mapping_b = get_dynamodb_forward_mapping(table_name, project_b_id, project_subject_id_b)
assert mapping_b is not None  # Other project's mapping remains

# Evidence NOT deleted (subject still exists)
evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
assert evidence_count > 0

# Full cascade verification
final_mapping_count = len(get_dynamodb_reverse_mappings(table_name, subject_uuid))
assert final_mapping_count == 0  # All mappings deleted

final_evidence_count = count_evidence_for_subject(query_athena, subject_uuid)
assert final_evidence_count == 0  # All evidence deleted
```

---

## Pending Test Files (Priority Order)

### High Priority - CRUD Operations

#### 📋 8. remove_project
- **Test Plan**: [remove_project_tests.md](remove_project_tests.md)
- **Tests**: 20 scenarios
- **Key Features**: GSI queries, conditional cascades
- **Verification**: DynamoDB + Athena

### Medium Priority - State Machines

#### 📋 9. update_hpo_statemachine (6 new hierarchy tests)
- **Test Plan**: [update_hpo_statemachine_tests.md](update_hpo_statemachine_tests.md)
- **Tests**: 21 scenarios
- **Key Features**: Hierarchy queries, depth ordering
- **Verification**: Athena on ontology_hierarchy table

#### 📋 10. bulk_import_statemachine (10 new bulk tests)
- **Test Plan**: [bulk_import_statemachine_tests.md](bulk_import_statemachine_tests.md)
- **Tests**: 25 scenarios
- **Key Features**: EMR, TTL validation, EventBridge
- **Verification**: Athena + S3 + SQS

#### 📋 11. import_phenopackets_statemachine (4 new tests)
- **Test Plan**: [import_phenopackets_statemachine_tests.md](import_phenopackets_statemachine_tests.md)
- **Tests**: 19 scenarios
- **Key Features**: ZIP import/export, data comparison
- **Verification**: ZIP contents + Athena

### Lower Priority - Utility Lambdas

Remaining 32 test plan files for utility and helper lambdas.

---

## Implementation Guidelines

### 1. Verification Strategy (Neptune-Free)

Since direct Neptune access is unavailable, all tests must use **indirect verification**:

**Primary Method**: Athena queries on analytical tables
```python
def test_something(query_athena):
    results = query_athena("""
        SELECT * FROM phebee.evidence
        WHERE subject_id = 'abc123'
    """)
    assert len(results) > 0
```

**Secondary Method**: API queries that depend on Neptune data
```python
# Verify project exists by creating a subject in it
result = create_subject(project_id, subject_id)
assert result["statusCode"] == 200  # Project must exist
```

**What NOT to do**:
- ❌ Direct SPARQL queries to Neptune
- ❌ Neptune client connections
- ❌ Assumptions about Neptune internal state

### 2. Test Structure Pattern

Follow this pattern from `test_create_project.py`:

```python
def test_feature_name(physical_resources, test_project_id):
    """
    Test X: Description

    Brief explanation of what this test verifies.
    """
    # Setup
    test_data = create_test_data()

    try:
        # Action
        result = invoke_lambda(...)

        # Assertions
        assert result["statusCode"] == 200
        assert result["body"]["field"] == expected_value

        # Verification (indirect)
        verify_data_exists(...)

    finally:
        # Cleanup
        cleanup_test_data()
```

### 3. Fixture Usage

**When to use test_subject**:
```python
def test_something(test_subject, create_evidence_helper):
    subject_uuid, project_subject_iri = test_subject

    # Use subject_uuid for CreateEvidence
    create_evidence_helper(
        subject_id=subject_uuid,
        term_iri="http://..."
    )

    # Use project_subject_iri for GetSubject queries
    result = get_subject(project_subject_iri)
```

**When to use standard_hpo_terms**:
```python
def test_hierarchy(standard_hpo_terms, update_hpo):
    # Use standard terms instead of hardcoding
    parent_term = standard_hpo_terms["phenotypic_abnormality"]
    child_term = standard_hpo_terms["seizure"]
```

### 4. Athena Query Patterns

**Verify evidence created**:
```python
results = query_athena(f"""
    SELECT COUNT(*) as count
    FROM phebee.evidence
    WHERE subject_id = '{subject_id}'
    AND term_iri = '{term_iri}'
""")
assert int(results[0]["count"]) > 0
```

**Verify deletion**:
```python
results = query_athena(f"""
    SELECT COUNT(*) as count
    FROM phebee.evidence
    WHERE evidence_id = '{evidence_id}'
""")
assert int(results[0]["count"]) == 0
```

**Verify qualifier format**:
```python
results = query_athena(f"""
    SELECT qualifiers
    FROM phebee.evidence
    WHERE evidence_id = '{evidence_id}'
""")
qualifiers = json.loads(results[0]["qualifiers"])
assert qualifiers.get("negated") == "true"  # Not boolean!
```

### 5. Concurrent Testing Pattern

For race condition tests, use ThreadPoolExecutor:
```python
import concurrent.futures

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(create_project, project_id, label, resources)
        for _ in range(5)
    ]
    results = [future.result() for future in futures]

# Verify exactly one succeeded
created_count = sum(1 for r in results if r["project_created"])
assert created_count == 1
```

---

## Running Tests

### Prerequisites

You **must** specify an existing CloudFormation stack to run against:

```bash
# Required: Specify your CloudFormation stack name
--existing-stack <your-stack-name>

# Optional: Specify AWS profile (if not using default)
--profile <profile-name>
```

### Run All Tests
```bash
pytest tests/integration-new/ -v \
  --existing-stack phebee-dev
```

### Run Specific Test File
```bash
pytest tests/integration-new/test_create_project.py -v \
  --existing-stack phebee-dev
```

### Run Specific Test
```bash
pytest tests/integration-new/test_create_project.py::test_create_project_success -v \
  --existing-stack phebee-dev
```

### Run with Specific AWS Profile
```bash
pytest tests/integration-new/ -v \
  --existing-stack phebee-dev \
  --profile my-aws-profile
```

### Skip Slow Tests
```bash
pytest tests/integration-new/ -v -m "not slow" \
  --existing-stack phebee-dev
```

### Common Error: "physical_resources did not yield a value"

This error means you forgot to specify `--existing-stack`. Always provide this option:

```bash
# ❌ Wrong - missing stack name
pytest tests/integration-new/test_create_project.py -v

# ✅ Correct - with stack name
pytest tests/integration-new/test_create_project.py -v --existing-stack phebee-dev
```

---

## Next Steps

### Immediate (Recommended Order)

1. **Implement create_subject_tests** - Builds on create_project
2. **Implement create_evidence_tests** - Include critical qualifier tests (21-25)
3. **Implement get_subject_tests** - Verify read operations work
4. **Implement get_subjects_pheno_tests** - Critical query tests (21-33)

### Then Continue With

5. Remove operations (evidence → subject → project)
6. State machines (HPO → bulk import → phenopackets)
7. Utility lambdas as needed

### Success Criteria

Before considering the test suite "complete":

✅ All CRUD operations tested (create/get/remove for project/subject/evidence)
✅ Qualifier tests passing (hash consistency, standardization)
✅ Query tests passing (pagination, filtering, hierarchy)
✅ Idempotency tests passing (safe retries)
✅ Race condition tests passing (concurrent operations)
✅ State machine tests passing (HPO, bulk import, phenopackets)

---

## Notes and Considerations

### Infrastructure Failure Testing

**Integration tests cannot reliably test infrastructure failures** (Neptune unavailability, DynamoDB throttling, etc.) because:
- Stopping Neptune cluster takes 5+ minutes and affects all tests
- Network manipulation requires complex security group changes
- IAM permission removal is dangerous and hard to restore mid-test
- Dedicated "broken" infrastructure is expensive to maintain

**How infrastructure failures ARE tested**:
- ✅ **Unit tests** with mocked boto3 clients raising exceptions (ConnectionError, etc.)
- ✅ **Manual chaos engineering** in staging environments
- ✅ **Real-world monitoring and alerting** catch actual failures

**Integration tests focus on**:
- Happy path functionality with real AWS services
- Edge cases and validation errors
- Race conditions and concurrency
- Data consistency across services

### Performance

- **State machine tests are slow** (10-30 minutes)
- Mark with `@pytest.mark.slow` and run separately
- Consider creating fast-path versions with minimal fixtures

### Data Cleanup

- Use `finally` blocks for cleanup
- Tests should be independent (no shared state)
- Project cleanup cascades to subjects/evidence

### Debugging Failed Tests

1. Check CloudWatch logs for lambda execution details
2. Use Athena Console to manually verify data state
3. Check S3 for TTL files and bulk import outputs
4. Verify CloudFormation resources exist

### Common Pitfalls

❌ **Don't hardcode term IRIs** - Use `standard_hpo_terms` fixture
❌ **Don't assume Neptune state** - Use indirect verification
❌ **Don't skip cleanup** - Always use try/finally blocks
❌ **Don't share test data** - Each test should be independent
❌ **Don't assert on cleanup** - Cleanup is best-effort, use try/except:

```python
finally:
    # ❌ Bad - cleanup failure causes test to fail
    result = remove_project(project_id, physical_resources)
    assert result["statusCode"] == 200

    # ✅ Good - cleanup is best-effort
    try:
        remove_project(project_id, physical_resources)
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")
```

---

**Status Last Updated**: 2026-02-14
**Implementation Progress**: 7/43 test files complete (create_project, create_subject, create_evidence, get_subject, get_subjects_pheno, remove_evidence, remove_subject)
**Tests Passing**: 83 passing (11 + 14 + 10 + 12 + 18 + 11 + 7)
**Critical Milestones**:
- ✅ All 4 critical qualifier tests (21-24) passing
- ✅ Subject_Terms synchronous updates verified in 5 tests
- ✅ Analytical table deletion updates verified (remove_evidence)
- ✅ Cascade deletion logic verified (remove_subject)
- ✅ Pagination optimization complete (50% performance improvement)
**Next Priority**: remove_project_tests.md
