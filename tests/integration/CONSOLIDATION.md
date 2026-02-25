# Integration Test Consolidation Plan

## Overview

Consolidating tests from `tests/integration-new/` into `tests/integration/` after verifying coverage against `tests/integration-old/`.

**Status**: In Progress
**Started**: 2026-02-16

## Directory Structure

- `tests/integration-new/` - New tests (passing, modern fixtures)
- `tests/integration-old/` - Old tests (moved from tests/integration)
- `tests/integration/` - Target location (currently empty)

## Consolidation Phases

### Phase 1: Compare Test Coverage ✅ READY
For each new test, compare against corresponding old test to ensure no cases are missed.

### Phase 2: Move Test Files
Move verified test files from integration-new → integration

### Phase 3: Merge conftest.py
Merge fixtures from integration-new/conftest.py into integration/conftest.py

### Phase 4: Handle Data Directories
Decide what to do with:
- `tests/integration-old/data/` (phenopackets)
- `tests/integration-old/api/` subdirectory
- `tests/integration-old/performance/` subdirectory with benchmark data

### Phase 5: Handle Utility Modules
Review and migrate needed utilities from integration-old:
- `cloudformation_utils.py`
- `general_utils.py`
- `s3_utils.py`
- `step_function_utils.py`
- `update_source_utils.py`
- `constants.py`

### Phase 6: Cleanup
- Remove integration-new directory
- Archive or delete integration-old
- Update CI/CD to point to tests/integration

### Phase 7: Register pytest marks
Add to pytest.ini:
```ini
markers =
    manual: marks tests as manual (deselect with '-m "not manual"')
    slow: marks tests as slow
    run_first: marks tests that should run first
    run_last: marks tests that should run last
```

---

## Test Migration Checklist

### State Machine Tests

- [x] **test_update_hpo_sfn.py**
  - Old test: `test_update_hpo.py`
  - Notes: 6 tests vs 8, focuses on state machine workflow (execution, DynamoDB updates, hierarchy validation, idempotency) vs utility function testing
  - Status: ✅ Reviewed and migrated

- [x] **test_update_eco_sfn.py**
  - Old test: `test_update_eco.py`
  - Notes: 3 tests vs 1, includes execution success, DynamoDB timestamp updates, idempotency check
  - Status: ✅ Reviewed and migrated

- [x] **test_update_mondo_sfn.py**
  - Old test: `test_update_mondo.py`
  - Notes: 6 tests vs 8, focuses on state machine workflow (execution, DynamoDB updates, hierarchy validation, idempotency) vs utility function testing
  - Status: ✅ Reviewed and migrated

- [x] **test_bulk_import_statemachine.py**
  - Old test: `test_bulk_import_stepfunction.py`
  - Notes: 8 tests (7 + 1 manual), module-scoped golden execution, validation, EMR processing, TTL generation, Iceberg verification (polling loops kept for state machine execution)
  - Status: ✅ Reviewed and migrated

- [x] **test_import_phenopackets_statemachine.py**
  - Old test: `test_phenopackets.py`
  - Notes: 9 tests (8 + 1 manual), module-scoped golden execution with synchronous state machine, ZIP parsing, distributed map, subject/evidence creation, materialization (uses start_sync_execution)
  - Status: ✅ Reviewed and migrated

### API/Lambda Tests

- [x] **test_create_project.py**
  - Old test: `test_create_project.py`
  - Notes: Project creation - 8 tests vs 1, adds validation, edge cases, concurrent creation
  - Status: ✅ Reviewed and migrated

- [x] **test_create_subject.py**
  - Old test: `test_create_subject.py`
  - Notes: Subject creation - 14 tests vs 3, adds DynamoDB bidirectional verification, error cases, race conditions
  - Status: ✅ Reviewed and migrated

- [x] **test_create_evidence.py**
  - Old test: `test_evidence.py` (partial)
  - Notes: Evidence creation - 13 tests, adds term_source testing (3 tests), comprehensive qualifier testing (5 tests), multi-project handling
  - Status: ✅ Reviewed and migrated

- [x] **test_get_evidence.py**
  - Old test: None (new test)
  - Notes: Evidence retrieval - 12 tests covering happy path, full fields, validation errors, qualifiers, immutability
  - Status: ✅ Reviewed and migrated

- [x] **test_remove_evidence.py**
  - Old test: None (new test)
  - Notes: Evidence removal - 11 tests covering deletion, cascade, analytics updates, validation, idempotency
  - Status: ✅ Reviewed and migrated

- [x] **test_get_subject.py**
  - Old test: None (new test)
  - Notes: 12 tests covering subject retrieval by ID and project IRI, DynamoDB mapping resolution, qualified terms, evidence count aggregation, validation (removed all wait logic)
  - Status: ✅ Reviewed and migrated

- [x] **test_remove_subject.py**
  - Old test: `test_remove_subject.py`
  - Notes: Subject removal - 7 tests vs 1, adds partial/cascade deletion, DynamoDB verification, analytical table cleanup, validation errors
  - Status: ✅ Reviewed and migrated

- [x] **test_remove_project.py**
  - Old test: None (new test)
  - Notes: Project removal - New comprehensive test coverage
  - Status: ✅ Reviewed and migrated

- [x] **test_get_subject_term_info.py**
  - Old test: `test_get_subject_term_info.py`
  - Notes: 13 tests vs 5, comprehensive termlink info retrieval with qualifiers, evidence counts, validation, concurrent queries (removed all wait logic)
  - Status: ✅ Reviewed and migrated

- [x] **test_get_subjects_pheno.py**
  - Old test: `test_query_subjects.py`
  - Notes: 18 tests covering query by phenotype, term filtering, qualifier filtering, pagination, gzip compression, response structure validation (removed all wait logic)
  - Status: ✅ Reviewed and migrated

- [x] **test_get_source_info.py**
  - Old test: `api/test_source_api.py`
  - Notes: Get ontology source information - 16 tests vs 2, adds structure validation, assets, versioning, concurrency, edge cases
  - Status: ✅ Reviewed and migrated

- [x] **test_query_evidence_by_run.py**
  - Old test: None (new test)
  - Notes: 16 tests covering pagination, limits, creator struct parsing, timestamp ordering, concurrent queries, validation, idempotency
  - Status: ✅ Reviewed and migrated

- [x] **test_validate_bulk_import.py**
  - Old test: None (new test)
  - Notes: 16 tests covering S3 validation, JSONL file discovery, project_id extraction, error handling, idempotency, concurrent validations
  - Status: ✅ Reviewed and migrated

- [x] **test_materialize_project_subject_terms.py**
  - Old test: None (new test)
  - Notes: 13 tests covering materialization of subject-terms analytical tables, qualifiers preservation, date aggregation, idempotency, multiple subjects
  - Status: ✅ Reviewed and migrated

### Special Tests

- [x] **test_reset_database.py**
  - Old test: `test_reset_database.py`
  - Notes: Database reset functionality - 6 tests vs 1, adds comprehensive Iceberg table verification, idempotency, payload handling
  - Status: ✅ Reviewed and migrated

---

## Old Tests Without Direct New Equivalent

These may be obsolete or need new tests written:

- `test_bulk_import_events.py` - EventBridge events from bulk import
- `test_cache_vs_neptune.py` - Cache vs Neptune comparison
- `test_cloudformation_stack.py` - Stack validation
- `test_emr_subject_mapping.py` - EMR subject mapping (may be covered in bulk import)
- `test_hierarchy_integration.py` - Ontology hierarchy tests
- `test_qualifier_deduplication.py` - Qualifier deduplication
- `test_qualifier_parsing_comprehensive.py` - Qualifier parsing
- `test_qualifiers.py` - General qualifier tests
- `test_term_link.py` - Term linking

**Action needed**: Review these to determine if coverage exists in new tests or if they need to be rewritten.

---

## Utility Module Decisions

### Keep
- TBD

### Obsolete (use fixtures from conftest.py instead)
- TBD

### Needs Review
- `cloudformation_utils.py` - CloudFormation helpers
- `general_utils.py` - General test utilities
- `s3_utils.py` - S3 helpers
- `step_function_utils.py` - Step Function helpers
- `update_source_utils.py` - Update source helpers
- `constants.py` - Test constants

---

## Data Directory Decisions

### tests/integration-old/data/phenopackets/
- Contains: Phenopacket test data
- Decision: TBD

### tests/integration-old/api/
- Contains: API test files
- Decision: TBD

### tests/integration-old/performance/
- Contains: Performance benchmark data
- Decision: TBD (likely move to tests/performance/)

---

## Notes

- All new tests use modern pytest fixtures from conftest.py
- New tests avoid global state and utility modules where possible
- New tests are more focused and faster (module-level fixtures, better cleanup)
- Old tests may have broader coverage in some areas (qualifiers, term links, etc.)

---

## Progress Tracking

**Tests migrated**: 20 / 21
**Phases completed**: 0 / 7
**Last updated**: 2026-02-15

### Migration Log
- 2026-02-16: Migrated test_create_project.py (complete coverage, adds 10+ scenarios)
- 2026-02-16: Migrated test_remove_project.py (new test, no old equivalent)
- 2026-02-16: Migrated test_create_subject.py (complete coverage, adds DynamoDB bidirectional checks + 11 new scenarios)
- 2026-02-15: Migrated test_remove_subject.py (complete coverage, adds partial/cascade deletion logic + 6 new scenarios)
- 2026-02-15: Migrated test_create_evidence.py (complete coverage, added missing term_source tests + comprehensive qualifier testing)
- 2026-02-15: Migrated test_remove_evidence.py (new test, comprehensive coverage of deletion with cascade and analytics)
- 2026-02-15: Migrated test_get_evidence.py (new test, comprehensive retrieval testing with module-scoped fixtures for efficiency)
- 2026-02-15: Migrated test_get_source_info.py (complete coverage, adds structure validation, assets, versioning, concurrency)
- 2026-02-15: Migrated test_reset_database.py (complete coverage, adds comprehensive Iceberg verification, idempotency tests)
- 2026-02-17: Migrated test_update_eco_sfn.py (complete coverage, adds execution success verification, DynamoDB timestamp updates, idempotency testing)
- 2026-02-17: Migrated test_update_hpo_sfn.py (state machine workflow focus, comprehensive hierarchy validation, idempotency testing)
- 2026-02-17: Migrated test_update_mondo_sfn.py (state machine workflow focus, comprehensive hierarchy validation, idempotency testing)
- 2026-02-17: Migrated test_query_evidence_by_run.py (new test, comprehensive Athena-based evidence query with pagination, creator parsing, validation)
- 2026-02-17: Migrated test_validate_bulk_import.py (new test, S3 validation with JSONL discovery, project_id extraction, error handling)
- 2026-02-17: Migrated test_materialize_project_subject_terms.py (new test, analytical table materialization with qualifiers, date aggregation, idempotency)
- 2026-02-17: Migrated test_get_subject_term_info.py (expanded coverage, removed all wait logic for analytical table updates)
- 2026-02-17: Migrated test_get_subject.py (new test, comprehensive subject retrieval with DynamoDB mapping, qualified terms, removed all wait logic)
- 2026-02-17: Migrated test_get_subjects_pheno.py (comprehensive phenotype query with term/qualifier filtering, pagination, compression, removed all wait logic)
- 2026-02-17: Migrated test_bulk_import_statemachine.py (end-to-end bulk import with module-scoped golden execution, validation, EMR, TTL, kept state machine polling)
- 2026-02-17: Migrated test_import_phenopackets_statemachine.py (end-to-end phenopacket import with synchronous execution, ZIP parsing, distributed map, materialization)
