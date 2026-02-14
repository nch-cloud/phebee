# Integration Test Plan: materialize_project_subject_terms.py

## Lambda Metadata

**Purpose**: Materialize subject-terms analytical tables from Neptune data for a project, creating optimized Iceberg tables for fast queries.

**Dependencies**:
- Neptune: Query subject and term data
- Iceberg: Write to subject_terms tables
- Athena: Execute materialization queries
- Input: project_id, optional batch_size

**Key Operations**:
- Calls materialize_project utility function
- Aggregates subject-term relationships from Neptune
- Writes to two Iceberg tables: by_subject and by_project_term
- Returns statistics: subjects_processed, terms_materialized

---

## Integration Test Scenarios

### Test 1: test_materialize_project_with_subjects
**Setup**: Create project with 10 subjects, each with 5 terms
**Action**: `{"project_id": "proj-123", "batch_size": 100}`
**Assertions**: statusCode 200, subjects_processed: 10, terms_materialized: 50, status: "success"

### Test 2: test_materialize_empty_project
**Setup**: Create project with no subjects
**Action**: `{"project_id": "proj-empty"}`
**Assertions**: statusCode 200, subjects_processed: 0, terms_materialized: 0, status: "success"

### Test 3: test_materialize_missing_project_id
**Action**: `{}`
**Assertions**: statusCode 500, error: "Missing or invalid project_id"

### Test 4: test_materialize_null_project_id
**Action**: `{"project_id": null}`
**Assertions**: statusCode 500, error about invalid project_id

### Test 5: test_materialize_string_null_project_id
**Action**: `{"project_id": "null"}`
**Assertions**: Error, project_id "null" invalid

### Test 6: test_materialize_nonexistent_project
**Action**: `{"project_id": "nonexistent-project"}`
**Assertions**: Succeeds with 0 subjects (project has no data)

### Test 7: test_materialize_custom_batch_size
**Setup**: Project with 500 subjects
**Action**: `{"project_id": "proj-large", "batch_size": 50}`
**Assertions**: Materialization processes in batches of 50, all subjects processed

### Test 8: test_materialize_default_batch_size
**Action**: `{"project_id": "proj-456"}`
**Assertions**: Uses default batch_size 100, materialization succeeds

### Test 9: test_materialize_large_project
**Setup**: Project with 1000+ subjects
**Action**: `{"project_id": "proj-xlarge"}`
**Assertions**: Completes successfully, handles large dataset, no timeout

### Test 10: test_materialize_iceberg_tables_created
**Setup**: New project
**Action**: Materialize project
**Assertions**: Query both Iceberg tables, data present, correct partitioning

### Test 11: test_materialize_subject_terms_by_subject
**Setup**: Materialize project
**Action**: Query subject_terms_by_subject table
**Assertions**: Partitioned by subject_id, all subjects present, terms aggregated per subject

### Test 12: test_materialize_subject_terms_by_project_term
**Setup**: Materialize project
**Action**: Query subject_terms_by_project_term table
**Assertions**: Partitioned by (project_id, term_id), optimized for term queries

### Test 13: test_materialize_replaces_existing_data
**Setup**: Materialize project, modify data in Neptune, rematerialize
**Action**: Second materialization
**Assertions**: Old data replaced, new data reflects current Neptune state

### Test 14: test_materialize_concurrent_projects
**Setup**: Create 3 projects
**Action**: Materialize all 3 concurrently
**Assertions**: All succeed, no data mixing, each project isolated in tables

### Test 15: test_materialize_step_functions_integration
**Setup**: Invoke as part of bulk import Step Functions workflow
**Action**: Materialize after bulk load completes
**Assertions**: Step Functions receives success response, workflow continues

### Test 16: test_materialize_error_handling
**Setup**: Simulate Athena query failure
**Action**: Attempt materialization
**Assertions**: statusCode 500, error in response, exception logged

### Test 17: test_materialize_returns_statistics
**Action**: Materialize project
**Assertions**: Response includes subjects_processed count, terms_materialized count, both integers >= 0

### Test 18: test_materialize_qualifiers_preserved
**Setup**: Create subjects with negated qualifiers
**Action**: Materialize
**Assertions**: Query Iceberg table, qualifiers array preserved correctly

### Test 19: test_materialize_evidence_dates_aggregated
**Setup**: Multiple evidence records per subject-term
**Action**: Materialize
**Assertions**: first_evidence_date and last_evidence_date correctly computed

### Test 20: test_materialize_idempotency
**Setup**: Materialize once
**Action**: Materialize again with same data
**Assertions**: Same results, data consistent, no duplication
