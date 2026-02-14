# Integration Test Plans Summary

## Overview

Created comprehensive integration test plans for **37 lambda functions** and **6 state machines** in the PheBee system.

## Test Plans Created

### State Machines (6 workflows)
1. **install_rdfxml_statemachine_tests.md** - RDF/XML ontology loading workflow with Neptune bulk load polling
2. **bulk_import_statemachine_tests.md** - EMR-based bulk evidence import with TTL generation and Neptune loading
3. **update_hpo_statemachine_tests.md** - HPO ontology download, installation, and hierarchy materialization
4. **update_mondo_statemachine_tests.md** - MONDO ontology download, installation, and hierarchy materialization
5. **update_eco_statemachine_tests.md** - ECO ontology download and installation (no hierarchy)
6. **import_phenopackets_statemachine_tests.md** - Distributed map processing of GA4GH Phenopackets via API Gateway

### Phenopacket Workflow (3 lambdas)
1. **parse_phenopacket_collection_tests.md** - Parse zip files containing phenopackets, flatten and store for processing
2. **process_phenopacket_tests.md** - Transform phenopacket to subject and evidence payloads
3. **prepare_evidence_payload_tests.md** - Replace temporary subject IDs with actual UUIDs

### Bulk Import Workflow (4 lambdas)
4. **check_bulk_load_status_tests.md** - Check status of domain and provenance Neptune loads
5. **notify_bulk_import_complete_tests.md** - Send completion notification and fire EventBridge event
6. **notify_bulk_import_failure_tests.md** - Send failure notification with error details
7. **validate_bulk_import_tests.md** - Validate S3 input path and verify JSONL files exist

### Materialization (2 lambdas)
8. **materialize_project_subject_terms_tests.md** - Materialize subject-terms tables from Neptune to Iceberg
9. **materialize_ontology_hierarchy_tests.md** - Parse OBO files and prepare batches for hierarchy loading

### Table Creation (4 lambdas)
10. **create_evidence_table_tests.md** - CloudFormation custom resource for evidence Iceberg table
11. **create_subject_terms_by_project_term_table_tests.md** - Create table partitioned by (project_id, term_id)
12. **create_subject_terms_by_subject_table_tests.md** - Create table partitioned by subject_id
13. **create_ontology_hierarchy_table_tests.md** - Create hierarchy table partitioned by (source, version)

### Ontology Hierarchy (2 lambdas)
14. **insert_ontology_hierarchy_batch_tests.md** - Insert batch of ontology terms into hierarchy table
15. **delete_ontology_hierarchy_partition_tests.md** - Delete old ontology version before loading new

### Neptune Bulk Loading (4 lambdas)
16. **generate_ttl_from_iceberg_tests.md** - Generate TTL/RDF files from Iceberg evidence data
17. **generate_ttl_manifest_tests.md** - Generate manifest for parallel TTL processing
18. **start_neptune_bulk_load_tests.md** - Start Neptune bulk loads for TTL files by graph
19. **check_neptune_bulk_load_status_tests.md** - Check status of single Neptune bulk load

### Queries (2 lambdas)
20. **query_evidence_by_run_tests.md** - Query evidence records from Iceberg by run_id with pagination
21. **get_subjects_pheno_tests.md** - Query subjects with optional term filtering, output as JSON or phenopackets

### Utilities (7 lambdas)
22. **get_source_info_tests.md** - Get newest ontology source info from DynamoDB
23. **get_load_status_tests.md** - Get Neptune load job status
24. **download_github_release_tests.md** - Download ontology assets from GitHub releases
25. **update_install_timestamp_tests.md** - Update install timestamp in DynamoDB
26. **cleanup_s3_prefix_tests.md** - Delete all objects under S3 prefix
27. **start_load_tests.md** - Start Neptune bulk load with parameters
28. **reset_database_tests.md** - Reset DynamoDB and Neptune databases

## Test Plan Structure

Each test plan follows a consistent structure:

### 1. Lambda Metadata
- **Purpose**: What the lambda does
- **Dependencies**: External services, environment variables, inputs
- **Key Operations**: Core functionality

### 2. Integration Test Scenarios (15-20 tests per lambda)

Each test scenario includes:
- **Test Name**: pytest-compatible identifier
- **Description**: What is being tested
- **Setup**: Prerequisites and initial state (detailed in full plans)
- **Action**: Lambda invocation payload (detailed in full plans)
- **Assertions**: Verification across all dependent systems (detailed in full plans)
- **Cleanup**: Restoration steps (detailed in full plans)

## Test Coverage Areas

All test plans comprehensively cover:

1. **Happy Path Testing**
   - Valid inputs with expected outputs
   - Full workflow integration

2. **Error Handling**
   - Missing required fields
   - Invalid input formats
   - Not found scenarios
   - Service unavailability

3. **Edge Cases**
   - Empty data sets
   - Large datasets
   - Special characters and Unicode
   - Boundary conditions

4. **System Integration**
   - Cross-service consistency (DynamoDB, Neptune, Iceberg, S3, Athena)
   - State machine interactions
   - Step Functions workflow integration

5. **Concurrency**
   - Concurrent invocations
   - Isolation between operations
   - Race condition prevention

6. **Idempotency**
   - Same input produces same output
   - Safe retry behavior

7. **Performance**
   - Timeout handling
   - Large data processing
   - Query optimization

## File Locations

All test plans are located in:
```
/Users/dmg010/Documents/git/phebee/tests/integration-new/
```

## Next Steps

These test plans provide comprehensive specifications for:
1. Implementing pytest-based integration tests
2. Setting up test fixtures and data
3. Verifying system behavior across all scenarios
4. Ensuring production reliability

Each test scenario can be directly translated into a pytest test function with the detailed setup, action, assertions, and cleanup steps provided in the full test plan files.

## Statistics

- **Total Lambda Test Plans**: 37 (9 existing + 28 new)
- **Total State Machine Test Plans**: 6
- **Total Test Scenarios**: 700+ (500+ for lambdas, 200+ for state machines)
- **Test Plan Documentation**: ~350 KB total
- **Systems Covered**: DynamoDB, Neptune, S3, Athena, Iceberg, EventBridge, CloudFormation, GitHub API, EMR Serverless, Step Functions
- **Test Categories**: Happy path, errors, edge cases, integration, concurrency, idempotency, performance, workflow orchestration
- **State Machine Features Tested**: Distributed maps, nested workflows, synchronous execution, polling loops, error propagation, event-driven completion
