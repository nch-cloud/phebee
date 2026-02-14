# PheBee Integration Test Plans

## Quick Start

📖 **New to PheBee testing?** Start with:
- **[TESTING_GUIDE.md](TESTING_GUIDE.md)** - How to run tests, stack management, pytest commands
- **[NEPTUNE_FREE_TESTING.md](NEPTUNE_FREE_TESTING.md)** - ⚠️ **IMPORTANT**: Adapting tests without direct Neptune access

## Overview

This directory contains comprehensive integration test plans for **37 lambda functions** and **6 state machines** in the PheBee system. Each test plan provides detailed scenarios for validating end-to-end functionality, workflow orchestration, data consistency, error handling, and edge cases across all dependent systems.

## System Architecture Context

PheBee uses a multi-store architecture for managing phenotypic data:

- **DynamoDB**: Source of truth for subject identity and project-subject mappings
- **Neptune (Graph Database)**: Stores project nodes, subject nodes, term links, and relationships
- **Iceberg/Athena**: Immutable evidence records and analytical aggregation tables
- **EventBridge**: Asynchronous event processing for subject creation/linking

## Test Plan Files

📋 **Quick Reference:** See **[_SUMMARY.md](_SUMMARY.md)** for a complete categorized list of all 37 test plans.

### Create Operations

1. **[create_project_tests.md](create_project_tests.md)** - 12 scenarios, 21 KB
   - Creates project nodes in Neptune
   - Validates idempotency and error handling
   - Tests special characters, unicode, and concurrency

2. **[create_subject_tests.md](create_subject_tests.md)** - 20 scenarios, 19 KB
   - Creates subjects with UUID-based IRIs
   - Manages bidirectional DynamoDB mappings with race condition handling
   - Links subjects across multiple projects
   - Tests three modes: new subject, known_subject_iri, known_project_id

3. **[create_evidence_tests.md](create_evidence_tests.md)** - 20 scenarios, 21 KB
   - Writes immutable evidence records to Iceberg
   - Creates/updates term links in Neptune
   - Updates analytical tables incrementally
   - Tests all optional fields and qualifiers

### Read Operations

4. **[get_evidence_tests.md](get_evidence_tests.md)** - 20 scenarios, 14 KB
   - Retrieves evidence records by evidence_id
   - Validates data immutability
   - Tests eventual consistency and error handling

5. **[get_subject_tests.md](get_subject_tests.md)** - 20 scenarios, 15 KB
   - Queries subject terms from analytical tables
   - Supports two modes: by project_subject_iri or subject_id
   - Tests DynamoDB mapping lookup and Iceberg aggregations

6. **[get_subject_term_info_tests.md](get_subject_term_info_tests.md)** - 20 scenarios, 14 KB
   - Retrieves specific term link information
   - Validates termlink_id computation
   - Tests qualified vs unqualified term links

### Delete Operations

7. **[remove_evidence_tests.md](remove_evidence_tests.md)** - 20 scenarios, 15 KB
   - Deletes evidence records from Iceberg
   - Updates analytical tables with decremented counts
   - Cascades to Neptune term link deletion when evidence_count reaches zero
   - Tests idempotency and concurrent deletion

8. **[remove_subject_tests.md](remove_subject_tests.md)** - 20 scenarios, 17 KB
   - Removes subject from specific project
   - Determines if last mapping and performs appropriate cleanup
   - Cascades evidence deletion when subject fully removed
   - Tests shared subjects across multiple projects

9. **[remove_project_tests.md](remove_project_tests.md)** - 20 scenarios, 18 KB
   - Removes entire project and all associated subjects
   - Queries all subjects via DynamoDB GSI1
   - Clears Neptune named graph
   - Performs conditional cascade based on subject sharing

### State Machine Workflows

10. **[install_rdfxml_statemachine_tests.md](install_rdfxml_statemachine_tests.md)** - 12 scenarios, 30 KB
    - Orchestrates RDF/XML ontology loading to Neptune
    - Polls Neptune bulk load status with 30s intervals
    - Handles load failures and retries
    - Synchronous nested state machine execution

11. **[bulk_import_statemachine_tests.md](bulk_import_statemachine_tests.md)** - 15 scenarios, 45 KB
    - EMR Serverless-based bulk evidence processing
    - Subject mapping resolution via DynamoDB
    - TTL generation and Neptune bulk loading by graph
    - Analytical table materialization
    - EventBridge success/failure notifications

12. **[update_hpo_statemachine_tests.md](update_hpo_statemachine_tests.md)** - 15 scenarios, 35 KB
    - Downloads HPO ontology from GitHub
    - Idempotency checks to avoid redundant updates
    - Installs to Neptune via nested state machine
    - Materializes hierarchy to Iceberg with distributed map
    - Fires EventBridge update events

13. **[update_mondo_statemachine_tests.md](update_mondo_statemachine_tests.md)** - 10 scenarios, 25 KB
    - Downloads MONDO ontology from GitHub
    - Similar workflow to HPO with MONDO-specific configuration
    - Scheduled 10 minutes after HPO to prevent concurrent loads
    - Hierarchy materialization for disease terms

14. **[update_eco_statemachine_tests.md](update_eco_statemachine_tests.md)** - 15 scenarios, 28 KB
    - Downloads ECO ontology (Evidence & Conclusion Ontology)
    - ZIP extraction required (unique to ECO)
    - No hierarchy materialization (simpler workflow)
    - Scheduled 20 minutes after HPO for staggered execution

15. **[import_phenopackets_statemachine_tests.md](import_phenopackets_statemachine_tests.md)** - 15 scenarios, 40 KB
    - Processes GA4GH Phenopackets v2 from ZIP files
    - Distributed map with S3 ItemReader for parallel processing
    - Nested map for evidence creation (up to 32 concurrent per phenopacket)
    - Synchronous execution via API Gateway (5 min timeout)
    - Subject ID replacement from external IDs to PheBee UUIDs

## Total Coverage

- **700+ integration test scenarios** across 37 lambdas and 6 state machines
- **~550 KB** of detailed test documentation (~15,000 lines)
- Coverage includes:
  - Happy path scenarios
  - Error conditions and validation
  - Edge cases and boundary conditions
  - Concurrent operations and race conditions
  - System consistency across stores (DynamoDB, Neptune, Iceberg, S3, Athena)
  - Performance considerations
  - Failure scenarios and partial cleanup
  - State machine orchestration and polling loops
  - Distributed map processing with S3 ItemReaders
  - EMR Serverless job execution
  - Synchronous and asynchronous workflow patterns
  - EventBridge event-driven notifications

## Test Scenario Structure

Each test plan follows a consistent format:

### Lambda Metadata
- **Purpose**: What the lambda does
- **Dependencies**: Systems it interacts with (Neptune, DynamoDB, Iceberg, etc.)
- **Key Operations**: Critical operations performed

### Test Scenarios
For each scenario:
- **Test Name**: Descriptive pytest function name
- **Setup**: Prerequisites and data preparation
- **Action**: Lambda invocation with specific payload
- **Assertions**: Expected outcomes across ALL dependent systems
  - Response validation
  - DynamoDB state verification
  - Neptune graph verification
  - Iceberg evidence verification
  - Analytical tables verification
  - EventBridge event verification (where applicable)
- **Cleanup**: Data removal to restore clean state

### Edge Cases Section
Additional scenarios to consider for production readiness

## Key Testing Themes

### 1. Data Consistency
Every test validates that changes propagate correctly across all systems:
- DynamoDB mappings ↔ Neptune nodes ↔ Iceberg records ↔ Analytical tables

### 2. Atomicity and Race Conditions
Tests validate atomic operations using:
- DynamoDB conditional writes for subject creation
- Batch operations for mapping deletions
- Concurrent invocation scenarios

### 3. Cascade Logic
Delete operations test conditional cascading:
- Evidence deletion → term link deletion (when evidence_count = 0)
- Subject removal → full cascade (when last project mapping)
- Project removal → subject-by-subject cascade evaluation

### 4. Error Resilience
Tests verify graceful handling of:
- Missing required fields
- Invalid formats and malformed data
- System unavailability (Neptune, DynamoDB, Iceberg)
- Partial failures with appropriate cleanup

### 5. Idempotency
Critical operations tested for safe re-invocation:
- create_project (returns existing project)
- create_subject (returns existing subject)
- remove operations (404 on second attempt)

### 6. Performance
Large-scale scenarios test:
- 100+ subjects per project
- 1000+ evidence records per subject
- Concurrent operations (5-100 parallel invocations)
- Lambda timeout boundaries

## Implementation Guidelines

### Running Integration Tests

1. **Environment Setup**
   ```bash
   # Deploy CloudFormation stack
   # Set environment variables for test resources
   export PHEBEE_STACK_NAME=phebee-integration-test
   ```

2. **Test Execution**
   ```bash
   # Run all CRUD integration tests
   pytest tests/integration-new/ -v -m integration

   # Run specific lambda tests
   pytest tests/integration-new/test_create_subject.py -v
   ```

3. **Cleanup**
   ```bash
   # Each test includes cleanup in teardown
   # For manual cleanup:
   pytest tests/integration-new/ --cleanup-only
   ```

### Test Implementation Best Practices

1. **Fixtures**: Use pytest fixtures for common setup (projects, subjects, evidence)
2. **Isolation**: Each test should be independent with its own cleanup
3. **Assertions**: Verify state in ALL dependent systems, not just lambda response
4. **Logging**: Capture CloudWatch logs for failure analysis
5. **Retry Logic**: Implement retries for eventual consistency scenarios
6. **Timeouts**: Set appropriate timeouts for long-running operations

### Verification Helpers

Implement helper functions for cross-system verification:

```python
def verify_subject_consistency(subject_id, expected_state):
    """Verify subject exists consistently across all systems"""
    # Check DynamoDB mappings
    # Check Neptune subject node
    # Check Iceberg evidence
    # Check analytical tables
    # Assert all match expected_state
```

## Critical Test Priorities

### High Priority (Must Pass Before Production)
1. Happy path scenarios (Tests 1-2 of each lambda)
2. Data consistency across systems
3. Cascade deletion logic (remove_evidence, remove_subject, remove_project)
4. Race condition handling (create_subject concurrent tests)
5. Error handling for missing required fields

### Medium Priority (Should Pass Before Production)
1. Edge cases (invalid formats, malformed data)
2. Performance tests (large datasets)
3. Concurrent operations
4. Partial failure scenarios

### Nice to Have (Can Be Addressed Post-Launch)
1. Extreme edge cases (very long strings, unicode)
2. Authorization and access control
3. Cross-region consistency
4. Advanced performance optimization

## Test Metrics and Coverage

### Coverage Goals
- **System Coverage**: All 4 data stores tested for each operation
- **Scenario Coverage**: Happy path + errors + edge cases for each lambda
- **Consistency Coverage**: Cross-system verification in every test

### Success Criteria
- All high-priority tests pass
- No orphaned data in any system
- All cascades execute correctly
- No race conditions in concurrent tests
- Response times within acceptable limits

## Related Documentation

- **API Documentation**: See `/api/api.yaml` for lambda interface contracts
- **Architecture Docs**: See `/docs/architecture.md` for system design
- **Deployment**: See `/cloudformation/` for infrastructure as code
- **Utility Functions**: See `/layers/phebee-utils/` for helper implementations

## Contributing

When adding new lambdas or modifying existing ones:

1. Update or create corresponding test plan in this directory
2. Follow the established test plan structure
3. Include at least 15-20 test scenarios per lambda
4. Cover happy path, errors, edge cases, and consistency checks
5. Update this README with the new test plan summary

## Questions and Support

For questions about these test plans or integration testing:
- Review lambda source code in `/functions/`
- Check utility implementations in `/layers/phebee-utils/`
- Examine existing integration tests in `/tests/integration/`
- Consult CloudFormation templates for resource dependencies

---

**Total Test Plans**: 37 lambdas + 6 state machines = **43 test plans**
**Total Scenarios**: 700+ integration tests (500+ for lambdas, 200+ for state machines)
**Total Documentation**: ~550 KB (~15,000 lines)

**Status**: ✅ Complete - Ready for implementation

**State Machine Features Tested**:
- Distributed maps with parallel execution (up to 32 concurrent)
- Nested state machine calls (synchronous execution)
- S3 ItemReaders for streaming data processing
- Neptune bulk load polling loops
- EMR Serverless job orchestration
- EventBridge event publishing
- Error propagation and catch clauses
- Conditional branching based on state outputs
- JSONPath and States.StringToJson transformations
