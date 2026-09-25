# PheBee Test Suite

This directory contains PheBee's comprehensive test suite, including unit tests, integration tests, and performance evaluation infrastructure.

## Test Structure

```
tests/
├── unit/                          # Unit tests (fast, no infrastructure)
├── integration/                   # Integration tests (require deployed stack)
│   ├── api/                       # API Gateway smoke tests
│   ├── evaluation/                # End-to-end functional evaluation (manuscript requirements)
│   ├── performance/               # Performance evaluation suite
│   └── *.py                       # Lambda and service integration tests
```

---

## Unit Tests

Unit tests validate individual functions and modules without requiring AWS infrastructure. These tests are fast and can run locally.

**Location**: `tests/unit/`

**What they test**:
- Athena query result parsing (`test_athena_struct_parsing.py`)
- Athena query metrics and cost estimates (`test_athena_metrics.py`)
- AWS utility functions (`test_aws_utilities.py`)
- Hash consistency for data integrity (`test_hash_consistency.py`)
- Hash agreement between the API, bulk import and rebuild paths (`test_bulk_hash_consistency.py`, `test_bulk_context_null_handling.py`, `test_rehash_hash_consistency.py`)
- Qualifier model, normalization and value canonicalization (`test_qualifier.py`, `test_qualifier_normalization.py`, `test_qualifier_canonicalization.py`)
- Monarch API utilities (`test_monarch_utilities.py`)
- OBO ontology file parsing (`test_obo_parsing.py`)
- Phenopacket data processing and export (`test_phenopacket_processing.py`, `test_phenopacket_utilities.py`, `test_phenopacket_export.py`)
- Evidence payload preparation (`test_prepare_evidence_payload.py`)
- SPARQL query utilities (`test_sparql_utilities.py`)
- String manipulation utilities (`test_string_utilities.py`)

**Running unit tests** (from the project root, so the `pythonpath` in `pytest.ini` applies):
```bash
# Run all unit tests
pytest tests/unit -v

# Run specific test file
pytest tests/unit/test_obo_parsing.py -v

# Run with coverage (requires pytest-cov)
pytest tests/unit --cov=layers/phebee-utils/phebee --cov=functions --cov=scripts --cov-report=html
```

Unit tests are not marked, and `pytest.ini` sets `testpaths = tests/integration`, so select them by path rather than with `-m`.

---

## Integration Tests

Integration tests validate the full system by deploying infrastructure and exercising Lambda functions, Step Functions, and API endpoints.

**Location**: `tests/integration/`

### Lambda Function Tests

Core integration tests for individual Lambda functions and orchestration workflows:

- **Project Management**: `test_create_project.py`, `test_remove_project.py`
- **Subject Operations**: `test_create_subject.py`, `test_get_subject.py`, `test_remove_subject.py`
- **Evidence Operations**: `test_create_evidence.py`, `test_get_evidence.py`, `test_remove_evidence.py`, `test_query_evidence.py`
- **Query Operations**: `test_get_subjects_pheno.py`, `test_get_subject_term_info.py`, `test_get_subject_qualifiers.py`, `test_query_evidence_by_run.py`
- **Ontology Updates**: `test_update_hpo_sfn.py`, `test_update_mondo_sfn.py`, `test_update_eco_sfn.py`
- **Bulk Operations**: `test_bulk_import_statemachine.py`, `test_import_phenopackets_statemachine.py`, `test_validate_bulk_import.py`
- **Materialization and Rebuild**: `test_materialize_project_subject_terms.py`, `test_rebuild_state_machine.py`
- **Utilities**: `test_get_source_info.py`, `test_reset_database.py` (erases all data in the target stack; see [Using Existing Stack](#using-existing-stack))

### Functional Evaluation

End-to-end run of the manuscript's functional workflows against a small synthetic cohort: descendant expansion, qualifier filtering, re-ingestion of identical evidence, subjects shared across projects, and term source metadata in the evidence table.

**Location**: `tests/integration/evaluation/test_evaluation_end_to_end.py`

### API Gateway Tests

Smoke tests for API Gateway → Lambda integration.

**Location**: `tests/integration/api/`

**What they test**:
- API endpoint reachability and routing
- Authentication (AWS SigV4)
- HTTP status codes and error handling
- CORS configuration
- Basic response structure validation

**Files**:
- `test_api_projects.py` - Project endpoints
- `test_api_subjects.py` - Subject endpoints
- `test_api_evidence.py` - Evidence endpoints
- `test_api_sources.py` - Source info endpoints
- `test_api_queries.py` - Query endpoints

### Performance Tests

Comprehensive performance evaluation infrastructure for measuring bulk import throughput and API query latency at scale.

**Location**: `tests/integration/performance/`

**What they test**:
- Bulk import performance (1K-100K subjects)
- API query latency under load (7 query patterns)
- Realistic clinical data patterns with disease clustering
- Reproducible benchmark datasets

**See**: [Performance Testing Guide](integration/performance/README.md) for detailed documentation.

---

## Running Integration Tests

### Prerequisites

Integration tests require:
1. AWS credentials configured (`aws configure`)
2. Python dependencies: `pytest`, `boto3`, `requests`, `requests-aws4auth`

### Basic Usage

```bash
# Deploy the stack first, then upload the EMR scripts it runs from S3
sam build && sam deploy --config-env integration-test --resolve-s3
./utilities/deploy-scripts.sh phebee-integration-test

# Run all integration tests
pytest tests/integration -v

# Run specific test category
pytest tests/integration/api -v                    # API Gateway tests only
pytest tests/integration/performance -v            # Performance tests only
pytest tests/integration/test_create_subject.py -v # Single test file
```

### Test Markers

Tests are marked for selective execution:

| Marker | Description | Example |
|--------|-------------|---------|
| `integration` | Requires deployed stack | `pytest -m integration` |
| `api` | API Gateway tests | `pytest -m api` |
| `perf` | Performance evaluation tests (slow; also require `PHEBEE_EVAL_SCALE=1`) | `pytest -m perf` |

**Note**: Only some integration modules carry the `integration` marker (API tests have both `api` and `integration`), so `pytest -m integration` runs a subset of the integration suite. Select by path to run all of it.

**Common patterns**:
```bash
# Fast development cycle (unit tests only)
pytest tests/unit -v

# All integration tests
pytest tests/integration -v

# Integration modules marked `integration`, excluding performance tests
pytest -m "integration and not perf" -v

# Performance evaluation only
PHEBEE_EVAL_SCALE=1 pytest -m perf -v -s
```

### Using Existing Stack

To speed up local development, you can run tests against an already-deployed stack instead of deploying a new stack each time. There are two methods:

#### Method 1: Command-line flag (one-time use)

```bash
pytest tests/integration --existing-stack phebee-integration-test -v
```

This skips deployment and uses the specified stack name for that test run only.

#### Method 2: Configuration file (persistent)

Create a `.phebee-test-stack` file in the project root directory:

```bash
# From project root
echo "phebee-integration-test" > .phebee-test-stack
```

Once created, all test runs will automatically use this stack without needing to pass `--existing-stack`:

```bash
# Automatically uses stack from .phebee-test-stack
pytest tests/integration -v
```

**Stack name resolution order:**
1. `--existing-stack` command-line flag (highest priority)
2. `.phebee-test-stack` file in project root
3. Generate new stack name and deploy (slowest)

**Notes:**
- The `.phebee-test-stack` file should contain only the stack name (single line, no extra whitespace)
- This file is already in `.gitignore` and won't be committed
- Performance tests **must** be run from the project root so the file can be found

> **Data safety**: `test_reset_database.py` invokes the stack's `ResetDatabaseFunction`, which erases the DynamoDB table, the Neptune database and the Iceberg tables, including the installed ontologies and their materialized hierarchies. It is deployed in every stack. A stack that has been reset needs `UpdateHPOSFN` run again before hierarchy expansion or term-label lookup will work. When the suite targets an existing stack (by flag or file), these tests are skipped unless `PHEBEE_ALLOW_DATABASE_RESET=1` is set. Never set it for a stack holding data you need.

---

## Test Configuration

### Environment Variables

**For Performance Tests**:
- `PHEBEE_EVAL_SCALE=1` - Enable performance tests
- `PHEBEE_EVAL_SCALE_SUBJECTS` - Dataset size (default: 10000)
- `PHEBEE_EVAL_CONCURRENCY` - Concurrent workers (default: 25)
- See [Performance Testing Guide](integration/performance/README.md) for full list

**For Stack Deployment**:
- Tests use `pytest.ini` configuration
- Stack deployment controlled by `conftest.py` fixtures
- Can override with `--existing-stack` flag or `.phebee-test-stack` file

### Fixtures

Shared fixtures are defined in `conftest.py` files:
- `tests/conftest.py` - Command-line options (`--existing-stack`, `--profile`, `--config-env`)
- `tests/integration/conftest.py` - Stack deployment/resolution, AWS clients, SigV4 auth and shared resources
- `tests/integration/performance/conftest.py` - Performance test fixtures (data generation)

---

## Troubleshooting

### "Stack deployment failed"

**Cause**: SAM CLI couldn't deploy the stack during test setup.

**Solution**: Deploy manually first, then use the existing stack:
```bash
sam build && sam deploy --config-env integration-test

# Option 1: Use command-line flag
pytest tests/integration --existing-stack phebee-integration-test -v

# Option 2: Create config file (recommended for repeated testing)
echo "phebee-integration-test" > .phebee-test-stack
pytest tests/integration -v
```

### "No module named 'phebee'" (or a Lambda module such as `create_subject`)

**Cause**: pytest was not started from the project root, so the `pythonpath` entries in `pytest.ini` (`functions`, `layers/phebee-utils`, `tests/integration`) were not applied.

**Solution**: Run pytest from the project root directory:
```bash
cd /path/to/phebee
pytest tests/integration -v
```

### API tests fail with 404

**Cause**: Stack not fully deployed or API Gateway not ready.

**Solution**:
1. Check stack status: `aws cloudformation describe-stacks --stack-name phebee-integration-test`
2. Verify API Gateway URL in stack outputs
3. Wait a few minutes for resources to stabilize

---

## Contributing

When adding new tests:

1. **Unit tests**: Add to `tests/unit/` - should be fast and not require AWS
2. **Integration tests**: Add to `tests/integration/` - mark with `@pytest.mark.integration`
3. **Performance tests**: Follow patterns in `tests/integration/performance/`
4. **Update documentation**: Keep this README current with new test categories

---

## Additional Resources

- [Performance Testing Guide](integration/performance/README.md) - Comprehensive performance evaluation documentation
- [pytest.ini](../pytest.ini) - Test configuration and markers
- [Main README](../README.md) - Project overview and deployment guide
