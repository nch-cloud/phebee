# PheBee Test Suite

This directory contains PheBee's comprehensive test suite, including unit tests, integration tests, and performance evaluation infrastructure.

## Test Structure

```
tests/
├── unit/                          # Unit tests (fast, no infrastructure)
├── integration/                   # Integration tests (require deployed stack)
│   ├── api/                       # API Gateway smoke tests
│   ├── performance/               # Performance evaluation suite
│   └── *.py                       # Lambda and service integration tests
```

---

## Unit Tests

Unit tests validate individual functions and modules without requiring AWS infrastructure. These tests are fast and can run locally.

**Location**: `tests/unit/`

**What they test**:
- Athena query result parsing (`test_athena_struct_parsing.py`)
- AWS utility functions (`test_aws_utilities.py`)
- Hash consistency for data integrity (`test_hash_consistency.py`)
- OBO ontology file parsing (`test_obo_parsing.py`)
- Phenopacket data processing (`test_phenopacket_processing.py`, `test_phenopacket_utilities.py`)
- Evidence payload preparation (`test_prepare_evidence_payload.py`)
- SPARQL query utilities (`test_sparql_utilities.py`)
- String manipulation utilities (`test_string_utilities.py`)

**Running unit tests**:
```bash
# Run all unit tests
pytest tests/unit -v

# Run specific test file
pytest tests/unit/test_obo_parsing.py -v

# Run with coverage
pytest tests/unit --cov=src --cov-report=html
```

---

## Integration Tests

Integration tests validate the full system by deploying infrastructure and exercising Lambda functions, Step Functions, and API endpoints.

**Location**: `tests/integration/`

### Lambda Function Tests

Core integration tests for individual Lambda functions and orchestration workflows:

- **Project Management**: `test_create_project.py`, `test_remove_project.py`
- **Subject Operations**: `test_create_subject.py`, `test_get_subject.py`, `test_remove_subject.py`
- **Evidence Operations**: `test_create_evidence.py`, `test_get_evidence.py`, `test_remove_evidence.py`
- **Query Operations**: `test_get_subjects_pheno.py`, `test_get_subject_term_info.py`, `test_query_evidence_by_run.py`
- **Ontology Updates**: `test_update_hpo_sfn.py`, `test_update_mondo_sfn.py`, `test_update_eco_sfn.py`
- **Bulk Operations**: `test_bulk_import_statemachine.py`, `test_import_phenopackets_statemachine.py`
- **Materialization**: `test_materialize_project_subject_terms.py`
- **Utilities**: `test_get_source_info.py`, `test_reset_database.py`

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
2. Python dependencies: `pytest`, `boto3`, `requests`, `aws-requests-auth`

### Basic Usage

```bash
# Deploy the stack first
sam build && sam deploy --config-env integration-test

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
| `unit` | Fast unit tests, no infrastructure | `pytest -m unit` |
| `integration` | Requires deployed stack | `pytest -m integration` |
| `api` | API Gateway tests | `pytest -m api` |
| `perf` | Performance evaluation tests (slow) | `pytest -m perf` |

**Common patterns**:
```bash
# Fast development cycle (unit tests only)
pytest -m unit -v

# All integration tests (including API tests)
pytest -m integration -v

# Skip performance tests (faster feedback)
pytest -m "integration and not perf" -v

# Performance evaluation only
pytest -m perf -v -s
```

### Using Existing Stack

To run tests against an already-deployed stack:

```bash
pytest tests/integration --existing-stack phebee-integration-test -v
```

This skips deployment and uses the specified stack name, making test suite execution significantly faster.

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
- Can override with `--existing-stack` flag

### Fixtures

Shared fixtures are defined in `conftest.py` files:
- `tests/conftest.py` - Root fixtures (stack deployment, AWS clients)
- `tests/integration/conftest.py` - Integration test fixtures (resources, auth)
- `tests/integration/performance/conftest.py` - Performance test fixtures (data generation)

---

## Troubleshooting

### "Stack deployment failed"

**Cause**: SAM CLI couldn't deploy the stack during test setup.

**Solution**: Deploy manually first:
```bash
sam build && sam deploy --config-env integration-test
pytest tests/integration --existing-stack phebee-integration-test -v
```

### "No module named 'src'"

**Cause**: Python path doesn't include the project root.

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
