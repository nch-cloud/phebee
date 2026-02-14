# PheBee Integration Testing Guide

## Overview

This guide explains how to run integration tests for PheBee, including stack management, test execution, and best practices.

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Stack Management](#stack-management)
3. [Running Tests](#running-tests)
4. [Test Structure](#test-structure)
5. [Writing New Tests](#writing-new-tests)
6. [CI/CD Integration](#cicd-integration)
7. [Troubleshooting](#troubleshooting)

---

## Testing Philosophy

PheBee integration tests verify end-to-end functionality across multiple AWS services:

- **DynamoDB** - Subject identity and project mappings
- **Neptune** - Graph relationships (projects, subjects, term links)
- **Iceberg/Athena** - Immutable evidence storage and analytical tables
- **S3** - Phenopacket storage, TTL files, bulk data
- **Step Functions** - Workflow orchestration
- **EventBridge** - Event-driven architecture

Tests ensure data consistency across all systems and validate complex workflows like phenopacket import and cascade deletion.

---

## Stack Management

### Option 1: Use an Existing Stack (Recommended for Development)

Using an existing stack is faster and more cost-effective for iterative development:

```bash
# Run tests against your development stack
pytest --existing-stack phebee-dev

# With database reset before tests
pytest --existing-stack phebee-dev --force-database-reset

# Using a specific AWS profile
pytest --existing-stack phebee-dev --profile myprofile
```

**Advantages:**
- Fast test execution (no stack creation/deletion overhead)
- Easier debugging with persistent infrastructure
- Lower cost for repeated test runs

**When to use:**
- Local development
- Debugging specific test failures
- Rapid iteration on test cases

**Important:** When using `--existing-stack`, consider using `--force-database-reset` to ensure clean test state. This resets Neptune and DynamoDB data but preserves the infrastructure.

### Option 2: Create a Fresh Stack (Recommended for CI/CD)

Create a new ephemeral stack for each test run:

```bash
# Create stack, run tests, destroy stack
pytest --config-env integration-test

# With specific AWS profile
pytest --config-env integration-test --profile myprofile
```

**Advantages:**
- Clean, isolated test environment
- Verifies full stack deployment process
- No state contamination between test runs

**When to use:**
- CI/CD pipelines
- Release validation
- Testing infrastructure changes

**Note:** Stack creation takes 5-10 minutes, and deletion takes another 5 minutes.

### Stack Configuration

The `--config-env` parameter specifies which SAM configuration to use from `samconfig.toml`:

```toml
[integration-test.deploy.parameters]
stack_name = "phebee-it"
region = "us-east-2"
capabilities = "CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND"
parameter_overrides = "Environment=integration-test"
```

Common configurations:
- `integration-test` - CI/CD testing environment
- `dev` - Development environment
- `staging` - Pre-production environment

---

## Running Tests

### Basic Test Execution

```bash
# Run all integration tests
pytest tests/integration/

# Run specific test file
pytest tests/integration/test_evidence.py

# Run specific test function
pytest tests/integration/test_evidence.py::test_create_evidence_minimal

# Run tests matching pattern
pytest tests/integration/ -k "evidence"

# Run with verbose output
pytest tests/integration/ -v

# Run with detailed output and no capture
pytest tests/integration/ -vv -s
```

### Test Markers

PheBee uses pytest markers to categorize tests:

```bash
# Run only integration tests
pytest -m integration

# Run only API tests
pytest -m api

# Run slow tests
pytest -m slow

# Run performance tests (requires --run-performance flag)
pytest -m perf --run-performance

# Exclude slow tests
pytest -m "not slow"

# Combine markers
pytest -m "integration and not slow"
```

### Performance Tests

Performance tests are skipped by default to speed up regular test runs:

```bash
# Run performance tests with default parameters
pytest --run-performance

# Customize performance test parameters
pytest --run-performance --num-subjects 100 --num-terms 2000

# Use existing bulk import data
pytest --run-performance --existing-run-id run-abc-123
```

### Ontology Updates

Some tests require ontology updates (HPO, MONDO, ECO) which can be slow:

```bash
# Skip ontology updates (faster, but some tests may fail)
pytest --skip-ontology-updates

# Include ontology updates (default, but slower)
pytest
```

### Complete Example Commands

**Fast development cycle:**
```bash
# Quick tests against existing stack, skip slow operations
pytest --existing-stack phebee-dev --skip-ontology-updates -m "not slow"
```

**Full validation before deployment:**
```bash
# Create fresh stack, run all tests including performance
pytest --config-env integration-test --run-performance
```

**CI/CD pipeline:**
```bash
# Clean environment, all tests except performance
pytest --config-env ci-test --force-database-reset
```

---

## Test Structure

### Session-Scoped Fixtures

PheBee tests use session-scoped fixtures to share expensive setup across tests:

```python
@pytest.fixture(scope="session")
def cloudformation_stack(request, aws_session, profile_name):
    """Manages CloudFormation stack lifecycle"""
    # Creates or uses existing stack
    # Yields stack name for tests
    # Tears down if created by test
```

Key session fixtures:
- `cloudformation_stack` - Stack name (creates/destroys as needed)
- `stack_outputs` - CloudFormation outputs (endpoints, resource names)
- `physical_resources` - Mapping of logical to physical resource IDs
- `aws_session` - Configured boto3 session
- `update_hpo` / `update_mondo` / `update_eco` - Ontology updates

### Test-Scoped Fixtures

Test-scoped fixtures provide isolated test data:

```python
@pytest.fixture
def test_project_id(cloudformation_stack):
    """Creates a test project, yields ID, cleans up after test"""
    lambda_client = get_client("lambda")
    project_id = f"test_project_{uuid.uuid4().hex[:8]}"

    # Create project
    create_response = lambda_client.invoke(...)

    yield project_id

    # Cleanup
    delete_response = lambda_client.invoke(...)
```

Common patterns:
- **Setup** - Create test data
- **Yield** - Provide data to test
- **Cleanup** - Delete test data

### Environment Variables

The `set_env_variables_from_stack` fixture automatically sets environment variables from stack outputs:

```python
os.environ["PheBeeDynamoTable"] = outputs["DynamoDBTableName"]
os.environ["Region"] = outputs["Region"]
os.environ["NeptuneEndpoint"] = outputs["NeptuneEndpoint"]
os.environ["ICEBERG_DATABASE"] = outputs["AthenaDatabase"]
```

These variables are available to all tests and utility functions.

---

## Writing New Tests

### Test File Organization

```
tests/integration/
├── conftest.py                      # Session-scoped fixtures
├── test_<lambda_name>.py            # Tests for specific lambda
├── api/
│   ├── conftest.py                  # API-specific fixtures
│   └── test_<endpoint>_api.py       # API endpoint tests
└── performance/
    ├── conftest.py                  # Performance test fixtures
    └── test_<scenario>_performance.py
```

### Test Template

```python
import pytest
import json
from phebee.utils.aws import get_client

@pytest.fixture
def test_data(physical_resources):
    """Setup test data"""
    # Create prerequisites
    yield data
    # Cleanup

@pytest.mark.integration
def test_lambda_function_happy_path(cloudformation_stack, physical_resources, test_data):
    """
    Test description explaining what this validates
    """
    lambda_client = get_client("lambda")

    # Invoke lambda
    response = lambda_client.invoke(
        FunctionName=physical_resources["FunctionLogicalId"],
        Payload=json.dumps({"key": "value"})
    )

    # Parse response
    result = json.loads(response["Payload"].read())

    # Assertions
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["expected_field"] == "expected_value"

    # Verify in dependent systems
    # - DynamoDB query
    # - Neptune query
    # - Iceberg query
```

### Multi-System Verification

Always verify data consistency across all affected systems:

```python
def test_create_evidence_consistency(physical_resources):
    """Verify evidence creation updates all systems"""

    # Create evidence
    evidence_response = invoke_lambda(...)
    evidence_id = evidence_response["evidence_id"]

    # 1. Verify in Iceberg
    athena_client = get_client("athena")
    result = execute_athena_query(
        f"SELECT * FROM evidence WHERE evidence_id = '{evidence_id}'"
    )
    assert len(result) == 1

    # 2. Verify term link in Neptune
    from phebee.utils.neptune import execute_query
    sparql = f"""
        SELECT ?termlink WHERE {{
            ?termlink <http://...#hasEvidence> <.../{evidence_id}>
        }}
    """
    neptune_result = execute_query(sparql)
    assert len(neptune_result["results"]["bindings"]) == 1

    # 3. Verify analytical tables updated
    result = execute_athena_query(
        f"SELECT evidence_count FROM subject_terms_by_subject ..."
    )
    assert result[0]["evidence_count"] == 1
```

### Error Testing

Test error conditions and edge cases:

```python
@pytest.mark.integration
def test_lambda_missing_required_field(physical_resources):
    """Test lambda returns 400 when required field is missing"""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({})  # Missing required fields
    )

    result = json.loads(response["Payload"].read())
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "required" in body["message"].lower()

@pytest.mark.integration
def test_lambda_not_found(physical_resources):
    """Test lambda returns 404 for non-existent resource"""
    # Test with ID that doesn't exist
    # Verify 404 response
    # Verify no data was modified
```

### Cleanup Best Practices

Always clean up test data to prevent interference:

```python
@pytest.fixture
def test_evidence(physical_resources):
    """Create test evidence, clean up after"""
    created_evidence_ids = []

    def _create_evidence(payload):
        response = invoke_lambda(...)
        evidence_id = response["evidence_id"]
        created_evidence_ids.append(evidence_id)
        return evidence_id

    yield _create_evidence

    # Cleanup all created evidence
    for evidence_id in created_evidence_ids:
        lambda_client.invoke(
            FunctionName=physical_resources["RemoveEvidenceFunction"],
            Payload=json.dumps({"evidence_id": evidence_id})
        )
```

### Test Ordering

Use markers to control test execution order when needed:

```python
@pytest.mark.run_first
def test_prerequisite_setup():
    """Runs before other tests"""
    pass

@pytest.mark.run_last
def test_final_validation():
    """Runs after other tests"""
    pass
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on:
  pull_request:
    branches: [ main ]
  push:
    branches: [ main ]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
    - uses: actions/checkout@v3

    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.12'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest

    - name: Setup AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-2

    - name: Install SAM CLI
      run: |
        pip install aws-sam-cli

    - name: Run integration tests
      run: |
        pytest tests/integration/ \
          --config-env ci-test \
          --skip-ontology-updates \
          -m "not slow and not perf" \
          -v

    - name: Upload test results
      if: always()
      uses: actions/upload-artifact@v3
      with:
        name: test-results
        path: test-results/
```

### Performance Testing in CI

Run performance tests nightly or on-demand:

```yaml
name: Performance Tests

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily
  workflow_dispatch:

jobs:
  performance-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 120

    steps:
    - uses: actions/checkout@v3
    - name: Setup and run performance tests
      run: |
        pytest tests/integration/performance/ \
          --config-env perf-test \
          --run-performance \
          --num-subjects 1000 \
          --num-terms 5000
```

---

## Troubleshooting

### Common Issues

**Issue: Stack creation times out**
```bash
# Check CloudFormation events for details
aws cloudformation describe-stack-events \
  --stack-name phebee-it-abc \
  --max-items 20

# Common causes:
# - Neptune cluster takes 5-10 minutes to create
# - Custom resources may have errors in Lambda functions
```

**Issue: Tests fail with authentication errors**
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check profile configuration
aws configure list --profile myprofile

# Test with explicit profile
pytest --existing-stack phebee-dev --profile myprofile
```

**Issue: Database has stale data**
```bash
# Reset database when using existing stack
pytest --existing-stack phebee-dev --force-database-reset

# Or manually reset
python tests/integration/test_reset_database.py
```

**Issue: Lambda timeout errors**
```bash
# Some operations (bulk import, materialization) take time
# Check Lambda logs in CloudWatch
aws logs tail /aws/lambda/phebee-dev-FunctionName --follow

# Increase Lambda timeout in template.yaml if needed
```

**Issue: Iceberg table not found**
```bash
# Tables may not be created yet
# Check if table creation completed
aws glue get-table --database-name phebee --table-name evidence

# Recreate tables if needed
pytest tests/integration/test_cloudformation_stack.py
```

### Debugging Tips

**1. Use verbose output**
```bash
pytest -vv -s tests/integration/test_evidence.py
```

**2. Run single test with print statements**
```python
def test_debug():
    print(f"Stack outputs: {stack_outputs}")
    print(f"Evidence ID: {evidence_id}")
    # Test code
```

**3. Keep stack alive for inspection**
```bash
# Use existing stack so it doesn't get deleted
pytest --existing-stack phebee-debug-$(date +%s)

# Or comment out stack deletion in conftest.py temporarily
```

**4. Check CloudWatch Logs**
```bash
# View recent Lambda logs
aws logs tail /aws/lambda/phebee-dev-CreateEvidenceFunction --follow

# Query specific time range
aws logs filter-log-events \
  --log-group-name /aws/lambda/phebee-dev-CreateEvidenceFunction \
  --start-time $(date -d '5 minutes ago' +%s)000
```

**5. Query databases directly**
```python
# Test in isolation
from phebee.utils.neptune import execute_query
from phebee.utils.iceberg import query_iceberg_evidence

# Check Neptune
result = execute_query("SELECT * WHERE { ?s ?p ?o } LIMIT 10")

# Check Iceberg via Athena
evidence = query_iceberg_evidence("SELECT * FROM evidence LIMIT 10")
```

### Getting Help

- **Documentation**: See test plan files in `tests/integration-new/`
- **Examples**: Review existing tests in `tests/integration/`
- **Issues**: Check GitHub issues for known problems
- **Logs**: Always check CloudWatch logs for detailed error messages

---

## Best Practices Summary

✅ **DO:**
- Use `--existing-stack` for rapid development
- Always clean up test data in fixtures
- Verify data across all affected systems
- Use descriptive test names
- Add markers (`@pytest.mark.integration`, etc.)
- Test error conditions and edge cases
- Document complex test scenarios

❌ **DON'T:**
- Leave test data in shared stacks
- Skip cleanup in fixtures
- Assume eventual consistency (verify explicitly)
- Hard-code resource IDs
- Ignore test failures in CI
- Run performance tests in regular test runs
- Share state between test functions

---

## Next Steps

1. Review the test plan files in `tests/integration-new/` for detailed scenarios
2. Implement high-priority tests for core CRUD operations
3. Add API endpoint tests for user-facing functionality
4. Set up CI/CD pipeline with automated testing
5. Add performance benchmarks for critical paths
6. Monitor test execution time and optimize slow tests
