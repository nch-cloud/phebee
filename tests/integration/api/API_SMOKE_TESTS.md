# API Gateway Smoke Tests

## Purpose

This directory contains **smoke tests** for the API Gateway layer. These tests validate the integration between API Gateway and Lambda functions, not the business logic (which is covered by the main integration tests).

## Philosophy

### What These Tests ARE
- ✅ API Gateway → Lambda integration validation
- ✅ Endpoint reachability and routing
- ✅ Authentication/authorization (SigV4, Cognito)
- ✅ HTTP status codes and headers
- ✅ Basic response structure
- ✅ CORS configuration
- ✅ Contract compliance with [api.yaml](../../../../api.yaml)

### What These Tests ARE NOT
- ❌ Comprehensive business logic testing (use main integration tests)
- ❌ Edge case validation (use Lambda tests)
- ❌ Performance testing (use dedicated performance tests)

## Coverage Goal

**One or two smoke tests per API endpoint:**
- One happy path (200 response)
- One common error case (404, 400, etc.)

**Total target: ~15-25 tests** (not hundreds)

## Test Structure

```
tests/integration/api/
├── API_SMOKE_TESTS.md       # This file
├── conftest.py              # Shared fixtures (api_base_url, sigv4_auth)
├── test_api_projects.py     # /projects endpoints
├── test_api_subjects.py     # /subjects endpoints
├── test_api_evidence.py     # /evidence endpoints
├── test_api_sources.py      # /source endpoints
└── test_api_queries.py      # Query endpoints (get_subjects_pheno, etc.)
```

## Test Template

```python
"""
Smoke tests for /projects API endpoints.

References:
- api.yaml: /projects, /projects/{project_id}
- Lambda: CreateProjectFunction, GetProjectFunction (tested separately)
"""

import pytest
import requests
import uuid

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]


def test_create_project_endpoint(api_base_url, sigv4_auth):
    """
    Smoke test: POST /projects endpoint via API Gateway.

    Validates:
    - Endpoint is reachable
    - Auth works
    - Returns 200
    - Response structure is valid
    """
    response = requests.post(
        f"{api_base_url}/projects",
        json={"project_id": f"api-test-{uuid.uuid4().hex[:8]}"},
        auth=sigv4_auth
    )

    # Validate API Gateway layer only
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"
    assert "Access-Control-Allow-Origin" in response.headers  # CORS

    # Basic structure check (not comprehensive validation)
    body = response.json()
    assert "project" in body


def test_get_project_not_found(api_base_url, sigv4_auth):
    """
    Smoke test: GET /projects/{id} returns 404 for nonexistent project.

    Validates:
    - Error handling through API Gateway
    - 404 status code is correct
    """
    response = requests.get(
        f"{api_base_url}/projects/nonexistent-project-id",
        auth=sigv4_auth
    )

    assert response.status_code == 404
    body = response.json()
    assert "error" in body or "message" in body
```

## Key Principles

### 1. Minimal Scope
- Test the **integration layer**, not business logic
- Each test should complete in <5 seconds
- Avoid complex test data setup

### 2. Test Markers
All tests should be marked:
```python
pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.slow]
```

This allows selective execution:
```bash
# Run all tests EXCEPT API Gateway smoke tests (default/fast)
pytest tests/integration -m "not api"

# Run ONLY API Gateway smoke tests
pytest tests/integration/api -m api

# Run everything (includes API smoke tests)
pytest tests/integration
```

### 3. Reference api.yaml
- Each test file should document which endpoints it covers
- Use comments to link back to api.yaml sections
- Helps ensure test coverage matches API spec

### 4. Shared Fixtures (conftest.py)
```python
import pytest
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth

@pytest.fixture(scope="session")
def api_base_url(physical_resources):
    """API Gateway base URL from CloudFormation outputs."""
    return physical_resources["ApiGatewayUrl"].rstrip("/")

@pytest.fixture(scope="session")
def sigv4_auth(physical_resources):
    """AWS SigV4 authentication for API Gateway."""
    session = boto3.Session()
    credentials = session.get_credentials()
    region = session.region_name or "us-east-1"

    return AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_token=credentials.token,
        aws_host=physical_resources["ApiGatewayUrl"].split("//")[1].split("/")[0],
        aws_region=region,
        aws_service="execute-api"
    )
```

## When to Run

### Development (default)
```bash
# Fast: Skip API Gateway smoke tests
pytest tests/integration -m "not api"
```

### Pre-Deployment / CI Pipeline
```bash
# Full: Include API Gateway smoke tests
pytest tests/integration
```

### Manual Testing
```bash
# Just API smoke tests (quick validation)
pytest tests/integration/api -m api
```

## Coverage Checklist

Based on [api.yaml](../../../../api.yaml), ensure smoke tests exist for:

### Projects
- [ ] POST /projects (create)
- [ ] GET /projects/{project_id} (get)
- [ ] DELETE /projects/{project_id} (delete)

### Subjects
- [ ] POST /subjects (create)
- [ ] GET /subjects/{subject_id} (get by ID)
- [ ] GET /subjects?project_subject_iri={iri} (get by IRI)
- [ ] DELETE /subjects/{subject_id} (delete)

### Evidence
- [ ] POST /evidence (create)
- [ ] GET /evidence/{evidence_id} (get)
- [ ] DELETE /evidence/{evidence_id} (delete)

### Sources
- [ ] GET /source/{source_id} (get source info)

### Queries
- [ ] GET /subjects (query subjects by phenotype)
- [ ] GET /subjects/{subject_id}/term/{term_iri} (get subject term info)
- [ ] GET /evidence/run/{run_id} (query evidence by run)

## What Gets Caught

These smoke tests will catch:

1. **API Gateway Misconfigurations**
   - Wrong Lambda integration target
   - Missing Lambda permissions
   - Broken resource paths

2. **Authentication Issues**
   - IAM authorizer not configured
   - Wrong IAM policies
   - Missing execute-api permissions

3. **CORS Problems**
   - Missing CORS headers
   - Wrong allowed origins/methods

4. **Integration Mapping Errors**
   - Wrong request/response transformations
   - Missing required parameters
   - Incorrect content-type handling

5. **Deployment Issues**
   - API Gateway stage not deployed
   - Lambda version/alias mismatch

## What Doesn't Get Caught

These smoke tests will NOT catch (use main integration tests for these):

- Business logic bugs
- Data validation edge cases
- Complex query scenarios
- Race conditions
- Database inconsistencies
- Iceberg/DynamoDB integration issues

## Maintenance

- **Add a test** when a new endpoint is added to api.yaml
- **Update tests** if API contract changes (status codes, headers)
- **Keep minimal** - resist the urge to add comprehensive validation
- **Review annually** - remove tests for deprecated endpoints

## Benefits

1. **Pre-Deployment Confidence**: Quick sanity check before releases
2. **API Contract Validation**: Ensures api.yaml matches reality
3. **External Consumer Support**: Shows how to actually call the API
4. **Integration Layer Coverage**: Catches AWS config issues

## Example Workflow

```bash
# Day-to-day development (fast)
$ pytest tests/integration -m "not api"
=================== 200 passed in 45.2s ===================

# Before deploying to staging
$ pytest tests/integration/api -m api
=================== 18 passed in 12.4s ====================

# Full regression before production
$ pytest tests/integration
=================== 218 passed in 57.6s ===================
```

---

**Remember**: These are smoke tests, not comprehensive tests. Keep them simple, fast, and focused on the API Gateway integration layer.
