# Neptune-Free Integration Testing Guide

## Overview

Many test scenarios in this directory include "Neptune verification" steps that assume direct Neptune access. However, **Neptune is locked down at the network/security level** - even in dev/test environments.

This guide explains the critical architectural constraint and how to adapt test scenarios.

## The Golden Rule: All Neptune Access Through Lambdas

**Neptune is network-isolated. Tests CANNOT directly access Neptune in any way.**

```
❌ Test → Neptune (Blocked by security/network)
✅ Test → Lambda → Neptune (Correct pattern)
```

### What This Means

**Tests CANNOT directly call ANY Neptune utilities:**
- ❌ `from phebee.utils.neptune import execute_query` - Blocked
- ❌ `from phebee.utils.neptune import execute_update` - Blocked
- ❌ `from phebee.utils.neptune import start_load` - Blocked
- ❌ `from phebee.utils.neptune import get_load_status` - Blocked
- ❌ Any direct SPARQL queries - Blocked

**Tests CAN invoke Lambdas that internally use Neptune:**
- ✅ Invoke `start_neptune_bulk_load` (Lambda calls `start_load()` internally)
- ✅ Invoke `check_neptune_bulk_load_status` (Lambda calls `get_load_status()` internally)
- ✅ Invoke `create_evidence` (Lambda calls `execute_update()` internally)
- ✅ Invoke `get_subject` (Lambda calls `execute_query()` internally)
- ✅ Invoke `reset_database` (Lambda calls `reset_neptune_database()` internally)

**All Neptune operations must go through Lambda functions, no exceptions.**

---

## Core Principle: Verify Through APIs, Not Direct Database Access

Instead of querying Neptune directly with SPARQL, use the PheBee Lambda functions to verify state:

### ❌ Don't Do This (Direct Neptune Access)
```python
from phebee.utils.neptune import execute_query

# BAD: Direct SPARQL query
sparql = f"""
    SELECT ?termlink WHERE {{
        <{subject_iri}> <http://.../hasTermLink> ?termlink .
    }}
"""
result = execute_query(sparql)
assert len(result["results"]["bindings"]) == 1
```

### ✅ Do This Instead (API Verification)
```python
# GOOD: Use Lambda API to verify state
response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectFunction"],
    Payload=json.dumps({"subject_id": subject_id})
)

result = json.loads(response["Payload"].read())
assert result["statusCode"] == 200

body = json.loads(result["body"])
assert len(body["terms"]) == 1
assert body["terms"][0]["term_iri"] == expected_term_iri
```

---

## Test Adaptations by Lambda Function

### 1. Create Evidence Tests

**Original Neptune Verification:**
```
- Term link exists with computed termlink_id
- Term link connects subject_iri to term_iri
- Creator IRI referenced in term link
```

**Adapted Verification:**
```python
# Instead of querying Neptune, verify through get_subject_term_info
response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectTermInfoFunction"],
    Payload=json.dumps({
        "subject_id": subject_id,
        "term_iri": term_iri,
        "qualifiers": qualifiers
    })
)

result = json.loads(response["Payload"].read())
assert result["statusCode"] == 200

body = json.loads(result["body"])
assert body["termlink_id"] == expected_termlink_id
assert body["term_iri"] == term_iri
assert body["evidence_count"] > 0
```

**Alternative: Query analytical tables (Iceberg/Athena)**
```python
# Analytical tables are derived from Neptune but accessible via Athena
from phebee.utils.iceberg import query_iceberg_evidence

# Query subject_terms_by_subject table
query = f"""
    SELECT term_iri, evidence_count
    FROM subject_terms_by_subject
    WHERE subject_id = '{subject_id}'
"""
result = execute_athena_query(query)
assert len(result) == 1
assert result[0]["term_iri"] == term_iri
```

---

### 2. Create Project Tests

**Original Neptune Verification:**
```
- Query Neptune for project IRI
- Project node has correct label
- Project node type is phebee:Project
```

**Adapted Verification:**
```python
# Projects don't have a dedicated "get" endpoint,
# but we can verify through subject creation
subject_response = lambda_client.invoke(
    FunctionName=physical_resources["CreateSubjectFunction"],
    Payload=json.dumps({
        "project_id": project_id,
        "project_subject_id": "test-subject"
    })
)

result = json.loads(subject_response["Payload"].read())
# If project doesn't exist in Neptune, subject creation would fail
assert result["statusCode"] == 200

# Cleanup test subject
lambda_client.invoke(
    FunctionName=physical_resources["RemoveSubjectFunction"],
    Payload=json.dumps({
        "project_subject_iri": result["body"]["subject"]["iri"]
    })
)
```

**Alternative: Check through get_subjects_pheno**
```python
# Query subjects for the project
response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectsPhenoFunction"],
    Payload=json.dumps({"project_id": project_id})
)

# If project exists in Neptune, query succeeds (even with 0 subjects)
result = json.loads(response["Payload"].read())
assert result["statusCode"] == 200
```

---

### 3. Create Subject Tests

**Original Neptune Verification:**
```
- Subject node exists in Neptune
- Subject linked to project via phebee:inProject
- project_subject_iri exists with sameAs link
```

**Adapted Verification:**
```python
# Use get_subject to verify Neptune state indirectly
response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectFunction"],
    Payload=json.dumps({
        "project_subject_iri": project_subject_iri
    })
)

result = json.loads(response["Payload"].read())
assert result["statusCode"] == 200

body = json.loads(result["body"])
assert body["subject"]["iri"] == expected_subject_iri
assert body["subject"]["projects"][project_id] == project_subject_id
```

---

### 4. Remove Evidence Tests

**Original Neptune Verification:**
```
- When evidence_count reaches 0:
  - Term link deleted from Neptune
  - Analytical tables updated
```

**Adapted Verification:**
```python
# Create evidence, then remove it
evidence_response = create_evidence(...)
evidence_id = evidence_response["evidence_id"]

# Remove evidence
remove_response = lambda_client.invoke(
    FunctionName=physical_resources["RemoveEvidenceFunction"],
    Payload=json.dumps({"evidence_id": evidence_id})
)

assert remove_response["statusCode"] == 200

# Verify term link is gone by checking get_subject_term_info
term_info_response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectTermInfoFunction"],
    Payload=json.dumps({
        "subject_id": subject_id,
        "term_iri": term_iri,
        "qualifiers": []
    })
)

result = json.loads(term_info_response["Payload"].read())
# Should return 404 if term link was deleted
assert result["statusCode"] == 404
```

---

### 5. Remove Subject Tests

**Original Neptune Verification:**
```
- project_subject_iri deleted from Neptune
- If last mapping: subject_iri deleted from Neptune
- Term links deleted when cascade happens
```

**Adapted Verification:**
```python
# Remove subject
remove_response = lambda_client.invoke(
    FunctionName=physical_resources["RemoveSubjectFunction"],
    Payload=json.dumps({
        "project_subject_iri": project_subject_iri
    })
)

assert remove_response["statusCode"] == 200

# Verify subject is gone
get_response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectFunction"],
    Payload=json.dumps({
        "project_subject_iri": project_subject_iri
    })
)

result = json.loads(get_response["Payload"].read())
assert result["statusCode"] == 404  # Subject not found

# Verify in DynamoDB (no Neptune access needed)
dynamodb = boto3.client('dynamodb')
response = dynamodb.query(
    TableName=os.environ["PheBeeDynamoTable"],
    KeyConditionExpression='PK = :pk AND SK = :sk',
    ExpressionAttributeValues={
        ':pk': {'S': f'PROJECT#{project_id}'},
        ':sk': {'S': f'SUBJECT#{project_subject_id}'}
    }
)
assert len(response['Items']) == 0  # Mapping removed
```

---

### 6. Remove Project Tests

**Original Neptune Verification:**
```
- Neptune named graph cleared
- All subject IRIs deleted
- All term links deleted
```

**Adapted Verification:**
```python
# Remove project
remove_response = lambda_client.invoke(
    FunctionName=physical_resources["RemoveProjectFunction"],
    Payload=json.dumps({"project_id": project_id})
)

assert remove_response["statusCode"] == 200

# Verify subjects are gone - try to get subjects for project
get_subjects_response = lambda_client.invoke(
    FunctionName=physical_resources["GetSubjectsPhenoFunction"],
    Payload=json.dumps({"project_id": project_id})
)

result = json.loads(get_subjects_response["Payload"].read())
# Project graph is cleared, so this should fail or return empty
assert result["statusCode"] in [200, 404]
if result["statusCode"] == 200:
    body = json.loads(result["body"])
    assert len(body.get("subjects", [])) == 0

# Verify in DynamoDB - all mappings should be gone
dynamodb = boto3.client('dynamodb')
response = dynamodb.query(
    TableName=os.environ["PheBeeDynamoTable"],
    IndexName='GSI1',
    KeyConditionExpression='GSI1PK = :pk',
    ExpressionAttributeValues={
        ':pk': {'S': f'PROJECT#{project_id}'}
    }
)
assert len(response['Items']) == 0
```

---

## General Verification Strategies

### Strategy 1: Use Read APIs After Write Operations

Every write operation should have a corresponding read operation:

| Write Operation | Read Verification |
|----------------|-------------------|
| create_evidence | get_evidence, get_subject, get_subject_term_info |
| create_subject | get_subject |
| create_project | create_subject (validates project exists) |
| remove_evidence | get_evidence (404), get_subject_term_info (404 or reduced count) |
| remove_subject | get_subject (404), DynamoDB query |
| remove_project | get_subjects_pheno (empty), DynamoDB GSI1 query |

### Strategy 2: Query Analytical Tables (Athena/Iceberg)

Analytical tables are derived from Neptune data and can be queried via Athena:

```python
from phebee.utils.iceberg import query_iceberg_evidence
import os

# Query subject_terms_by_subject
database = os.environ["ICEBERG_DATABASE"]
query = f"""
    SELECT subject_id, term_iri, evidence_count, last_updated
    FROM {database}.subject_terms_by_subject
    WHERE subject_id = '{subject_id}'
"""

# Execute via Athena
athena_client = boto3.client('athena')
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': 's3://...'}
)

# Wait for results and verify
```

**Benefits:**
- No Neptune access required
- Validates data flow from Neptune → Analytical tables
- Tests the actual query path used by applications

### Strategy 3: Verify Through Side Effects

Test cascading behaviors by observing downstream effects:

```python
# Test: Removing last evidence should delete term link

# 1. Create subject with one piece of evidence
create_evidence(subject_id, term_iri, ...)

# 2. Verify term link exists via get_subject
subject = get_subject(subject_id)
assert any(t["term_iri"] == term_iri for t in subject["terms"])

# 3. Remove the evidence
remove_evidence(evidence_id)

# 4. Verify term link is gone via get_subject
subject = get_subject(subject_id)
assert not any(t["term_iri"] == term_iri for t in subject["terms"])

# Neptune was updated (term link deleted) but we verified it through API
```

### Strategy 4: Test at System Boundaries

Focus on testing the interfaces, not internal state:

```python
# Instead of checking if Neptune has exact triples,
# verify the system behaves correctly end-to-end

# Create subject
subject = create_subject(project_id, project_subject_id)

# Add evidence
evidence = create_evidence(subject["subject_id"], term_iri, ...)

# Query subject - should include the term
retrieved_subject = get_subject(subject["subject_id"])
assert term_iri in [t["term_iri"] for t in retrieved_subject["terms"]]

# Export to phenopacket - should include the term
phenopackets = get_subjects_pheno(project_id, output_format="phenopackets")
phenopacket = find_phenopacket(phenopackets, project_subject_id)
assert term_iri in [f["type"]["id"] for f in phenopacket["phenotypicFeatures"]]
```

---

## DynamoDB Access is Allowed

Unlike Neptune, **DynamoDB can be accessed directly** in tests for verification:

```python
import boto3
import os

dynamodb = boto3.client('dynamodb')
table_name = os.environ["PheBeeDynamoTable"]

# Verify subject mapping exists
response = dynamodb.get_item(
    TableName=table_name,
    Key={
        'PK': {'S': f'SUBJECT#{subject_id}'},
        'SK': {'S': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'}
    }
)

assert 'Item' in response
assert response['Item']['GSI1PK']['S'] == f'PROJECT#{project_id}'
```

**Why DynamoDB is OK but Neptune is not:**
- DynamoDB is the source of truth for identity/mappings
- Direct queries don't expose graph structure
- Lower security risk
- Standard boto3 client access

---

## Test Plan Adaptation Checklist

When implementing tests from the test plans, replace Neptune verification sections:

- [ ] Replace "Query Neptune for..." with Lambda API calls
- [ ] Use `get_subject`, `get_evidence`, `get_subject_term_info` for read verification
- [ ] Query analytical tables via Athena instead of Neptune graph
- [ ] Verify through DynamoDB for mapping/identity checks
- [ ] Test cascading behaviors through observable side effects
- [ ] Focus on system behavior, not internal graph structure

---

## Example: Complete Test Adaptation

**Original Test Plan (with Neptune verification):**

```markdown
### Test: Create Evidence with Qualifiers

**Action**: Create evidence with qualifiers: ["negated"]

**Assertions**:
- Response 201
- Evidence stored in Iceberg
- **Neptune verification:**
  - **Term link exists with qualifier**
  - **Qualifier value set to "negated"**
  - **termlink_id includes qualifier in hash**
- Analytical tables updated
```

**Adapted Test (Neptune-free):**

```python
@pytest.mark.integration
def test_create_evidence_with_qualifiers(physical_resources):
    """Test evidence creation with qualifiers updates all systems"""

    # Create subject
    subject = create_subject(project_id, project_subject_id)
    subject_id = subject["subject_id"]

    # Create evidence with negated qualifier
    evidence_response = lambda_client.invoke(
        FunctionName=physical_resources["CreateEvidenceFunction"],
        Payload=json.dumps({
            "subject_id": subject_id,
            "term_iri": term_iri,
            "creator_id": "test-user",
            "qualifiers": ["negated"]
        })
    )

    result = json.loads(evidence_response["Payload"].read())
    assert result["statusCode"] == 201

    body = json.loads(result["body"])
    evidence_id = body["evidence_id"]

    # VERIFICATION 1: Check evidence in Iceberg via get_evidence
    get_evidence_response = lambda_client.invoke(
        FunctionName=physical_resources["GetEvidenceFunction"],
        Payload=json.dumps({"evidence_id": evidence_id})
    )

    evidence_result = json.loads(get_evidence_response["Payload"].read())
    assert evidence_result["statusCode"] == 200

    evidence_body = json.loads(evidence_result["body"])
    assert evidence_body["qualifiers"] == ["negated"]

    # VERIFICATION 2: Check term link via get_subject_term_info (replaces Neptune query)
    term_info_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectTermInfoFunction"],
        Payload=json.dumps({
            "subject_id": subject_id,
            "term_iri": term_iri,
            "qualifiers": ["negated"]  # Include qualifiers in lookup
        })
    )

    term_info_result = json.loads(term_info_response["Payload"].read())
    assert term_info_result["statusCode"] == 200

    term_info_body = json.loads(term_info_result["body"])
    # termlink_id is computed from (subject, term, qualifiers)
    assert term_info_body["termlink_id"] is not None
    assert "negated" in term_info_body.get("qualifiers", [])
    assert term_info_body["evidence_count"] == 1

    # VERIFICATION 3: Check analytical tables via Athena
    query = f"""
        SELECT term_iri, evidence_count, qualifiers
        FROM subject_terms_by_subject
        WHERE subject_id = '{subject_id}'
        AND term_iri = '{term_iri}'
    """
    athena_result = execute_athena_query(query)
    assert len(athena_result) == 1
    assert athena_result[0]["evidence_count"] == 1

    # Cleanup
    lambda_client.invoke(
        FunctionName=physical_resources["RemoveEvidenceFunction"],
        Payload=json.dumps({"evidence_id": evidence_id})
    )
```

---

## Summary

**Key Takeaways:**

1. ✅ **Use Lambda APIs** to verify Neptune state indirectly
2. ✅ **Query analytical tables** (Athena/Iceberg) instead of Neptune graph
3. ✅ **Access DynamoDB directly** for mapping/identity verification
4. ✅ **Test system behavior** through observable effects
5. ✅ **Neptune Loader API is OK** - `start_load()`, `get_load_status()` for bulk load testing
6. ❌ **Never use SPARQL** - `execute_query()` or `execute_update()` in tests
7. ❌ **Don't query graph structure** - verify through Lambda APIs instead

---

## Quick Reference: What's Allowed

| Operation | Allowed? | How to Test |
|-----------|----------|-------------|
| Check bulk load status | ✅ Yes | `check_neptune_bulk_load_status` lambda |
| Start bulk load | ✅ Yes | `start_neptune_bulk_load` lambda |
| Reset database | ✅ Yes | `reset_database` lambda (admin operation) |
| Query term links | ❌ No | Use `get_subject_term_info` lambda |
| Query subject nodes | ❌ No | Use `get_subject` lambda |
| Query project nodes | ❌ No | Use `create_subject` or `get_subjects_pheno` |
| Check if term link exists | ❌ No | Use `get_subject` and check terms array |
| Verify evidence linkage | ❌ No | Use `get_evidence` and analytical tables |
| Count subject's terms | ❌ No | Use `get_subject` and count terms array |
| Query DynamoDB | ✅ Yes | Direct boto3 client access |
| Query Athena/Iceberg | ✅ Yes | Direct boto3 client or utility functions |

---

## Why This Matters

This approach:
- ✅ Maintains security boundaries (no graph data exposure)
- ✅ Tests the actual API contracts applications use
- ✅ Validates end-to-end data flow
- ✅ Catches integration issues missed by direct database queries
- ✅ Allows testing bulk load workflows (loader API)
- ❌ Prevents bypassing Lambda security and validation logic
