# Integration Test Plan: prepare_evidence_payload.py

## Lambda Metadata

**Purpose**: Replace temporary subject identifier in evidence payload with actual UUID from CreateSubject response. This bridges the gap between process_phenopacket and create_evidence lambdas.

**Dependencies**:
- None (pure transformation logic)

**Key Operations**:
- Receives evidence_payload array and subject_id (UUID)
- Replaces temporary subject_id (format: "project_id#project_subject_id") with actual UUID
- Returns fixed_evidence_payload ready for CreateEvidence

---

## Integration Test Scenarios

### Test 1: test_prepare_single_evidence_record
**Description**: Replace subject_id in single evidence record

**Setup**:
- None (stateless transformation)

**Action**:
```json
{
  "subject_id": "550e8400-e29b-41d4-a716-446655440000",
  "evidence_payload": [
    {
      "subject_id": "test-project#patient-001",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "system",
      "creator_type": "system",
      "qualifiers": []
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload is array with 1 element
- fixed_evidence_payload[0].subject_id: "550e8400-e29b-41d4-a716-446655440000"
- All other fields unchanged (term_iri, evidence_type, creator_id, creator_type, qualifiers)
- Temporary ID fully replaced

**Cleanup**:
- None

### Test 2: test_prepare_multiple_evidence_records
**Description**: Replace subject_id in multiple evidence records

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "650e8400-e29b-41d4-a716-446655440001",
  "evidence_payload": [
    {
      "subject_id": "project-456#patient-002",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "clinician-1",
      "creator_type": "system",
      "qualifiers": []
    },
    {
      "subject_id": "project-456#patient-002",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001263",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "clinician-1",
      "creator_type": "system",
      "qualifiers": ["negated"]
    },
    {
      "subject_id": "project-456#patient-002",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001252",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "clinician-1",
      "creator_type": "system",
      "qualifiers": []
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload has 3 elements
- All 3 records have subject_id updated to "650e8400-e29b-41d4-a716-446655440001"
- All term_iri values preserved (different for each)
- All qualifiers preserved (different for each)
- All other fields unchanged

**Cleanup**:
- None

### Test 3: test_prepare_empty_evidence_payload
**Description**: Handle empty evidence array

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "750e8400-e29b-41d4-a716-446655440002",
  "evidence_payload": []
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload: []
- Empty array returned without error
- No processing errors

**Cleanup**:
- None

### Test 4: test_prepare_missing_evidence_payload
**Description**: Handle missing evidence_payload parameter

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "850e8400-e29b-41d4-a716-446655440003"
}
```

**Assertions**:
- statusCode: 400
- body.message contains "Missing required fields: evidence_payload and subject_id"
- No fixed_evidence_payload in response
- Error is descriptive

**Cleanup**:
- None

### Test 5: test_prepare_missing_subject_id
**Description**: Handle missing subject_id parameter

**Setup**:
- None

**Action**:
```json
{
  "evidence_payload": [
    {
      "subject_id": "temp-id",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "system",
      "creator_type": "system",
      "qualifiers": []
    }
  ]
}
```

**Assertions**:
- statusCode: 400
- body.message contains "Missing required fields"
- No processing occurs
- Error message is clear

**Cleanup**:
- None

### Test 6: test_prepare_missing_both_parameters
**Description**: Handle missing both required parameters

**Setup**:
- None

**Action**:
```json
{}
```

**Assertions**:
- statusCode: 400
- body.message: "Missing required fields: evidence_payload and subject_id"
- Clear error message
- No processing attempted

**Cleanup**:
- None

### Test 7: test_prepare_preserves_qualifiers
**Description**: Verify qualifiers array is preserved correctly

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "950e8400-e29b-41d4-a716-446655440004",
  "evidence_payload": [
    {
      "subject_id": "temp#id",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "system",
      "creator_type": "system",
      "qualifiers": ["negated"]
    },
    {
      "subject_id": "temp#id",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001263",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "system",
      "creator_type": "system",
      "qualifiers": []
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].qualifiers: ["negated"]
- fixed_evidence_payload[1].qualifiers: []
- Qualifiers preserved exactly as input
- Arrays not modified

**Cleanup**:
- None

### Test 8: test_prepare_preserves_all_evidence_fields
**Description**: Verify all evidence record fields are preserved

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "a50e8400-e29b-41d4-a716-446655440005",
  "evidence_payload": [
    {
      "subject_id": "temp#id",
      "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
      "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",
      "creator_id": "clinician-123",
      "creator_type": "system",
      "qualifiers": ["negated"],
      "custom_field": "preserved"
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].term_iri preserved
- fixed_evidence_payload[0].evidence_type preserved
- fixed_evidence_payload[0].creator_id preserved
- fixed_evidence_payload[0].creator_type preserved
- fixed_evidence_payload[0].qualifiers preserved
- fixed_evidence_payload[0].custom_field preserved (if present)
- Only subject_id field is modified

**Cleanup**:
- None

### Test 9: test_prepare_large_evidence_batch
**Description**: Handle large number of evidence records

**Setup**:
- Generate 100 evidence records with temporary subject_id

**Action**:
```json
{
  "subject_id": "b50e8400-e29b-41d4-a716-446655440006",
  "evidence_payload": [
    ... (100 evidence records)
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload has 100 elements
- All 100 records have subject_id updated
- All other fields preserved for each record
- Lambda completes within reasonable time
- No memory issues

**Cleanup**:
- None

### Test 10: test_prepare_uuid_format_validation
**Description**: Verify different UUID formats are accepted

**Setup**:
- None

**Action** (test with various UUID formats):
```json
{
  "subject_id": "c50e8400-e29b-41d4-a716-446655440007",
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 200
- subject_id accepted regardless of format (lowercase, uppercase, with/without hyphens)
- UUID string used as-is without validation
- No format enforcement (accepting any string)

**Cleanup**:
- None

### Test 11: test_prepare_special_characters_in_subject_id
**Description**: Handle special characters in UUID

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "subject-id-with-special-chars@#$",
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].subject_id: "subject-id-with-special-chars@#$"
- Special characters accepted and used as-is
- No validation or sanitization

**Cleanup**:
- None

### Test 12: test_prepare_idempotency
**Description**: Same input produces same output

**Setup**:
- None

**Action** (invoke twice with identical input):
```json
{
  "subject_id": "d50e8400-e29b-41d4-a716-446655440008",
  "evidence_payload": [
    {"subject_id": "temp1", "term_iri": "http://example.com/term1", "evidence_type": "type", "creator_id": "c1", "creator_type": "s", "qualifiers": []},
    {"subject_id": "temp2", "term_iri": "http://example.com/term2", "evidence_type": "type", "creator_id": "c2", "creator_type": "s", "qualifiers": ["negated"]}
  ]
}
```

**Assertions**:
- Both invocations return identical results
- fixed_evidence_payload is identical in both responses
- Transformation is deterministic
- No side effects between invocations

**Cleanup**:
- None

### Test 13: test_prepare_null_subject_id
**Description**: Handle null/None subject_id

**Setup**:
- None

**Action**:
```json
{
  "subject_id": null,
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 400
- Error message indicates missing subject_id
- OR fixed_evidence_payload[0].subject_id is null (depending on implementation)
- Appropriate error handling

**Cleanup**:
- None

### Test 14: test_prepare_unicode_in_subject_id
**Description**: Handle Unicode characters in subject_id

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "患者-550e8400-e29b-41d4-a716-446655440009",
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].subject_id: "患者-550e8400-e29b-41d4-a716-446655440009"
- Unicode characters preserved correctly
- No encoding errors

**Cleanup**:
- None

### Test 15: test_prepare_nested_evidence_fields
**Description**: Verify nested objects in evidence are preserved

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "e50e8400-e29b-41d4-a716-446655440010",
  "evidence_payload": [
    {
      "subject_id": "temp",
      "term_iri": "http://example.com/term",
      "evidence_type": "type",
      "creator_id": "c",
      "creator_type": "s",
      "qualifiers": [],
      "metadata": {
        "nested": {
          "field": "value"
        }
      }
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].metadata.nested.field: "value"
- Nested objects fully preserved
- Deep cloning works correctly

**Cleanup**:
- None

### Test 16: test_prepare_evidence_with_arrays
**Description**: Verify arrays in evidence records are preserved

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "f50e8400-e29b-41d4-a716-446655440011",
  "evidence_payload": [
    {
      "subject_id": "temp",
      "term_iri": "http://example.com/term",
      "evidence_type": "type",
      "creator_id": "c",
      "creator_type": "s",
      "qualifiers": ["negated", "severe"],
      "tags": ["tag1", "tag2", "tag3"]
    }
  ]
}
```

**Assertions**:
- statusCode: 200
- fixed_evidence_payload[0].qualifiers: ["negated", "severe"]
- fixed_evidence_payload[0].tags: ["tag1", "tag2", "tag3"]
- All array contents preserved in order

**Cleanup**:
- None

### Test 17: test_prepare_response_structure
**Description**: Verify response structure is correct

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "g50e8400-e29b-41d4-a716-446655440012",
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- Response has statusCode field
- Response has fixed_evidence_payload field
- Response does NOT have evidence_payload field (renamed)
- Response structure matches expected format for downstream lambdas

**Cleanup**:
- None

### Test 18: test_prepare_immutability_of_input
**Description**: Verify input evidence_payload is not mutated

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "h50e8400-e29b-41d4-a716-446655440013",
  "evidence_payload": [
    {"subject_id": "temp-original", "term_iri": "http://example.com/term", "evidence_type": "type", "creator_id": "c", "creator_type": "s", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 200
- Input evidence_payload preserved with original subject_id
- Output fixed_evidence_payload has new subject_id
- No mutation of input data (dict() creates new objects)

**Cleanup**:
- None

### Test 19: test_prepare_step_functions_integration
**Description**: Verify output format works with Step Functions workflow

**Setup**:
- None

**Action**:
```json
{
  "subject_id": "i50e8400-e29b-41d4-a716-446655440014",
  "evidence_payload": [
    {"subject_id": "temp", "term_iri": "http://purl.obolibrary.org/obo/HP_0001250", "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311", "creator_id": "system", "creator_type": "system", "qualifiers": []}
  ]
}
```

**Assertions**:
- statusCode: 200
- Response can be passed directly to CreateEvidence lambda
- fixed_evidence_payload is valid JSON array
- Each element is valid CreateEvidence payload
- No additional transformation needed

**Cleanup**:
- None

### Test 20: test_prepare_error_logging
**Description**: Verify errors are logged with context

**Setup**:
- None

**Action**:
```json
{
  "subject_id": null,
  "evidence_payload": null
}
```

**Assertions**:
- statusCode: 400
- Error logged with event context
- Error message is JSON formatted
- Log includes missing field information

**Cleanup**:
- None
