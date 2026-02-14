# Integration Test Plan: process_phenopacket.py

## Lambda Metadata

**Purpose**: Process a single phenopacket to create subject and evidence payloads for downstream Step Functions tasks. Maps phenotypic features to evidence records and handles qualifiers.

**Dependencies**:
- None (pure transformation logic)

**Key Operations**:
- Extracts subject information from phenopacket
- Creates subject payload with project_id and project_subject_id
- Maps phenotypicFeatures to evidence records
- Converts term IDs to full IRIs using metaData resources
- Maps "excluded" field to "negated" qualifier
- Sets evidence type to ECO:0000311 (imported information)
- Extracts creator from metaData.createdBy

---

## Integration Test Scenarios

### Test 1: test_process_simple_phenopacket
**Description**: Process phenopacket with single phenotypic feature

**Setup**:
- None required (stateless transformation)

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-001",
    "project_id": "test-project-123",
    "phenotypicFeatures": [
      {
        "type": {
          "id": "HP:0001250",
          "label": "Seizure"
        }
      }
    ],
    "metaData": {
      "createdBy": "clinician-123",
      "resources": [
        {
          "namespacePrefix": "HP",
          "iriPrefix": "http://purl.obolibrary.org/obo/HP_"
        }
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- subject_payload.project_id: "test-project-123"
- subject_payload.project_subject_id: "patient-001"
- evidence_payload is array with 1 element
- evidence_payload[0].subject_id: "test-project-123#patient-001" (temporary)
- evidence_payload[0].term_iri: "http://purl.obolibrary.org/obo/HP_0001250"
- evidence_payload[0].evidence_type: "http://purl.obolibrary.org/obo/ECO_0000311"
- evidence_payload[0].creator_id: "clinician-123"
- evidence_payload[0].creator_type: "system"
- evidence_payload[0].qualifiers: []

**Cleanup**:
- None required

### Test 2: test_process_multiple_phenotypic_features
**Description**: Process phenopacket with multiple features

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-002",
    "project_id": "test-project-456",
    "phenotypicFeatures": [
      {
        "type": {"id": "HP:0001250", "label": "Seizure"}
      },
      {
        "type": {"id": "HP:0001263", "label": "Developmental delay"}
      },
      {
        "type": {"id": "HP:0001252", "label": "Hypotonia"}
      }
    ],
    "metaData": {
      "createdBy": "import-system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- subject_payload.project_subject_id: "patient-002"
- evidence_payload is array with 3 elements
- All evidence records have same subject_id
- All evidence records have different term_iri values
- All evidence records have creator_id: "import-system"
- Term IRIs correctly converted from HP: prefix

**Cleanup**:
- None

### Test 3: test_process_phenopacket_with_excluded_feature
**Description**: Verify excluded feature maps to negated qualifier

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-003",
    "project_id": "test-project-789",
    "phenotypicFeatures": [
      {
        "type": {"id": "HP:0001250", "label": "Seizure"},
        "excluded": false
      },
      {
        "type": {"id": "HP:0001263", "label": "Developmental delay"},
        "excluded": true
      }
    ],
    "metaData": {
      "createdBy": "clinician-456",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload has 2 elements
- First evidence (Seizure): qualifiers is empty array []
- Second evidence (Developmental delay): qualifiers contains "negated"
- Both evidence records have same structure otherwise

**Cleanup**:
- None

### Test 4: test_process_phenopacket_special_characters_in_id
**Description**: Handle special characters in patient ID

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient@hospital#123",
    "project_id": "test-project-special",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- subject_payload.project_subject_id is escaped version of "patient@hospital#123"
- Special characters properly escaped for URI safety
- Evidence subject_id uses escaped version
- No errors from special characters

**Cleanup**:
- None

### Test 5: test_process_phenopacket_multiple_ontologies
**Description**: Handle multiple ontology resources (HPO, MONDO)

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-004",
    "project_id": "test-project-multi",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}},
      {"type": {"id": "MONDO:0007739"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"},
        {"namespacePrefix": "MONDO", "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload has 2 elements
- First term_iri starts with HPO IRI prefix
- Second term_iri starts with MONDO IRI prefix
- Both IRIs correctly converted from namespace prefixes
- Correct IRI prefix applied to each term

**Cleanup**:
- None

### Test 6: test_process_phenopacket_no_creator
**Description**: Handle missing createdBy field

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-005",
    "project_id": "test-project-nocreator",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].creator_id: "phenopacket-importer" (default)
- evidence_payload[0].creator_type: "system"
- No errors from missing createdBy

**Cleanup**:
- None

### Test 7: test_process_phenopacket_empty_features
**Description**: Handle phenopacket with no phenotypic features

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-006",
    "project_id": "test-project-empty",
    "phenotypicFeatures": [],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- subject_payload is valid with project_id and project_subject_id
- evidence_payload is empty array []
- No errors thrown

**Cleanup**:
- None

### Test 8: test_process_phenopacket_full_iri_already
**Description**: Handle term that's already a full IRI (no namespace prefix)

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-007",
    "project_id": "test-project-fulliri",
    "phenotypicFeatures": [
      {"type": {"id": "http://purl.obolibrary.org/obo/HP_0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].term_iri: "http://purl.obolibrary.org/obo/HP_0001250" (unchanged)
- Full IRI passed through without modification
- No double-prefixing

**Cleanup**:
- None

### Test 9: test_process_phenopacket_missing_project_id
**Description**: Handle missing project_id field

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-008",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- Lambda raises KeyError or returns error
- Error indicates missing required field
- OR subject_payload.project_id is None (depending on implementation)

**Cleanup**:
- None

### Test 10: test_process_phenopacket_missing_id
**Description**: Handle missing patient id field

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "project_id": "test-project-noid",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- Lambda raises KeyError or returns error
- Error indicates missing required field 'id'

**Cleanup**:
- None

### Test 11: test_process_phenopacket_unicode_text
**Description**: Handle Unicode characters in labels and IDs

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "患者-001",
    "project_id": "test-project-unicode",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250", "label": "発作"}}
    ],
    "metaData": {
      "createdBy": "医師-123",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- subject_payload.project_subject_id preserves Unicode
- evidence_payload[0].creator_id preserves Unicode
- No encoding errors
- All Unicode text properly handled

**Cleanup**:
- None

### Test 12: test_process_phenopacket_unknown_namespace
**Description**: Handle term with namespace not in resources

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-009",
    "project_id": "test-project-unknown",
    "phenotypicFeatures": [
      {"type": {"id": "UNKNOWN:0001234"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].term_iri: "UNKNOWN:0001234" (unchanged, passed through)
- No IRI conversion performed for unknown namespace
- Processing continues without error

**Cleanup**:
- None

### Test 13: test_process_phenopacket_modifiers_ignored
**Description**: Verify phenopacket modifiers are not mapped (as per comment)

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-010",
    "project_id": "test-project-modifiers",
    "phenotypicFeatures": [
      {
        "type": {"id": "HP:0001250"},
        "modifiers": [
          {"id": "HP:0012828", "label": "Severe"}
        ]
      }
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].qualifiers: [] (modifiers not converted)
- Modifiers field ignored per code comment
- Only excluded field maps to qualifiers

**Cleanup**:
- None

### Test 14: test_process_phenopacket_temporary_subject_id_format
**Description**: Verify temporary subject_id format for evidence

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-011",
    "project_id": "project-abc-123",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].subject_id: "project-abc-123#patient-011"
- Format is "{project_id}#{project_subject_id}"
- This temporary ID will be replaced by prepare_evidence_payload

**Cleanup**:
- None

### Test 15: test_process_phenopacket_large_feature_set
**Description**: Handle phenopacket with many phenotypic features

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-012",
    "project_id": "test-project-large",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}},
      {"type": {"id": "HP:0001251"}},
      ... (100 total features)
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload has 100 elements
- All evidence records properly formatted
- All have unique term_iri values
- All share same subject_id
- Lambda completes within reasonable time

**Cleanup**:
- None

### Test 16: test_process_phenopacket_nested_event_structure
**Description**: Verify lambda handles event wrapper correctly

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-013",
    "project_id": "test-project-event",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- Lambda correctly extracts phenopacket from event.Phenopacket
- statusCode: 200
- All fields processed correctly
- Event structure doesn't cause errors

**Cleanup**:
- None

### Test 17: test_process_phenopacket_idempotency
**Description**: Same input produces same output

**Setup**:
- None

**Action** (invoke twice with identical payload):
```json
{
  "Phenopacket": {
    "id": "patient-014",
    "project_id": "test-project-idem",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}},
      {"type": {"id": "HP:0001263"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- Both invocations return identical results
- subject_payload is identical
- evidence_payload is identical (same order, same values)
- Transformation is deterministic

**Cleanup**:
- None

### Test 18: test_process_phenopacket_malformed_metadata
**Description**: Handle missing or malformed metaData

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-015",
    "project_id": "test-project-badmeta",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {}
  }
}
```

**Assertions**:
- Lambda raises error for missing resources
- OR returns with default behavior
- Error message is informative

**Cleanup**:
- None

### Test 19: test_process_phenopacket_evidence_type_constant
**Description**: Verify evidence type is always ECO_0000311

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-016",
    "project_id": "test-project-eco",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}},
      {"type": {"id": "HP:0001263"}},
      {"type": {"id": "HP:0001252"}}
    ],
    "metaData": {
      "createdBy": "system",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- All evidence records have evidence_type: "http://purl.obolibrary.org/obo/ECO_0000311"
- Evidence type is consistent across all records
- Evidence type represents "Imported information"

**Cleanup**:
- None

### Test 20: test_process_phenopacket_creator_type_constant
**Description**: Verify creator_type is always "system"

**Setup**:
- None

**Action**:
```json
{
  "Phenopacket": {
    "id": "patient-017",
    "project_id": "test-project-creator",
    "phenotypicFeatures": [
      {"type": {"id": "HP:0001250"}}
    ],
    "metaData": {
      "createdBy": "different-creators",
      "resources": [
        {"namespacePrefix": "HP", "iriPrefix": "http://purl.obolibrary.org/obo/HP_"}
      ]
    }
  }
}
```

**Assertions**:
- statusCode: 200
- evidence_payload[0].creator_type: "system"
- Creator type is always "system" for imported phenopackets
- Consistent regardless of createdBy value

**Cleanup**:
- None
