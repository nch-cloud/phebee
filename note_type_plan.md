# Clinical Note Type Implementation Plan

## Overview
Add `note_type` as a required path component in clinical note IRIs to support multiple notes with the same ID but different types.

**New IRI Pattern:** `{encounter_iri}/note/{clinical_note_id}/{note_type}`

## Required Changes

### 1. Data Model Updates

**File:** `model/phebee.ttl`
- Add `phebee:noteType` property:
```turtle
phebee:noteType a owl:DatatypeProperty ;
    rdfs:domain phebee:ClinicalNote ;
    rdfs:range xsd:string ;
    rdfs:label "Note Type" ;
    rdfs:comment "The type/category of the clinical note (e.g., discharge, progress, admission)." .
```

### 2. Core SPARQL Functions

**File:** `src/phebee/utils/sparql.py`

**Function:** `create_clinical_note()`
- Add `note_type: str` parameter (required)
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Add note_type encoding with exception: `encoded_note_type = encode_note_type(note_type)`
- Update IRI construction: `f"{encounter_iri}/note/{clinical_note_id}/{encoded_note_type}"`
- Add note type triple: `f'<{clinical_note_iri}> phebee:noteType "{note_type}"'` (store original, not encoded)

**Function:** `get_clinical_note()`
- Add `note_type: str` parameter (required)
- Import encoding: `from phebee.utils.validation import encode_note_type, decode_note_type`
- Encode for IRI construction: `encoded_note_type = encode_note_type(note_type)`
- Update IRI construction: `f"{encounter_iri}/note/{clinical_note_id}/{encoded_note_type}"`
- **Return decoded note_type** in response for user readability

**Function:** `delete_clinical_note()`
- Add `note_type: str` parameter (required)
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Encode for IRI construction: `encoded_note_type = encode_note_type(note_type)`
- Update IRI construction: `f"{encounter_iri}/note/{clinical_note_id}/{encoded_note_type}"`

**Function:** `check_existing_clinical_notes()`
- Update to handle new IRI pattern with note types
- Ensure all callers pass IRIs with the new pattern including note_type
- Add validation that incoming IRIs match expected pattern format
- Verify consistency between IRI construction and existence checking

### 3. Lambda Functions

**File:** `functions/create_clinical_note.py`
- Add `note_type = body.get("note_type")` extraction
- Add validation: `if not note_type: return 400 error`
- Update function call: `create_clinical_note(encounter_iri, clinical_note_id, note_type, ...)`
- **Add exception handling:** Catch `ValueError` from encoding and return 400 error
- **Fix response IRI:** Use encoded note_type: `from phebee.utils.validation import encode_note_type`
- **Response IRI:** `note_iri = f"{encounter_iri}/note/{clinical_note_id}/{encode_note_type(note_type)}"`

**File:** `functions/get_clinical_note.py`
- Add `note_type = body.get("note_type")` extraction
- Add validation: `if not note_type: return 400 error`
- Update function call: `get_clinical_note(encounter_iri, clinical_note_id, note_type)`
- **Add exception handling:** Catch `ValueError` from encoding and return 400 error

**File:** `functions/remove_clinical_note.py`
- Add `note_type = body.get("note_type")` extraction
- Add validation: `if not note_type: return 400 error`
- Update function call: `delete_clinical_note(encounter_iri, clinical_note_id, note_type)`
- **Add exception handling:** Catch `ValueError` from encoding and return 400 error
- **Fix response IRI:** Use encoded note_type: `from phebee.utils.validation import encode_note_type`
- **Response IRI:** `note_iri = f"{encounter_iri}/note/{clinical_note_id}/{encode_note_type(note_type)}"`

### 4. Text Annotation Functions (Critical Dependency)

**Files:** `functions/create_text_annotation.py`, `functions/get_text_annotation.py`, `functions/remove_text_annotation.py`

Text annotations reference clinical notes as their `text_source_iri`. All text annotation functions that accept or return clinical note IRIs need updates:

- Update any hardcoded clinical note IRI construction
- Ensure text annotation payloads include note_type when referencing clinical notes
- Update text annotation tests to provide note_type for clinical note creation

### 5. Bulk Upload Function

**File:** `functions/perform_bulk_upload.py`

**Class:** `ClinicalNoteEvidence`
- Add `note_type: str` field

**IRI Construction Updates:**
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Line 151: `note_iri = f"{encounter_iri}/note/{evidence.clinical_note_id}/{encode_note_type(evidence.note_type)}"`
- Line 278: `note_iri_str = f"{encounter_iri_str}/note/{evidence.clinical_note_id}/{encode_note_type(evidence.note_type)}"`

**RDF Triple Addition:**
- Add: `g.add((note_iri, PHEBEE_NS.noteType, RdfLiteral(evidence.note_type)))` (store original, not encoded)

**IRI Consistency:**
- Verify that bulk upload constructs IRIs correctly before calling `check_existing_clinical_notes()`
- Ensure all IRI construction uses the same pattern: `/note/{id}/{type}`

### 6. Term Link Functions (Additional Dependency)

**Files:** `functions/create_term_link.py`, `functions/get_term_link.py`, `functions/remove_term_link.py`

Term links can use clinical notes as source nodes via `source_node_iri`. While the term link functions themselves don't need changes:

- **API callers must construct clinical note IRIs correctly** when creating term links with clinical notes as sources
- **Integration tests** that create term links with clinical note sources need IRI pattern updates
- **Any code that passes clinical note IRIs** as `source_node_iri` must use new pattern

### 7. API Specification

**File:** `api.yaml`

**Endpoint:** `/clinical-note/create`
- Add `note_type` to required fields array
- Add `note_type` property:
```yaml
note_type:
  type: string
  description: "Type/category of the clinical note (automatically URL-encoded for URI safety)"
```

**Endpoint:** `/clinical-note` (GET)
- Add `note_type` to required fields array
- Add `note_type` property to schema

**Endpoint:** `/clinical-note/remove`
- Add `note_type` to required fields array
- Add `note_type` property to schema

### 8. Integration Tests

**File:** `tests/integration/test_clinical_note.py`
- Add `"note_type": "progress"` to all test payloads
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Update assertions to expect note_type in responses
- **Fix hardcoded IRI patterns** to use encoding: `encode_note_type(note_type)`

**File:** `tests/integration/api/test_clinical_note_api.py`
- Add `"note_type": "discharge"` to all test payloads
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Update IRI assertions to use encoding: `encode_note_type(note_type)`

**File:** `tests/integration/test_text_annotation.py`
- **Critical:** Add `"note_type": "clinical"` to clinical note creation
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Update `text_source_iri` expectations to use encoding

**File:** `tests/integration/api/test_text_annotation_api.py`
- Update any clinical note references to include note_type
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Use encoding for IRI construction

**File:** `tests/integration/test_bulk_upload.py`
- Add `"note_type": "progress"` to all ClinicalNoteEvidence objects
- Import encoding: `from phebee.utils.validation import encode_note_type`
- **Fix hardcoded IRI patterns:**
  - `note1_iri = f"{encounter_iri}/note/note-enc-1/{encode_note_type(note_type)}"`
  - `note2_iri = f"{encounter_iri}/note/note-enc-2/{encode_note_type(note_type)}"`
  - `note_iri = f"{encounter_iri}/note/note-batch-{i}/{encode_note_type(note_type)}"`
- Update expected IRI patterns in assertions to use encoding

**Term Link Integration Tests:**
- **Search for any tests that create term links with clinical note sources**
- Import encoding: `from phebee.utils.validation import encode_note_type`
- Update clinical note IRI construction in term link test payloads to use encoding
- Verify `source_node_iri` uses encoded pattern when referencing clinical notes

**Special Character Testing:**
- Add tests with note types containing spaces, slashes, and special characters
- Verify encoding/decoding works correctly in all scenarios

### 9. Validation Helper Function

**File:** `src/phebee/utils/validation.py` (new file)
```python
from urllib.parse import quote, unquote

def encode_note_type(note_type: str) -> str:
    """URL encode note_type to make it URI-safe"""
    if not note_type or not isinstance(note_type, str):
        raise ValueError("note_type must be a non-empty string")
    return quote(note_type, safe='')

def decode_note_type(encoded_note_type: str) -> str:
    """URL decode note_type back to original"""
    return unquote(encoded_note_type)
```

### 10. SAM Template (No Changes Required)
The Lambda function definitions in `template.yaml` don't need updates as they reference the function files by handler name.

## Implementation Order

1. **Validation Helper** - Create URI validation utility
2. **Data Model** - Update `phebee.ttl`
3. **Core Functions** - Update SPARQL utilities
4. **Lambda Functions** - Update individual function handlers
5. **Bulk Upload** - Update bulk processing logic
6. **Text Annotations** - Update text annotation functions (critical dependency)
7. **Term Links** - Update any code that creates term links with clinical note sources
8. **API Spec** - Update OpenAPI documentation
9. **Tests** - Update all integration tests (fix hardcoded IRIs and term link sources)

## Critical Issues Addressed

### 1. Text Annotation Dependency
Text annotations use clinical notes as `text_source_iri`. All text annotation functions and tests must be updated to handle the new IRI pattern.

### 2. Hardcoded IRI Patterns in Tests
Multiple test files construct clinical note IRIs directly using the old pattern. These must all be updated.

### 3. URI Safety Encoding
Added URL encoding to automatically make any note_type URI-safe, allowing special characters, spaces, and symbols without restriction.

### 4. Response IRI Construction
Lambda functions that return clinical note IRIs in responses must include the encoded note_type in the constructed IRI.

### 5. SPARQL Query Dependencies
The `check_existing_clinical_notes()` function queries for existing notes by IRI. All callers must construct IRIs with the new pattern including encoded note_type, and the function needs validation to ensure IRI format consistency.

### 6. Term Link Source Dependencies
Term links can reference clinical notes as `source_node_iri`. Any code that creates term links with clinical note sources must construct the clinical note IRI using the new pattern with encoded note_type.

### 7. Encoding Centralization
URL encoding is applied in SPARQL functions and Lambda response construction to ensure URI safety. Original note_type values are stored in RDF triples for user readability, while encoded versions are used only in IRIs.

## Testing Strategy

1. **URL Encoding Tests** - Test note_type encoding with special characters, spaces, and symbols
2. **Text Annotation Integration** - Verify text annotations work with new clinical note IRIs
3. **Collision Prevention** - Test same note_id with different note_types
4. **IRI Uniqueness** - Verify IRI uniqueness across note types
5. **Special Character Handling** - Test note types with spaces, slashes, unicode characters
6. **Backward Compatibility** - Ensure no existing functionality breaks

## Breaking Changes

- All clinical note API calls now require `note_type` parameter
- All text annotation functions that reference clinical notes need note_type
- **Term links with clinical note sources** require the new IRI pattern for `source_node_iri`
- Existing clinical note IRIs will be invalid (no production impact)
- Bulk upload payloads must include note_type for clinical note evidence
- Test data must include note_type for all clinical note operations

## Example Usage

**Before:**
```json
{
  "encounter_iri": "project123/subject456/encounter789",
  "clinical_note_id": "12345"
}
```

**After:**
```json
{
  "encounter_iri": "project123/subject456/encounter789", 
  "clinical_note_id": "12345",
  "note_type": "discharge"
}
```

**Resulting IRI:** `project123/subject456/encounter789/note/12345/discharge`

**With Special Characters:**
```json
{
  "encounter_iri": "project123/subject456/encounter789", 
  "clinical_note_id": "12345",
  "note_type": "discharge summary"
}
```

**Resulting IRI:** `project123/subject456/encounter789/note/12345/discharge%20summary`

## Encoding Rules

- `note_type` can contain any characters (spaces, symbols, unicode, etc.)
- `note_type` is automatically URL-encoded for URI construction
- Original `note_type` values are preserved in RDF data
- `note_type` is required for all clinical note operations
- Examples: "discharge summary" → "discharge%20summary", "pre/post-op" → "pre%2Fpost-op"
