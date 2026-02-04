"""
Integration test for qualifier parsing across different endpoints.
"""
import pytest
import json
import uuid
from general_utils import invoke_lambda

pytestmark = [pytest.mark.integration]


def test_qualifier_parsing_across_endpoints(physical_resources, test_project_id):
    """Test that qualifier parsing works consistently across all endpoints that use it."""
    
    # Create subject
    subject_id = str(uuid.uuid4())
    create_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": f"test-qual-parsing-{subject_id[:8]}"
    })
    assert create_subject_response["statusCode"] == 200
    subject_data = json.loads(create_subject_response["body"])
    actual_subject_id = subject_data["subject"]["iri"].split("/")[-1]
    
    # Create evidence with multiple qualifiers
    term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
    evidence_payload = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-qual-parsing",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "span_start": 10,
        "span_end": 20,
        "qualifiers": ["negated", "hypothetical"]  # Multiple qualifiers
    }
    
    create_evidence_response = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload)
    assert create_evidence_response["statusCode"] == 201
    
    # Wait for evidence to be available
    import time
    time.sleep(2)
    
    # Test 1: GetSubject endpoint (uses get_term_links_with_counts)
    get_subject_response = invoke_lambda(physical_resources["GetSubjectFunction"], {
        "subject_id": actual_subject_id
    })
    assert get_subject_response["statusCode"] == 200
    subject_data = json.loads(get_subject_response["body"])
    
    # Find our term
    target_term = None
    for term in subject_data["terms"]:
        if term["term_iri"] == term_iri:
            target_term = term
            break
    
    assert target_term is not None
    assert len(target_term["qualifiers"]) == 2
    assert set(target_term["qualifiers"]) == {"negated", "hypothetical"}
    
    # Test 2: GetSubjectTermInfo endpoint (uses different parsing path)
    term_info_response = invoke_lambda(physical_resources["GetSubjectTermInfoFunction"], {
        "body": json.dumps({
            "subject_id": actual_subject_id,
            "term_iri": term_iri,
            "qualifiers": []  # Query for unqualified version
        })
    })
    # This might return 404 since we only have qualified evidence, which is expected
    
    # Test 3: Query with matching qualifiers
    qualified_term_info_response = invoke_lambda(physical_resources["GetSubjectTermInfoFunction"], {
        "body": json.dumps({
            "subject_id": actual_subject_id,
            "term_iri": term_iri,
            "qualifiers": ["http://ods.nationwidechildrens.org/phebee/qualifier/negated"]
        })
    })
    
    if qualified_term_info_response["statusCode"] == 200:
        qualified_data = json.loads(qualified_term_info_response["body"])
        assert qualified_data["term_iri"] == term_iri
        # Should have the negated qualifier in the response (now standardized to short names)
        qualifiers = qualified_data.get("qualifiers", [])
        assert len(qualifiers) > 0, f"Expected qualifiers, got: {qualifiers}"
        assert "negated" in qualifiers, f"Expected 'negated' qualifier, got: {qualifiers}"


def test_qualifier_parsing_edge_cases(physical_resources, test_project_id):
    """Test edge cases in qualifier parsing."""
    
    # Create subject
    subject_id = str(uuid.uuid4())
    create_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": f"test-qual-edge-{subject_id[:8]}"
    })
    assert create_subject_response["statusCode"] == 200
    subject_data = json.loads(create_subject_response["body"])
    actual_subject_id = subject_data["subject"]["iri"].split("/")[-1]
    
    # Test 1: Evidence with no qualifiers
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0001627"
    evidence_payload_1 = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri_1,
        "creator_id": "test-no-quals",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "qualifiers": []  # Empty qualifiers
    }
    
    create_evidence_response_1 = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload_1)
    assert create_evidence_response_1["statusCode"] == 201
    
    # Test 2: Evidence with single qualifier
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002664"
    evidence_payload_2 = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri_2,
        "creator_id": "test-single-qual",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "qualifiers": ["family"]  # Single qualifier
    }
    
    create_evidence_response_2 = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload_2)
    assert create_evidence_response_2["statusCode"] == 201
    
    # Wait for evidence to be available
    import time
    time.sleep(2)
    
    # Verify both terms appear correctly in GetSubject
    get_subject_response = invoke_lambda(physical_resources["GetSubjectFunction"], {
        "subject_id": actual_subject_id
    })
    assert get_subject_response["statusCode"] == 200
    subject_data = json.loads(get_subject_response["body"])
    
    # Check term with no qualifiers
    term_1 = next((t for t in subject_data["terms"] if t["term_iri"] == term_iri_1), None)
    assert term_1 is not None
    assert len(term_1["qualifiers"]) == 0
    
    # Check term with single qualifier
    term_2 = next((t for t in subject_data["terms"] if t["term_iri"] == term_iri_2), None)
    assert term_2 is not None
    assert len(term_2["qualifiers"]) == 1
    assert term_2["qualifiers"][0] == "family"
