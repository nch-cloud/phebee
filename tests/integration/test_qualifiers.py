import pytest
import json
import uuid
import boto3
from general_utils import invoke_lambda


@pytest.mark.integration
def test_qualifier_standardization_comprehensive(physical_resources, test_project_id):
    """Test that qualifier standardization works end-to-end with 'true'/'false' format"""
    
    # Create unique test data
    subject_id = str(uuid.uuid4())
    term_iri = "http://purl.obolibrary.org/obo/HP_0001627"
    
    # Create subject first
    create_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": f"test-qualifier-{subject_id[:8]}"
    })
    assert create_subject_response["statusCode"] == 200
    subject_data = json.loads(create_subject_response["body"])
    
    # Extract subject_id from the IRI
    if "subject" in subject_data and "iri" in subject_data["subject"]:
        subject_iri = subject_data["subject"]["iri"]
        actual_subject_id = subject_iri.split("/")[-1]  # Extract UUID from IRI
    elif "subject_iri" in subject_data:
        actual_subject_id = subject_data["subject_iri"].split("/")[-1]
    else:
        pytest.fail(f"Cannot find subject IRI in response: {subject_data}")
    
    # Test 1: Create evidence with positive qualifiers
    evidence_payload = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-qualifier-creator",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "span_start": 10,
        "span_end": 20,
        "qualifiers": ["negated", "family"]  # Two positive qualifiers
    }
    
    create_evidence_response = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload)
    assert create_evidence_response["statusCode"] == 201
    evidence_id = json.loads(create_evidence_response["body"])["evidence_id"]
    
    # Test 2: Retrieve evidence and validate qualifier format
    get_evidence_response = invoke_lambda(physical_resources["GetEvidenceFunction"], {
        "evidence_id": evidence_id
    })
    assert get_evidence_response["statusCode"] == 200
    evidence_data = json.loads(get_evidence_response["body"])
    
    print(f"Evidence response keys: {evidence_data.keys()}")
    print(f"Full evidence data: {evidence_data}")
    
    # Validate qualifiers are stored in standardized format
    qualifiers = evidence_data.get("qualifiers", {})
    print(f"Raw qualifiers from API: {qualifiers}")
    print(f"Qualifiers type: {type(qualifiers)}")
    
    assert isinstance(qualifiers, dict), f"Expected qualifiers dict, got: {type(qualifiers)}"
    assert len(qualifiers) >= 2, f"Expected at least 2 qualifiers, got {len(qualifiers)}: {qualifiers}"
    
    # Check that we have the expected qualifiers with "true" values
    assert qualifiers.get("negated") == "true", f"Expected negated='true', got {qualifiers.get('negated')}"
    assert qualifiers.get("family") == "true", f"Expected family='true', got {qualifiers.get('family')}"
    
    # Test 3: Validate hash consistency - same qualifiers should produce same hash
    # Create another evidence with same qualifiers
    evidence_payload2 = evidence_payload.copy()
    evidence_payload2["clinical_note_id"] = f"note-{uuid.uuid4()}"
    evidence_payload2["encounter_id"] = f"encounter-{uuid.uuid4()}"
    
    create_evidence_response2 = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload2)
    assert create_evidence_response2["statusCode"] == 201
    evidence_id2 = json.loads(create_evidence_response2["body"])["evidence_id"]
    
    get_evidence_response2 = invoke_lambda(physical_resources["GetEvidenceFunction"], {
        "evidence_id": evidence_id2
    })
    assert get_evidence_response2["statusCode"] == 200
    evidence_data2 = json.loads(get_evidence_response2["body"])
    
    # Both evidence records should have same termlink_id (same hash)
    assert evidence_data["termlink_id"] == evidence_data2["termlink_id"], \
        "Same qualifiers should produce same termlink hash"
    
    # Test 4: Validate GetSubjectTermInfo works with standardized qualifiers
    get_term_info_response = invoke_lambda(physical_resources["GetSubjectTermInfoFunction"], {
        "body": json.dumps({
            "subject_id": actual_subject_id,
            "term_iri": term_iri,
            "qualifiers": []
        })
    })
    assert get_term_info_response["statusCode"] == 200
    term_info = json.loads(get_term_info_response["body"])
    
    # Should find the evidence with qualifiers
    assert term_info["evidence_count"] >= 2, "Should find both evidence records"
    assert len(term_info["term_links"]) >= 1, "Should have termlink data"
    
    # Test 5: Validate different qualifier values produce different hashes
    evidence_payload3 = evidence_payload.copy()
    evidence_payload3["clinical_note_id"] = f"note-{uuid.uuid4()}"
    evidence_payload3["encounter_id"] = f"encounter-{uuid.uuid4()}"
    evidence_payload3["qualifiers"] = ["hypothetical"]  # Different qualifier
    
    create_evidence_response3 = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload3)
    assert create_evidence_response3["statusCode"] == 201
    evidence_id3 = json.loads(create_evidence_response3["body"])["evidence_id"]
    
    get_evidence_response3 = invoke_lambda(physical_resources["GetEvidenceFunction"], {
        "evidence_id": evidence_id3
    })
    assert get_evidence_response3["statusCode"] == 200
    evidence_data3 = json.loads(get_evidence_response3["body"])
    
    # Different qualifiers should produce different hash
    assert evidence_data["termlink_id"] != evidence_data3["termlink_id"], \
        "Different qualifiers should produce different termlink hash"
    
    # Validate the hypothetical qualifier is stored correctly
    qualifiers3 = evidence_data3.get("qualifiers", {})
    assert len(qualifiers3) == 1, f"Expected 1 qualifier, got {len(qualifiers3)}"
    assert qualifiers3.get("hypothetical") == "true", f"Expected hypothetical='true', got {qualifiers3.get('hypothetical')}"
    
    # Test 6: Validate empty qualifiers work
    evidence_payload4 = evidence_payload.copy()
    evidence_payload4["clinical_note_id"] = f"note-{uuid.uuid4()}"
    evidence_payload4["encounter_id"] = f"encounter-{uuid.uuid4()}"
    evidence_payload4["qualifiers"] = []  # No qualifiers
    
    create_evidence_response4 = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_payload4)
    assert create_evidence_response4["statusCode"] == 201
    evidence_id4 = json.loads(create_evidence_response4["body"])["evidence_id"]
    
    get_evidence_response4 = invoke_lambda(physical_resources["GetEvidenceFunction"], {
        "evidence_id": evidence_id4
    })
    assert get_evidence_response4["statusCode"] == 200
    evidence_data4 = json.loads(get_evidence_response4["body"])
    
    # Empty qualifiers should produce different hash than qualified evidence
    assert evidence_data["termlink_id"] != evidence_data4["termlink_id"], \
        "Empty qualifiers should produce different hash than qualified evidence"
    
    # Should have no qualifiers or empty qualifiers dict
    qualifiers4 = evidence_data4.get("qualifiers", {})
    assert len(qualifiers4) == 0, f"Expected 0 qualifiers, got {len(qualifiers4)}"
    
    print("✅ All qualifier standardization tests passed:")
    print(f"  - Qualifiers stored as 'true' values: ✓")
    print(f"  - Hash consistency for same qualifiers: ✓") 
    print(f"  - Hash uniqueness for different qualifiers: ✓")
    print(f"  - Empty qualifiers handled correctly: ✓")
    print(f"  - API retrieval works with standardized format: ✓")
    
    # Cleanup
    invoke_lambda(physical_resources["RemoveEvidenceFunction"], {"evidence_id": evidence_id})
    invoke_lambda(physical_resources["RemoveEvidenceFunction"], {"evidence_id": evidence_id2})
    invoke_lambda(physical_resources["RemoveEvidenceFunction"], {"evidence_id": evidence_id3})
    invoke_lambda(physical_resources["RemoveEvidenceFunction"], {"evidence_id": evidence_id4})
    invoke_lambda(physical_resources["RemoveSubjectFunction"], {"subject_id": actual_subject_id})
