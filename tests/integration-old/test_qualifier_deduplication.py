import pytest
import json
import uuid
import gzip
import base64
from general_utils import invoke_lambda


def decompress_lambda_response(result):
    """Helper function to decompress gzipped Lambda responses."""
    if "body" in result:
        try:
            # Try to decode as base64 and decompress
            compressed_data = base64.b64decode(result["body"])
            decompressed_data = gzip.decompress(compressed_data)
            return json.loads(decompressed_data.decode('utf-8'))
        except Exception:
            raise ValueError("Unable to parse response body")
    else:
        raise ValueError("No body in response")


@pytest.mark.integration
def test_get_subjects_pheno_qualifier_deduplication(physical_resources, test_project_id):
    """Test that get_subjects_pheno returns distinct qualifiers and doesn't show duplicates"""
    
    # Create unique test data
    subject_id = str(uuid.uuid4())
    term_iri = "http://purl.obolibrary.org/obo/HP_0001627"  # Abnormal heart morphology
    
    # Create subject first
    create_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": f"test-qualifier-dedup-{subject_id[:8]}"
    })
    assert create_subject_response["statusCode"] == 200
    subject_data = json.loads(create_subject_response["body"])
    
    # Extract actual subject_id from the response
    if "subject" in subject_data and "iri" in subject_data["subject"]:
        subject_iri = subject_data["subject"]["iri"]
        actual_subject_id = subject_iri.split("/")[-1]
    elif "subject_iri" in subject_data:
        actual_subject_id = subject_data["subject_iri"].split("/")[-1]
    else:
        pytest.fail(f"Cannot find subject IRI in response: {subject_data}")
    
    # Create termlink 1: Same term with NO qualifiers
    termlink_payload_1 = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-qualifier-creator-1",
        "qualifiers": []  # No qualifiers
    }
    
    create_termlink_response_1 = invoke_lambda(physical_resources["CreateTermLinkFunction"], termlink_payload_1)
    assert create_termlink_response_1["statusCode"] == 200
    
    # Create termlink 2: Same term with NEGATED qualifier
    termlink_payload_2 = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-qualifier-creator-2",
        "qualifiers": ["http://ods.nationwidechildrens.org/phebee/qualifier/negated"]  # Full IRI
    }
    
    create_termlink_response_2 = invoke_lambda(physical_resources["CreateTermLinkFunction"], termlink_payload_2)
    assert create_termlink_response_2["statusCode"] == 200
    
    # Create termlink 3: Same term with FAMILY qualifier
    termlink_payload_3 = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-qualifier-creator-3",
        "qualifiers": ["http://ods.nationwidechildrens.org/phebee/qualifier/family"]  # Full IRI
    }
    
    create_termlink_response_3 = invoke_lambda(physical_resources["CreateTermLinkFunction"], termlink_payload_3)
    assert create_termlink_response_3["statusCode"] == 200
    
    # Query subjects with phenotypes
    query_payload = {
        "project_id": test_project_id,
        "project_subject_ids": [f"test-qualifier-dedup-{subject_id[:8]}"]
    }
    
    query_response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], query_payload)
    assert query_response["statusCode"] == 200
    
    # Decompress and parse response
    response_body = decompress_lambda_response(query_response)
    subjects = response_body["body"]
    
    # Find our test subject
    test_subject = None
    for subject in subjects:
        if subject["project_subject_id"] == f"test-qualifier-dedup-{subject_id[:8]}":
            test_subject = subject
            break
    
    assert test_subject is not None, "Test subject not found in response"
    
    # Find term links for our test term
    test_term_links = [
        link for link in test_subject["term_links"] 
        if link["term_iri"] == term_iri
    ]
    
    print(f"Found {len(test_term_links)} term links for {term_iri}")
    for i, link in enumerate(test_term_links):
        print(f"  Link {i+1}: qualifiers = {link['qualifiers']}")
    
    # Assertions
    assert len(test_term_links) == 3, f"Expected 3 distinct term links, got {len(test_term_links)}"
    
    # Extract qualifier sets for comparison
    qualifier_sets = [set(link["qualifiers"]) for link in test_term_links]
    
    # Expected qualifier sets (using PheBee namespace)
    expected_qualifier_sets = [
        set(),  # No qualifiers
        {"http://ods.nationwidechildrens.org/phebee/qualifier/negated"},  # Negated
        {"http://ods.nationwidechildrens.org/phebee/qualifier/family"}   # Family
    ]
    
    # Verify we have all expected qualifier combinations
    for expected_set in expected_qualifier_sets:
        assert expected_set in qualifier_sets, f"Missing qualifier set: {expected_set}"
    
    # Verify no duplicates (all qualifier sets should be unique)
    assert len(qualifier_sets) == len(set(frozenset(qs) for qs in qualifier_sets)), \
        "Found duplicate qualifier sets - this indicates the bug is still present"
    
    # Verify all term links have the same term_iri and term_label
    term_iris = {link["term_iri"] for link in test_term_links}
    term_labels = {link["term_label"] for link in test_term_links}
    
    assert len(term_iris) == 1, "All term links should have the same term_iri"
    assert len(term_labels) == 1, "All term links should have the same term_label"
    assert list(term_iris)[0] == term_iri, "Term IRI should match what we created"


@pytest.mark.integration
def test_get_subjects_pheno_multiple_qualifiers_per_termlink(physical_resources, test_project_id):
    """Test that a single termlink with multiple qualifiers is handled correctly"""
    
    # Create unique test data
    subject_id = str(uuid.uuid4())
    term_iri = "http://purl.obolibrary.org/obo/HP_0000924"  # Abnormality of the skeletal system
    
    # Create subject first
    create_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": f"test-multi-qual-{subject_id[:8]}"
    })
    assert create_subject_response["statusCode"] == 200
    subject_data = json.loads(create_subject_response["body"])
    
    # Extract actual subject_id from the response
    if "subject" in subject_data and "iri" in subject_data["subject"]:
        subject_iri = subject_data["subject"]["iri"]
        actual_subject_id = subject_iri.split("/")[-1]
    elif "subject_iri" in subject_data:
        actual_subject_id = subject_data["subject_iri"].split("/")[-1]
    else:
        pytest.fail(f"Cannot find subject IRI in response: {subject_data}")
    
    # Create termlink with multiple qualifiers
    termlink_payload = {
        "subject_id": actual_subject_id,
        "term_iri": term_iri,
        "creator_id": "test-multi-qualifier-creator",
        "qualifiers": [
            "http://ods.nationwidechildrens.org/phebee/qualifier/negated",
            "http://ods.nationwidechildrens.org/phebee/qualifier/family"
        ]  # Multiple full IRIs
    }
    
    create_termlink_response = invoke_lambda(physical_resources["CreateTermLinkFunction"], termlink_payload)
    assert create_termlink_response["statusCode"] == 200
    
    # Query subjects with phenotypes
    query_payload = {
        "project_id": test_project_id,
        "project_subject_ids": [f"test-multi-qual-{subject_id[:8]}"]
    }
    
    query_response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], query_payload)
    assert query_response["statusCode"] == 200
    
    # Decompress and parse response
    response_body = decompress_lambda_response(query_response)
    subjects = response_body["body"]
    
    # Find our test subject
    test_subject = None
    for subject in subjects:
        if subject["project_subject_id"] == f"test-multi-qual-{subject_id[:8]}":
            test_subject = subject
            break
    
    assert test_subject is not None, "Test subject not found in response"
    
    # Find term links for our test term
    test_term_links = [
        link for link in test_subject["term_links"] 
        if link["term_iri"] == term_iri
    ]
    
    print(f"Found {len(test_term_links)} term links for {term_iri}")
    for i, link in enumerate(test_term_links):
        print(f"  Link {i+1}: qualifiers = {link['qualifiers']}")
    
    # Assertions
    assert len(test_term_links) == 1, f"Expected 1 term link with multiple qualifiers, got {len(test_term_links)}"
    
    # Verify the single term link has both qualifiers
    qualifiers = set(test_term_links[0]["qualifiers"])
    expected_qualifiers = {
        "http://ods.nationwidechildrens.org/phebee/qualifier/negated",
        "http://ods.nationwidechildrens.org/phebee/qualifier/family"
    }
    
    assert qualifiers == expected_qualifiers, f"Expected qualifiers {expected_qualifiers}, got {qualifiers}"
