"""
Integration test for term hierarchy functionality using API endpoints.
"""

import pytest
import json
import requests
import gzip
import base64
from general_utils import invoke_lambda


def decode_response_body(response):
    """Decode response body, handling gzip compression if present."""
    body = response["body"]
    
    # Check if response is base64 encoded and gzipped
    if response.get("isBase64Encoded", False):
        # Decode base64
        compressed_data = base64.b64decode(body)
        # Decompress gzip
        decompressed_data = gzip.decompress(compressed_data)
        # Convert to string
        return decompressed_data.decode('utf-8')
    else:
        return body


@pytest.mark.integration
def test_get_subjects_with_hierarchy(physical_resources, test_project_id):
    """Test that querying a parent term returns subjects with child terms via API."""
    
    # Use HPO hierarchy: HP_0000152 (Abnormality of head or neck) -> HP_0000234 (Abnormality of the head)
    parent_term = "http://purl.obolibrary.org/obo/HP_0000152"  # Abnormality of head or neck
    child_term = "http://purl.obolibrary.org/obo/HP_0000234"   # Abnormality of the head
    
    # Create test subjects via API
    parent_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "hierarchy-parent-subject"
    })
    assert parent_subject_response["statusCode"] == 200
    parent_subject_data = json.loads(parent_subject_response["body"])
    print(f"CreateSubject response: {parent_subject_data}")
    
    # Extract the correct field - let's see what's actually returned
    if "subject" in parent_subject_data and "iri" in parent_subject_data["subject"]:
        parent_subject_iri = parent_subject_data["subject"]["iri"]
    elif "subject_iri" in parent_subject_data:
        parent_subject_iri = parent_subject_data["subject_iri"]
    else:
        pytest.fail(f"Cannot find subject IRI in response: {parent_subject_data}")
    
    child_subject_response = invoke_lambda(physical_resources["CreateSubjectFunction"], {
        "project_id": test_project_id,
        "project_subject_id": "hierarchy-child-subject"
    })
    assert child_subject_response["statusCode"] == 200
    child_subject_data = json.loads(child_subject_response["body"])
    
    if "subject" in child_subject_data and "iri" in child_subject_data["subject"]:
        child_subject_iri = child_subject_data["subject"]["iri"]
    elif "subject_iri" in child_subject_data:
        child_subject_iri = child_subject_data["subject_iri"]
    else:
        pytest.fail(f"Cannot find subject IRI in response: {child_subject_data}")
    
    # Extract UUID from IRI (last part after the last slash)
    parent_subject_id = parent_subject_iri.split('/')[-1]
    child_subject_id = child_subject_iri.split('/')[-1]
    
    # Create termlinks via API - one with parent term, one with child term
    parent_termlink_response = invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": parent_subject_id,
        "term_iri": parent_term,
        "creator_id": "test-hierarchy-creator",
        "source_node_iri": f"phebee:clinical_note#parent_note",
        "evidence_type": "clinical_annotation",
        "qualifiers": []
    })
    print(f"Parent termlink response: {parent_termlink_response}")
    assert parent_termlink_response["statusCode"] == 200
    
    child_termlink_response = invoke_lambda(physical_resources["CreateTermLinkFunction"], {
        "subject_id": child_subject_id,
        "term_iri": child_term,
        "creator_id": "test-hierarchy-creator",
        "source_node_iri": f"phebee:clinical_note#child_note",
        "evidence_type": "clinical_annotation",
        "qualifiers": []
    })
    print(f"Child termlink response: {child_termlink_response}")
    assert child_termlink_response["statusCode"] == 200
    
    # Test: Query parent term with include_child_terms=True (default)
    # Should return both subjects (one with parent term, one with child term)
    hierarchy_response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], {
        "project_id": test_project_id,
        "term_iri": parent_term,
        "include_child_terms": True,
        "output_type": "json"
    })
    
    print(f"Hierarchy query response: {hierarchy_response}")
    assert hierarchy_response["statusCode"] == 200
    
    # Decode the potentially compressed response
    hierarchy_body_str = decode_response_body(hierarchy_response)
    hierarchy_body = json.loads(hierarchy_body_str)
    print(f"Hierarchy response body: {hierarchy_body}")
    
    # Let's see what the actual structure is and adapt
    if "body" in hierarchy_body:
        subjects = hierarchy_body["body"]
        print(f"Found {len(subjects)} subjects")
        if len(subjects) > 0:
            print(f"First subject structure: {subjects[0]}")
            
        # Extract subject IRIs from the response
        subject_iris = [subject["subject_iri"] for subject in subjects]
        print(f"Subject IRIs found: {subject_iris}")
        print(f"Looking for parent: {parent_subject_iri}")
        print(f"Looking for child: {child_subject_iri}")
        
        # Check if we found our subjects
        found_parent = parent_subject_iri in subject_iris
        found_child = child_subject_iri in subject_iris
        
        print(f"Found parent subject: {found_parent}")
        print(f"Found child subject: {found_child}")
        
        # Hierarchy test - parent term query MUST find both subjects
        assert found_parent, f"Should find the parent subject {parent_subject_iri}"
        assert found_child, f"Hierarchy FAILED: Parent term query should find child subject {child_subject_iri} due to hierarchy, but only found: {subject_iris}"

        print(f"✅ Hierarchy test PASSED - Parent term found both parent and child subjects via hierarchy")

        # Verify term labels are populated from cache
        for subject in subjects:
            if "term_links" in subject:
                for term_link in subject["term_links"]:
                    assert "term_label" in term_link, f"term_label missing from term_link: {term_link}"
                    assert isinstance(term_link["term_label"], str), f"term_label should be string, found {type(term_link['term_label'])}"
                    assert len(term_link["term_label"]) > 0, f"term_label should not be empty"

                    # Verify expected labels for our test terms
                    if term_link["term_iri"] == parent_term:
                        assert term_link["term_label"] == "Abnormality of head or neck", \
                            f"Expected label 'Abnormality of head or neck', found '{term_link['term_label']}'"
                    elif term_link["term_iri"] == child_term:
                        assert term_link["term_label"] == "Abnormality of the head", \
                            f"Expected label 'Abnormality of the head', found '{term_link['term_label']}'"

        print("✅ Term labels verified - All terms have labels from cache")
    else:
        pytest.fail(f"No 'body' field in response: {hierarchy_body}")
    
    # Test 2: Query the child term directly - should find the child subject
    child_query_response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], {
        "project_id": test_project_id,
        "term_iri": child_term,
        "include_child_terms": True,
        "output_type": "json"
    })
    
    print(f"Child term query response: {child_query_response}")
    assert child_query_response["statusCode"] == 200
    
    child_body_str = decode_response_body(child_query_response)
    child_body = json.loads(child_body_str)
    print(f"Child term response body: {child_body}")
    
    if "body" in child_body:
        child_subjects = child_body["body"]
        child_subject_iris = [subject["subject_iri"] for subject in child_subjects]
        
        print(f"Child term query found {len(child_subjects)} subjects: {child_subject_iris}")
        
        # Should find the child subject when querying the child term directly
        assert child_subject_iri in child_subject_iris, f"Should find child subject {child_subject_iri} when querying child term {child_term}"

        # Verify labels exist for child term query
        for subject in child_subjects:
            if "term_links" in subject:
                for term_link in subject["term_links"]:
                    assert "term_label" in term_link, "term_label missing from child query term_link"
                    assert len(term_link["term_label"]) > 0, "term_label should not be empty"

        print("✅ Child term query test passed - Found child subject when querying child term directly")
    else:
        pytest.fail(f"No 'body' field in child term response: {child_body}")
    
    # Test 3: Query parent term with include_child_terms=False
    # Should only return the parent subject, NOT the child subject
    exact_only_response = invoke_lambda(physical_resources["GetSubjectsPhenotypesFunction"], {
        "project_id": test_project_id,
        "term_iri": parent_term,
        "include_child_terms": False,
        "output_type": "json"
    })
    
    print(f"Exact only query response: {exact_only_response}")
    assert exact_only_response["statusCode"] == 200
    
    exact_only_body_str = decode_response_body(exact_only_response)
    exact_only_body = json.loads(exact_only_body_str)
    print(f"Exact only response body: {exact_only_body}")
    
    if "body" in exact_only_body:
        exact_only_subjects = exact_only_body["body"]
        exact_only_subject_iris = [subject["subject_iri"] for subject in exact_only_subjects]
        
        print(f"Exact only query found {len(exact_only_subjects)} subjects: {exact_only_subject_iris}")
        
        # Should find ONLY the parent subject, NOT the child subject
        assert parent_subject_iri in exact_only_subject_iris, f"Should find parent subject {parent_subject_iri} when querying parent term exactly"
        assert child_subject_iri not in exact_only_subject_iris, f"Should NOT find child subject {child_subject_iri} when include_child_terms=False"
        assert len(exact_only_subjects) == 1, f"Should find exactly 1 subject when include_child_terms=False, but found {len(exact_only_subjects)}"

        # Verify labels exist for exact match query
        for subject in exact_only_subjects:
            if "term_links" in subject:
                for term_link in subject["term_links"]:
                    assert "term_label" in term_link, "term_label missing from exact match query term_link"
                    assert len(term_link["term_label"]) > 0, "term_label should not be empty"

        print("✅ Exact matching test passed - Parent term with include_child_terms=False found only parent subject")
    else:
        pytest.fail(f"No 'body' field in exact only response: {exact_only_body}")
