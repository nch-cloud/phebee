import uuid
import json
import pytest
import requests
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration, pytest.mark.api]

@pytest.mark.usefixtures("update_hpo")
def test_subjects_query_with_term_and_evidence(
    api_base_url,
    sigv4_auth,
    physical_resources,
    create_test_subject
):
    lambda_client = get_client("lambda")

    # --- Create subject ---
    subject = create_test_subject()
    
    subject_iri = subject["iri"]
    project_id = next(iter(subject["projects"].keys()))
    project_subject_id = subject["projects"][project_id]

    print(f"subject: {subject}")
    print(f"project_id: {project_id}")
    print(f"project_subject_id: {project_subject_id}")

    # --- Create creator ---
    creator_id = f"test-creator-{uuid.uuid4().hex[:6]}"
    creator_payload = {
        "creator_id": creator_id,
        "creator_type": "automated",
        "name": "Test NLP Bot",
        "version": "1.0.0"
    }
    lambda_client.invoke(
        FunctionName=physical_resources["CreateCreatorFunction"],
        Payload=json.dumps({"body": json.dumps(creator_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id}"

    # --- Create evidence (TextAnnotation) ---
    text_source_iri = "http://example.org/text-source/test"
    annotation_payload = {
        "text_source_iri": text_source_iri,
        "span_start": 0,
        "span_end": 10,
        "creator_iri": creator_iri,
        "metadata": "{\"source\": \"test\"}"
    }
    create_annotation_resp = lambda_client.invoke(
        FunctionName=physical_resources["CreateTextAnnotationFunction"],
        Payload=json.dumps({"body": json.dumps(annotation_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    annotation_body = json.loads(json.loads(create_annotation_resp["Payload"].read())["body"])
    annotation_iri = annotation_body["annotation_iri"]

    # --- Create TermLink ---
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    termlink_payload = {
        "source_node_iri": subject_iri,
        "term_iri": term_iri,
        "creator_iri": creator_iri,
        "evidence_iris": [annotation_iri]
    }
    lambda_client.invoke(
        FunctionName=physical_resources["CreateTermLinkFunction"],
        Payload=json.dumps({"body": json.dumps(termlink_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    # --- Run API query ---
    resp = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": project_id,
            "term_iri": term_iri,
            "term_source": "hpo",
        },
        auth=sigv4_auth
    )

    assert resp.status_code == 200, f"Unexpected status: {resp.status_code} - {resp.text}"
    response_data = resp.json()
    results = response_data["body"]  # Handle both old and new format
    print(f"results: {results}")
    assert isinstance(results, list)

    subject = next((s for s in results if s["project_subject_id"] == project_subject_id), None)
    assert subject is not None
    assert "term_links" in subject

    term_links = subject["term_links"]
    assert any(tl["term_iri"] == term_iri for tl in term_links)


@pytest.mark.usefixtures("update_hpo")
def test_subjects_query_pagination_basic(
    api_base_url,
    sigv4_auth,
    physical_resources,
    create_test_subject
):
    """Test basic pagination functionality via API."""
    # Create multiple subjects for pagination testing
    subjects = [create_test_subject() for _ in range(3)]
    project_id = next(iter(subjects[0]["projects"].keys()))

    # Test with limit parameter
    resp = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": project_id,
            "limit": 2
        },
        auth=sigv4_auth
    )

    assert resp.status_code == 200, f"Unexpected status: {resp.status_code} - {resp.text}"
    response_data = resp.json()
    
    # Check response structure
    assert "body" in response_data, "Response should contain 'body'"
    assert "pagination" in response_data, "Response should contain 'pagination'"
    
    subjects_data = response_data["body"]
    pagination = response_data["pagination"]
    
    # Should return exactly 2 subjects
    assert len(subjects_data) == 2, f"Expected 2 subjects, got {len(subjects_data)}"
    
    # Check pagination metadata
    assert pagination["limit"] == 2, "Pagination limit should match request"
    assert pagination["has_more"] is True, "Should have more pages available"
    assert pagination["next_cursor"] is not None, "Should provide next_cursor"


@pytest.mark.usefixtures("update_hpo")
def test_subjects_query_pagination_cursor(
    api_base_url,
    sigv4_auth,
    physical_resources,
    create_test_subject
):
    """Test cursor-based pagination across multiple pages via API."""
    # Create multiple subjects for pagination testing
    subjects = [create_test_subject() for _ in range(4)]
    project_id = next(iter(subjects[0]["projects"].keys()))

    all_subjects = []
    cursor = None
    page_count = 0
    
    # Fetch all subjects using pagination
    while True:
        page_count += 1
        query_data = {
            "project_id": project_id,
            "limit": 2
        }
        if cursor:
            query_data["cursor"] = cursor
            
        resp = requests.post(
            f"{api_base_url}/subjects/query",
            json=query_data,
            auth=sigv4_auth
        )
        
        assert resp.status_code == 200, f"Page {page_count} failed: {resp.status_code} - {resp.text}"
        response_data = resp.json()
        
        page_subjects = response_data["body"]
        all_subjects.extend(page_subjects)
        
        pagination = response_data["pagination"]
        if not pagination["has_more"]:
            break
            
        cursor = pagination["next_cursor"]
        assert cursor is not None, "next_cursor should be provided when has_more=True"
        
        # Safety check to prevent infinite loops
        assert page_count < 10, "Too many pages, possible infinite loop"
    
    # Should have collected all subjects across pages
    assert len(all_subjects) >= 4, f"Expected at least 4 subjects, got {len(all_subjects)}"
    
    # Subject IRIs should be unique (no duplicates across pages)
    subject_iris = [s["subject_iri"] for s in all_subjects]
    assert len(set(subject_iris)) == len(subject_iris), "Subjects should be unique across pages"


@pytest.mark.usefixtures("update_hpo")
def test_subjects_query_pagination_invalid_cursor(
    api_base_url,
    sigv4_auth,
    physical_resources,
    create_test_subject
):
    """Test pagination with invalid cursor via API."""
    subject = create_test_subject()
    project_id = next(iter(subject["projects"].keys()))

    # Test with invalid cursor (should still work, cursor ignored)
    resp = requests.post(
        f"{api_base_url}/subjects/query",
        json={
            "project_id": project_id,
            "limit": 2,
            "cursor": "invalid-cursor-value"
        },
        auth=sigv4_auth
    )

    assert resp.status_code == 200, f"Unexpected status: {resp.status_code} - {resp.text}"
    response_data = resp.json()
    
    # Should still return results and pagination metadata
    assert "body" in response_data, "Response should contain 'body'"
    assert "pagination" in response_data, "Response should contain 'pagination'"