import json
import pytest


def invoke_lambda(name, payload):
    """Invoke a Lambda and return the parsed body if API-proxy shaped, else the raw payload."""
    import boto3
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName=name,
        Payload=json.dumps(payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    
    payload_data = json.loads(response["Payload"].read())
    
    # If it looks like an API Gateway proxy response, extract the body
    if isinstance(payload_data, dict) and "statusCode" in payload_data and "body" in payload_data:
        if payload_data.get("statusCode") != 200:
            return payload_data  # Return error responses as-is
        return json.loads(payload_data["body"])
    
    # Otherwise return the raw payload
    return payload_data


@pytest.mark.integration
def test_get_subject_term_info_basic(test_payload, test_project_id, physical_resources):
    """Test GetSubjectTermInfo with basic term (no qualifiers)."""
    from test_bulk_upload import bulk_upload_run
    
    # Upload test data first
    bulk_upload_run([test_payload], physical_resources)
    
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_subject_term_info_fn = physical_resources["GetSubjectTermInfoFunction"]
    
    # Get subject to find a term to query
    entry = test_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    subject_iri = subject_result["subject_iri"]
    first_term = subject_result["terms"][0]
    
    # Test GetSubjectTermInfo
    result = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": subject_iri,
            "term_iri": first_term["term_iri"],
            "qualifiers": first_term["qualifiers"]
        })
    })
    
    # Verify response structure
    assert result["term_iri"] == first_term["term_iri"]
    assert result["qualifiers"] == first_term["qualifiers"]
    assert result["evidence_count"] == first_term["evidence_count"]
    assert len(result["term_links"]) == first_term["term_link_count"]
    
    # Verify term_links structure
    for term_link in result["term_links"]:
        assert "termlink_iri" in term_link
        assert "evidence_count" in term_link
        assert "source_iri" in term_link
        assert "source_type" in term_link
        assert isinstance(term_link["evidence_count"], int)
        assert term_link["evidence_count"] > 0


@pytest.mark.integration
def test_get_subject_term_info_not_found(physical_resources):
    """Test GetSubjectTermInfo with non-existent term."""
    get_subject_term_info_fn = physical_resources["GetSubjectTermInfoFunction"]
    
    # Try to get info for non-existent term
    response = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/nonexistent",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0000001",
            "qualifiers": []
        })
    })
    
    assert response["statusCode"] == 404
    body = json.loads(response["body"])
    assert "not found" in body["message"].lower()


@pytest.mark.integration
def test_get_subject_term_info_missing_params(physical_resources):
    """Test GetSubjectTermInfo with missing required parameters."""
    get_subject_term_info_fn = physical_resources["GetSubjectTermInfoFunction"]
    
    # Missing subject_iri
    response = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "term_iri": "http://purl.obolibrary.org/obo/HP_0000001"
        })
    })
    
    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "subject_iri" in body["message"]
    
    # Missing term_iri
    response = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/test"
        })
    })
    
    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "term_iri" in body["message"]


@pytest.mark.integration
def test_get_subject_term_info_with_qualifiers(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Test GetSubjectTermInfo with qualifiers."""
    from test_bulk_upload import bulk_upload_run
    
    # Upload test data with qualifiers
    bulk_upload_run([test_payload_with_qualifiers], physical_resources)
    
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_subject_term_info_fn = physical_resources["GetSubjectTermInfoFunction"]
    
    # Get subject to find terms with qualifiers
    entry = test_payload_with_qualifiers[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    subject_iri = subject_result["subject_iri"]
    
    # Find a term with qualifiers
    term_with_qualifiers = None
    for term in subject_result["terms"]:
        if term["qualifiers"]:
            term_with_qualifiers = term
            break
    
    assert term_with_qualifiers is not None, "Should have at least one term with qualifiers"
    
    # Test GetSubjectTermInfo with qualifiers
    result = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": subject_iri,
            "term_iri": term_with_qualifiers["term_iri"],
            "qualifiers": term_with_qualifiers["qualifiers"]
        })
    })
    
    # Verify response includes qualifiers
    assert result["term_iri"] == term_with_qualifiers["term_iri"]
    assert result["qualifiers"] == term_with_qualifiers["qualifiers"]
    assert len(result["qualifiers"]) > 0
    assert result["evidence_count"] == term_with_qualifiers["evidence_count"]
@pytest.mark.integration
def test_get_subject_term_info_negated_qualifier_performance(test_payload_with_qualifiers, test_project_id, physical_resources):
    """Test GetSubjectTermInfo with negated qualifier - this was timing out before optimization."""
    import time
    from test_bulk_upload import bulk_upload_run
    
    # Upload test data with qualifiers
    bulk_upload_run([test_payload_with_qualifiers], physical_resources)
    
    get_subject_fn = physical_resources["GetSubjectFunction"]
    get_subject_term_info_fn = physical_resources["GetSubjectTermInfoFunction"]
    
    # Get subject to find terms
    entry = test_payload_with_qualifiers[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{entry['project_id']}/{entry['project_subject_id']}"
    subject_result = invoke_lambda(get_subject_fn, {"body": json.dumps({"project_subject_iri": project_subject_iri})})
    
    subject_iri = subject_result["subject_iri"]
    term_iri = entry["term_iri"]
    
    # Test with empty qualifiers (should be fast)
    start_time = time.time()
    result_empty = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": subject_iri,
            "term_iri": term_iri,
            "qualifiers": []
        })
    })
    empty_duration = time.time() - start_time
    
    # Test with negated qualifier (was timing out before optimization)
    start_time = time.time()
    result_negated = invoke_lambda(get_subject_term_info_fn, {
        "body": json.dumps({
            "subject_iri": subject_iri,
            "term_iri": term_iri,
            "qualifiers": ["http://ods.nationwidechildrens.org/phebee/qualifier/negated"]
        })
    })
    negated_duration = time.time() - start_time
    
    # Both should complete successfully
    assert result_empty["term_iri"] == term_iri
    assert result_negated["term_iri"] == term_iri
    
    # The negated query should not be significantly slower (within 10x)
    assert negated_duration < empty_duration * 10, f"Negated query took {negated_duration:.2f}s vs empty {empty_duration:.2f}s"
    
    # Both should complete within reasonable time (30 seconds)
    assert empty_duration < 30, f"Empty qualifiers query took {empty_duration:.2f}s"
    assert negated_duration < 30, f"Negated qualifier query took {negated_duration:.2f}s"


