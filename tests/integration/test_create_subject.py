import uuid
import json
import pytest
import os
from phebee.utils.aws import get_client
from general_utils import parse_lambda_response
import boto3

pytestmark = [pytest.mark.integration]


def test_create_new_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]
    get_fn = physical_resources["GetSubjectFunction"]

    project_id = test_project_id
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    print(f"project_id: {project_id}")

    payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id
    }

    response = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    status_code, body = parse_lambda_response(response)
    assert status_code == 200
    
    assert "subject" in body
    assert "iri" in body["subject"]
    assert body["subject_created"] is True
    assert body["subject"]["projects"][project_id] == project_subject_id

    # Verify Neptune graph creation using project_subject_iri
    get_payload = {
        "project_subject_iri": project_subject_iri
    }

    get_response = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps(get_payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    print(get_response)
    get_status_code, get_body = parse_lambda_response(get_response)
    assert get_status_code == 200
    assert "subject_iri" in get_body
    assert "project_subject_iri" in get_body
    assert "subject_id" in get_body  # Should now include subject_id
    assert get_body["project_subject_iri"] == project_subject_iri

    # Test the new subject_id path
    subject_id = get_body["subject_id"]
    get_by_id_payload = {
        "subject_id": subject_id
    }

    get_by_id_response = lambda_client.invoke(
        FunctionName=get_fn,
        Payload=json.dumps(get_by_id_payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    get_by_id_status_code, get_by_id_body = parse_lambda_response(get_by_id_response)
    assert get_by_id_status_code == 200
    assert "subject_iri" in get_by_id_body
    assert "subject_id" in get_by_id_body
    assert get_by_id_body["subject_id"] == subject_id
    assert get_by_id_body["subject_iri"] == get_body["subject_iri"]
    # Note: subject_id path won't have project_subject_iri since it doesn't know the project context
    assert "project_subject_iri" not in get_by_id_body

    # Verify DynamoDB mapping creation
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ['PheBeeDynamoTable']  # Use environment variable set by test setup
    table = dynamodb.Table(table_name)
    
    response = table.get_item(
        Key={
            'PK': f'PROJECT#{project_id}',
            'SK': f'SUBJECT#{project_subject_id}'
        }
    )
    
    assert 'Item' in response
    assert response['Item']['subject_id'] == body["subject"]["iri"]


def test_link_existing_subject(physical_resources, test_project_id):
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]

    project_id = test_project_id
    project_subject_id1 = f"subj1-{uuid.uuid4().hex[:6]}"
    project_subject_id2 = f"subj2-{uuid.uuid4().hex[:6]}"

    # First creation: creates subject
    first_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id1
    }

    first_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(first_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    first_status_code, first_body = parse_lambda_response(first_resp)
    print(f"first_body: {first_body}")
    subject_iri = first_body["subject"]["iri"]
    assert first_status_code == 200

    # Second call: link same subject to a new project_subject_iri
    second_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id2,
        "known_subject_iri": subject_iri
    }

    second_resp = lambda_client.invoke(
        FunctionName=create_fn,
        Payload=json.dumps({"body": json.dumps(second_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    second_status_code, second_body = parse_lambda_response(second_resp)
    assert second_status_code == 200

    print(f"second_body: {second_body}")
    assert second_body["subject_created"] is False
    assert second_body["subject"]["iri"] == subject_iri
    assert second_body["subject"]["projects"][project_id] == project_subject_id2


def test_get_subject_with_qualifiers(physical_resources, test_project_id):
    """Test that get_subject returns qualifiers correctly"""
    lambda_client = get_client("lambda")
    create_subject_fn = physical_resources["CreateSubjectFunction"]
    create_evidence_fn = physical_resources["CreateEvidenceFunction"]
    get_subject_fn = physical_resources["GetSubjectFunction"]

    project_id = test_project_id
    project_subject_id = f"test-qual-{uuid.uuid4().hex[:6]}"

    # Create subject
    subject_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id
    }

    subject_response = lambda_client.invoke(
        FunctionName=create_subject_fn,
        Payload=json.dumps({"body": json.dumps(subject_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    subject_status_code, subject_body = parse_lambda_response(subject_response)
    assert subject_status_code == 200
    subject_id = subject_body["subject"]["iri"].split("/")[-1]

    # Create evidence with qualifiers (negated=true, family=false, hypothetical=false)
    evidence_payload = {
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001627",
        "creator_id": "test-qualifier-creator",
        "evidence_type": "clinical_note",
        "clinical_note_id": f"note-{uuid.uuid4()}",
        "encounter_id": f"encounter-{uuid.uuid4()}",
        "span_start": 10,
        "span_end": 20,
        "qualifiers": ["negated"]  # This should result in negated=true
    }

    evidence_response = lambda_client.invoke(
        FunctionName=create_evidence_fn,
        Payload=json.dumps(evidence_payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    evidence_status_code, evidence_body = parse_lambda_response(evidence_response)
    assert evidence_status_code == 201

    # Wait a moment for the evidence to be available in Iceberg
    import time
    time.sleep(2)

    # Get subject and verify qualifiers are returned
    get_payload = {
        "subject_id": subject_id
    }

    get_response = lambda_client.invoke(
        FunctionName=get_subject_fn,
        Payload=json.dumps(get_payload).encode("utf-8"),
        InvocationType="RequestResponse"
    )

    get_status_code, get_body = parse_lambda_response(get_response)
    assert get_status_code == 200
    assert "terms" in get_body
    
    # Find the term we created evidence for
    target_term = None
    for term in get_body["terms"]:
        if term["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001627":
            target_term = term
            break
    
    assert target_term is not None, "Should find the term we created evidence for"
    assert "qualifiers" in target_term
    assert len(target_term["qualifiers"]) > 0, f"Should have qualifiers, got: {target_term}"
    assert "negated" in target_term["qualifiers"], f"Should include 'negated' qualifier, got: {target_term['qualifiers']}"
    assert target_term["evidence_count"] > 0, "Should have evidence count"