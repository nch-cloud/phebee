import json
import pytest
import uuid
from phebee.utils.aws import get_client

pytestmark = [pytest.mark.integration]

@pytest.mark.parametrize(
    "creator_payload,expected_evidence_type,expected_assertion_type",
    [
        # Manually created evidence based on ClinicalNote
        (
            {"creator_type": "human", "name": "Test Human"},
            "http://purl.obolibrary.org/obo/ECO_0006161",
            "http://purl.obolibrary.org/obo/ECO_0000218",
        ),
        # Computationally created evidence based on ClinicalNote
        (
            {"creator_type": "automated", "name": "Test Bot", "version": "1.0"},
            "http://purl.obolibrary.org/obo/ECO_0006162",
            "http://purl.obolibrary.org/obo/ECO_0000203",
        ),
        # Test default fallbacks
        (
            None,  # No creator payload
            "http://purl.obolibrary.org/obo/ECO_0000000",  # fallback evidence
            "http://purl.obolibrary.org/obo/ECO_0000217",  # fallback assertion
        ),
    ]
)
def test_text_annotation_with_inferred_evidence_and_assertion(
    physical_resources,
    creator_payload,
    expected_evidence_type,
    expected_assertion_type,
):
    lambda_client = get_client("lambda")

    create_clinical_note_fn = physical_resources["CreateClinicalNoteFunction"]
    create_annotation_fn = physical_resources["CreateTextAnnotationFunction"]
    get_annotation_fn = physical_resources["GetTextAnnotationFunction"]
    remove_annotation_fn = physical_resources["RemoveTextAnnotationFunction"]
    create_creator_fn = physical_resources["CreateCreatorFunction"]

    # --- Create Creator ---
    if creator_payload:
        creator_id = f"test-creator-{uuid.uuid4()}"
        creator_payload["creator_id"] = creator_id
        creator_resp = lambda_client.invoke(
            FunctionName=create_creator_fn,
            Payload=json.dumps({"body": json.dumps(creator_payload)}).encode("utf-8"),
            InvocationType="RequestResponse"
        )
        assert creator_resp["StatusCode"] == 200
        creator_body = json.loads(json.loads(creator_resp["Payload"].read())["body"])
        print(f"creator_body: {creator_body}")
        creator_iri = creator_body["creator_iri"]
    else:
        creator_iri = None

    # --- Create ClinicalNote (text source) ---
    clinical_note_id = f"note-{uuid.uuid4()}"
    encounter_iri = "http://example.org/phebee/subject/test-subject/encounter/test-encounter"
    note_payload = {
        "encounter_iri": encounter_iri,
        "clinical_note_id": clinical_note_id,
        "note_timestamp": "2024-01-01T12:00:00"
    }
    note_resp = lambda_client.invoke(
        FunctionName=create_clinical_note_fn,
        Payload=json.dumps({"body": json.dumps(note_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert note_resp["StatusCode"] == 200
    note_body = json.loads(json.loads(note_resp["Payload"].read())["body"])
    print(f"note_body: {note_body}")
    text_source_iri = note_body["clinical_note_iri"]

    # --- Create TextAnnotation ---
    annotation_payload = {
        "text_source_iri": text_source_iri,
        "creator_iri": creator_iri,
        "span_start": 5,
        "span_end": 25,
        "metadata": "{\"tool\": \"StarAnnotator\", \"score\": 0.98}"
    }
    create_resp = lambda_client.invoke(
        FunctionName=create_annotation_fn,
        Payload=json.dumps({"body": json.dumps(annotation_payload)}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert create_resp["StatusCode"] == 200
    annotation_body = json.loads(json.loads(create_resp["Payload"].read())["body"])
    annotation_iri = annotation_body["annotation_iri"]
    assert annotation_iri.startswith(text_source_iri + "/annotation/")

    # --- Get and check fields ---
    get_resp = lambda_client.invoke(
        FunctionName=get_annotation_fn,
        Payload=json.dumps({"body": json.dumps({"annotation_iri": annotation_iri})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert get_resp["StatusCode"] == 200
    get_body = json.loads(json.loads(get_resp["Payload"].read())["body"])
    print(f"get_body: {get_body}")
    assert get_body["annotation_iri"] == annotation_iri
    assert get_body["evidence_type"] == expected_evidence_type
    assert get_body["assertion_type"] == expected_assertion_type

    # --- Clean up ---
    remove_resp = lambda_client.invoke(
        FunctionName=remove_annotation_fn,
        Payload=json.dumps({"body": json.dumps({"annotation_iri": annotation_iri})}).encode("utf-8"),
        InvocationType="RequestResponse"
    )
    assert remove_resp["StatusCode"] == 200