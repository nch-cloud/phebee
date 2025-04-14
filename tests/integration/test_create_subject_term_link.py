import pytest
import json
from smart_open import open

from phebee.utils.aws import get_client, get_current_timestamp

from constants import TEST_PROJECT_ID, TEST_PROJECT_LABEL
from test_create_project import create_project, remove_project
from test_create_subject import create_subject, remove_subject, get_subject
from general_utils import check_timestamp_in_test


SUBJECT_1_ID = "subject_1"

TERM_1_IRI = "http://purl.obolibrary.org/obo/HP_0012345"
TERM_2_IRI = "http://purl.obolibrary.org/obo/HP_0034567"

AUTOMATIC_ASSERTION_ID = "http://purl.obolibrary.org/obo/ECO_0000203"
HIGH_THROUGHPUT_EVIDENCE_ID = "http://purl.obolibrary.org/obo/ECO_0006057"


@pytest.fixture(scope="module")
def prepare_create_subject_term_link(physical_resources, update_hpo):
    """Ensure that the project doesn't exist before the test and is removed after the test"""

    lambda_client = get_client("lambda")

    create_project(
        TEST_PROJECT_ID, TEST_PROJECT_LABEL, lambda_client, physical_resources
    )

    yield

    response = remove_project(TEST_PROJECT_ID, lambda_client, physical_resources)

    assert (
        response["statusCode"] == 200
    ), f"Failed to remove the {TEST_PROJECT_ID} project"


def create_subject_term_link_from_evidence_list(
    evidence_list, lambda_client, physical_resources
):
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectTermLinkFunction"],
        Payload=json.dumps({"evidence_list": evidence_list}),
    )
    result = json.loads(response["Payload"].read())
    print(result)
    return result


def create_subject_term_link_from_s3_evidence(
    s3_path, lambda_client, physical_resources
):
    response = lambda_client.invoke(
        FunctionName=physical_resources["CreateSubjectTermLinkFunction"],
        Payload=json.dumps({"evidence_s3_path": s3_path}),
    )
    result = json.loads(response["Payload"].read())
    print(result)
    return result


@pytest.mark.integration
def test_create_new_link(
    cloudformation_stack, physical_resources, prepare_create_subject_term_link
):
    lambda_client = get_client("lambda")

    test_start_time = get_current_timestamp()

    create_subject(TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources)

    evidence_list_1 = [
        {
            "project_id": TEST_PROJECT_ID,
            "project_subject_id": SUBJECT_1_ID,
            "term_iri": TERM_1_IRI,
            "evidence": {
                "creator": "integration tests",
                "evidence_type": HIGH_THROUGHPUT_EVIDENCE_ID,
                "assertion_method": AUTOMATIC_ASSERTION_ID,
            },
        }
    ]

    # Add a link from our subject to a term

    create_subject_term_link_from_evidence_list(
        evidence_list_1, lambda_client, physical_resources
    )

    subject = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )

    print(subject)

    assert TERM_1_IRI in subject["terms"]
    term_1_evidence = subject["terms"][TERM_1_IRI]["evidence"]
    assert len(term_1_evidence) == 1
    assert term_1_evidence[0]["creator"] == "integration tests"
    assert check_timestamp_in_test(term_1_evidence[0]["created"], test_start_time)
    assert check_timestamp_in_test(
        term_1_evidence[0]["evidence_created"], test_start_time
    )
    assert term_1_evidence[0]["evidence_type"] == HIGH_THROUGHPUT_EVIDENCE_ID
    assert term_1_evidence[0]["assertion_method"] == AUTOMATIC_ASSERTION_ID

    # Add a link to a second term, this time with some optional properties

    evidence_list_2 = [
        {
            "project_id": TEST_PROJECT_ID,
            "project_subject_id": SUBJECT_1_ID,
            "term_iri": TERM_2_IRI,
            "evidence": {
                "creator": "integration tests",
                "evidence_type": HIGH_THROUGHPUT_EVIDENCE_ID,
                "assertion_method": AUTOMATIC_ASSERTION_ID,
                "excluded": "true",
                "creator_version": "1.0",
                "term_source": "hpo",
                "term_source_version": "2.0",
                "evidence_text": "this patient has optic pits",
                "evidence_reference_id": "FAKE_PUBMED_ID",
                "evidence_reference_description": "This is a fake article",
                "evidence_created": "2000-01-01",
            },
        }
    ]

    create_subject_term_link_from_evidence_list(
        evidence_list_2, lambda_client, physical_resources
    )

    subject = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )

    # Make sure we now have both terms
    assert set(subject["terms"]) == set(subject["terms"].keys())
    term_2_evidence = subject["terms"][TERM_2_IRI]["evidence"]

    print(term_2_evidence)

    # Check that we can retrieve the optional properties
    assert term_2_evidence[0]["excluded"]
    assert term_2_evidence[0]["creator_version"] == "1.0"
    assert term_2_evidence[0]["term_source"] == "hpo"
    assert term_2_evidence[0]["term_source_version"] == "2.0"
    assert term_2_evidence[0]["evidence_text"] == "this patient has optic pits"
    assert term_2_evidence[0]["evidence_reference_id"] == "FAKE_PUBMED_ID"
    assert (
        term_2_evidence[0]["evidence_reference_description"] == "This is a fake article"
    )
    assert ~check_timestamp_in_test(
        term_2_evidence[0]["evidence_created"], test_start_time
    )

    # Add another line of evidence to our second term, make sure the term list and evidence list are correct

    evidence_list_3 = [
        {
            "project_id": TEST_PROJECT_ID,
            "project_subject_id": SUBJECT_1_ID,
            "term_iri": TERM_1_IRI,
            "evidence": {
                "creator": "another integration test",
                "evidence_type": HIGH_THROUGHPUT_EVIDENCE_ID,
                "assertion_method": AUTOMATIC_ASSERTION_ID,
            },
        }
    ]

    create_subject_term_link_from_evidence_list(
        evidence_list_3, lambda_client, physical_resources
    )

    subject = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )

    assert len(subject["terms"]) == 2 and set(subject["terms"]) == set(
        subject["terms"].keys()
    )
    term_1_evidence = subject["terms"][TERM_1_IRI]["evidence"]
    print(term_1_evidence)
    assert len(term_1_evidence) == 2
    assert set([e["creator"] for e in term_1_evidence]) == set(
        ["integration tests", "another integration test"]
    )

    response = remove_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    print(response)
    assert response["statusCode"] == 200, f"Failed to remove the {SUBJECT_1_ID} subject"


@pytest.mark.integration
def test_create_new_link_from_s3_evidence(
    cloudformation_stack, physical_resources, prepare_create_subject_term_link
):
    test_start_time = get_current_timestamp()

    lambda_client = get_client("lambda")
    s3_client = get_client("s3")

    create_subject(TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources)

    evidence_list = [
        {
            "project_id": TEST_PROJECT_ID,
            "project_subject_id": SUBJECT_1_ID,
            "term_iri": TERM_1_IRI,
            "evidence": {
                "creator": "integration tests using s3 evidence",
                "evidence_type": HIGH_THROUGHPUT_EVIDENCE_ID,
                "assertion_method": AUTOMATIC_ASSERTION_ID,
            },
        }
    ]

    s3_path = f"s3://{physical_resources['PheBeeBucket']}/subject_term_evidence.json"
    with open(s3_path, "w", transport_params={'client': s3_client}) as s3_output:
        json.dump(evidence_list, s3_output)

    create_subject_term_link_from_s3_evidence(
        s3_path, lambda_client, physical_resources
    )

    subject = get_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )

    print(subject)

    assert TERM_1_IRI in subject["terms"]
    term_1_evidence = subject["terms"][TERM_1_IRI]["evidence"]
    assert len(term_1_evidence) == 1
    assert term_1_evidence[0]["creator"] == "integration tests using s3 evidence"
    assert check_timestamp_in_test(term_1_evidence[0]["created"], test_start_time)
    assert check_timestamp_in_test(
        term_1_evidence[0]["evidence_created"], test_start_time
    )
    assert term_1_evidence[0]["evidence_type"] == HIGH_THROUGHPUT_EVIDENCE_ID
    assert term_1_evidence[0]["assertion_method"] == AUTOMATIC_ASSERTION_ID

    response = remove_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    print(response)
    assert response["statusCode"] == 200, f"Failed to remove the {SUBJECT_1_ID} subject"

    s3_client = get_client("s3")
    s3_client.delete_object(
        Bucket=physical_resources["PheBeeBucket"],
        Key="subject_term_evidence.json",
    )


@pytest.mark.integration
def test_create_link_to_nonexistent_term(
    cloudformation_stack, physical_resources, prepare_create_subject_term_link
):
    lambda_client = get_client("lambda")

    create_subject(TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources)

    evidence_list = [
        {
            "project_id": TEST_PROJECT_ID,
            "project_subject_id": SUBJECT_1_ID,
            "term_iri": "FAKE:123",
            "evidence": {
                "creator": "integration tests",
                "evidence_type": HIGH_THROUGHPUT_EVIDENCE_ID,
                "assertion_method": AUTOMATIC_ASSERTION_ID,
            },
        }
    ]

    # Write our evidence to s3 and then use it to add a subject term link

    response = create_subject_term_link_from_evidence_list(
        evidence_list, lambda_client, physical_resources
    )
    print(response)

    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"].startswith("Term does not exist")

    response = remove_subject(
        TEST_PROJECT_ID, SUBJECT_1_ID, lambda_client, physical_resources
    )
    print(response)
    assert response["statusCode"] == 200, f"Failed to remove the {SUBJECT_1_ID} subject"


# TODO - Test correct firing of SUBJECT_TERM_LINK_CREATED event
