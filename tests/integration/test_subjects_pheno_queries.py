import json
import pytest
from botocore.exceptions import ClientError

from phebee.utils.aws import get_client
from constants import PROJECT_CONFIGS


@pytest.mark.integration
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_project_query(
    physical_resources, create_test_project, upload_phenopacket_s3, import_phenopacket
):
    """
    Test querying with only the project_id. This should return all subjects for the given project.
    """
    config = PROJECT_CONFIGS["phenopacket"]
    subject_pheno_query_input = json.dumps({"project_id": config["PROJECT_ID"]})

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        print("result")
        print(result)
        
        # Expected projectSubjectIds
        expected_project_subject_ids = {
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_a",
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_b",
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_c",
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_d",
        }

        actual_project_subject_ids = {}
        if "body" in result:
            body = json.loads(result["body"])

            print("body")
            print(body)

            # Extract actual projectSubjectIds from the result
            actual_project_subject_ids = {
                item[0]["projectSubjectId"] for item in body["body"].values()
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert (
            set(expected_project_subject_ids) == set(actual_project_subject_ids)
        ), "Mismatch in projectSubjectIds"

    except ClientError as e:
        pytest.fail(f"Project query function failed: {e}")


@pytest.mark.integration
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_subject_specific_query(
    physical_resources, create_test_project, upload_phenopacket_s3, import_phenopacket
):
    """
    Test querying with project_subject_ids to filter results by specific subjects.
    """
    config = PROJECT_CONFIGS["phenopacket"]
    subject_pheno_query_input = json.dumps(
        {
            "project_id": config["PROJECT_ID"],
            "project_subject_ids": [
                "subject_b",
                "subject_c",
            ],
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        
        print("test_subject_specific_query result")
        print(result)

        # Expected projectSubjectIds
        expected_project_subject_ids = {
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_b",
            "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_c",
        }

        result_project_subject_ids = {}
        if "body" in result:
            body = json.loads(result["body"])
            print(body)
            # Extract returned projectSubjectIds from the result
            result_project_subject_ids = {
                item[0]["projectSubjectId"] for item in body["body"].values()
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert (
            expected_project_subject_ids == result_project_subject_ids
        ), "Mismatch in projectSubjectIds"

    except ClientError as e:
        pytest.fail(f"Subject-specific query function failed: {e}")


inputs = ["HP_0001250", "HP_0001332", "HP_0007530AAA", "HP_0000962"]
include_descendants = [False, True, False, False]
outputs = [
    {
        "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_a",
        "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_b"
    },
    {
        "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_c",
        "http://ods.nationwidechildrens.org/phebee/projects/phenopacket-test#subject_d"
    },
    set(),
    set(),
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "term_iri, include_descendants, expected_output",
    zip(inputs, include_descendants, outputs),
)
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_term_filtering_query(
    physical_resources,
    create_test_project,
    upload_phenopacket_s3,
    import_phenopacket,
    term_iri,
    include_descendants,
    expected_output,
):
    """
    Test querying with term_iri to filter subjects by specific phenotypes.
    """
    config = PROJECT_CONFIGS["phenopacket"]
    subject_pheno_query_input = json.dumps(
        {
            "project_id": config["PROJECT_ID"],
            "term_iri": f"http://purl.obolibrary.org/obo/{term_iri}",
            "term_source": "hpo",
            "include_descendants": include_descendants,
            "include_phenotypes": True,
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        actual_project_subject_ids = {}
        if "body" in result:
            body = json.loads(result["body"])
            # Extract actual projectSubjectIds from the result
            actual_project_subject_ids = {
                item[0]["projectSubjectId"] for item in body["body"].values()
            }

        # return result
        assert (
            set(actual_project_subject_ids) == set(expected_output)
        ), f"Mismatch in expected output for term_iri {term_iri}"
    except ClientError as e:
        pytest.fail(f"Term filtering query function failed: {e}")


@pytest.mark.integration
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_include_phenotypes_and_evidence(
    physical_resources, create_test_project, upload_phenopacket_s3, import_phenopacket
):
    """
    Test querying with include_phenotypes and include_evidence to get subjects with phenotypes and evidence details.
    """
    config = PROJECT_CONFIGS["phenopacket"]
    subject_pheno_query_input = json.dumps(
        {
            "project_id": config["PROJECT_ID"],
            "term_source": "hpo",
            # "term_source_version": "v2024-12-12",
            "include_phenotypes": True,
            "include_evidence": True,
            "return_excluded_terms": True,
            "optional_evidence": [
                "creatorVersion",
                "termSource",
                "termSourceVersion",
                "evidenceText",
                "evidenceCreated",
                "evidenceReferenceId",
                "evidenceReferenceDescription",
            ],
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        
        print("test_include_phenotypes_and_evidence result")
        print(result)

        # Check the shape of the resulting data
        if "body" in result:
            body = json.loads(result["body"])

            print("test_include_phenotypes_and_evidence body")
            print(body)

            records = [
                record
                for value_list in body["body"].values()
                for record in value_list
            ]
            assert len(records) == 9, "Resulting dataset has incorrect number of rows"
            assert (
                len(records[0]) == 16
            ), "Resulting dataset has incorrect number of columns"

    except ClientError as e:
        pytest.fail(f"Include phenotypes and evidence query function failed: {e}")
