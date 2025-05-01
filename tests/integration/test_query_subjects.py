import json
import pytest
from botocore.exceptions import ClientError
from step_function_utils import start_step_function, wait_for_step_function_completion
from phebee.utils.aws import get_client, download_and_extract_zip

pytestmark = pytest.mark.slow

ZIP_KEY = "tests/integration/data/phenopackets/sample_phenopackets.zip"
IMPORT_OUTPUT_KEY = (
    "tests/integration/output/phenopackets/phenopacket_import_result.zip"
)
EXPORT_KEY = "tests/integration/output/phenopackets/phenopacket_export.zip"


@pytest.fixture(scope="session")
def upload_phenopacket_s3(physical_resources):
    s3_client = get_client("s3")

    try:
        with open(ZIP_KEY, "rb") as data:
            bucket = physical_resources["PheBeeBucket"]
            print(f"Uploading phenopacket data to s3://{bucket}/{ZIP_KEY}")
            s3_client.put_object(Bucket=bucket, Key=ZIP_KEY, Body=data)
        yield
    except ClientError as e:
        pytest.fail(f"Failed to upload ZIP to S3: {e}")

    # Cleanup after tests
    finally:
        s3_client.delete_object(Bucket=physical_resources["PheBeeBucket"], Key=ZIP_KEY)


@pytest.mark.integration
def test_phenopackets(
    upload_phenopacket_s3, test_project_id, physical_resources, update_hpo
):
    project_id = test_project_id
    print(f"Using project {project_id} for phenopacket tests")

    import_phenopacket(physical_resources, project_id)

    imported_phenopacket_data = download_and_extract_zip(ZIP_KEY)

    # Do an export of Phenopacket data with direct data return and compare to the original import
    exported_phenopacket_data = export_phenopacket(physical_resources, project_id)
    print(f"exported_phenopacket_data: {exported_phenopacket_data}")

    compare_phenopacket_data(imported_phenopacket_data, exported_phenopacket_data)

    # Do an export to s3 of Phenopacket data and compare to the original import
    bucket = physical_resources["PheBeeBucket"]
    key = f"export/{project_id}.json"

    export_path = export_phenopacket_zip_to_s3(
        physical_resources, project_id, bucket, key
    )
    s3_export_phenopacket_data = download_and_extract_zip(export_path)

    compare_phenopacket_data(imported_phenopacket_data, s3_export_phenopacket_data)

    # Clean up files
    s3_client = get_client("s3")
    s3_client.delete_object(
        Bucket=physical_resources["PheBeeBucket"], Key=IMPORT_OUTPUT_KEY
    )
    s3_client.delete_object(Bucket=physical_resources["PheBeeBucket"], Key=EXPORT_KEY)


@pytest.mark.integration
def test_project_query(physical_resources, test_project_id, upload_phenopacket_s3):
    """
    Test querying with only the project_id. This should return all subjects for the given project.
    """
    project_id = test_project_id

    import_phenopacket(physical_resources, project_id)

    subject_pheno_query_input = json.dumps({"project_id": project_id})

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
        expected_project_subject_iris = {
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_a",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_b",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_c",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_d",
        }

        actual_project_subject_iris = set()
        if "body" in result:
            body = json.loads(result["body"])

            print("body")
            print(body)

            # Extract actual projectSubjectIds from the result
            actual_project_subject_iris = {
                item["project_subject_iri"] for item in body["body"]
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert expected_project_subject_iris == actual_project_subject_iris, (
            "Mismatch in project_subject_iris"
        )

    except ClientError as e:
        pytest.fail(f"Project query function failed: {e}")


@pytest.mark.integration
def test_subject_specific_query(
    physical_resources, test_project_id, upload_phenopacket_s3
):
    """
    Test querying with project_subject_iris to filter results by specific subjects.
    """
    project_id = test_project_id

    import_phenopacket(physical_resources, project_id)

    subject_pheno_query_input = json.dumps(
        {
            "project_id": project_id,
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
        expected_project_subject_iris = {
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_b",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subject_c",
        }

        actual_project_subject_iris = set()
        if "body" in result:
            body = json.loads(result["body"])

            print("body")
            print(body)

            # Extract actual projectSubjectIds from the result
            actual_project_subject_iris = {
                item["project_subject_iri"] for item in body["body"]
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert expected_project_subject_iris == actual_project_subject_iris, (
            "Mismatch in project_subject_iris"
        )

    except ClientError as e:
        pytest.fail(f"Subject-specific query function failed: {e}")


inputs = ["HP_0001250", "HP_0001332", "HP_0007530AAA", "HP_0000962"]
include_descendants = [False, True, False, False]
outputs = [
    {
        "http://ods.nationwidechildrens.org/phebee/projects/<PROJECT>/subject_a",
        "http://ods.nationwidechildrens.org/phebee/projects/<PROJECT>/subject_b",
    },
    {
        "http://ods.nationwidechildrens.org/phebee/projects/<PROJECT>/subject_c",
        "http://ods.nationwidechildrens.org/phebee/projects/<PROJECT>/subject_d",
    },
    set(),
    set(),
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "term_id, include_descendants, expected_output",
    zip(inputs, include_descendants, outputs),
)
def test_term_filtering_query(
    physical_resources,
    test_project_id,
    upload_phenopacket_s3,
    term_id,
    include_descendants,
    expected_output,
    update_hpo,
):
    """
    Test querying with term_iri to filter subjects by specific phenotypes.
    """
    project_id = test_project_id

    import_phenopacket(physical_resources, project_id)

    subject_pheno_query_input = json.dumps(
        {
            "project_id": project_id,
            "term_iri": f"http://purl.obolibrary.org/obo/{term_id}",
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
        actual_project_subject_iris = set()
        if "body" in result:
            body = json.loads(result["body"])
            # Extract actual projectSubjectIds from the result
            actual_project_subject_iris = {
                item["project_subject_iri"] for item in body["body"]
            }

        assert actual_project_subject_iris == set(
            [o.replace("<PROJECT>", project_id) for o in expected_output]
        ), f"Mismatch in expected output for term {term_id}"

    except ClientError as e:
        pytest.fail(f"Term filtering query function failed: {e}")


def import_phenopacket(physical_resources, project_id):
    # Start the step function for import
    bucket = physical_resources["PheBeeBucket"]
    s3_path = f"s3://{bucket}/{ZIP_KEY}"
    step_function_input = json.dumps(
        {
            "project_id": project_id,
            "s3_path": s3_path,
            "output_s3_path": f"{s3_path}/{IMPORT_OUTPUT_KEY}",
        }
    )

    try:
        # Start the step function and wait for completion
        execution_arn = start_step_function(
            physical_resources["ImportPhenopacketsSFN"],
            step_function_input,
        )
        wait_for_step_function_completion(execution_arn, return_response=True)
    except ClientError as e:
        pytest.fail(f"Step function failed: {e}")


def export_phenopacket(physical_resources, project_id):
    # use the raw json data from the input data and compare it to results of calling the get_subjects_pheno lambda function

    # Call query lambda function
    subject_pheno_query_input = json.dumps(
        {
            "project_id": project_id,
            "return_excluded_terms": True,
            "include_descendants": False,
            "include_phenotypes": True,
            "output_type": "phenopacket",
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input,
        )
        result = json.loads(response["Payload"].read())

        print(result)

        body = json.loads(result["body"])

        print("body")
        print(body)

        assert result["statusCode"] == 200, (
            "Failed to query the subjects and phenotypes."
        )

        return body["body"]

    except ClientError as e:
        pytest.fail(f"Export function failed: {e}")


# @pytest.mark.integration
# @pytest.mark.parametrize(
#     "create_test_project, upload_phenopacket_s3, import_phenopacket",
#     [("phenopacket", "phenopacket", "phenopacket")],
#     indirect=True,
# )
# def test_import_phenopacket(create_test_project, import_phenopacket):
#     result = import_phenopacket
#     config = PROJECT_CONFIGS["phenopacket"]

#     # Validate the result of the step function
#     assert result["status"] == "SUCCEEDED", "Step function did not succeed"

#     # TODO this is just the number of parsed packets - not necessarily imported. Check how to get the state of successful imports from the MAP state.
#     output_data = json.loads(result["output"])
#     assert "number_phenopackets" in output_data["Payload"], (
#         "Missing importedPacketCount in result"
#     )
#     n_packets = output_data["Payload"]["number_phenopackets"]
#     assert n_packets == config["EXPECTED_PACKET_COUNT"], (
#         f"Expected {config['EXPECTED_PACKET_COUNT']} packets, but got {n_packets}"
#     )

#     # Note: Test of the data itself is happening in test_subject_pheno_queries


def export_phenopacket_zip_to_s3(physical_resources, project_id, bucket, key):
    bucket = physical_resources["PheBeeBucket"]

    export_s3_path = f"s3://{bucket}/{EXPORT_KEY}"
    # Call query lambda function
    subject_pheno_query_input = json.dumps(
        {
            "project_id": project_id,
            "return_excluded_terms": True,
            "include_descendants": False,
            "include_phenotypes": True,
            "output_s3_path": export_s3_path,
            "output_type": "phenopacket_zip",
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input,
        )
        result = json.loads(response["Payload"].read())

        print(result)

        body = json.loads(result["body"])

        print("body")
        print(body)

        export_s3_path = body["s3_path"]

        assert result["statusCode"] == 200, (
            "ExportPhenopacketsFunction lambda did not succeed"
        )

        return export_s3_path

    except ClientError as e:
        pytest.fail(f"Export function failed: {e}")


def compare_lists(imported_list, exported_list, item_schema):
    """
    Compare two lists of JSON objects based on a schema for list items.
    :param imported_list: List of imported items.
    :param exported_list: List of exported items.
    :param item_schema: Schema for comparing items in the list.
    :return: (True/False, details) where details contain mismatched items.
    """
    status = True
    details = []

    # Track unmatched items
    unmatched_imported = []
    unmatched_exported = []

    # Create a dict of the list items, keyed by the unique identifier (e.g., type.id or resource.id)
    try:
        imported_dict = {
            item["type"]["id"]: item
            for item in imported_list
            if "type" in item and "id" in item["type"]
        }
        exported_dict = {
            item["type"]["id"]: item
            for item in exported_list
            if "type" in item and "id" in item["type"]
        }
    except KeyError as e:
        details.append(
            f"KeyError: {e} - Ensure 'type' and 'id' are present in all items in the lists."
        )
        return False, details

    # Compare items by id
    for id_, imported_item in imported_dict.items():
        if id_ not in exported_dict:
            unmatched_imported.append(id_)
            status = False
        else:
            # Recursively compare list items based on the schema
            sub_status, sub_details = compare_json_data(
                imported_item, exported_dict[id_], item_schema
            )
            if not sub_status:
                details.append(f"Mismatch in list item with id '{id_}':")
                details.extend(sub_details)
                status = False

    for id_, exported_item in exported_dict.items():
        if id_ not in imported_dict:
            unmatched_exported.append(id_)
            status = False

    # Add details for unmatched items
    if unmatched_imported:
        details.append(
            f"Missing list items in exported data: {', '.join(unmatched_imported)}"
        )
    if unmatched_exported:
        details.append(
            f"Extra list items in exported data: {', '.join(unmatched_exported)}"
        )

    return status, details


def compare_json_data(imported_data, exported_data, schema):
    """
    Compare two dictionaries of JSON data (imported vs exported) based on a comparison schema.
    :param imported_data: Dictionary of imported subject data.
    :param exported_data: Dictionary of exported subject data.
    :param schema: Comparison schema defining how each field should be compared.
    :return: (True/False, details) where details contain mismatched fields.
    """
    status = True
    details = []

    for key, rule in schema.items():
        if isinstance(rule, dict) and rule.get("type") == "list":
            # Handle lists (e.g., phenotypic features or resources)
            if key in imported_data and key in exported_data:
                if isinstance(imported_data[key], list) and isinstance(
                    exported_data[key], list
                ):
                    sub_status, sub_details = compare_lists(
                        imported_data[key], exported_data[key], rule["items"]
                    )
                    if not sub_status:
                        details.append(f"Mismatch in list field '{key}':")
                        details.extend(sub_details)
                        status = False
                else:
                    details.append(
                        f"Field '{key}' should be a list in both datasets, but found '{type(imported_data[key])}' in imported and '{type(exported_data[key])}' in exported"
                    )
                    status = False
            elif key not in imported_data or key not in exported_data:
                if rule == "optional_export" and key not in imported_data:
                    continue
                if rule == "optional_import" and key not in exported_data:
                    continue
                details.append(f"Missing key '{key}' in one of the datasets")
                status = False
            continue

        if key not in imported_data or key not in exported_data:
            if rule == "optional_export" and key not in imported_data:
                continue
            if rule == "optional_import" and key not in exported_data:
                continue
            if rule == "strict_if_present":
                if key not in imported_data and key not in exported_data:
                    continue
            details.append(
                f"Missing key '{key}' in one of the datasets (imported: {key in imported_data}, exported: {key in exported_data})"
            )
            status = False
            continue

        if rule == "strict":
            if imported_data[key] != exported_data[key]:
                details.append(
                    f"Field '{key}' differs: imported='{imported_data[key]}' vs exported='{exported_data[key]}'"
                )
                status = False

        elif rule == "strict_if_present":
            if key in imported_data and key in exported_data:
                if imported_data[key] != exported_data[key]:
                    details.append(
                        f"Field '{key}' differs: imported='{imported_data[key]}' vs exported='{exported_data[key]}'"
                    )
                    status = False

        elif rule == "present":
            if key not in imported_data or key not in exported_data:
                details.append(
                    f"Field '{key}' must be present in both datasets (imported: {key in imported_data}, exported: {key in exported_data})"
                )
                status = False

        elif isinstance(rule, dict):
            # Recursively compare nested dictionaries
            sub_status, sub_details = compare_json_data(
                imported_data[key], exported_data[key], rule
            )
            if not sub_status:
                details.append(f"Mismatch in nested field '{key}':")
                details.extend(sub_details)
                status = False

    return status, details


comparison_schema = {
    "id": "strict",
    "subject": {
        "id": "strict",
        "sex": "optional_import",
        "timeAtLastEncounter": "optional_import",
    },
    "phenotypicFeatures": {
        "type": "list",
        "items": {
            "type": {"id": "strict", "label": "strict"},
            "excluded": "strict_if_present",  # Strict comparison if present, valid if absent in both
            "source": "optional_export",
            "onset": "optional_import",
        },
    },
    "metaData": {
        "resources": {
            "type": "list",
            "items": {
                "id": "strict",
                "name": "strict",
                "url": "strict",
                "namespacePrefix": "strict",
                "iriPrefix": "strict",
            },
        },
        "created": "present",
        "createdBy": "present",
        "phenopacketSchemaVersion": "optional_import",
        "externalReferences": "optional_import",
    },
    "interpretations": "optional_import",
}


# # Download the phenopacket file from s3 and check that it matches the imported phenopacket
# @pytest.mark.integration
# @pytest.mark.parametrize(
#     "create_test_project, upload_phenopacket_s3, import_phenopacket",
#     [("phenopacket", "phenopacket", "phenopacket")],
#     indirect=True,
# )
# def test_compare_phenopacket(
#     physical_resources, upload_phenopacket_s3, import_phenopacket, update_hpo
# ):
#     config = PROJECT_CONFIGS["phenopacket"]

#     import_data = download_and_extract_zip(config["ZIP_PATH"])

#     export_data = export_phenopacket(physical_resources, config["PROJECT_ID"])

#     compare_phenopacket_data(import_data, export_data)

#     bucket = physical_resources["PheBeeBucket"]
#     key = f"export/{config['PROJECT_ID']}.json"

#     export_path = export_phenopacket_to_s3(
#         physical_resources, config["PROJECT_ID"], bucket, key
#     )
#     s3_export_data = download_and_extract_zip(export_path)

#     compare_phenopacket_data(import_data, s3_export_data)

#     s3_client = get_client("s3")
#     s3_client.delete_object(
#         Bucket=bucket,
#         Key=key,
#     )


def compare_phenopacket_data(pp_data_1, pp_data_2):
    pp_data_1_dict = {p["id"]: p for p in pp_data_1}
    pp_data_2_dict = {p["id"]: p for p in pp_data_2}

    # Iterate over k,v pairs and call the comparison on identical pairs.
    for subject_id in pp_data_1_dict:
        status, results = compare_json_data(
            pp_data_1_dict[subject_id], pp_data_2_dict[subject_id], comparison_schema
        )

    # No changes expected, might need to adjust test set if HPO terms drift
    assert results == []
