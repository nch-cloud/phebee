import json
import pytest
from botocore.exceptions import ClientError

from phebee.utils.aws import get_client, download_and_extract_zip
from constants import PROJECT_CONFIGS

pytestmark = pytest.mark.slow

@pytest.mark.integration
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_import_phenopacket(create_test_project, import_phenopacket):
    result = import_phenopacket
    config = PROJECT_CONFIGS["phenopacket"]

    # Validate the result of the step function
    assert result["status"] == "SUCCEEDED", "Step function did not succeed"

    # TODO this is just the number of parsed packets - not necessarily imported. Check how to get the state of successful imports from the MAP state.
    output_data = json.loads(result["output"])
    assert "number_phenopackets" in output_data["Payload"], (
        "Missing importedPacketCount in result"
    )
    n_packets = output_data["Payload"]["number_phenopackets"]
    assert n_packets == config["EXPECTED_PACKET_COUNT"], (
        f"Expected {config['EXPECTED_PACKET_COUNT']} packets, but got {n_packets}"
    )

    # Note: Test of the data itself is happening in test_subject_pheno_queries


@pytest.fixture(scope="module")
def export_phenopacket(
    request, physical_resources, create_test_project, import_phenopacket
):
    # use the raw json data from the input data and compare it to results of calling the get_subjects_pheno lambda function
    config = PROJECT_CONFIGS[request.param]
    query_s3_path = f"s3://{physical_resources['PheBeeBucket']}/export_flat/{config['PROJECT_ID']}.json"
    export_s3_path = f"s3://{physical_resources['PheBeeBucket']}/export_phenopacket/{config['PROJECT_ID']}.json"
    # Call query lambda function
    subject_pheno_query_input = json.dumps(
        {
            "project_id": config["PROJECT_ID"],
            "return_excluded_terms": True,
            "include_descendants": False,
            "include_phenotypes": True,
            "include_evidence": True,
            "output_s3_path": query_s3_path,
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
        query_s3_path = body["s3_path"]
        n_subjects = body["n_subjects"]
        assert n_subjects == config["EXPECTED_PACKET_COUNT"], (
            f"Number of subjects in export ({n_subjects}) does not match number of subjects in import ({config['EXPECTED_PACKET_COUNT']})"
        )

        # Call export lambda function
        response = lambda_client.invoke(
            FunctionName=physical_resources["ExportPhenopacketsFunction"],
            Payload=json.dumps(
                {"s3_path": query_s3_path, "output_s3_path": export_s3_path}
            ),
        )
        result = json.loads(response["Payload"].read())
        body = json.loads(result["body"])
        print("Export result: " + str(result))
        
        export_s3_path = body["s3_path"]
        n_packets = body["n_packets"]

        assert result["statusCode"] == 200, (
            "ExportPhenopacketsFunction lambda did not succeed"
        )
        assert n_packets == config["EXPECTED_PACKET_COUNT"], (
            f"Number of packets in export ({n_packets}) does not match number of packets in import ({config['EXPECTED_PACKET_COUNT']})"
        )

        yield query_s3_path, export_s3_path

    except ClientError as e:
        pytest.fail(f"Export function failed: {e}")

    finally:
        s3_client = get_client("s3")
        s3_client.delete_object(
            Bucket=physical_resources["PheBeeBucket"],
            Key=f"export_flat/{config['PROJECT_ID']}.json",
        )
        s3_client.delete_object(
            Bucket=physical_resources["PheBeeBucket"],
            Key=f"export_phenopacket/{config['PROJECT_ID']}.json",
        )


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


# Download the phenopacket file from s3 and check that it matches the imported phenopacket
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_test_project, upload_phenopacket_s3, import_phenopacket, export_phenopacket",
    [("phenopacket", "phenopacket", "phenopacket", "phenopacket")],
    indirect=True,
)
def test_compare_phenopacket(create_test_project, export_phenopacket):
    config = PROJECT_CONFIGS["phenopacket"]
    query_path, export_path = export_phenopacket
    import_data = download_and_extract_zip(config["ZIP_PATH"])
    export_data = download_and_extract_zip(export_path)

    # Iterate over k,v pairs and call the comparison on identical pairs.
    for k, v in import_data.items():
        status, results = compare_json_data(v, export_data[k], comparison_schema)

    # No changes expected, might need to adjust test set if HPO terms drift
    assert results == []
