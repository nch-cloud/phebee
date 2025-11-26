import json
import pytest
from botocore.exceptions import ClientError
from phebee.utils.aws import get_client
import sys
import os
# Add the tests/integration directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from step_function_utils import start_step_function, wait_for_step_function_completion

def compare_json_data(data1, data2, schema=None):
    """Simple JSON comparison function"""
    return data1 == data2, "Data mismatch" if data1 != data2 else "Match"

# Phenopacket-related constants
ZIP_KEY = "tests/integration/data/phenopackets/sample_phenopackets.zip"
IMPORT_OUTPUT_KEY = "tests/integration/output/phenopackets/phenopacket_import_result.zip"
EXPORT_KEY = "tests/integration/output/phenopackets/phenopacket_export.zip"

# Comparison schema for phenopacket validation
comparison_schema = {
    "id": "exact",
    "subject": {
        "id": "exact",
        "sex": "exact",
        "timeAtLastEncounter": "optional_import",
        "vitalStatus": "optional_import",
    },
    "phenotypicFeatures": {
        "type": {
            "id": "exact",
            "label": "exact",
        },
        "excluded": "optional_import",
        "modifiers": "optional_import",
        "onset": "optional_import",
        "resolution": "optional_import",
        "evidence": "optional_import",
    },
    "diseases": {
        "term": {
            "id": "exact",
            "label": "exact",
        },
        "excluded": "optional_import",
        "onset": "optional_import",
        "resolution": "optional_import",
        "diseaseStage": "optional_import",
        "clinicalTnmFinding": "optional_import",
        "primarySite": "optional_import",
        "laterality": "optional_import",
    },
    "medicalActions": "optional_import",
    "measurements": "optional_import",
    "biosamples": "optional_import",
    "interpretations": "optional_import",
    "files": "optional_import",
    "metaData": {
        "created": "present",
        "createdBy": "present",
        "phenopacketSchemaVersion": "optional_import",
        "externalReferences": "optional_import",
    },
}


@pytest.fixture(scope="session")
def upload_phenopacket_s3(physical_resources):
    s3_client = get_client("s3")

    try:
        with open(ZIP_KEY, "rb") as data:
            bucket = physical_resources["PheBeeBucket"]
            print(f"Uploading phenopacket data to s3://{bucket}/{ZIP_KEY}")
            s3_client.put_object(Bucket=bucket, Key=ZIP_KEY, Body=data)
        yield
    finally:
        # Clean up the uploaded file
        try:
            s3_client.delete_object(Bucket=bucket, Key=ZIP_KEY)
        except Exception as e:
            print(f"Failed to clean up {ZIP_KEY}: {e}")


def import_phenopacket(physical_resources, project_id):
    # Start the step function for import
    bucket = physical_resources["PheBeeBucket"]
    s3_path = f"s3://{bucket}/{ZIP_KEY}"
    output_s3_path = f"s3://{bucket}/{IMPORT_OUTPUT_KEY}"
    step_function_input = json.dumps(
        {
            "project_id": project_id,
            "s3_path": s3_path,
            "output_s3_path": output_s3_path,
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
            "term_source": "hpo",
            "include_descendants": False,
            "include_phenotypes": True,
            "output_type": "phenopacket",
        }
    )

    lambda_client = get_client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())

        if "body" in result:
            body = json.loads(result["body"])
            return body["body"]
        else:
            return []

    except ClientError as e:
        pytest.fail(f"Export phenopacket function failed: {e}")


def export_phenopacket_zip_to_s3(physical_resources, project_id, bucket, key):
    bucket = physical_resources["PheBeeBucket"]

    export_s3_path = f"s3://{bucket}/{EXPORT_KEY}"
    subject_pheno_query_input = json.dumps(
        {
            "project_id": project_id,
            "term_source": "hpo",
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
            Payload=subject_pheno_query_input.encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())

        print(f"Export response: {result}")

        assert result["statusCode"] == 200, (
            "ExportPhenopacketsFunction lambda did not succeed"
        )

        return EXPORT_KEY

    except ClientError as e:
        pytest.fail(f"Export phenopacket zip function failed: {e}")


def download_and_extract_zip(zip_key):
    """Download and extract phenopacket zip file from S3."""
    import zipfile
    import tempfile
    import os

    s3_client = get_client("s3")
    
    # Get bucket from physical resources or use a default
    # This is a simplified version - in real usage, bucket should be passed
    try:
        # Download the zip file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            s3_client.download_fileobj("your-bucket", zip_key, tmp_file)
            tmp_file_path = tmp_file.name

        # Extract and read the JSON files
        phenopacket_data = []
        with zipfile.ZipFile(tmp_file_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.json'):
                    with zip_ref.open(file_name) as json_file:
                        data = json.load(json_file)
                        phenopacket_data.append(data)

        # Clean up
        os.unlink(tmp_file_path)
        return phenopacket_data

    except Exception as e:
        pytest.fail(f"Failed to download and extract zip: {e}")


def compare_phenopacket_data(pp_data_1, pp_data_2):
    pp_data_1_dict = {p["id"]: p for p in pp_data_1}
    pp_data_2_dict = {p["id"]: p for p in pp_data_2}

    # Iterate over k,v pairs and call the comparison on identical pairs.
    for subject_id in pp_data_1_dict:
        status, results = compare_json_data(
            pp_data_1_dict[subject_id], pp_data_2_dict[subject_id], comparison_schema
        )

        assert status, f"Phenopacket comparison failed for {subject_id}: {results}"


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
def test_term_labels_functionality(physical_resources, test_project_id, upload_phenopacket_s3, update_hpo):
    """Test that term labels are returned in query responses."""
    project_id = test_project_id
    import_phenopacket(physical_resources, project_id)

    # Query with include_phenotypes=True to get term labels
    query = json.dumps({
        "project_id": project_id,
        "include_phenotypes": True
    })

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    # Import decompress function
    from test_query_subjects import decompress_lambda_response
    body = decompress_lambda_response(result)
    subjects = body["body"]

    # Verify subjects have term_links with labels
    assert len(subjects) > 0, "Should return subjects"
    
    for subject in subjects:
        if "phenotypes" in subject and len(subject["phenotypes"]) > 0:
            for phenotype in subject["phenotypes"]:
                assert "term_iri" in phenotype, "Phenotype should have term_iri"
                assert "term_label" in phenotype, "Phenotype should have term_label"


@pytest.mark.integration
def test_include_qualified_parameter(physical_resources, test_project_id, upload_phenopacket_s3, update_hpo):
    """Test include_qualified parameter excludes subjects with negated/hypothetical/family qualifiers by default."""
    project_id = test_project_id
    import_phenopacket(physical_resources, project_id)

    # Test with a term that might have qualified annotations
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"  # Seizure

    # Test default behavior (should exclude qualified)
    default_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo",
        "include_phenotypes": True
    })

    lambda_client = get_client("lambda")
    default_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=default_query.encode("utf-8"),
    )
    default_result = json.loads(default_response["Payload"].read())
    
    # Import decompress function
    from test_query_subjects import decompress_lambda_response
    default_body = decompress_lambda_response(default_result)
    default_subjects = default_body["body"]

    # Test with include_qualified=True
    qualified_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo",
        "include_phenotypes": True,
        "include_qualified": True
    })

    qualified_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=qualified_query.encode("utf-8"),
    )
    qualified_result = json.loads(qualified_response["Payload"].read())
    qualified_body = decompress_lambda_response(qualified_result)
    qualified_subjects = qualified_body["body"]

    # Should have same or more subjects when including qualified
    assert len(qualified_subjects) >= len(default_subjects), \
        "Including qualified should return same or more subjects"
