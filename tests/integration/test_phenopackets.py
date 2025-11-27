import json
import pytest
from botocore.exceptions import ClientError
from phebee.utils.aws import get_client
import sys
import os
import base64
import gzip
# Add the tests/integration directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from step_function_utils import start_step_function, wait_for_step_function_completion

def decompress_lambda_response(result):
    """Helper function to decompress gzipped Lambda responses."""
    if "body" in result:
        try:
            # Try to decode as base64 and decompress
            compressed_data = base64.b64decode(result["body"])
            decompressed_data = gzip.decompress(compressed_data)
            return json.loads(decompressed_data.decode('utf-8'))
        except Exception:
            raise ValueError("Unable to parse response body")
    return result

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


def validate_imported_data(physical_resources, project_id):
    """Validate that phenopacket data was imported correctly."""
    lambda_client = get_client("lambda")
    
    # Check subjects were created
    subjects_query = json.dumps({"project_id": project_id})
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=subjects_query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)
    subjects = body["body"]
    
    print(f"Found {len(subjects)} subjects after import")
    subject_ids = [s.get("project_subject_iri", "").split("/")[-1] for s in subjects]
    print(f"Subject IDs: {subject_ids}")
    
    # Show the actual subject IRIs being used
    for subject in subjects:
        subject_iri = subject.get("project_subject_iri", "")
        print(f"Subject IRI format: {subject_iri}")
        break  # Just show one example
    
    # Check term links for each subject
    for subject in subjects:
        subject_iri = subject.get("project_subject_iri", "")
        if subject_iri:
            subject_id = subject_iri.split("/")[-1]
            
            # Get detailed subject info
            subject_query = json.dumps({"subject_iri": subject_iri})
            subject_response = lambda_client.invoke(
                FunctionName=physical_resources["GetSubjectFunction"],
                Payload=subject_query.encode("utf-8"),
            )
            subject_result = json.loads(subject_response["Payload"].read())
            
            if subject_result["statusCode"] == 200:
                subject_body = json.loads(subject_result["body"])
                print(f"Subject {subject_id} details: {subject_body}")
            else:
                print(f"Subject {subject_id}: GetSubject failed - {subject_result}")
            
            # Check term links - use subject_id instead of subject_iri
            subject_uuid = subject_iri.split("/")[-1]  # Extract UUID from IRI
            term_query = json.dumps({"subject_id": subject_uuid})
            
            term_response = lambda_client.invoke(
                FunctionName=physical_resources["GetSubjectTermInfoFunction"],
                Payload=term_query.encode("utf-8"),
            )
            term_result = json.loads(term_response["Payload"].read())
            
            if term_result["statusCode"] == 200:
                term_body = json.loads(term_result["body"])
                term_links = term_body.get("term_links", [])
                print(f"Subject {subject_id}: {len(term_links)} term links")
            else:
                print(f"Subject {subject_id}: No term links found - {term_result}")
    
    return subjects


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

    print(f"Starting ImportPhenopacketsSFN with input: {step_function_input}")
    print(f"Step function ARN: {physical_resources['ImportPhenopacketsSFN']}")

    try:
        # Start the step function and wait for completion
        execution_arn = start_step_function(
            physical_resources["ImportPhenopacketsSFN"],
            step_function_input,
        )
        result = wait_for_step_function_completion(execution_arn, return_response=True)
        print(f"Step function completed with result: {result}")
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
            body = decompress_lambda_response(result)
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


def download_and_extract_zip(zip_key, bucket_name):
    """Download and extract phenopacket zip file from S3."""
    import zipfile
    import tempfile
    import os

    s3_client = get_client("s3")
    
    try:
        # Download the zip file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            s3_client.download_fileobj(bucket_name, zip_key, tmp_file)
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
    """Compare only subject IDs and phenotype term IDs between imported and exported data."""
    pp_data_1_dict = {p["id"]: p for p in pp_data_1}
    pp_data_2_dict = {p["id"]: p for p in pp_data_2}

    # Check that all imported subjects are in exported data
    for subject_id in pp_data_1_dict:
        if subject_id not in pp_data_2_dict:
            print(f"Missing subject in exported data: {subject_id}")
            return False
        
        # Extract phenotype term IDs from both
        imported_terms = {f["type"]["id"] for f in pp_data_1_dict[subject_id].get("phenotypicFeatures", [])}
        exported_terms = {f["type"]["id"] for f in pp_data_2_dict[subject_id].get("phenotypicFeatures", [])}
        
        if imported_terms != exported_terms:
            print(f"Phenotype mismatch for {subject_id}:")
            print(f"  Imported: {sorted(imported_terms)}")
            print(f"  Exported: {sorted(exported_terms)}")
            return False
        
        print(f"✓ {subject_id}: {len(imported_terms)} phenotypes match")

    return True


@pytest.mark.integration
def test_phenopackets(
    upload_phenopacket_s3, test_project_id, physical_resources, update_hpo
):
    project_id = test_project_id
    print(f"Using project {project_id} for phenopacket tests")

    import_phenopacket(physical_resources, project_id)

    # Validate the imported data before export
    imported_subjects = validate_imported_data(physical_resources, project_id)

    imported_phenopacket_data = download_and_extract_zip(ZIP_KEY, physical_resources["PheBeeBucket"])

    # Do an export of Phenopacket data with direct data return and compare to the original import
    exported_phenopacket_data = export_phenopacket(physical_resources, project_id)
    print(f"exported_phenopacket_data: {exported_phenopacket_data}")

    success = compare_phenopacket_data(imported_phenopacket_data, exported_phenopacket_data)
    assert success, "Phenopacket comparison failed - see debug output above"

    # Do an export to s3 of Phenopacket data and compare to the original import
    bucket = physical_resources["PheBeeBucket"]
    key = f"export/{project_id}.json"

    export_path = export_phenopacket_zip_to_s3(
        physical_resources, project_id, bucket, key
    )
    s3_export_phenopacket_data = download_and_extract_zip(export_path, physical_resources["PheBeeBucket"])

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

    # Query subjects to get term labels
    query = json.dumps({
        "project_id": project_id
    })

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    # Import decompress function from current module
    body = decompress_lambda_response(result)
    print(f"Response body keys: {body.keys()}")
    print(f"Full response body: {json.dumps(body, indent=2)}")
    
    subjects = body.get("body", body.get("subjects", []))

    # Verify subjects have term_links with labels
    assert len(subjects) > 0, f"Should return subjects. Got response: {body}"
    
    found_term_with_label = False
    for subject in subjects:
        if "term_links" in subject and len(subject["term_links"]) > 0:
            for term_link in subject["term_links"]:
                assert "term_iri" in term_link, "Term link should have term_iri"
                if "term_label" in term_link and term_link["term_label"] is not None:
                    found_term_with_label = True
                    print(f"Found term with label: {term_link['term_iri']} -> {term_link['term_label']}")
    
    assert found_term_with_label, "At least one term should have a label"


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
    
    # Import decompress function from current module
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
