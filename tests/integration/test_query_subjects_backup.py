import json
import pytest
import gzip
import base64
import time
from botocore.exceptions import ClientError
from step_function_utils import start_step_function, wait_for_step_function_completion
from phebee.utils.aws import get_client, download_and_extract_zip


def wait_for_loader_success(load_id, physical_resources, timeout_seconds=600):
    """Wait for a single Neptune bulk loader job to finish."""
    print(f"Waiting for load success for job {load_id}")
    start = time.time()
    while True:
        lambda_client = get_client("lambda")
        result_response = lambda_client.invoke(
            FunctionName=physical_resources["GetLoadStatusFunction"],
            Payload=json.dumps({"load_job_id": load_id})
        )
        result = json.loads(result_response["Payload"].read())
        print(result)
        overall = (result or {}).get("payload", {}).get("overallStatus", {}) or {}
        status = (overall.get("status") or "").lower()
        print(f"Loader status: {status}")

        if status == "load_completed":
            print("Load completed successfully")
            return result
        elif status in ["load_failed", "load_cancelled"]:
            raise RuntimeError(f"Neptune bulk loader failed with status: {status}")

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Neptune bulk loader did not finish in {timeout_seconds} seconds")

        time.sleep(8)

pytestmark = pytest.mark.slow


def decompress_lambda_response(result):
    """Helper function to decompress gzipped Lambda responses."""
    if "body" in result:
        try:
            return json.loads(result["body"])
        except json.JSONDecodeError:
            # Try to decompress if it's gzipped
            try:
                compressed_data = base64.b64decode(result["body"])
                decompressed_data = gzip.decompress(compressed_data)
                return json.loads(decompressed_data.decode('utf-8'))
            except Exception:
                raise ValueError("Unable to parse response body")
    else:
        raise ValueError("No body in response")


@pytest.mark.integration
def test_project_query(physical_resources, test_project_id):
    """
    Test querying with only the project_id. This should return all subjects for the given project.
    """
    project_id = test_project_id

    create_test_subjects(physical_resources, project_id)

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
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_a",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_b",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_c",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_d",
        }

        actual_project_subject_iris = set()
        if "body" in result:
            body = decompress_lambda_response(result)

            print("body")
            print(body)

            # Extract actual projectSubjectIds from the result - handle new pagination format
            subjects_data = body["body"]  # Handle both old and new format
            actual_project_subject_iris = {
                item["project_subject_iri"] for item in subjects_data
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert expected_project_subject_iris == actual_project_subject_iris, (
            "Mismatch in project_subject_iris"
        )

        # Clean up files
        s3_client = get_client("s3")
        s3_client.delete_object(
            Bucket=physical_resources["PheBeeBucket"],
            Key=IMPORT_OUTPUT_KEY,
        )

    except ClientError as e:
        pytest.fail(f"Project query function failed: {e}")


@pytest.mark.integration
def test_subject_specific_query(physical_resources, test_project_id):
    """
    Test querying with project_subject_iris to filter results by specific subjects.
    """
    project_id = test_project_id

    create_test_subjects(physical_resources, project_id)

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
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_b",
            f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/subjects/subject_c",
        }

        actual_project_subject_iris = set()
        if "body" in result:
            body = decompress_lambda_response(result)

            print("body")
            print(body)

            # Extract actual projectSubjectIds from the result - handle new pagination format
            subjects_data = body["body"]  # Handle both old and new format
            actual_project_subject_iris = {
                item["project_subject_iri"] for item in subjects_data
            }

        # Assert that the expected projectSubjectIds match the actual projectSubjectIds
        assert expected_project_subject_iris == actual_project_subject_iris, (
            "Mismatch in project_subject_iris"
        )

        # Clean up files
        s3_client = get_client("s3")
        s3_client.delete_object(
            Bucket=physical_resources["PheBeeBucket"],
            Key=IMPORT_OUTPUT_KEY,
        )

    except ClientError as e:
        pytest.fail(f"Subject-specific query function failed: {e}")


@pytest.mark.integration  
def test_term_filtering_basic(physical_resources, test_project_id, update_hpo):
    """Test basic term filtering functionality - verifies query handles term_iri parameter correctly."""
    project_id = test_project_id
    lambda_client = get_client("lambda")
    
    # Create some basic subjects
    create_test_subjects(physical_resources, project_id)
    
    # Test 1: Query with a specific term_iri (should handle gracefully even if no matches)
    query = json.dumps({
        "project_id": project_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
        "term_source": "hpo",
        "include_phenotypes": True,
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    # Should return successful response even if no subjects match the term
    assert result["statusCode"] == 200
    body = decompress_lambda_response(result)
    assert "body" in body
    assert "n_subjects" in body
    assert "pagination" in body
    
    # Test 2: Query with include_descendants parameter
    query = json.dumps({
        "project_id": project_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001332", 
        "term_source": "hpo",
        "include_descendants": True,
        "include_phenotypes": True,
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    # Should return successful response
    assert result["statusCode"] == 200
    body = decompress_lambda_response(result)
    assert isinstance(body["body"], list)
    
    # Test 3: Query with non-existent term should return empty results
    query = json.dumps({
        "project_id": project_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_9999999",
        "term_source": "hpo",
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    assert result["statusCode"] == 200
    body = decompress_lambda_response(result)
    assert len(body["body"]) == 0


def create_test_subjects(physical_resources, project_id):
    """Create test subjects using CreateSubjectFunction to ensure DynamoDB mappings exist."""
    lambda_client = get_client("lambda")
    create_fn = physical_resources["CreateSubjectFunction"]
    
    # Create 4 test subjects
    subjects = ["subject_a", "subject_b", "subject_c", "subject_d"]
    
    for subject_id in subjects:
        payload = {
            "project_id": project_id,
            "project_subject_id": subject_id
        }
        
        response = lambda_client.invoke(
            FunctionName=create_fn,
            Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
            InvocationType="RequestResponse"
        )
        
        result = json.loads(response["Payload"].read())
        if result.get("statusCode") != 200:
            pytest.fail(f"Failed to create subject {subject_id}: {result}")
    
    print(f"Created {len(subjects)} test subjects for project {project_id}")


@pytest.mark.integration
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
            "type": {"id": "strict", "label": "optional_export"},
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


@pytest.mark.integration
def test_pagination_basic(physical_resources, test_project_id):
    """Test basic pagination functionality with limit parameter."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # Test with limit=2
    subject_pheno_query_input = json.dumps({
        "project_id": project_id,
        "limit": 2
    })

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=subject_pheno_query_input.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)

    # Should return exactly 2 subjects
    subjects_data = body["body"]  # Handle both old and new format
    assert len(subjects_data) == 2, f"Expected 2 subjects, got {len(subjects_data)}"
    
    # Should have pagination metadata
    assert "pagination" in body, "Response should include pagination metadata"
    pagination = body["pagination"]
    assert pagination["limit"] == 2, "Pagination limit should match request"
    assert pagination["has_more"] is True, "Should have more pages available"
    assert pagination["next_cursor"] is not None, "Should provide next_cursor"


@pytest.mark.integration 
def test_pagination_cursor(physical_resources, test_project_id):
    """Test cursor-based pagination across multiple pages."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # First, check how many subjects were actually imported without pagination
    lambda_client = get_client("lambda")
    check_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=json.dumps({"project_id": project_id}).encode("utf-8"),
    )
    check_result = json.loads(check_response["Payload"].read())
    check_body = decompress_lambda_response(check_result)
    total_imported = len(check_body["body"])
    
    print(f"Total subjects imported: {total_imported}")
    assert total_imported == 4, f"Import failed - expected 4 subjects, got {total_imported}"

    all_subjects = []
    cursor = None
    page_count = 0
    
    # Fetch all subjects using pagination
    while True:
        page_count += 1
        query_input = {
            "project_id": project_id,
            "limit": 2
        }
        if cursor:
            query_input["cursor"] = cursor
            
        print(f"Page {page_count} query: {query_input}")
            
        response = lambda_client.invoke(
            FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
            Payload=json.dumps(query_input).encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        body = decompress_lambda_response(result)
        
        page_subjects = body["body"]
        all_subjects.extend(page_subjects)
        
        pagination = body["pagination"]
        print(f"Page {page_count} returned {len(page_subjects)} subjects")
        print(f"Pagination info: {pagination}")
        
        if not pagination["has_more"]:
            print(f"No more pages after page {page_count}")
            break
            
        cursor = pagination["next_cursor"]
        print(f"Next cursor: {cursor}")
        assert cursor is not None, "next_cursor should be provided when has_more=True"
        
        # Safety check to prevent infinite loops
        assert page_count < 10, "Too many pages, possible infinite loop"
    
    print(f"Total pages: {page_count}")
    print(f"Total subjects collected: {len(all_subjects)}")
    
    # Should have collected all subjects across pages
    assert len(all_subjects) == total_imported, f"Pagination failed - expected {total_imported} subjects, got {len(all_subjects)}"
    
    # Subject IRIs should be unique (no duplicates across pages)
    subject_iris = [s["subject_iri"] for s in all_subjects]
    assert len(set(subject_iris)) == len(subject_iris), "Subjects should be unique across pages"


@pytest.mark.integration
def test_pagination_empty_cursor(physical_resources, test_project_id):
    """Test pagination with invalid/empty cursor."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # Test with empty cursor (should work like no cursor)
    subject_pheno_query_input = json.dumps({
        "project_id": project_id,
        "limit": 2,
        "cursor": ""
    })

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=subject_pheno_query_input.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)

    # Should still return results (empty cursor ignored)
    subjects_data = body["body"]  # Handle both old and new format
    assert len(subjects_data) == 2, "Should return results with empty cursor"
    assert "pagination" in body, "Should include pagination metadata"


@pytest.mark.integration
def test_term_labels_functionality(physical_resources, test_project_id, update_hpo):
    """Test that term labels are returned in query responses."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

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
    body = decompress_lambda_response(result)
    subjects = body["body"]
    
    # Should return subjects (even if they don't have phenotypes yet)
    assert len(subjects) > 0, "Should return subjects"
    
    # Verify response structure includes phenotypes field
    for subject in subjects:
        assert "phenotypes" in subject, "Subject should have phenotypes field"
        # phenotypes may be empty list if no term links exist, which is fine


@pytest.mark.integration
def test_include_qualified_parameter(physical_resources, test_project_id, update_hpo):
    """Test include_qualified parameter excludes subjects with negated/hypothetical/family qualifiers by default."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # Test with a term that might have qualified annotations
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"  # Seizure
    
    lambda_client = get_client("lambda")
    
    # Test default behavior (include_qualified=false)
    default_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo",
        "include_phenotypes": True
    })
    
    default_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=default_query.encode("utf-8"),
    )
    default_result = json.loads(default_response["Payload"].read())
    default_body = decompress_lambda_response(default_result)
    default_subjects = {item["project_subject_iri"] for item in default_body["body"]}
    
    # Test explicit include_qualified=false
    explicit_false_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo",
        "include_phenotypes": True,
        "include_qualified": False
    })
    
    explicit_false_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=explicit_false_query.encode("utf-8"),
    )
    explicit_false_result = json.loads(explicit_false_response["Payload"].read())
    explicit_false_body = decompress_lambda_response(explicit_false_result)
    explicit_false_subjects = {item["project_subject_iri"] for item in explicit_false_body["body"]}
    
    # Test include_qualified=true
    include_qualified_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo", 
        "include_phenotypes": True,
        "include_qualified": True
    })
    
    include_qualified_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=include_qualified_query.encode("utf-8"),
    )
    include_qualified_result = json.loads(include_qualified_response["Payload"].read())
    include_qualified_body = decompress_lambda_response(include_qualified_result)
    include_qualified_subjects = {item["project_subject_iri"] for item in include_qualified_body["body"]}
    
    # Assertions
    assert default_subjects == explicit_false_subjects, "Default behavior should match explicit include_qualified=false"
    assert len(include_qualified_subjects) >= len(default_subjects), "include_qualified=true should return same or more subjects"
    
    # Validate that subjects with non-qualified terms are included in both cases
    assert default_subjects.issubset(include_qualified_subjects), "All non-qualified subjects should be included when include_qualified=true"
    
    print(f"Default/explicit false subjects: {len(default_subjects)}")
    print(f"Include qualified subjects: {len(include_qualified_subjects)}")
    print(f"Non-qualified subjects always included: {default_subjects.issubset(include_qualified_subjects)}")
    
    # Clean up files
    s3_client = get_client("s3")
    s3_client.delete_object(
        Bucket=physical_resources["PheBeeBucket"],
        Key=IMPORT_OUTPUT_KEY,
    )


@pytest.mark.integration
def test_include_qualified_with_direct_subjects(physical_resources, test_project_id, update_hpo):
    """Test include_qualified parameter using direct subject creation."""
    project_id = test_project_id
    lambda_client = get_client("lambda")
    
    # Create subjects with qualified and unqualified term links
    create_test_subjects(physical_resources, project_id, [
        {
            "project_subject_id": "subject_normal",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
            "contexts": {}  # No contexts - unqualified
        },
        {
            "project_subject_id": "subject_negated", 
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001250",
            "contexts": {"negated": 1}  # Qualified
        },
        {
            "project_subject_id": "subject_hypothetical",
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001250", 
            "contexts": {"hypothetical": 1}  # Qualified
        }
    ])
    
    term_iri = "http://purl.obolibrary.org/obo/HP_0001250"
    
    # Test default (exclude qualified) - should return only subject_normal
    default_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo"
    })
    
    default_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=default_query.encode("utf-8"),
    )
    default_result = json.loads(default_response["Payload"].read())
    default_body = decompress_lambda_response(default_result)
    default_subjects = {item["project_subject_iri"] for item in default_body["body"]}
    
    # Test include_qualified=true - should return all subjects
    qualified_query = json.dumps({
        "project_id": project_id,
        "term_iri": term_iri,
        "term_source": "hpo",
        "include_qualified": True
    })
    
    qualified_response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=qualified_query.encode("utf-8"),
    )
    qualified_result = json.loads(qualified_response["Payload"].read())
    qualified_body = decompress_lambda_response(qualified_result)
    qualified_subjects = {item["project_subject_iri"] for item in qualified_body["body"]}
    
    # Assertions
    assert len(default_subjects) == 1, f"Default should return 1 subject, got {len(default_subjects)}"
    assert len(qualified_subjects) == 3, f"Include qualified should return 3 subjects, got {len(qualified_subjects)}"
    assert default_subjects.issubset(qualified_subjects), "Unqualified subjects should be included in both queries"


@pytest.mark.integration
def test_project_subject_ids_filter(physical_resources, test_project_id, update_hpo):
    """Test filtering by specific project_subject_ids parameter."""
    project_id = test_project_id
    lambda_client = get_client("lambda")
    
    # Create multiple subjects using CreateSubjectFunction
    create_fn = physical_resources["CreateSubjectFunction"]
    subjects = ["subject_filter_a", "subject_filter_b", "subject_filter_c"]
    
    for subject_id in subjects:
        payload = {
            "project_id": project_id,
            "project_subject_id": subject_id
        }
        
        response = lambda_client.invoke(
            FunctionName=create_fn,
            Payload=json.dumps({"body": json.dumps(payload)}).encode("utf-8"),
            InvocationType="RequestResponse"
        )
        
        result = json.loads(response["Payload"].read())
        if result.get("statusCode") != 200:
            pytest.fail(f"Failed to create subject {subject_id}: {result}")
    
    # Test filtering to specific subjects
    query = json.dumps({
        "project_id": project_id,
        "project_subject_ids": ["subject_filter_a", "subject_filter_c"]
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)
    subjects = body["body"]
    
    # Should return only the 2 requested subjects
    assert len(subjects) == 2
    subject_ids = {s["project_subject_iri"].split("/")[-1] for s in subjects}
    assert subject_ids == {"subject_filter_a", "subject_filter_c"}


@pytest.mark.integration
def test_empty_result_sets(physical_resources, test_project_id, update_hpo):
    """Test queries that return empty result sets."""
    project_id = test_project_id
    lambda_client = get_client("lambda")
    
    # Test 1: Query for non-existent term
    query = json.dumps({
        "project_id": project_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_9999999",  # Non-existent term
        "term_source": "hpo"
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)
    
    assert result["statusCode"] == 200
    assert len(body["body"]) == 0
    assert body["n_subjects"] == 0
    
    # Test 2: Query for non-existent project_subject_ids
    query = json.dumps({
        "project_id": project_id,
        "project_subject_ids": ["non_existent_subject_1", "non_existent_subject_2"]
    })
    
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=query.encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)
    
    assert result["statusCode"] == 200
    assert len(body["body"]) == 0
    assert body["n_subjects"] == 0
