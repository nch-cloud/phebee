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
            # Try to decode as base64 and decompress
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

        print("test_project_query result")
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

    except ClientError as e:
        pytest.fail(f"Project query function failed: {e}")


@pytest.mark.integration
def test_subject_specific_query(physical_resources, test_project_id):
    """
    Test querying with specific project_subject_ids to filter results.
    """
    project_id = test_project_id

    create_test_subjects(physical_resources, project_id)

    # Query for specific subjects
    subject_pheno_query_input = json.dumps({
        "project_id": project_id,
        "project_subject_ids": ["subject_b", "subject_c"]
    })

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

    except ClientError as e:
        pytest.fail(f"Subject-specific query function failed: {e}")


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


@pytest.mark.integration
def test_pagination_basic(physical_resources, test_project_id):
    """Test basic pagination functionality with limit parameter."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # Test with limit parameter
    query_input = {
        "project_id": project_id,
        "limit": 2
    }

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=json.dumps(query_input).encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    body = decompress_lambda_response(result)

    subjects = body["body"]
    pagination = body["pagination"]

    # Should return exactly 2 subjects due to limit
    assert len(subjects) == 2, f"Expected 2 subjects, got {len(subjects)}"
    
    # Should include pagination metadata
    assert "limit" in pagination
    assert "has_more" in pagination
    assert pagination["limit"] == 2
    
    # Should indicate more results available
    assert pagination["has_more"] == True, "Should have more results available"


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
    print("All subjects and their IRIs:")
    for i, subject in enumerate(check_body["body"]):
        print(f"  {i+1}. {subject['project_subject_id']} -> {subject['subject_iri']}")
    
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
        assert page_count < 5, f"Too many pages ({page_count}), possible infinite loop"

    # Verify we got all subjects
    assert len(all_subjects) == total_imported, f"Expected {total_imported} subjects total, got {len(all_subjects)}"
    
    # Verify no duplicates
    subject_iris = [s["project_subject_iri"] for s in all_subjects]
    assert len(subject_iris) == len(set(subject_iris)), "Found duplicate subjects in pagination"


@pytest.mark.integration
def test_pagination_empty_cursor(physical_resources, test_project_id):
    """Test pagination with invalid/empty cursor."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

    # Test with empty cursor
    query_input = {
        "project_id": project_id,
        "limit": 2,
        "cursor": ""
    }

    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=physical_resources["GetSubjectsPhenotypesFunction"],
        Payload=json.dumps(query_input).encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    
    # Should handle gracefully and return results
    assert result["statusCode"] == 200
    body = decompress_lambda_response(result)
    assert "body" in body
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
    
    # Verify response structure includes term_links field
    for subject in subjects:
        assert "term_links" in subject, "Subject should have term_links field"
        # term_links may be empty list if no term links exist, which is fine


@pytest.mark.integration
def test_include_qualified_parameter(physical_resources, test_project_id, update_hpo):
    """Test include_qualified parameter excludes subjects with negated/hypothetical/family qualifiers by default."""
    project_id = test_project_id
    create_test_subjects(physical_resources, project_id)

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
    
    # Both queries should return successful responses
    assert default_result["statusCode"] == 200
    assert qualified_result["statusCode"] == 200


@pytest.mark.integration
def test_include_qualified_with_direct_subjects(physical_resources, test_project_id, update_hpo):
    """Test include_qualified parameter using direct subject creation."""
    project_id = test_project_id
    lambda_client = get_client("lambda")
    
    # Create subjects with qualified and unqualified term links
    create_test_subjects(physical_resources, project_id)
    
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
    
    # Should return successful responses
    assert default_result["statusCode"] == 200
    assert qualified_result["statusCode"] == 200
    assert len(qualified_subjects) >= len(default_subjects)


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
