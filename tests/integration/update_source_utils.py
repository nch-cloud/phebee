import pytest
from datetime import datetime
import boto3
from phebee.utils.aws import get_current_timestamp
from phebee.utils.dynamodb import get_source_records, get_current_term_source_version
from general_utils import check_timestamp_in_test

from step_function_utils import start_step_function, wait_for_step_function_completion
from cloudformation_utils import get_output_value_from_stack
from s3_utils import check_s3_file_exists


def update_source(cloudformation_stack, source_name, sfn_output_key, test=True):
    """
    Utility function to trigger the update step function and return the test_start_time.
    """

    # Get the ARN of the Step Function (assuming it is stored in the stack's output)
    step_function_arn = get_output_value_from_stack(
        cloudformation_stack, sfn_output_key
    )

    # Capture the start time of the test
    test_start_time = get_current_timestamp()

    # Start the execution of the Step Function (no input needed)
    try:
        execution_arn = start_step_function(step_function_arn, {"test": test})
        print(f"Step function execution started: {execution_arn}")
    except RuntimeError as e:
        pytest.fail(str(e))

    # Wait for the Step Function to complete
    execution_status = wait_for_step_function_completion(execution_arn)

    # Verify the Step Function execution result
    if execution_status != "SUCCEEDED":
        pytest.fail(f"Step function execution failed with status: {execution_status}")

    print("Step function executed successfully.")

    return test_start_time

def extract_timestamp(record):
        timestamp_str = record.get("InstallTimestamp", {}).get("S")
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str)
            except ValueError:
                pass
        return datetime.min

def check_dynamodb_record(source_name, test_start_time, dynamodb=None):
    source_records = get_source_records(source_name, dynamodb=dynamodb)

    # Check that a record was created during our unit test
    newest_record = max(
        source_records,
        key=extract_timestamp,
        default=None  # in case source_records is empty
    )

    print("newest_record")
    print(newest_record)

    assert check_timestamp_in_test(newest_record["SK"]["S"], test_start_time)
    assert check_timestamp_in_test(
        newest_record["CreationTimestamp"]["S"], test_start_time
    )
    assert check_timestamp_in_test(
        newest_record["InstallTimestamp"]["S"], test_start_time
    )
    assert newest_record.get("Assets") is not None
    assert newest_record.get("Version") is not None
    assert (
        newest_record["GraphName"]["S"]
        == f'{source_name}~{newest_record["Version"]["S"]}'
    )

    current_source_version = get_current_term_source_version(source_name, dynamodb)
    assert current_source_version is not None

    # Check that all assets for the source exist
    asset_paths = [a["M"]["asset_path"]["S"] for a in newest_record["Assets"]["L"]]
    for asset_path in asset_paths:
        assert check_s3_file_exists(asset_path)


def check_ontology_cache_populated(cloudformation_stack, source_name, version):
    """
    Verify that the DynamoDB cache table was populated with ontology hierarchy paths.

    Args:
        cloudformation_stack: CloudFormation stack info
        source_name: Ontology source (e.g., "hpo")
        version: Ontology version (e.g., "2026-01-08")
    """
    # Get cache table name from stack outputs
    cache_table_name = get_output_value_from_stack(cloudformation_stack, "DynamoDBCacheTableName")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(cache_table_name)

    # Scan for TERM_PATH# entries for this ontology/version
    # Use a limit to avoid scanning the entire table
    response = table.scan(
        FilterExpression="begins_with(PK, :pk_prefix)",
        ExpressionAttributeValues={
            ":pk_prefix": f"TERM_PATH#{source_name}|{version}|"
        },
        Limit=1000  # Sample check - HPO has ~18K terms with multiple inheritance
    )

    items = response.get("Items", [])

    print(f"\nCache table verification:")
    print(f"  Table: {cache_table_name}")
    print(f"  Source: {source_name} version {version}")
    print(f"  Sample size: {len(items)} paths")

    # Verify we have paths in the cache
    assert len(items) > 0, f"No TERM_PATH entries found for {source_name}|{version} in cache table"

    # For HPO, we expect thousands of paths (each term can have multiple paths due to multiple inheritance)
    # Let's check we have at least 100 paths in our sample
    assert len(items) >= 100, f"Expected at least 100 paths in sample, found {len(items)}"

    # Verify structure of a sample path entry
    sample_item = items[0]
    print(f"\n  Sample cache entry:")
    print(f"    PK: {sample_item['PK']}")
    print(f"    SK: {sample_item['SK']}")
    print(f"    term_id: {sample_item.get('term_id')}")
    print(f"    path_length: {sample_item.get('path_length')}")

    # Verify required fields
    assert "PK" in sample_item, "Cache entry missing PK"
    assert "SK" in sample_item, "Cache entry missing SK"
    assert "term_id" in sample_item, "Cache entry missing term_id"
    assert "ontology" in sample_item, "Cache entry missing ontology"
    assert "version" in sample_item, "Cache entry missing version"
    assert "path_length" in sample_item, "Cache entry missing path_length"

    # Verify PK format: TERM_PATH#hpo|version|term_id
    assert sample_item["PK"].startswith(f"TERM_PATH#{source_name}|{version}|"), \
        f"Invalid PK format: {sample_item['PK']}"

    # Verify SK format: pipe-delimited term IDs (ancestor path)
    assert "|" in sample_item["SK"] or sample_item["path_length"] == 1, \
        f"Invalid SK format (expected pipe-delimited path): {sample_item['SK']}"

    # Verify SK ends with the term_id from PK (path should end at the term itself)
    assert sample_item["SK"].endswith(sample_item["term_id"]), \
        f"SK path should end with term_id. SK={sample_item['SK']}, term_id={sample_item['term_id']}"

    # Verify path_length matches number of terms in SK
    path_terms = sample_item["SK"].split("|")
    assert len(path_terms) == sample_item["path_length"], \
        f"path_length mismatch: SK has {len(path_terms)} terms, path_length={sample_item['path_length']}"

    # Validate specific known term paths for HPO
    if source_name == "hpo":
        print(f"\n  Validating known HPO term paths:")

        # Test 1: Root term (HP_0000001 - "All")
        root_response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": f"TERM_PATH#{source_name}|{version}|HP_0000001"
            },
            Limit=5
        )
        root_items = root_response.get("Items", [])
        assert len(root_items) > 0, "HPO root term (HP_0000001) not found in cache"

        root_paths = [item["SK"] for item in root_items]
        print(f"    HP_0000001 (All): {root_paths}")
        assert "HP_0000001" in root_paths, \
            f"Expected root term path 'HP_0000001' in cache, found: {root_paths}"

        # Test 2: Phenotypic abnormality (HP_0000118 - child of root)
        pheno_response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": f"TERM_PATH#{source_name}|{version}|HP_0000118"
            }
        )
        pheno_items = pheno_response.get("Items", [])
        assert len(pheno_items) > 0, "HP_0000118 (Phenotypic abnormality) not found in cache"

        pheno_paths = [item["SK"] for item in pheno_items]
        print(f"    HP_0000118 (Phenotypic abnormality): {len(pheno_paths)} path(s)")

        # Should have at least one path from root to HP_0000118
        expected_path = "HP_0000001|HP_0000118"
        assert any(expected_path in path for path in pheno_paths), \
            f"Expected path containing '{expected_path}', found: {pheno_paths}"

        # Test 3: Abnormal heart morphology (HP_0001627 - the cardiovascular term from perf issue)
        cardiac_response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": f"TERM_PATH#{source_name}|{version}|HP_0001627"
            }
        )
        cardiac_items = cardiac_response.get("Items", [])

        if len(cardiac_items) > 0:  # May not exist in test HPO versions
            cardiac_paths = [item["SK"] for item in cardiac_items]
            print(f"    HP_0001627 (Abnormal heart morphology): {len(cardiac_paths)} path(s)")

            # Verify all paths start with root
            for path in cardiac_paths:
                assert path.startswith("HP_0000001|"), \
                    f"Path should start with root term HP_0000001, found: {path}"

            # Verify all paths end with HP_0001627
            for path in cardiac_paths:
                assert path.endswith("HP_0001627"), \
                    f"Path should end with HP_0001627, found: {path}"

            # Should have multiple paths (has multiple parents in HPO)
            if len(cardiac_paths) > 1:
                print(f"      ✓ Multiple inheritance detected ({len(cardiac_paths)} paths)")

            # Check for expected parent terms in at least one path
            # HP_0001627 typically has parents like HP_0001626 (Abnormality of cardiovascular system)
            # and HP_0000118 (Phenotypic abnormality)
            path_contains_1626 = any("HP_0001626" in path for path in cardiac_paths)
            path_contains_0118 = any("HP_0000118" in path for path in cardiac_paths)

            if path_contains_1626:
                print(f"      ✓ Found path through HP_0001626 (Abnormality of cardiovascular system)")
            if path_contains_0118:
                print(f"      ✓ Found path through HP_0000118 (Phenotypic abnormality)")

            # Verify path structure: intermediate terms should be between root and leaf
            for path in cardiac_paths:
                path_terms = path.split("|")
                # Should have at least 3 terms: root, intermediate(s), and HP_0001627
                assert len(path_terms) >= 3, \
                    f"Expected multi-level path (at least root -> parent -> term), found: {path}"

                # If this path contains HP_0001626, verify ordering
                if "HP_0001626" in path:
                    idx_1626 = path_terms.index("HP_0001626")
                    idx_1627 = path_terms.index("HP_0001627")
                    assert idx_1626 < idx_1627, \
                        f"Parent HP_0001626 should come before child HP_0001627 in path: {path}"
        else:
            print(f"    HP_0001627 (Abnormal heart morphology): Not in this HPO version")

        # Test 4: Verify path ordering (parent before child)
        # Pick any term with a multi-level path
        for item in pheno_items:
            path = item["SK"]
            path_terms = path.split("|")

            # Root should always be first in multi-term paths
            if len(path_terms) > 1:
                assert path_terms[0] == "HP_0000001", \
                    f"Multi-term path should start with root HP_0000001, found: {path}"

            # Last term should be the term itself
            assert path_terms[-1] == "HP_0000118", \
                f"Path should end with term HP_0000118, found: {path}"

    print(f"\n  ✓ Cache table populated correctly")
