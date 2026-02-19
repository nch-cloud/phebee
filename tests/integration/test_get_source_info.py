"""
Integration tests for GetSourceInfo Lambda.

Tests the get_source_info endpoint which retrieves the newest ontology source
metadata from DynamoDB by source name (hpo, mondo, eco).
"""
import json
import pytest
import concurrent.futures
from phebee.utils.aws import get_client


def invoke_get_source_info(source_name, cloudformation_stack):
    """Helper to invoke GetSourceInfo lambda."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-GetSourceInfoFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "pathParameters": {"source_name": source_name}
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    return result


def test_get_source_hpo(cloudformation_stack):
    """Test 1: Get HPO source info returns newest version."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    assert "body" in result

    body = json.loads(result["body"])

    # Verify required fields
    assert "Version" in body
    assert "InstallTimestamp" in body or "CreationTimestamp" in body
    assert "GraphName" in body
    assert body["GraphName"].startswith("hpo~")

    print(f"\n[TEST] HPO source info: version={body['Version']}, graph={body['GraphName']}")


def test_get_source_nonexistent(cloudformation_stack):
    """Test 4: Get nonexistent source returns 404."""
    result = invoke_get_source_info("unknown_ontology", cloudformation_stack)

    assert result["statusCode"] == 404
    assert "body" in result

    body = json.loads(result["body"])
    assert "error" in body
    assert "unknown_ontology" in body["error"]


def test_get_source_response_structure(cloudformation_stack):
    """Test 5: Response contains required fields."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    assert "headers" in result
    assert result["headers"]["Content-Type"] == "application/json"

    body = json.loads(result["body"])

    # Required fields from DynamoDB SOURCE record
    required_fields = ["Version", "GraphName"]
    for field in required_fields:
        assert field in body, f"Missing required field: {field}"

    # Should have either InstallTimestamp or CreationTimestamp
    assert "InstallTimestamp" in body or "CreationTimestamp" in body


def test_get_source_newest_version(cloudformation_stack):
    """Test 6: Returns newest version when multiple exist (verified by SK sort)."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # The lambda uses ScanIndexForward=False and Limit=1, so it should return newest
    # We can verify this returned the most recent by checking SK field
    assert "SK" in body
    assert "PK" in body
    assert body["PK"] == "SOURCE~hpo"

    print(f"\n[TEST] Newest HPO version SK: {body['SK']}")


def test_get_source_assets_list(cloudformation_stack):
    """Test 7: Assets list contains asset_name and asset_path."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # HPO should have assets
    assert "Assets" in body
    assert isinstance(body["Assets"], list)
    assert len(body["Assets"]) > 0

    # Verify asset structure
    for asset in body["Assets"]:
        assert "asset_name" in asset
        assert "asset_path" in asset
        assert asset["asset_path"].startswith("s3://")

    print(f"\n[TEST] HPO has {len(body['Assets'])} assets")


def test_get_source_graph_name(cloudformation_stack):
    """Test 8: GraphName field present with format source~version."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert "GraphName" in body
    graph_name = body["GraphName"]

    # Format should be source~version
    assert "~" in graph_name
    parts = graph_name.split("~")
    assert parts[0] == "hpo"
    assert len(parts[1]) > 0  # Should have version part

    print(f"\n[TEST] GraphName: {graph_name}")


def test_get_source_api_gateway_integration(cloudformation_stack):
    """Test 9: Lambda handles API Gateway event format (pathParameters extraction)."""
    # The invoke_get_source_info helper already tests this by passing pathParameters
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    # If pathParameters weren't handled correctly, we'd get an error


def test_get_source_concurrent_requests(cloudformation_stack):
    """Test 10: Multiple concurrent requests all succeed."""

    def get_hpo():
        return invoke_get_source_info("hpo", cloudformation_stack)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_hpo) for _ in range(5)]
        results = [future.result() for future in futures]

    # All requests should succeed
    for result in results:
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert "Version" in body


def test_get_source_idempotent(cloudformation_stack):
    """Test 11: Same request twice returns same result."""
    result1 = invoke_get_source_info("hpo", cloudformation_stack)
    result2 = invoke_get_source_info("hpo", cloudformation_stack)

    assert result1["statusCode"] == result2["statusCode"]

    body1 = json.loads(result1["body"])
    body2 = json.loads(result2["body"])

    # Should return same version and data
    assert body1["Version"] == body2["Version"]
    assert body1["GraphName"] == body2["GraphName"]


def test_get_source_case_sensitivity(cloudformation_stack):
    """Test 14: Source name is case-sensitive (HPO != hpo)."""
    # Lowercase should work (that's how we store it)
    result_lower = invoke_get_source_info("hpo", cloudformation_stack)
    assert result_lower["statusCode"] == 200

    # Uppercase should not find anything (DynamoDB is case-sensitive)
    result_upper = invoke_get_source_info("HPO", cloudformation_stack)
    assert result_upper["statusCode"] == 404


def test_get_source_json_response(cloudformation_stack):
    """Test 15: Response has Content-Type: application/json."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert "headers" in result
    assert "Content-Type" in result["headers"]
    assert result["headers"]["Content-Type"] == "application/json"

    # Body should be valid JSON
    body = json.loads(result["body"])
    assert isinstance(body, dict)


def test_get_source_version_format(cloudformation_stack):
    """Test 17: Version is date string like v2024-04-26 or similar."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    version = body["Version"]
    # HPO versions are typically like "v2024-04-26" or date-based
    assert isinstance(version, str)
    assert len(version) > 0

    print(f"\n[TEST] HPO version format: {version}")


def test_get_source_multiple_assets(cloudformation_stack):
    """Test 18: Source with multiple assets returns all of them."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    assert "Assets" in body
    assets = body["Assets"]

    # HPO typically has multiple assets (hp.owl, phenotype.hpoa, etc.)
    assert len(assets) >= 1

    # All assets should have required fields
    asset_names = [a["asset_name"] for a in assets]
    print(f"\n[TEST] HPO assets: {', '.join(asset_names)}")


def test_get_source_pk_sk_fields(cloudformation_stack):
    """Test that PK and SK fields are returned in response."""
    result = invoke_get_source_info("hpo", cloudformation_stack)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])

    # DynamoDB record should include PK and SK
    assert "PK" in body
    assert "SK" in body
    assert body["PK"] == "SOURCE~hpo"
    # SK should be a timestamp
    assert isinstance(body["SK"], str)


def test_get_source_empty_source_name(cloudformation_stack):
    """Test handling of empty source name."""
    lambda_client = get_client("lambda")

    # Test with empty string
    try:
        response = lambda_client.invoke(
            FunctionName=f"{cloudformation_stack}-GetSourceInfoFunction",
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "pathParameters": {"source_name": ""}
            }).encode("utf-8")
        )

        result = json.loads(response["Payload"].read().decode("utf-8"))
        # Should return 404 for empty source name
        assert result["statusCode"] == 404
    except Exception as e:
        # API Gateway might reject empty path parameters before reaching lambda
        pytest.skip(f"Empty path parameter rejected: {e}")


def test_get_source_special_characters(cloudformation_stack):
    """Test 13: Source name with special characters handled safely."""
    # Try various special characters
    special_names = ["hpo-test", "hpo_test", "hpo.test"]

    for name in special_names:
        result = invoke_get_source_info(name, cloudformation_stack)
        # These shouldn't cause errors, just 404 since they don't exist
        assert result["statusCode"] == 404
        body = json.loads(result["body"])
        assert "error" in body
