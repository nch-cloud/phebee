"""
Integration tests for ValidateBulkImport Lambda.

Tests the validation logic for bulk import which checks S3 paths,
finds JSONL files, and extracts project_id from path structure.
"""
import json
import pytest
import time
import concurrent.futures
from phebee.utils.aws import get_client


@pytest.fixture
def s3_bucket(cloudformation_stack):
    """Get S3 bucket name from CloudFormation stack."""
    cf_client = get_client("cloudformation")
    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    outputs = {o['OutputKey']: o['OutputValue'] for o in response['Stacks'][0]['Outputs']}
    return outputs['PheBeeBucketName']


def invoke_validate_bulk_import(run_id, input_path, cloudformation_stack):
    """Helper to invoke ValidateBulkImport lambda."""
    lambda_client = get_client("lambda")

    response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-ValidateBulkImportFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "run_id": run_id,
            "input_path": input_path
        }).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))
    return result


def upload_test_jsonl_files(bucket, prefix, file_count=3):
    """Helper to upload test JSONL files to S3."""
    s3_client = get_client("s3")
    jsonl_prefix = f"{prefix}jsonl/"

    uploaded_files = []
    for i in range(file_count):
        file_key = f"{jsonl_prefix}test_data_{i}.jsonl"
        content = '{"test": "data", "index": ' + str(i) + '}\n'

        s3_client.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=content.encode('utf-8')
        )
        uploaded_files.append(f"s3://{bucket}/{file_key}")

    return uploaded_files


def cleanup_s3_prefix(bucket, prefix):
    """Helper to cleanup test S3 files."""
    s3_client = get_client("s3")

    try:
        # List all objects with prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' in response:
            # Delete all objects
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")


def test_validate_valid_bulk_import(cloudformation_stack, s3_bucket):
    """Test 1: Validate path with JSONL files succeeds."""
    run_id = f"test-run-{int(time.time())}"
    project_id = "test-project-123"
    prefix = f"test/projects/{project_id}/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        # Upload test files
        upload_test_jsonl_files(s3_bucket, prefix, file_count=2)

        # Validate
        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        assert "body" in result

        body = result["body"]
        assert body["validated"] is True
        assert body["run_id"] == run_id
        assert body["project_id"] == project_id
        assert len(body["jsonl_files"]) == 2
        assert body["total_size"] > 0
        assert body["bucket"] == s3_bucket

        print(f"\n[TEST] Validated: {len(body['jsonl_files'])} files, {body['total_size']} bytes")

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_multiple_jsonl_files(cloudformation_stack, s3_bucket):
    """Test 2: Multiple JSONL files are all found and counted."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        # Upload 5 files
        uploaded = upload_test_jsonl_files(s3_bucket, prefix, file_count=5)

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        body = result["body"]

        assert len(body["jsonl_files"]) == 5
        # All should be S3 URIs
        for file_uri in body["jsonl_files"]:
            assert file_uri.startswith("s3://")
            assert file_uri.endswith(".jsonl")

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_missing_jsonl_directory(cloudformation_stack, s3_bucket):
    """Test 3: Path without jsonl/ subdirectory raises error."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        # Create prefix but don't upload any files
        s3_client = get_client("s3")
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=f"{prefix}dummy.txt",
            Body=b"test"
        )

        # Validation should fail
        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        # Lambda returns error in response
        assert "errorMessage" in result or "errorType" in result
        error_msg = result.get("errorMessage", "")
        assert "No JSONL files found" in error_msg

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_empty_jsonl_directory(cloudformation_stack, s3_bucket):
    """Test 4: Empty jsonl/ directory raises error."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        # Create jsonl/ directory but no files
        s3_client = get_client("s3")
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=f"{prefix}jsonl/.keep",
            Body=b""
        )

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        # Lambda returns error in response
        assert "errorMessage" in result or "errorType" in result
        error_msg = result.get("errorMessage", "")
        assert "No JSONL files found" in error_msg

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_non_jsonl_files_ignored(cloudformation_stack, s3_bucket):
    """Test 5: Non-JSONL files are ignored."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        s3_client = get_client("s3")
        jsonl_prefix = f"{prefix}jsonl/"

        # Upload 2 JSONL files
        for i in range(2):
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=f"{jsonl_prefix}data_{i}.jsonl",
                Body=b'{"test": "data"}\n'
            )

        # Upload non-JSONL files
        s3_client.put_object(Bucket=s3_bucket, Key=f"{jsonl_prefix}data.txt", Body=b"text")
        s3_client.put_object(Bucket=s3_bucket, Key=f"{jsonl_prefix}data.csv", Body=b"csv")
        s3_client.put_object(Bucket=s3_bucket, Key=f"{jsonl_prefix}data.parquet", Body=b"parquet")

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        body = result["body"]

        # Only 2 JSONL files should be counted
        assert len(body["jsonl_files"]) == 2

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_missing_run_id(cloudformation_stack, s3_bucket):
    """Test 6: Missing run_id raises error."""
    lambda_client = get_client("lambda")

    with pytest.raises(Exception) as exc_info:
        response = lambda_client.invoke(
            FunctionName=f"{cloudformation_stack}-ValidateBulkImportFunction",
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "input_path": f"s3://{s3_bucket}/test/"
            }).encode("utf-8")
        )
        result = json.loads(response["Payload"].read().decode("utf-8"))
        if "errorMessage" in result:
            raise Exception(result["errorMessage"])

    assert "run_id and input_path are required" in str(exc_info.value)


def test_validate_missing_input_path(cloudformation_stack, s3_bucket):
    """Test 7: Missing input_path raises error."""
    lambda_client = get_client("lambda")

    with pytest.raises(Exception) as exc_info:
        response = lambda_client.invoke(
            FunctionName=f"{cloudformation_stack}-ValidateBulkImportFunction",
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "run_id": "test-run-123"
            }).encode("utf-8")
        )
        result = json.loads(response["Payload"].read().decode("utf-8"))
        if "errorMessage" in result:
            raise Exception(result["errorMessage"])

    assert "run_id and input_path are required" in str(exc_info.value)


def test_validate_invalid_s3_uri_format(cloudformation_stack):
    """Test 8: Invalid S3 URI format raises error."""
    result = invoke_validate_bulk_import("test-run", "not-an-s3-path", cloudformation_stack)

    # Lambda returns error in response
    assert "errorMessage" in result or "errorType" in result
    error_msg = result.get("errorMessage", "")
    assert "input_path must be an S3 URI" in error_msg


def test_validate_nonexistent_bucket(cloudformation_stack):
    """Test 9: Nonexistent bucket raises error."""
    result = invoke_validate_bulk_import(
        "test-run",
        "s3://nonexistent-bucket-12345678/path/",
        cloudformation_stack
    )

    # Should get bucket not found error
    assert "errorMessage" in result or "errorType" in result
    error_msg = result.get("errorMessage", "")
    assert "Bucket not found" in error_msg or "NoSuchBucket" in error_msg


def test_validate_nonexistent_prefix(cloudformation_stack, s3_bucket):
    """Test 10: Nonexistent prefix raises error."""
    result = invoke_validate_bulk_import(
        "test-run",
        f"s3://{s3_bucket}/nonexistent/prefix/path/",
        cloudformation_stack
    )

    # Lambda returns error in response
    assert "errorMessage" in result or "errorType" in result
    error_msg = result.get("errorMessage", "")
    assert "No JSONL files found" in error_msg


def test_extract_project_id_from_path(cloudformation_stack, s3_bucket):
    """Test 11: Project ID is correctly extracted from path."""
    run_id = f"test-run-{int(time.time())}"
    project_id = "my-project-123"
    prefix = f"test/projects/{project_id}/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        upload_test_jsonl_files(s3_bucket, prefix)

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        body = result["body"]

        assert body["project_id"] == project_id
        print(f"\n[TEST] Extracted project_id: {body['project_id']}")

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_project_id_not_in_path(cloudformation_stack, s3_bucket):
    """Test 12: Path without 'projects/' structure returns None for project_id."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/data/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        upload_test_jsonl_files(s3_bucket, prefix)

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        body = result["body"]

        # project_id should be None when not in path
        assert body["project_id"] is None

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_mixed_jsonl_and_json(cloudformation_stack, s3_bucket):
    """Test 16: Both .jsonl and .json extensions are accepted."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        s3_client = get_client("s3")
        jsonl_prefix = f"{prefix}jsonl/"

        # Upload both .jsonl and .json files
        s3_client.put_object(Bucket=s3_bucket, Key=f"{jsonl_prefix}data1.jsonl", Body=b'{"test": 1}\n')
        s3_client.put_object(Bucket=s3_bucket, Key=f"{jsonl_prefix}data2.json", Body=b'{"test": 2}\n')

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        body = result["body"]

        assert len(body["jsonl_files"]) == 2

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_response_structure(cloudformation_stack, s3_bucket):
    """Test 17: Response has required fields."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        upload_test_jsonl_files(s3_bucket, prefix)

        result = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result["statusCode"] == 200
        assert "body" in result

        body = result["body"]
        required_fields = ["validated", "run_id", "bucket", "prefix", "jsonl_files", "total_size"]

        for field in required_fields:
            assert field in body, f"Missing required field: {field}"

        assert isinstance(body["jsonl_files"], list)
        assert isinstance(body["total_size"], int)
        assert body["validated"] is True

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_idempotency(cloudformation_stack, s3_bucket):
    """Test 19: Same validation twice returns identical results."""
    run_id = f"test-run-{int(time.time())}"
    prefix = f"test/runs/{run_id}/"
    input_path = f"s3://{s3_bucket}/{prefix}"

    try:
        upload_test_jsonl_files(s3_bucket, prefix, file_count=3)

        result1 = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)
        result2 = invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        assert result1["statusCode"] == result2["statusCode"]

        body1 = result1["body"]
        body2 = result2["body"]

        assert body1["run_id"] == body2["run_id"]
        assert len(body1["jsonl_files"]) == len(body2["jsonl_files"])
        assert body1["total_size"] == body2["total_size"]

    finally:
        cleanup_s3_prefix(s3_bucket, prefix)


def test_validate_concurrent_validations(cloudformation_stack, s3_bucket):
    """Test 20: Concurrent validations don't interfere."""
    run_ids = [f"test-run-{int(time.time())}-{i}" for i in range(3)]
    prefixes = [f"test/runs/{run_id}/" for run_id in run_ids]

    try:
        # Upload files for each run
        for prefix in prefixes:
            upload_test_jsonl_files(s3_bucket, prefix, file_count=2)

        # Validate concurrently
        def validate(run_id, prefix):
            input_path = f"s3://{s3_bucket}/{prefix}"
            return invoke_validate_bulk_import(run_id, input_path, cloudformation_stack)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(validate, run_id, prefix)
                for run_id, prefix in zip(run_ids, prefixes)
            ]
            results = [future.result() for future in futures]

        # All should succeed
        for result in results:
            assert result["statusCode"] == 200
            assert len(result["body"]["jsonl_files"]) == 2

    finally:
        for prefix in prefixes:
            cleanup_s3_prefix(s3_bucket, prefix)
