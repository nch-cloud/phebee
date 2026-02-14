# Integration Test Plan: validate_bulk_import.py

## Lambda Metadata

**Purpose**: Validate bulk import input by checking that the S3 path exists, contains JSONL files, and extracting project_id from path structure.

**Dependencies**:
- S3: list_objects_v2 to verify files exist
- Input: run_id, input_path

**Key Operations**:
- Parses S3 URI into bucket and prefix
- Lists objects to find JSONL files in jsonl/ subdirectory
- Extracts project_id from path pattern: projects/{project_id}/runs/{run_id}/
- Calculates total size of JSONL files
- Returns validation result with file list and metadata

---

## Integration Test Scenarios

### Test 1: test_validate_valid_bulk_import
**Setup**: Upload JSONL files to s3://bucket/projects/proj-123/runs/run-456/jsonl/
**Action**: `{"run_id": "run-456", "input_path": "s3://bucket/projects/proj-123/runs/run-456/"}`
**Assertions**: statusCode 200, validated true, project_id extracted, jsonl_files list populated, total_size > 0

### Test 2: test_validate_multiple_jsonl_files
**Setup**: Upload 5 JSONL files in jsonl/ subdirectory
**Action**: Validate the path
**Assertions**: jsonl_files contains 5 S3 URIs, all end with .jsonl or .json, total_size is sum of all files

### Test 3: test_validate_missing_jsonl_directory
**Setup**: Create path without jsonl/ subdirectory
**Action**: Attempt validation
**Assertions**: Raises ValueError "No JSONL files found", statusCode 500

### Test 4: test_validate_empty_jsonl_directory
**Setup**: Create jsonl/ directory with no files
**Action**: Validate
**Assertions**: Raises ValueError "No JSONL files found"

### Test 5: test_validate_non_jsonl_files_ignored
**Setup**: Upload .txt, .csv, .parquet files along with 2 .jsonl files
**Action**: Validate
**Assertions**: Only 2 JSONL files in result, other formats ignored

### Test 6: test_validate_missing_run_id
**Action**: `{"input_path": "s3://bucket/path/"}`
**Assertions**: Raises ValueError "run_id and input_path are required"

### Test 7: test_validate_missing_input_path
**Action**: `{"run_id": "run-123"}`
**Assertions**: Raises ValueError about required fields

### Test 8: test_validate_invalid_s3_uri_format
**Action**: `{"run_id": "run-123", "input_path": "not-an-s3-path"}`
**Assertions**: Raises ValueError "input_path must be an S3 URI"

### Test 9: test_validate_nonexistent_bucket
**Action**: `{"run_id": "run-123", "input_path": "s3://nonexistent-bucket/path/"}`
**Assertions**: Raises ValueError "Bucket not found"

### Test 10: test_validate_nonexistent_prefix
**Action**: Valid bucket but nonexistent prefix
**Assertions**: ValueError "No JSONL files found"

### Test 11: test_extract_project_id_from_path
**Setup**: Path: s3://bucket/projects/my-project-123/runs/run-456/
**Action**: Validate
**Assertions**: project_id: "my-project-123" extracted correctly

### Test 12: test_project_id_not_in_path
**Setup**: Path without "projects/" structure
**Action**: Validate
**Assertions**: project_id is None, warning logged, validation continues

### Test 13: test_validate_large_jsonl_files
**Setup**: Upload JSONL files totaling 1GB
**Action**: Validate
**Assertions**: total_size reflects actual size, all files listed, no timeout

### Test 14: test_validate_special_characters_in_prefix
**Setup**: Path with spaces, special chars: s3://bucket/projects/proj@123/runs/run#456/
**Action**: Validate
**Assertions**: Proper URL encoding handled, validation succeeds

### Test 15: test_validate_deeply_nested_jsonl
**Setup**: Place JSONL in: path/jsonl/subfolder/file.jsonl
**Action**: Validate
**Assertions**: Files in jsonl/ subdirectories are found and included

### Test 16: test_validate_mixed_jsonl_and_json
**Setup**: Upload both .jsonl and .json extensions
**Action**: Validate
**Assertions**: Both extensions accepted and counted

### Test 17: test_validate_response_structure
**Action**: Successful validation
**Assertions**: Response has statusCode, body.validated, body.run_id, body.bucket, body.prefix, body.jsonl_files, body.total_size

### Test 18: test_validate_s3_access_denied
**Setup**: Path with no read permissions
**Action**: Validate
**Assertions**: Raises error about S3 access, meaningful error message

### Test 19: test_validate_idempotency
**Setup**: Upload files
**Action**: Validate twice with same input
**Assertions**: Both return identical results, same file count, same total_size

### Test 20: test_validate_concurrent_validations
**Setup**: Multiple import paths
**Action**: Validate 3 paths concurrently
**Assertions**: All succeed, no interference, each returns correct file list
