# Integration Test Plan: parse_phenopacket_collection.py

## Lambda Metadata

**Purpose**: Parse a zip file containing phenopackets or subfolders with phenopackets, extract JSON files, flatten them into a list, and store in S3 for downstream processing by Step Functions Map state.

**Dependencies**:
- S3: Read zip file input, write flattened JSON output
- Environment Variables: `PheBeeBucketName`

**Key Operations**:
- Downloads zip file from S3
- Extracts all .json files from zip (including nested folders)
- Adds project_id to each phenopacket
- Stores flattened array in S3
- Optionally deletes source zip after processing
- Returns metadata for Step Functions Map state (bucket, key, count)

---

## Integration Test Scenarios

### Test 1: test_parse_single_phenopacket_zip
**Description**: Parse zip containing a single phenopacket JSON file

**Setup**:
- Create test project in DynamoDB
- Create zip with single phenopacket JSON: `patient001.json`
- Upload zip to S3: `s3://test-bucket/input/phenopackets.zip`

**Action**:
```json
{
  "project_id": "test-project-123",
  "s3_path": "s3://test-bucket/input/phenopackets.zip",
  "delete_collection": false
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 1
- bucket and key returned
- S3 output file exists at returned location
- Output JSON is valid array with 1 element
- Each element contains original phenopacket data
- Each element has project_id field set to "test-project-123"
- Source zip still exists in S3

**Cleanup**:
- Delete S3 output file
- Delete S3 input zip
- Delete test project

### Test 2: test_parse_multiple_phenopackets_flat
**Description**: Parse zip with multiple phenopackets in root directory

**Setup**:
- Create test project
- Create zip with 5 phenopackets: `patient001.json` through `patient005.json`
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-456",
  "s3_path": "s3://test-bucket/input/multi-phenopackets.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 5
- Output array contains 5 elements
- All 5 phenopackets have project_id set correctly
- Phenopacket data is preserved accurately
- Order of phenopackets is deterministic

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 3: test_parse_nested_phenopackets_cohorts
**Description**: Parse zip with nested folder structure (cohorts)

**Setup**:
- Create zip with structure:
  - `cohort-a/patient001.json`
  - `cohort-a/patient002.json`
  - `cohort-b/patient003.json`
  - `cohort-b/patient004.json`
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-789",
  "s3_path": "s3://test-bucket/input/cohorts.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 4
- All phenopackets extracted regardless of folder nesting
- project_id added to all phenopackets
- Cohort folder structure is flattened (not preserved in output)

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 4: test_parse_with_delete_collection
**Description**: Verify source zip is deleted after successful processing

**Setup**:
- Create test project
- Create zip with 3 phenopackets
- Upload to S3 at known location

**Action**:
```json
{
  "project_id": "test-project-delete",
  "s3_path": "s3://test-bucket/input/delete-me.zip",
  "delete_collection": true
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 3
- Output file exists in S3
- Source zip file DOES NOT exist in S3 (deleted)
- All phenopackets successfully extracted before deletion

**Cleanup**:
- Delete S3 output file
- Delete test project

### Test 5: test_parse_with_custom_output_path
**Description**: Specify custom output S3 path instead of generated path

**Setup**:
- Create test project
- Create zip with 2 phenopackets
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-custom",
  "s3_path": "s3://test-bucket/input/phenopackets.zip",
  "output_s3_path": "s3://test-bucket/custom/output/my-phenopackets.json"
}
```

**Assertions**:
- statusCode: 200
- Output file created at EXACT custom path
- bucket: "test-bucket"
- key: "custom/output/my-phenopackets.json"
- No generated filename used
- JSON content is valid

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 6: test_parse_skip_non_json_files
**Description**: Verify non-JSON files in zip are ignored

**Setup**:
- Create zip containing:
  - `patient001.json` (valid)
  - `patient002.json` (valid)
  - `README.txt` (should be skipped)
  - `data.csv` (should be skipped)
  - `image.png` (should be skipped)
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-mixed",
  "s3_path": "s3://test-bucket/input/mixed-files.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 2 (only JSON files)
- Output contains only valid phenopacket JSON objects
- Non-JSON files are ignored (logged but not included)
- No errors thrown for non-JSON files

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 7: test_parse_invalid_json_file
**Description**: Handle malformed JSON gracefully

**Setup**:
- Create zip containing:
  - `valid001.json` (valid JSON)
  - `invalid.json` (malformed JSON: `{incomplete`)
  - `valid002.json` (valid JSON)
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-invalid",
  "s3_path": "s3://test-bucket/input/invalid-json.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 2 (only valid JSONs)
- Invalid JSON is skipped (error logged)
- Processing continues for other files
- Valid phenopackets are included in output

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 8: test_parse_empty_zip
**Description**: Handle zip file with no JSON content

**Setup**:
- Create zip containing only non-JSON files or empty
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-empty",
  "s3_path": "s3://test-bucket/input/empty.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 0
- Output array is empty but valid JSON: `[]`
- No errors thrown
- Output file still created in S3

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 9: test_parse_missing_s3_file
**Description**: Handle missing input S3 file

**Setup**:
- No file uploaded to S3

**Action**:
```json
{
  "project_id": "test-project-missing",
  "s3_path": "s3://test-bucket/input/nonexistent.zip"
}
```

**Assertions**:
- Lambda raises ClientError (NoSuchKey)
- Error contains meaningful message
- No output file created

**Cleanup**:
- None needed

### Test 10: test_parse_missing_project_id
**Description**: Handle missing project_id parameter

**Setup**:
- Create zip with phenopackets
- Upload to S3

**Action**:
```json
{
  "s3_path": "s3://test-bucket/input/phenopackets.zip"
}
```

**Assertions**:
- statusCode: 200 (project_id is optional per TODO comment)
- number_phenopackets matches file count
- project_id field in output is None or not set
- Processing continues normally

**Cleanup**:
- Delete S3 files

### Test 11: test_parse_invalid_s3_path_format
**Description**: Handle malformed S3 path

**Setup**:
- None

**Action**:
```json
{
  "project_id": "test-project-123",
  "s3_path": "not-an-s3-path"
}
```

**Assertions**:
- Lambda raises ValueError or parsing error
- Error message indicates invalid S3 path format
- No output file created

**Cleanup**:
- None

### Test 12: test_parse_large_phenopacket_collection
**Description**: Parse large collection (100+ phenopackets)

**Setup**:
- Create test project
- Generate zip with 150 phenopacket JSON files
- Each phenopacket ~5KB
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-large",
  "s3_path": "s3://test-bucket/input/large-collection.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 150
- Output file size is reasonable (compressed if possible)
- All phenopackets have project_id
- Lambda completes within timeout (15 minutes default)
- Memory usage is acceptable

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 13: test_parse_special_characters_in_filenames
**Description**: Handle phenopacket filenames with special characters

**Setup**:
- Create zip with files:
  - `patient-001_2024.json`
  - `patient@hospital#123.json`
  - `patient (copy).json`
  - `患者データ.json` (Unicode)
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-special",
  "s3_path": "s3://test-bucket/input/special-chars.zip"
}
```

**Assertions**:
- statusCode: 200
- All valid JSON files extracted regardless of filename
- number_phenopackets matches count
- Unicode filenames handled correctly
- No encoding errors

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 14: test_parse_preserves_phenopacket_structure
**Description**: Verify complex phenopacket structure is preserved

**Setup**:
- Create zip with phenopacket containing:
  - Nested objects (metaData, phenotypicFeatures)
  - Arrays of objects
  - Special characters in values
  - Unicode text
  - Null values
  - Boolean values
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-complex",
  "s3_path": "s3://test-bucket/input/complex-phenopacket.zip"
}
```

**Assertions**:
- statusCode: 200
- Output phenopacket structure exactly matches input
- All nested objects preserved
- All data types preserved (string, number, boolean, null, array, object)
- No data loss or corruption
- project_id added without modifying original structure

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 15: test_parse_generated_filename_uniqueness
**Description**: Verify generated filenames are unique across invocations

**Setup**:
- Create test project
- Create zip with 1 phenopacket
- Upload to S3

**Action** (invoke twice in quick succession):
```json
{
  "project_id": "test-project-unique",
  "s3_path": "s3://test-bucket/input/phenopackets.zip"
}
```

**Assertions**:
- Both invocations return statusCode: 200
- Generated output filenames are different
- Filename format includes timestamp and UUID
- No file overwriting occurs
- Both output files exist in S3

**Cleanup**:
- Delete both S3 output files
- Delete S3 input file
- Delete test project

### Test 16: test_parse_with_directory_entries
**Description**: Handle zip with explicit directory entries

**Setup**:
- Create zip with directory entries:
  - `cohort-a/` (directory entry)
  - `cohort-a/patient001.json`
  - `cohort-b/` (directory entry)
  - `cohort-b/patient002.json`
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-dirs",
  "s3_path": "s3://test-bucket/input/with-dirs.zip"
}
```

**Assertions**:
- statusCode: 200
- number_phenopackets: 2
- Directory entries are skipped (not treated as files)
- JSON files within directories are extracted
- No errors from directory entries

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 17: test_parse_output_compatibility_with_map_state
**Description**: Verify output format is compatible with Step Functions Map ItemReader

**Setup**:
- Create test project
- Create zip with 3 phenopackets
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-map",
  "s3_path": "s3://test-bucket/input/phenopackets.zip"
}
```

**Assertions**:
- statusCode: 200
- Response includes "bucket" and "key" fields (not s3:// path)
- bucket and key are separate fields (required by Map ItemReader)
- Output S3 file is valid JSON array
- Each array element is a complete JSON object
- Array is not nested (flat structure)

**Cleanup**:
- Delete S3 files
- Delete test project

### Test 18: test_parse_concurrent_invocations
**Description**: Multiple lambdas parsing different collections simultaneously

**Setup**:
- Create 3 test projects
- Create 3 different zip files
- Upload all to S3

**Action** (invoke 3 times concurrently):
```json
[
  {"project_id": "project-1", "s3_path": "s3://test-bucket/input/collection1.zip"},
  {"project_id": "project-2", "s3_path": "s3://test-bucket/input/collection2.zip"},
  {"project_id": "project-3", "s3_path": "s3://test-bucket/input/collection3.zip"}
]
```

**Assertions**:
- All 3 invocations complete successfully
- Each returns correct number_phenopackets for its collection
- Output files are unique (no collision)
- Each output has correct project_id
- No cross-contamination between invocations

**Cleanup**:
- Delete all S3 files
- Delete all test projects

### Test 19: test_parse_corrupted_zip
**Description**: Handle corrupted or invalid zip file

**Setup**:
- Create invalid zip file (truncated or corrupted bytes)
- Upload to S3

**Action**:
```json
{
  "project_id": "test-project-corrupt",
  "s3_path": "s3://test-bucket/input/corrupted.zip"
}
```

**Assertions**:
- Lambda raises zipfile.BadZipFile or similar error
- Error message is descriptive
- No partial output file created
- Source file not deleted (even if delete_collection=true)

**Cleanup**:
- Delete S3 input file
- Delete test project

### Test 20: test_parse_idempotency
**Description**: Verify same input produces same output

**Setup**:
- Create test project
- Create zip with 5 phenopackets
- Upload to S3

**Action** (invoke twice with same parameters):
```json
{
  "project_id": "test-project-idem",
  "s3_path": "s3://test-bucket/input/phenopackets.zip",
  "output_s3_path": "s3://test-bucket/output/fixed-path.json"
}
```

**Assertions**:
- Both invocations succeed
- Output file content is identical between invocations
- number_phenopackets is same both times
- project_id assignment is consistent
- Second invocation overwrites first output (same path)

**Cleanup**:
- Delete S3 files
- Delete test project
