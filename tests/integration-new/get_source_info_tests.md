# Integration Test Plan: get_source_info.py

## Lambda Metadata

**Purpose**: Get newest ontology source info from DynamoDB by source name (HPO, MONDO, etc.).

**Dependencies**:
- DynamoDB: Query SOURCE~{name} partition, sort by SK descending
- API Gateway: Path parameter source_name

**Key Operations**:
- Queries DynamoDB for newest source version
- Returns source metadata: version, assets, creation timestamp

---

## Integration Test Scenarios

### Test 1: test_get_source_hpo (source_name=hpo, returns newest HPO version)
### Test 2: test_get_source_mondo (source_name=mondo, returns newest MONDO version)
### Test 3: test_get_source_eco (source_name=eco, returns ECO version)
### Test 4: test_get_source_nonexistent (source_name=unknown, statusCode 404)
### Test 5: test_get_source_response_structure (Contains version, assets array, CreationTimestamp)
### Test 6: test_get_source_newest_version (Multiple versions exist, returns newest by SK)
### Test 7: test_get_source_assets_list (Assets list contains asset_name and asset_path)
### Test 8: test_get_source_graph_name (GraphName field present, format: source~version)
### Test 9: test_get_source_api_gateway_integration (Invoked via API, pathParameters extracted)
### Test 10: test_get_source_concurrent_requests (5 concurrent requests, all succeed)
### Test 11: test_get_source_idempotent (Same request twice, same result)
### Test 12: test_get_source_dynamodb_error (DynamoDB unavailable, error handled)
### Test 13: test_get_source_special_chars_in_name (source_name with chars, handled safely)
### Test 14: test_get_source_case_sensitivity (hpo vs HPO, handled correctly)
### Test 15: test_get_source_json_response (Content-Type: application/json)
### Test 16: test_get_source_after_download (After download_github_release, source retrievable)
### Test 17: test_get_source_version_format (Version is date string like 2024-04-26)
### Test 18: test_get_source_multiple_assets (Source with multiple assets, all returned)
### Test 19: test_get_source_logging (Source name logged for observability)
### Test 20: test_get_source_performance (<100ms response time)
