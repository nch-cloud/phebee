# Integration Test Plan: download_github_release.py

## Lambda Metadata

**Purpose**: Download ontology assets from GitHub releases, cache in S3, and store metadata in DynamoDB.

**Dependencies**:
- GitHub API: Find latest release, download assets
- S3: Store assets
- DynamoDB: Store release metadata
- Environment: PheBeeBucketName, PheBeeDynamoTable

**Key Operations**:
- Queries GitHub for newest release
- Checks DynamoDB cache to avoid re-downloading
- Downloads assets or extracts from zip
- Stores assets in S3
- Records metadata in DynamoDB with CreationTimestamp

---

## Integration Test Scenarios

### Test 1: test_download_new_release (New HPO release, downloads and caches)
### Test 2: test_download_cached_release (Release already cached, skips download)
### Test 3: test_download_with_test_flag (test=true, forces download even if cached)
### Test 4: test_download_multiple_assets (asset_names list, all assets downloaded)
### Test 5: test_download_extract_zip (extract_zip=true, downloads zipball and extracts assets)
### Test 6: test_download_direct_assets (extract_zip=false, downloads assets directly from release)
### Test 7: test_download_stores_s3 (Assets stored at s3://bucket/sources/source/version/assets/)
### Test 8: test_download_stores_dynamodb (Metadata in DynamoDB with PK=SOURCE~name, SK=timestamp)
### Test 9: test_download_response_downloaded_true (New download, downloaded=true, assets list)
### Test 10: test_download_response_downloaded_false (Cached, downloaded=false)
### Test 11: test_download_github_api_error (GitHub unavailable, error raised)
### Test 12: test_download_nonexistent_repo (Invalid user/repo, error)
### Test 13: test_download_nonexistent_asset (Asset name not in release, error)
### Test 14: test_download_version_extraction (Version extracted from tag_name)
### Test 15: test_download_cache_check_query (DynamoDB queried for existing versions)
### Test 16: test_download_multiple_versions (Different versions coexist in S3 and DynamoDB)
### Test 17: test_download_concurrent_sources (Download HPO and MONDO concurrently)
### Test 18: test_download_idempotent (Download twice with test=true, both succeed)
### Test 19: test_download_asset_metadata_format (DynamoDB Assets field is list of {asset_name, asset_path})
### Test 20: test_download_integration_with_get_source (After download, get_source_info retrieves it)
