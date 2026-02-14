# Integration Test Plan: generate_ttl_from_iceberg.py

## Lambda Metadata

**Purpose**: Generate TTL (Turtle) RDF files from Iceberg evidence data for Neptune bulk loading, processing one page of data.

**Dependencies**:
- Athena: Query evidence by page
- DynamoDB: Lookup project info for subjects
- S3: Write TTL files
- Environment: DYNAMODB_TABLE_NAME

**Key Operations**:
- Reads manifest to get page query
- Executes Athena query for page
- Looks up project-subject mappings in DynamoDB
- Generates TTL for subjects graph and project graphs
- Writes separate TTL files per graph
- Returns TTL file locations

---

## Integration Test Scenarios

### Test 1: test_generate_ttl_single_page (Page 0 with 100 records, TTL files created)
### Test 2: test_generate_ttl_subjects_graph (Subjects TTL contains Subject, TermLink, hasTermLink triples)
### Test 3: test_generate_ttl_project_graph (Project TTL contains Project, hasProjectSubjectId triples)
### Test 4: test_generate_ttl_with_qualifiers (Negated qualifier mapped to hasQualifyingTerm)
### Test 5: test_generate_ttl_multiple_projects (Multiple projects, separate TTL files per project)
### Test 6: test_generate_ttl_namespace_prefixes (TTL has @prefix declarations, valid Turtle syntax)
### Test 7: test_generate_ttl_term_uri_format (Term URIs are full IRIs in angle brackets)
### Test 8: test_generate_ttl_avoid_duplicate_termlinks (Same termlink_id not declared twice)
### Test 9: test_generate_ttl_dynamodb_lookup (Queries DynamoDB for subject→project mapping)
### Test 10: test_generate_ttl_missing_project_info (Subject without project, handled gracefully)
### Test 11: test_generate_ttl_empty_page (Query returns 0 rows, TTL files still created but minimal)
### Test 12: test_generate_ttl_large_page (5000 records, TTL files created, no memory issues)
### Test 13: test_generate_ttl_special_chars_in_ids (Subject IDs with special chars, URL-encoded in URIs)
### Test 14: test_generate_ttl_unicode_in_labels (Unicode text handled, valid UTF-8 TTL)
### Test 15: test_generate_ttl_s3_upload (TTL files uploaded to correct S3 paths)
### Test 16: test_generate_ttl_content_type (S3 objects have ContentType: text/turtle)
### Test 17: test_generate_ttl_response_structure (Returns ttl_files dict, record_count, ttl_prefix)
### Test 18: test_generate_ttl_integration_with_manifest (Reads page query from manifest.json)
### Test 19: test_generate_ttl_athena_pagination (Handles NextToken for large result sets)
### Test 20: test_generate_ttl_concurrent_pages (Multiple pages processed concurrently, no conflicts)
