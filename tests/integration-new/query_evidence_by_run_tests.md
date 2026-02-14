# Integration Test Plan: query_evidence_by_run.py

## Lambda Metadata

**Purpose**: Query evidence records from Iceberg table by run_id with pagination support.

**Dependencies**:
- Athena: Query Iceberg evidence table
- S3: Query results
- Environment: ICEBERG_DATABASE, ICEBERG_EVIDENCE_TABLE, ATHENA_WORKGROUP, PHEBEE_BUCKET_NAME

**Key Operations**:
- Queries evidence table filtering by run_id
- Supports pagination via next_token
- Returns up to 10k records per page (configurable limit)
- Includes total count on first page
- Parses structured fields (creator struct)

---

## Integration Test Scenarios

### Test 1: test_query_evidence_by_run_success (run_id with 100 records, returns all evidence)
### Test 2: test_query_evidence_pagination (1500 records, first page has next_token, second page completes)
### Test 3: test_query_evidence_limit_parameter (limit=50, returns max 50 records)
### Test 4: test_query_evidence_max_10k_limit (limit=20000, capped at 10k)
### Test 5: test_query_evidence_empty_run (run_id with no data, returns empty array)
### Test 6: test_query_evidence_missing_run_id (run_id not in body, statusCode 400)
### Test 7: test_query_evidence_total_count_first_page (First page includes total_count)
### Test 8: test_query_evidence_no_count_subsequent_pages (Subsequent pages omit total_count for performance)
### Test 9: test_query_evidence_creator_struct_parsed (creator field parsed from ROW format to dict)
### Test 10: test_query_evidence_fields_complete (All evidence fields present: id, run_id, batch_id, type, subject_id, term_iri, creator, timestamp)
### Test 11: test_query_evidence_ordered_by_timestamp (Results ordered by created_timestamp ascending)
### Test 12: test_query_evidence_athena_workgroup (Uses configured workgroup)
### Test 13: test_query_evidence_results_in_s3 (Query results written to S3 athena-results/)
### Test 14: test_query_evidence_concurrent_queries (3 queries for different runs concurrently, no interference)
### Test 15: test_query_evidence_api_gateway_integration (Invoke via API, extract_body handles event format)
### Test 16: test_query_evidence_idempotent (Same query twice, same results)
### Test 17: test_query_evidence_athena_failure (Query fails, statusCode 500, error message)
### Test 18: test_query_evidence_next_token_format (next_token is Athena pagination token)
### Test 19: test_query_evidence_has_more_flag (has_more: true when more pages available)
### Test 20: test_query_evidence_performance (Query completes in <5s for 1000 records)
