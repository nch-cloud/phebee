# Integration Test Plan: start_load.py

## Lambda Metadata

**Purpose**: Start Neptune bulk load with specified parameters (source, format, role, graph name).

**Dependencies**:
- Neptune: start_load utility
- Input: source (S3 path), role (IAM ARN), region, format, graph_name

**Key Operations**:
- Builds load_params with configuration
- Sets failOnError, updateSingleCardinalityProperties, queueRequest
- Configures parser with baseUri and namedGraphUri
- Starts Neptune load
- Returns load response with load ID

---

## Integration Test Scenarios

### Test 1: test_start_load_rdf_xml (Format=rdfxml, loads RDF/XML data)
### Test 2: test_start_load_turtle (Format=turtle, loads Turtle data)
### Test 3: test_start_load_nquads (Format=nquads, loads N-Quads)
### Test 4: test_start_load_with_graph_name (graph_name sets namedGraphUri correctly)
### Test 5: test_start_load_fail_on_error_true (failOnError=TRUE, strict loading)
### Test 6: test_start_load_queue_request (queueRequest=TRUE, load queued)
### Test 7: test_start_load_missing_source (source not provided, error)
### Test 8: test_start_load_missing_role (role not provided, error)
### Test 9: test_start_load_missing_region (region not provided, error)
### Test 10: test_start_load_missing_format (format not provided, error)
### Test 11: test_start_load_response_structure (status="200 OK", payload with loadId)
### Test 12: test_start_load_raises_on_failure (Neptune error, exception raised)
### Test 13: test_start_load_concurrent_loads (Start 3 loads concurrently, all succeed)
### Test 14: test_start_load_idempotent (Start same load twice, both succeed with different IDs)
### Test 15: test_start_load_integration_with_check_status (Start → Check status workflow)
### Test 16: test_start_load_parser_configuration (baseUri and namedGraphUri set correctly)
### Test 17: test_start_load_update_single_cardinality (updateSingleCardinalityProperties=TRUE)
### Test 18: test_start_load_s3_source_path (Source is s3:// URI)
### Test 19: test_start_load_logging (Source and graph_name logged)
### Test 20: test_start_load_performance (Load started in <1s)
