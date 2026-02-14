# Integration Test Plan: materialize_ontology_hierarchy.py

## Lambda Metadata

**Purpose**: Parse OBO ontology file, compute ancestor closure and depth for all terms, split into batches for distributed processing.

**Dependencies**:
- S3: Read OBO file, write batch files
- Environment: PHEBEE_BUCKET_NAME
- Input: obo_file_s3_path, ontology_source, version

**Key Operations**:
- Downloads and parses OBO file from S3
- Computes transitive ancestor closure for each term
- Calculates term depth in hierarchy
- Splits terms into batches (200 per batch)
- Writes batches to S3 for distributed Map state processing
- Returns batch metadata for Step Functions

---

## Integration Test Scenarios

### Test 1: test_parse_small_obo_file (HPO subset with 100 terms, verify parsing, ancestors computed, batches created)
### Test 2: test_parse_full_hpo_ontology (Full HPO ~17000 terms, verify completion, ~85 batches created)
### Test 3: test_parse_mondo_ontology (MONDO ontology, different prefix structure handled)
### Test 4: test_parse_missing_obo_file (Nonexistent S3 path, error handling)
### Test 5: test_parse_corrupted_obo_file (Invalid OBO syntax, error raised)
### Test 6: test_compute_ancestor_closure (Verify transitive closure correct, parent→grandparent→root)
### Test 7: test_compute_term_depth (Root terms depth 0, children incremented properly)
### Test 8: test_filter_cross_ontology_references (HP: file with MONDO: refs, filtered out)
### Test 9: test_batch_size_200 (Verify batches ~200 terms each, even distribution)
### Test 10: test_batch_files_written_to_s3 (All batches in S3, correct path structure)
### Test 11: test_parse_terms_with_multiple_parents (DAG structure, ancestors include all paths)
### Test 12: test_parse_obsolete_terms_excluded (Obsolete terms not included in output)
### Test 13: test_parse_handle_unicode_labels (Unicode in term labels preserved correctly)
### Test 14: test_parse_is_a_with_qualifiers (is_a with {qualifier} annotations parsed correctly)
### Test 15: test_environment_variable_missing (PHEBEE_BUCKET_NAME not set, error raised)
### Test 16: test_response_includes_batch_metadata (Response has batch_count, total_terms, s3_prefix)
### Test 17: test_batch_payload_structure (Each batch JSON has ontology_source, version, terms array)
### Test 18: test_step_functions_map_compatibility (Output format works with Distributed Map state)
### Test 19: test_concurrent_ontology_parsing (Parse HPO and MONDO concurrently, no interference)
### Test 20: test_idempotent_parsing (Parse twice, same batches created, deterministic order)
