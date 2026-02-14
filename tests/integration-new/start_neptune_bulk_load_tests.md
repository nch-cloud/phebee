# Integration Test Plan: start_neptune_bulk_load.py

## Lambda Metadata

**Purpose**: Start Neptune bulk load operations for TTL files organized by named graphs (subjects and projects).

**Dependencies**:
- S3: Discover TTL directories
- Neptune: start_load API
- Environment: AWS_REGION, NEPTUNE_LOAD_ROLE_ARN

**Key Operations**:
- Discovers TTL directories in S3 (subjects/, projects/*)
- Starts Neptune bulk load for each directory
- Configures named graphs for subjects and per-project
- Queues loads with HIGH parallelism
- Returns list of load IDs

---

## Integration Test Scenarios

### Test 1: test_start_load_subjects_graph (Subjects TTL loaded into subjects named graph)
### Test 2: test_start_load_project_graphs (Each project directory loaded into project-specific graph)
### Test 3: test_start_load_multiple_projects (3 projects, 3+1 loads started)
### Test 4: test_start_load_named_graph_uris (Subjects: /subjects, Projects: /projects/{project_id})
### Test 5: test_start_load_format_turtle (format: turtle in load params)
### Test 6: test_start_load_parallelism_high (parallelism: HIGH for fast loading)
### Test 7: test_start_load_fail_on_error_false (failOnError: FALSE, partial load succeeds)
### Test 8: test_start_load_queue_request (queueRequest: TRUE, loads queued)
### Test 9: test_start_load_iam_role (iamRoleArn from environment used)
### Test 10: test_start_load_missing_ttl_directory (No TTL files, directory skipped, no load started)
### Test 11: test_start_load_empty_project_directory (Project dir with no .ttl files, skipped)
### Test 12: test_start_load_response_structure (Returns run_id, loads array with load_ids, total_loads)
### Test 13: test_start_load_all_type (load_type: all, starts both subjects and projects)
### Test 14: test_start_load_subjects_only (load_type: subjects, only subjects loaded)
### Test 15: test_start_load_projects_only (load_type: projects, only project graphs loaded)
### Test 16: test_start_load_concurrent_runs (Start loads for 2 runs concurrently, isolated)
### Test 17: test_start_load_integration_with_generate_ttl (Loads TTL files from generate_ttl output)
### Test 18: test_start_load_missing_role_arn (NEPTUNE_LOAD_ROLE_ARN not set, error)
### Test 19: test_start_load_status_in_response (Each load has status: LOAD_IN_PROGRESS)
### Test 20: test_start_load_duplicate_project_dirs (Duplicate handling, loads correct directories)
