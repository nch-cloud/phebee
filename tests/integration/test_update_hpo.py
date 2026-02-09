import pytest
from update_source_utils import check_dynamodb_record, check_ontology_cache_populated
from phebee.utils.aws import get_client
from phebee.utils.dynamodb import get_current_term_source_version
from phebee.utils.dynamodb_cache import get_term_ancestor_paths, get_term_label

@pytest.mark.integration
def test_update_hpo(cloudformation_stack, update_hpo):
    test_start_time = update_hpo

    # Check that DynamoDB metadata was created
    dynamodb = get_client("dynamodb")
    check_dynamodb_record("hpo", test_start_time, dynamodb=dynamodb)

    # Get the version that was just installed
    hpo_version = get_current_term_source_version("hpo", dynamodb=dynamodb)
    assert hpo_version is not None, "Failed to get installed HPO version"

    # Verify that the cache table was populated with ontology hierarchy
    check_ontology_cache_populated(cloudformation_stack, "hpo", hpo_version)


@pytest.mark.integration
def test_cache_query_functions(cloudformation_stack, update_hpo):
    """
    Test that cache query functions work correctly with populated cache.

    Tests get_term_ancestor_paths() and get_term_label() using known HPO terms.
    """
    import boto3
    from cloudformation_utils import get_output_value_from_stack

    # Get the installed HPO version
    dynamodb = get_client("dynamodb")
    hpo_version = get_current_term_source_version("hpo", dynamodb=dynamodb)
    assert hpo_version is not None, "Failed to get installed HPO version"

    # Get cache table for testing
    cache_table_name = get_output_value_from_stack(cloudformation_stack, "DynamoDBCacheTableName")
    cache_table = boto3.resource("dynamodb").Table(cache_table_name)

    print(f"\nTesting cache query functions with HPO version {hpo_version}")

    # Test 1: Root term (HP_0000001 - "All")
    print("\n  Test 1: Root term (HP_0000001)")
    root_paths = get_term_ancestor_paths("hpo", hpo_version, "HP_0000001", table=cache_table)
    print(f"    Found {len(root_paths)} path(s)")

    assert len(root_paths) == 1, f"Root term should have exactly 1 path, found {len(root_paths)}"
    assert root_paths[0] == ["HP_0000001"], f"Root path should be ['HP_0000001'], found {root_paths[0]}"

    root_label = get_term_label("hpo", hpo_version, "HP_0000001", table=cache_table)
    print(f"    Label: {root_label}")
    assert root_label == "All", f"Root label should be 'All', found '{root_label}'"
    print("    ✓ Root term passed")

    # Test 2: Single inheritance term (HP_0000118 - "Phenotypic abnormality")
    print("\n  Test 2: Single inheritance (HP_0000118)")
    pheno_paths = get_term_ancestor_paths("hpo", hpo_version, "HP_0000118", table=cache_table)
    print(f"    Found {len(pheno_paths)} path(s)")

    assert len(pheno_paths) >= 1, f"HP_0000118 should have at least 1 path, found {len(pheno_paths)}"

    # Verify at least one path goes from root to this term
    expected_path = ["HP_0000001", "HP_0000118"]
    assert expected_path in pheno_paths, f"Expected path {expected_path} not found in {pheno_paths}"

    pheno_label = get_term_label("hpo", hpo_version, "HP_0000118", table=cache_table)
    print(f"    Label: {pheno_label}")
    assert pheno_label == "Phenotypic abnormality", \
        f"HP_0000118 label should be 'Phenotypic abnormality', found '{pheno_label}'"
    print("    ✓ Single inheritance passed")

    # Test 3: Check for multiple inheritance term (HP_0001627 - may not exist in test version)
    print("\n  Test 3: Checking for multiple inheritance (HP_0001627)")
    cardiac_paths = get_term_ancestor_paths("hpo", hpo_version, "HP_0001627", table=cache_table)

    if len(cardiac_paths) > 0:
        print(f"    Found {len(cardiac_paths)} path(s)")

        # Verify all paths end with the term itself
        for path in cardiac_paths:
            assert path[-1] == "HP_0001627", f"Path should end with HP_0001627, found {path}"
            assert path[0] == "HP_0000001", f"Path should start with root HP_0000001, found {path}"

        cardiac_label = get_term_label("hpo", hpo_version, "HP_0001627", table=cache_table)
        print(f"    Label: {cardiac_label}")
        assert cardiac_label == "Abnormal heart morphology", \
            f"HP_0001627 label should be 'Abnormal heart morphology', found '{cardiac_label}'"

        if len(cardiac_paths) > 1:
            print(f"    ✓ Multiple inheritance detected ({len(cardiac_paths)} paths)")
        else:
            print("    ✓ Single path found (multiple inheritance not present in this term)")
    else:
        print("    HP_0001627 not found in this HPO version - skipping")

    # Test 4: Non-existent term
    print("\n  Test 4: Non-existent term (HP_9999999)")
    nonexistent_paths = get_term_ancestor_paths("hpo", hpo_version, "HP_9999999", table=cache_table)
    nonexistent_label = get_term_label("hpo", hpo_version, "HP_9999999", table=cache_table)

    assert nonexistent_paths == [], f"Non-existent term should return empty list, found {nonexistent_paths}"
    assert nonexistent_label is None, f"Non-existent term should return None label, found '{nonexistent_label}'"
    print("    ✓ Non-existent term handled correctly")

    print("\n  ✓ All cache query function tests passed")
