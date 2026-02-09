import pytest
from update_source_utils import check_dynamodb_record, check_ontology_cache_populated
from phebee.utils.aws import get_client
from phebee.utils.dynamodb import get_current_term_source_version

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
