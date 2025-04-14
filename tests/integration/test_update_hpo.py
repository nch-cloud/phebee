import pytest
from update_source_utils import check_dynamodb_record
from phebee.utils.aws import get_client

@pytest.mark.integration
def test_update_hpo(cloudformation_stack, update_hpo):
    test_start_time = update_hpo
    check_dynamodb_record("hpo", test_start_time, dynamodb=get_client("dynamodb"))
