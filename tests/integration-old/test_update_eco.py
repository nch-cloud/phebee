import pytest
from update_source_utils import check_dynamodb_record
from phebee.utils.aws import get_client

@pytest.mark.integration
def test_update_eco(cloudformation_stack, update_eco):
    test_start_time = update_eco
    check_dynamodb_record("eco", test_start_time, dynamodb=get_client("dynamodb"))
