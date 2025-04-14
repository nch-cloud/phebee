import pytest
from update_source_utils import check_dynamodb_record
from phebee.utils.aws import get_client

@pytest.mark.integration
def test_update_mondo(cloudformation_stack, update_mondo):
    test_start_time = update_mondo
    check_dynamodb_record("mondo", test_start_time, dynamodb=get_client("dynamodb"))
