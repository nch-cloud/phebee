import pytest


@pytest.mark.integration
def test_cloudformation_stack(aws_session, cloudformation_stack):
    """
    This function checks the current status of a CloudFormation stack.

    :param cloudformation_stack: The name of the CloudFormation stack to check.
    """
    cf_client = aws_session.client("cloudformation")

    response = cf_client.describe_stacks(StackName=cloudformation_stack)
    # Get the current stack status
    stack_status = response["Stacks"][0]["StackStatus"]
    assert stack_status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]
