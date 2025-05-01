import pytest
from botocore.config import Config
from cloudformation_utils import get_output_value_from_stack
from phebee.utils.dynamodb import get_source_records
from phebee.utils.aws import get_client


def reset_database(cloudformation_stack):
    config = Config(read_timeout=900, connect_timeout=900, retries={"max_attempts": 0})
    lambda_client = get_client("lambda", config=config)

    reset_database_lambda_arn = get_output_value_from_stack(
        cloudformation_stack, "ResetDatabaseFunctionArn"
    )

    response = lambda_client.invoke(FunctionName=reset_database_lambda_arn)

    return response


@pytest.mark.run_last
@pytest.mark.integration
def test_reset_database(request, cloudformation_stack):
    """
    Integration test to reset the Neptune and DynamoDB databases at the end of all tests.

    This test should only run at the very end, after all other tests have completed.
    """
    if request.config.getoption("--force-database-reset"):
        print("All tests finished, running test_reset_database...")
        response = reset_database(cloudformation_stack)
        assert response["StatusCode"] == 200

        # TODO Add queries to check that both DynamoDB and Neptune databases are empty
        assert len(get_source_records("hpo", get_client("dynamodb"))) == 0

        print("Database reset completed.")
    else:
        pytest.skip("Skipping test because --force-database-reset is not set.")
