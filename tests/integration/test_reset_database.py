import pytest
import boto3
from botocore.config import Config
from cloudformation_utils import get_output_value_from_stack
from phebee.utils.dynamodb import get_source_records
from phebee.utils.aws import get_client


def reset_database(cloudformation_stack):
    lambda_client = get_client("lambda")

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

        # Verify main DynamoDB table is empty
        main_table_records = get_source_records("hpo", get_client("dynamodb"))
        assert len(main_table_records) == 0, f"Main DynamoDB table should be empty, found {len(main_table_records)} HPO records"
        print("✓ Main DynamoDB table cleared")

        # Verify cache DynamoDB table is empty
        cache_table_name = get_output_value_from_stack(cloudformation_stack, "DynamoDBCacheTableName")
        dynamodb_resource = boto3.resource("dynamodb")
        cache_table = dynamodb_resource.Table(cache_table_name)
        cache_response = cache_table.scan(Limit=10)
        cache_items = cache_response.get("Items", [])
        assert len(cache_items) == 0, f"Cache DynamoDB table should be empty, found {len(cache_items)} items"
        print("✓ Cache DynamoDB table cleared")

        # TODO: Add Neptune verification query
        print("Database reset completed.")
    else:
        pytest.skip("Skipping test because --force-database-reset is not set.")
