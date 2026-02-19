import pytest
from datetime import datetime
from phebee.utils.aws import get_current_timestamp
from phebee.utils.dynamodb import get_source_records, get_current_term_source_version
from general_utils import check_timestamp_in_test

from step_function_utils import start_step_function, wait_for_step_function_completion
from cloudformation_utils import get_output_value_from_stack
from s3_utils import check_s3_file_exists


def update_source(cloudformation_stack, source_name, sfn_output_key, test=True, timeout=600):
    """
    Utility function to trigger the update step function and return the test_start_time.

    Args:
        cloudformation_stack: Stack name
        source_name: Source identifier (e.g., 'hpo', 'mondo')
        sfn_output_key: CloudFormation output key for Step Function ARN
        test: Whether to run in test mode
        timeout: Timeout in seconds for waiting for Step Function completion (default: 600)
    """

    # Get the ARN of the Step Function (assuming it is stored in the stack's output)
    step_function_arn = get_output_value_from_stack(
        cloudformation_stack, sfn_output_key
    )

    # Capture the start time of the test
    test_start_time = get_current_timestamp()

    # Start the execution of the Step Function (no input needed)
    try:
        execution_arn = start_step_function(step_function_arn, {"test": test})
        print(f"Step function execution started: {execution_arn}")
    except RuntimeError as e:
        pytest.fail(str(e))

    # Wait for the Step Function to complete
    execution_status = wait_for_step_function_completion(execution_arn, timeout=timeout)

    # Verify the Step Function execution result
    if execution_status != "SUCCEEDED":
        pytest.fail(f"Step function execution failed with status: {execution_status}")

    print("Step function executed successfully.")

    return test_start_time

def extract_timestamp(record):
        timestamp_str = record.get("InstallTimestamp", {}).get("S")
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str)
            except ValueError:
                pass
        return datetime.min

def check_dynamodb_record(source_name, test_start_time, dynamodb=None):
    source_records = get_source_records(source_name, dynamodb=dynamodb)
    
    # Check that a record was created during our unit test
    newest_record = max(
        source_records,
        key=extract_timestamp,
        default=None  # in case source_records is empty
    )

    print("newest_record")
    print(newest_record)

    assert check_timestamp_in_test(newest_record["SK"]["S"], test_start_time)
    assert check_timestamp_in_test(
        newest_record["CreationTimestamp"]["S"], test_start_time
    )
    assert check_timestamp_in_test(
        newest_record["InstallTimestamp"]["S"], test_start_time
    )
    assert newest_record.get("Assets") is not None
    assert newest_record.get("Version") is not None
    assert (
        newest_record["GraphName"]["S"]
        == f'{source_name}~{newest_record["Version"]["S"]}'
    )

    current_source_version = get_current_term_source_version(source_name, dynamodb)
    assert current_source_version is not None

    # Check that all assets for the source exist
    asset_paths = [a["M"]["asset_path"]["S"] for a in newest_record["Assets"]["L"]]
    for asset_path in asset_paths:
        assert check_s3_file_exists(asset_path)
