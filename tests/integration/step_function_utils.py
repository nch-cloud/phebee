import time
import json
from botocore.exceptions import ClientError
from phebee.utils.aws import get_client


def start_step_function(step_function_arn, step_function_input=None):
    """
    Start the execution of a Step Function.
    :param step_function_arn: ARN of the Step Function to be executed.
    :return: The ARN of the execution.
    """
    client = get_client("stepfunctions")
    params = {"stateMachineArn": step_function_arn}

    if step_function_input is not None:
        if not isinstance(step_function_input, str):
            params["input"] = json.dumps(step_function_input)
        else:
            params["input"] = step_function_input
    try:
        response = client.start_execution(**params)
        return response["executionArn"]
    except ClientError as e:
        raise RuntimeError(f"Failed to start step function execution: {e}")


def wait_for_step_function_completion(
    execution_arn, timeout=600, poll_interval=10, return_response=False
):
    """
    Poll the Step Function execution until it completes or times out.
    :param execution_arn: The ARN of the Step Function execution.
    :param timeout: How long to wait for the Step Function to complete.
    :param poll_interval: How often to check the status of the Step Function.
    :return: The final status of the Step Function execution.
    """
    client = get_client("stepfunctions")
    elapsed_time = 0
    while elapsed_time < timeout:
        response = client.describe_execution(executionArn=execution_arn)
        status = response["status"]
        if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            if return_response:
                return response
            else:
                return status
        time.sleep(poll_interval)
        elapsed_time += poll_interval
    return "TIMED_OUT"
