from phebee.utils.aws import get_current_timestamp, get_client
import json
from datetime import datetime


def invoke_lambda(name, payload):
    """Helper function to invoke a Lambda function."""
    lambda_client = get_client("lambda")
    response = lambda_client.invoke(
        FunctionName=name,
        Payload=json.dumps(payload).encode("utf-8"),
        InvocationType="RequestResponse",
    )
    return json.loads(response["Payload"].read().decode("utf-8"))


def check_timestamp_in_test(timestamp, test_start_time):
    return timestamp and timestamp > test_start_time and timestamp < get_current_timestamp()

def parse_lambda_response(response):
    """
    Parses a boto3 Lambda invoke response structured for API Gateway proxy integration.
    
    Returns:
        status_code (int): The HTTP statusCode from the Lambda's return value.
        body (dict): The parsed JSON body from the Lambda's return value.
    """
    raw_payload = response["Payload"].read()
    lambda_response = json.loads(raw_payload)

    status_code = lambda_response.get("statusCode", 500)  # default to 500 if missing
    body_str = lambda_response.get("body", "{}")
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError:
        body = {"raw_body": body_str}

    return status_code, body

def parse_iso8601(s):
    # Accepts either with or without milliseconds
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Fallback if needed
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")