import datetime
import zipfile
import json
import os
import boto3
import requests
import tempfile

from urllib.parse import urlparse, quote
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError


def escape_string(s):
    return quote(s, safe="")


def get_current_timestamp():
    return (
        datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None).isoformat()
    )

def extract_body(event):
    """Safely extracts the body from a Lambda event, supporting API Gateway, direct, and internal invocation."""
    if "body" in event:
        if isinstance(event["body"], str):
            return json.loads(event["body"])
        else:
            return event["body"]
    return event

def deserialize_dynamodb_json_list(items):
    deserialized_data = []
    for item in items:
        deserialized_data.append(deserialize_dynamodb_json_item(item))

    return deserialized_data


def deserialize_dynamodb_json_item(item):
    # Use boto3's own DynamoDb deserialization tools to get prettier JSON
    td = TypeDeserializer()

    deserialized_item = {}
    for key in item.keys():
        deserialized_item[key] = td.deserialize(item[key])

    return deserialized_item


def exponential_backoff(attempt, initial_delay, max_delay):
    """Calculate sleep duration with exponential backoff, capped at max_delay."""
    return min(initial_delay * (2**attempt), max_delay)


def parse_s3_path(s3_url):
    """
    Parse a fully qualified S3 path (e.g., 's3://my-bucket/path/to/object') into bucket and key.

    :param s3_url: S3 path (e.g., 's3://my-bucket/path/to/object')
    :return: Tuple containing (bucket, key)
    :raises ValueError: If the S3 URL is not valid or does not follow the expected format.
    """
    try:
        parsed_url = urlparse(s3_url)

        # Ensure the scheme is 's3'
        if parsed_url.scheme != "s3":
            raise ValueError(
                f"Invalid URL scheme '{parsed_url.scheme}', expected 's3://'"
            )

        # Ensure the bucket is present (parsed_url.netloc should be the bucket)
        bucket = parsed_url.netloc
        if not bucket:
            raise ValueError(f"Missing bucket in S3 URL: '{s3_url}'")

        # The key is the path part of the URL, stripped of leading '/'
        key = parsed_url.path.lstrip("/")
        if not key:
            raise ValueError(f"Missing key in S3 URL: '{s3_url}'")

        return bucket, key

    except Exception as e:
        raise ValueError(f"Error parsing S3 URL '{s3_url}': {e}")


def store_json_in_s3(json_list, s3_path):
    """
    Store a list of JSON objects in S3 and return the S3 path.

    :param json_list: List of JSON objects to store
    :param s3_path: Fully qualified S3 path to use
    :return: S3 path, bucket and key of the stored object
    """

    s3_client = get_client("s3")
    bucket, key = parse_s3_path(s3_path)

    try:
        # Convert the list to a JSON string
        json_data = json.dumps(json_list, indent=2)

        # Upload the JSON string to S3
        s3_client.put_object(Bucket=bucket, Key=key, Body=json_data)

        # Return all three objects since some functions can only work with bucket and key
        return f"s3://{bucket}/{key}", bucket, key
    except ClientError as e:
        print(f"Error storing data in S3: {e}")
        raise


def read_json_and_s3(input_data, delete_s3=False):
    """
    Process input data dictionary. If it contains an 's3_path' key, retrieve the object from S3,
    parse it as JSON, and append it to the input_data.
    :param input_data: A dictionary that may contain an 's3_path' key
    :param delete_s3: Whether to delete the S3 object after reading (default: False)
    :return: The processed input data with S3 contents appended
    """
    if not isinstance(input_data, dict):
        raise ValueError("input_data must be a dictionary")

    if "s3_path" in input_data:
        s3_client = get_client("s3")
        s3_path = input_data["s3_path"]
        if isinstance(s3_path, str) and s3_path.startswith("s3://"):

            bucket_name, key = s3_path[5:].split("/", 1)
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                s3_json_data = json.loads(response["Body"].read().decode("utf-8"))

                # TODO deleting here will not allow redo if import fails later
                if delete_s3:
                    s3_client.delete_object(Bucket=bucket_name, Key=key)
                    print(f"Deleted S3 object: {s3_path}")

                # Append S3 contents to input_data
                input_data.update(s3_json_data)

                # Remove the s3_path key from input_data
                del input_data["s3_path"]

            except ClientError as e:
                print(f"Error retrieving data from S3: {e}")
                raise

    return input_data


# TODO Replace part of the phenopacket import code with this? Temp dir might make it complicated.
def download_and_extract_zip(source_path):
    """
    Extract JSON files from a ZIP file located on S3, a URL, or local storage into a dictionary.
    :param source_path: Path to the ZIP file (S3 URL, HTTP URL, or local path).
    :return: Dictionary of {subject_id: JSON data}.
    """
    subject_list = []
    s3_client = get_client("s3")
    with tempfile.TemporaryDirectory() as temp_dir:
        if source_path.startswith("s3://"):
            # Handle S3
            # s3_client = boto3.client("s3")
            bucket_name, zip_key = parse_s3_path(source_path)
            zip_path = os.path.join(temp_dir, "downloaded.zip")

            # Download the ZIP file from S3
            s3_client.download_file(bucket_name, zip_key, zip_path)

        elif source_path.startswith("http://") or source_path.startswith("https://"):
            # Handle HTTP/HTTPS URLs
            zip_path = os.path.join(temp_dir, "downloaded.zip")
            response = requests.get(source_path)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to download ZIP file from URL: {source_path}"
                )
            with open(zip_path, "wb") as f:
                f.write(response.content)

        else:
            # Handle local file path
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Local file '{source_path}' does not exist.")
            zip_path = source_path

        # Extract the ZIP file and load JSON files into a dictionary
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

            # Iterate over the extracted JSON files
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith(".json"):
                        with open(os.path.join(root, file), "r") as f:
                            subject_list.append(json.load(f))

    return subject_list


""" Helper functions for local development.
"""

# Global dictionary to store initialized clients by client name.
# This is especially useful when using an AWS profile, e.g. running integration test suite, so we don't have to pass it everywhere.
_clients = {}

def get_client(client_name, profile=None, config=None):
    """
    Returns a boto3 client. Uses an authenticated session when running locally,
    and the default client when running on AWS.
    :param client_name: The name of the boto3 client to create (e.g., "s3", "cloudformation")
    :return: The initialized Boto3 client for the specified service.
    """
    # TODO Consider adding args, kwargs that are passed on to boto3.client().

    global _clients

    # Check if the client is already initialized
    if client_name not in _clients:
        # Check if we're running on AWS Lambda
        if "AWS_EXECUTION_ENV" in os.environ:
            # We're on AWS, use the default client
            _clients[client_name] = boto3.client(client_name, config=config)
        else:
            # Running locally, use an authenticated session
            if profile is None:
                profile = os.environ.get("AWS_PROFILE")
            session = boto3.Session(profile_name=profile)
            _clients[client_name] = session.client(client_name, config=config)

    return _clients[client_name]


def get_aws_session_and_resources(add_to_env=True):
    """
    Get the AWS session and resources.
    :param add_to_env: Whether to add the resources to the environment variables (default: True)
    :return: The AWS session and resources
    """
    profile_name = os.environ["AWS_PROFILE"]
    stack_name = os.environ["AWS_STACK_NAME"]

    # Use the credentials from authenticated CLI session
    if profile_name is None:
        print(f"Available profiles: {boto3.Session().available_profiles}")
        session = boto3.Session()
    else:
        session = boto3.Session(profile_name=profile_name)

    resources = get_physical_resources(session, stack_name)
    if resources:
        for logical_id, details in resources.items():
            print(f"Logical ID: {logical_id}")
            print(f"  Physical ID: {details['PhysicalResourceId']}")
            print(f"  Resource Type: {details['ResourceType']}")
            print()
    else:
        print("Failed to retrieve stack resources.")

    # Add the resources to environment variables
    if add_to_env:
        for logical_id, details in resources.items():
            os.environ[logical_id] = details["PhysicalResourceId"]

    return session, resources


def get_physical_resources(session, stack_name):
    cf_client = session.client("cloudformation")
    resource_mapping = {}

    try:
        next_token = None  # Initialize next_token

        # Loop through all pages of results
        while True:
            if next_token:
                # If next_token is present, fetch the next page of results
                response = cf_client.list_stack_resources(
                    StackName=stack_name, NextToken=next_token
                )
            else:
                # Fetch the first page of results
                response = cf_client.list_stack_resources(StackName=stack_name)

            # Process the current page of results
            for resource in response["StackResourceSummaries"]:
                logical_id = resource["LogicalResourceId"]
                physical_id = resource["PhysicalResourceId"]
                resource_type = resource["ResourceType"]

                resource_mapping[logical_id] = {
                    "PhysicalResourceId": physical_id,
                    "ResourceType": resource_type,
                }

            # Check if more pages are available (NextToken exists)
            next_token = response.get("NextToken")
            if not next_token:
                break  # No more pages to retrieve, exit the loop

        return resource_mapping

    except cf_client.exceptions.ClientError as e:
        print(f"Error retrieving stack resources: {e}")
        return None
