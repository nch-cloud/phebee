""" Parse a zip file containing phenopackets or subfolders with phenopackets.
"""

import os
import json
import zipfile
import io
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.aws import get_client, parse_s3_path, store_json_in_s3

s3_client = get_client("s3")

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    # TODO this assumes the project already exists - add checks or create if needed
    project_id = event.get("project_id")
    s3_path = event["s3_path"]
    delete_collection = event.get("delete_collection", False)
    json_objects = list(parse_zip_file(s3_path, delete=delete_collection))

    # Add the project id to each json object, since the ItemReader in the Map state doesn't pass on variables.
    for obj in json_objects:
        obj["project_id"] = project_id

    if "output_s3_path" in event:
        output_s3_path = event["output_s3_path"]
    else:
        # Generate a filename with timestamp and unique 8-character UUID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"{timestamp}_{unique_id}"
        bucket = os.environ["PheBeeBucketName"]
        output_s3_path = f"s3://{bucket}/step_function_data/{filename}.zip"

    # Store the json objects in s3
    s3_path, bucket, key = store_json_in_s3(json_objects, output_s3_path)

    # Return the number of phenopackets, the bucket and key of the flattened json list
    # The map reader requires bucket and key instead of an S3 path.
    return {
        "statusCode": 200,
        "project_id": project_id,
        "number_phenopackets": len(json_objects),
        "bucket": bucket,
        "key": key,
    }


def parse_zip_file(s3_path, delete=False):
    print("Parsing zip file: {}".format(s3_path))
    bucket_name, key = parse_s3_path(s3_path)

    try:
        # Get the zip file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        zip_content = response["Body"].read()

        # Create a file-like object from the zip content
        with io.BytesIO(zip_content) as bytes_io:
            with zipfile.ZipFile(bytes_io) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith(".json"):
                        # This is a JSON file, yield its content
                        with zip_file.open(file_name) as json_file:
                            json_content = json_file.read().decode("utf-8")
                            try:
                                json_object = json.loads(json_content)
                                yield json_object
                            except json.JSONDecodeError:
                                print(f"Error decoding JSON in file: {file_name}")
                    elif file_name.endswith("/"):
                        # This is a directory (cohort), skip it
                        continue
                    else:
                        # This is a non-JSON file, skip it
                        print(f"Skipping non-JSON file: {file_name}")

        if delete:
            # Delete the S3 object after successful processing
            s3_client.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted S3 object: s3://{bucket_name}/{key}")

    except ClientError as e:
        print(f"Error accessing S3 object: {e}")
        raise
