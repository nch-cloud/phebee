import boto3
import json
import os
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.aws import deserialize_dynamodb_json_item

logger = Logger()
tracer = Tracer()
metrics = Metrics()

dynamodb = boto3.client("dynamodb")


def lambda_handler(event, context):
    source_name = event["pathParameters"]["source_name"]

    # Define the parameters for the query to find the newest record for the given source
    params = {
        "TableName": os.environ["PheBeeDynamoTable"],
        "KeyConditionExpression": "PK = :pk_value",
        "ExpressionAttributeValues": {":pk_value": {"S": f"SOURCE~{source_name}"}},
        "ScanIndexForward": False,  # Set to False to sort in descending order
        "Limit": 1,  # Limit to 1 to get the newest item
    }

    # Execute the query
    response = dynamodb.query(**params)

    logger.info(response)

    # Check if there are any items in the response
    if "Items" in response and len(response["Items"]) > 0:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(deserialize_dynamodb_json_item(response["Items"][0])),
        }
    else:
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"No source found for name {source_name}"}),
        }
