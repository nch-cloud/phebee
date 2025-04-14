import os
import boto3


def get_source_records(source_name: str, dynamodb=None):
    dynamo_table_name = os.environ["PheBeeDynamoTable"]

    if not dynamodb:
        dynamodb = boto3.client("dynamodb")

    query_args = {
        "TableName": dynamo_table_name,
        "KeyConditionExpression": "PK = :pk_value",
        "ExpressionAttributeValues": {":pk_value": {"S": f"SOURCE~{source_name}"}},
    }

    dynamo_response = dynamodb.query(**query_args)

    return dynamo_response["Items"]


def get_current_term_source_version(source_name: str, dynamodb=None):
    source_records = get_source_records(source_name, dynamodb)

    # Each record has a timestamp from when it was installed.  Sort in ascending order.
    sorted_records = sorted(
        source_records, key=lambda x: x["InstallTimestamp"]["S"], reverse=True
    )

    # The first record is the newest install of this source.  Get its version and return it.
    if len(sorted_records) > 0:
        newest_record_version = (
            sorted_records[0]["Version"]["S"] if sorted_records else None
        )
    else:
        return None

    return newest_record_version


def reset_dynamodb_table():

    table_name = os.environ["PheBeeDynamoTable"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            key = {
                k["AttributeName"]: item[k["AttributeName"]] for k in table.key_schema
            }
            batch.delete_item(Key=key)
