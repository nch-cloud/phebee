import os
import boto3
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.dynamodb import get_source_records
from phebee.utils.aws import get_current_timestamp

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    source = event.get("source")
    version = event.get("version")

    table = boto3.resource("dynamodb", "us-east-2").Table(
        os.environ["PheBeeDynamoTable"]
    )

    install_timestamp = get_current_timestamp()

    records = get_source_records(source)

    logger.info(records)
    sk = [record["SK"]["S"] for record in records if record["Version"]["S"] == version][
        -1
    ]

    update_response = table.update_item(
        Key={"PK": f"SOURCE~{source}", "SK": sk},
        UpdateExpression="SET InstallTimestamp = :val",
        ExpressionAttributeValues={":val": install_timestamp},
        ReturnValues="UPDATED_NEW",
    )

    return update_response
