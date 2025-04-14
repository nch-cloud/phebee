from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.neptune import reset_neptune_database
from phebee.utils.dynamodb import reset_dynamodb_table

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    try:
        reset_dynamodb_table()
        reset_neptune_database()
        return {"statusCode": 200, "success": True}
    except Exception as e:
        logger.error(e)

        return {"statusCode": 500, "success": False, "message": str(e)}
