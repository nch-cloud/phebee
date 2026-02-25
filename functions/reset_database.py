from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.neptune import reset_neptune_database
from phebee.utils.dynamodb import reset_dynamodb_table
from phebee.utils.iceberg import reset_iceberg_tables

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    try:
        logger.info("Step 1/3: Resetting DynamoDB table...")
        reset_dynamodb_table()
        logger.info("Step 1/3: DynamoDB reset complete")

        logger.info("Step 2/3: Resetting Neptune database...")
        reset_neptune_database()
        logger.info("Step 2/3: Neptune reset complete")

        logger.info("Step 3/3: Resetting Iceberg tables...")
        reset_iceberg_tables()
        logger.info("Step 3/3: Iceberg reset complete")

        logger.info("All resets completed successfully")
        return {"statusCode": 200, "success": True}
    except Exception as e:
        logger.error(e)

        return {"statusCode": 500, "success": False, "message": str(e)}
