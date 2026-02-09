from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.neptune import initiate_neptune_reset, wait_for_neptune_ready
from phebee.utils.dynamodb import reset_dynamodb_table
from phebee.utils.dynamodb_cache import reset_dynamodb_cache_table

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    """
    Reset all PheBee databases for testing.

    Execution order is optimized for safety - Neptune reset is initiated first,
    then DynamoDB tables are cleared while Neptune restarts in the background.
    Finally, we wait for Neptune to become available.

    This ensures that even if the Lambda times out during the Neptune wait,
    all data has been cleared successfully.
    """
    logger.info(event)

    try:
        # Step 1: Initiate Neptune reset (fast - just triggers the reset)
        logger.info("Step 1/4: Initiating Neptune database reset...")
        initiate_neptune_reset()
        logger.info("Neptune reset initiated (cluster restarting in background)")

        # Step 2: Clear main DynamoDB table (fast - runs while Neptune restarts)
        logger.info("Step 2/4: Clearing main DynamoDB table...")
        reset_dynamodb_table()
        logger.info("Main DynamoDB table cleared")

        # Step 3: Clear cache DynamoDB table (fast - runs while Neptune restarts)
        logger.info("Step 3/4: Clearing cache DynamoDB table...")
        reset_dynamodb_cache_table()
        logger.info("Cache DynamoDB table cleared")

        # Step 4: Wait for Neptune to become available (slow - typically 5-7 min)
        logger.info("Step 4/4: Waiting for Neptune to become available...")
        wait_for_neptune_ready()
        logger.info("Neptune is ready")

        logger.info("Database reset completed successfully")
        return {"statusCode": 200, "success": True}
    except Exception as e:
        logger.error(e)

        return {"statusCode": 500, "success": False, "message": str(e)}
