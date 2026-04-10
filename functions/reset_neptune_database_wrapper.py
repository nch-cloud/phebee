"""
Reset Neptune Database Lambda Wrapper

Resets the entire Neptune database (all graphs) using Neptune's fast reset API.
This is a wrapper around the existing reset_neptune_database() utility.
"""

import os
import logging
from phebee.utils.neptune import reset_neptune_database

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Reset Neptune database (all graphs).

    This function initiates a full database reset using Neptune's native reset API,
    which is significantly faster than SPARQL-based triple deletion (~5-10 min vs hours).

    Input:
        {} (no parameters required)

    Output:
        {
            "status": "success",
            "message": "Database reset complete"
        }

    The underlying reset_neptune_database() utility handles:
    - Two-phase reset (initiateDatabaseReset + performDatabaseReset with token)
    - Cluster availability waiting (exponential backoff, 20 max retries)
    - SPARQL endpoint availability waiting (12 max retries)
    """
    try:
        logger.info("Initiating Neptune database reset...")
        logger.info("This will delete ALL graphs in the Neptune database")

        # Call existing utility - handles the entire reset process
        reset_neptune_database()

        logger.info("Neptune database reset complete")
        logger.info("All graphs have been cleared and cluster is available")

        return {
            "status": "success",
            "message": "Database reset complete"
        }

    except Exception as e:
        logger.error(f"Failed to reset Neptune database: {str(e)}", exc_info=True)
        raise
