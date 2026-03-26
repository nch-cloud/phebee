"""
Clear Neptune Graph Lambda

Clears the subjects graph in Neptune using SPARQL CLEAR GRAPH command.
"""

import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Clear a named graph in Neptune.

    Input:
        {
            "graphUri": "http://ods.nationwidechildrens.org/phebee/subjects"
        }

    Output:
        {
            "statusCode": 200,
            "graphUri": "...",
            "status": "CLEARED"
        }
    """
    try:
        # Import phebee utilities (from Lambda layer)
        from phebee.utils import sparql

        graph_uri = event['graphUri']
        logger.info(f"Clearing Neptune graph: {graph_uri}")

        # Execute CLEAR GRAPH command
        query = f"CLEAR GRAPH <{graph_uri}>"
        sparql.execute_update(query)

        logger.info(f"Successfully cleared graph: {graph_uri}")

        return {
            "statusCode": 200,
            "graphUri": graph_uri,
            "status": "CLEARED"
        }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error clearing Neptune graph: {str(e)}", exc_info=True)
        # Check if it's a timeout error (large graph)
        if "timeout" in str(e).lower():
            logger.error("Graph clear timed out. This may indicate a very large graph.")
            logger.error("Consider using manual Neptune database reset if this persists.")
        raise
