"""
Clear Neptune Graph Lambda

Clears the subjects graph in Neptune by deleting one batch of triples.
Designed to be called repeatedly by Step Functions until graph is empty.
"""

import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Delete one batch of triples from Neptune graph.
    Returns status indicating if more batches remain.

    Input:
        {
            "graphUri": "http://ods.nationwidechildrens.org/phebee/subjects",
            "batchSize": 50000,  // optional, default 50000
            "totalDeleted": 0,    // optional, cumulative count
            "iteration": 0        // optional, iteration counter
        }

    Output:
        {
            "graphUri": "...",
            "status": "IN_PROGRESS" | "COMPLETE",
            "remaining": 123456,
            "totalDeleted": 50000,
            "iteration": 1
        }
    """
    try:
        # Import phebee utilities (from Lambda layer)
        from phebee.utils import sparql

        graph_uri = event['graphUri']
        batch_size = event.get('batchSize', 500000)  # Larger batches for ~5 min execution
        total_deleted = event.get('totalDeleted', 0)
        iteration = event.get('iteration', 0)

        iteration += 1

        logger.info(f"Clearing Neptune graph: {graph_uri}")
        logger.info(f"Iteration {iteration}, batch size: {batch_size:,}, total deleted so far: {total_deleted:,}")

        # Delete a batch of triples
        query = f"""
        DELETE WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }} LIMIT {batch_size}
        """

        logger.info(f"Deleting batch of up to {batch_size:,} triples")
        sparql.execute_update(query)

        # Check remaining count
        count_query = f"""
        SELECT (COUNT(*) AS ?count)
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }} LIMIT 1
        """

        result = sparql.execute_query(count_query)
        remaining = 0

        if result and 'results' in result and 'bindings' in result['results']:
            bindings = result['results']['bindings']
            if bindings and 'count' in bindings[0]:
                remaining = int(bindings[0]['count']['value'])

        logger.info(f"Remaining triples: {remaining:,}")

        if remaining == 0:
            logger.info(f"Graph cleared successfully!")
            logger.info(f"Total deleted: ~{total_deleted + batch_size:,} (in {iteration} iterations)")

            return {
                "graphUri": graph_uri,
                "status": "COMPLETE",
                "remaining": 0,
                "totalDeleted": total_deleted + batch_size,
                "iteration": iteration
            }
        else:
            logger.info(f"More triples remain, will continue...")

            return {
                "graphUri": graph_uri,
                "status": "IN_PROGRESS",
                "remaining": remaining,
                "totalDeleted": total_deleted + batch_size,
                "iteration": iteration
            }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error clearing Neptune graph: {str(e)}", exc_info=True)
        raise
