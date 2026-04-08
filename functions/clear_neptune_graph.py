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
        batch_size = event.get('batchSize', 1000)  # Number of subjects per batch
        total_deleted = event.get('totalDeleted', 0)
        iteration = event.get('iteration', 0)

        iteration += 1

        logger.info(f"Clearing Neptune graph: {graph_uri}")
        logger.info(f"Iteration {iteration}, batch size: {batch_size} subjects, total deleted so far: {total_deleted:,}")

        # Get a batch of distinct subjects from the graph
        select_query = f"""
        SELECT DISTINCT ?s
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }}
        LIMIT {batch_size}
        """

        logger.info(f"Fetching batch of up to {batch_size} subjects")
        result = sparql.execute_query(select_query)

        if not result or 'results' not in result or 'bindings' not in result['results']:
            logger.error("No results from SELECT query")
            raise Exception("Failed to fetch subjects from graph")

        subjects = [binding['s']['value'] for binding in result['results']['bindings']]
        subject_count = len(subjects)

        if subject_count == 0:
            logger.info("No subjects found, graph is empty")
        else:
            logger.info(f"Found {subject_count} subjects to delete")

            # Build DELETE query for these specific subjects
            # Use explicit DELETE/WHERE with VALUES
            subjects_values = ' '.join(f'<{s}>' for s in subjects)
            delete_query = f"""
            DELETE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
            }}
            WHERE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
                VALUES ?s {{ {subjects_values} }}
            }}
            """

            logger.info(f"Deleting all triples for {subject_count} subjects")
            sparql.execute_update(delete_query)

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

        new_total = total_deleted + subject_count

        if remaining == 0:
            logger.info(f"Graph cleared successfully!")
            logger.info(f"Total subjects deleted: {new_total:,} (in {iteration} iterations)")

            return {
                "graphUri": graph_uri,
                "status": "COMPLETE",
                "remaining": 0,
                "totalDeleted": new_total,
                "iteration": iteration
            }
        else:
            logger.info(f"More triples remain, will continue...")

            return {
                "graphUri": graph_uri,
                "status": "IN_PROGRESS",
                "remaining": remaining,
                "totalDeleted": new_total,
                "iteration": iteration
            }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error clearing Neptune graph: {str(e)}", exc_info=True)
        raise
