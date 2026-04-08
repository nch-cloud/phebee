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
            "batchSize": 1000,            // optional, subjects per batch
            "totalSubjectsDeleted": 0,    // optional, cumulative subject count
            "iteration": 0                 // optional, iteration counter
        }

    Output:
        {
            "graphUri": "...",
            "status": "IN_PROGRESS" | "COMPLETE",
            "remainingSubjects": 1234,
            "remainingTriples": 123456,
            "totalSubjectsDeleted": 1000,
            "iteration": 1
        }
    """
    try:
        # Import phebee utilities (from Lambda layer)
        from phebee.utils import sparql

        graph_uri = event['graphUri']
        batch_size = event.get('batchSize', 10000)  # Number of subjects per batch
        total_subjects_deleted = event.get('totalSubjectsDeleted', 0)
        iteration = event.get('iteration', 0)

        iteration += 1

        logger.info(f"Clearing Neptune graph: {graph_uri}")
        logger.info(f"Iteration {iteration}, batch size: {batch_size} subjects, total subjects deleted so far: {total_subjects_deleted:,}")

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

        # Check remaining counts (both triples and subjects)
        count_query = f"""
        SELECT (COUNT(*) AS ?tripleCount) (COUNT(DISTINCT ?s) AS ?subjectCount)
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }}
        """

        result = sparql.execute_query(count_query)
        remaining_triples = 0
        remaining_subjects = 0

        if result and 'results' in result and 'bindings' in result['results']:
            bindings = result['results']['bindings']
            if bindings:
                if 'tripleCount' in bindings[0]:
                    remaining_triples = int(bindings[0]['tripleCount']['value'])
                if 'subjectCount' in bindings[0]:
                    remaining_subjects = int(bindings[0]['subjectCount']['value'])

        logger.info(f"Remaining: {remaining_subjects:,} subjects, {remaining_triples:,} triples")

        new_total_subjects = total_subjects_deleted + subject_count

        if remaining_triples == 0:
            logger.info(f"Graph cleared successfully!")
            logger.info(f"Total subjects deleted: {new_total_subjects:,} (in {iteration} iterations)")

            return {
                "graphUri": graph_uri,
                "status": "COMPLETE",
                "remainingSubjects": 0,
                "remainingTriples": 0,
                "totalSubjectsDeleted": new_total_subjects,
                "iteration": iteration
            }
        else:
            logger.info(f"More data remains, will continue...")

            return {
                "graphUri": graph_uri,
                "status": "IN_PROGRESS",
                "remainingSubjects": remaining_subjects,
                "remainingTriples": remaining_triples,
                "totalSubjectsDeleted": new_total_subjects,
                "iteration": iteration
            }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error clearing Neptune graph: {str(e)}", exc_info=True)
        raise
