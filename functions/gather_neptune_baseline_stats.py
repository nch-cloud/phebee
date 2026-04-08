"""
Gather Neptune Baseline Stats Lambda

Queries Neptune via SPARQL and analytical tables via Athena to gather
baseline statistics before rebuilding Neptune subjects graph.
"""

import logging
from phebee.utils.iceberg import execute_athena_query

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Gather baseline statistics before rebuilding Neptune subjects graph.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "bySubjectTable": "table",
            "region": "aws-region"
        }

    Output:
        {
            "currentSubjectCount": 1234,  # from Neptune (if available)
            "expectedSubjectCount": 1234,  # from analytical table
            "expectedTermlinkCount": 12345,  # from analytical table
            "timestamp": "iso-timestamp"
        }
    """
    try:
        from phebee.utils import sparql

        run_id = event['runId']
        database = event['icebergDatabase']
        by_subject_table = event['bySubjectTable']
        region = event.get('region', 'us-east-1')

        logger.info(f"Gathering baseline stats for Neptune rebuild")

        # Query 1: Get current Neptune counts (if graph exists)
        current_subject_count = None
        try:
            logger.info("Querying Neptune for current subject count...")
            subject_count_query = """
            SELECT (COUNT(DISTINCT ?s) AS ?subjectCount)
            WHERE {
                GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {
                    ?s a <http://ods.nationwidechildrens.org/phebee#Subject>
                }
            }
            """
            result = sparql.execute_query(subject_count_query)

            # Parse SPARQL JSON result
            if result and 'results' in result and 'bindings' in result['results']:
                bindings = result['results']['bindings']
                if bindings and 'subjectCount' in bindings[0]:
                    current_subject_count = int(bindings[0]['subjectCount']['value'])
                    logger.info(f"Current Neptune subject count: {current_subject_count:,}")

        except Exception as e:
            logger.warning(f"Could not query Neptune (graph may be empty): {str(e)}")
            current_subject_count = 0

        # Query 2: Get expected counts from analytical table
        logger.info("Querying analytical table for expected counts...")
        expected_counts_query = f"""
        SELECT
            COUNT(DISTINCT subject_id) as expected_subject_count,
            COUNT(*) as expected_termlink_count
        FROM {database}.{by_subject_table}
        """

        results = execute_athena_query(expected_counts_query, database)

        data_row = results['ResultSet']['Rows'][1]['Data']
        expected_subject_count = int(data_row[0]['VarCharValue'])
        expected_termlink_count = int(data_row[1]['VarCharValue'])

        logger.info(f"Expected subject count: {expected_subject_count:,}")
        logger.info(f"Expected termlink count: {expected_termlink_count:,}")

        from datetime import datetime
        baseline_stats = {
            "currentSubjectCount": current_subject_count,
            "expectedSubjectCount": expected_subject_count,
            "expectedTermlinkCount": expected_termlink_count,
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info("Neptune baseline stats gathered successfully")
        return baseline_stats

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error gathering Neptune baseline stats: {str(e)}", exc_info=True)
        raise
