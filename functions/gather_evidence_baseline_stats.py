"""
Gather Evidence Baseline Stats Lambda

Queries evidence table via Athena to gather baseline statistics before rehashing.
Collects total row count, distinct subject count, and sample evidence IDs.
"""

import logging
import random
from phebee.utils.iceberg import execute_athena_query

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Gather baseline statistics from evidence table before rehashing.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "evidenceTable": "table",
            "region": "aws-region"
        }

    Output:
        {
            "totalCount": 123456,
            "subjectCount": 1234,
            "sampleIds": ["id1", "id2", ...],
            "timestamp": "iso-timestamp"
        }
    """
    try:
        run_id = event['runId']
        database = event['icebergDatabase']
        table = event['evidenceTable']
        region = event.get('region', 'us-east-1')

        logger.info(f"Gathering baseline stats for {database}.{table}")

        # Query 1: Get total count and distinct subject count
        count_query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT subject_id) as subject_count
        FROM {database}.{table}
        """

        logger.info("Executing count query...")
        results = execute_athena_query(count_query, database)

        # Parse count results (row 0 is header, row 1 is data)
        data_row = results['ResultSet']['Rows'][1]['Data']
        total_count = int(data_row[0]['VarCharValue'])
        subject_count = int(data_row[1]['VarCharValue'])

        logger.info(f"Total evidence count: {total_count:,}")
        logger.info(f"Distinct subject count: {subject_count:,}")

        # Query 2: Sample random evidence records using stable identifiers
        # Sample by (subject_id, clinical_note_id, encounter_id, term_iri, span_start, span_end)
        # These don't change during rehash, unlike evidence_id
        # Use higher percentage for small datasets to ensure we get samples
        sample_percentage = 10 if total_count < 1000 else 1
        sample_query = f"""
        SELECT
            subject_id,
            clinical_note_id,
            encounter_id,
            term_iri,
            text_annotation.span_start as span_start,
            text_annotation.span_end as span_end,
            creator.creator_id as creator_id
        FROM {database}.{table}
        TABLESAMPLE BERNOULLI ({sample_percentage})
        LIMIT 100
        """

        logger.info("Executing sample query...")
        sample_results = execute_athena_query(sample_query, database)

        # Parse sample records (skip header row)
        sample_records = []
        for row in sample_results['ResultSet']['Rows'][1:]:  # Skip header
            if row['Data'] and len(row['Data']) >= 7:
                record = {
                    'subject_id': row['Data'][0].get('VarCharValue'),
                    'clinical_note_id': row['Data'][1].get('VarCharValue'),
                    'encounter_id': row['Data'][2].get('VarCharValue'),
                    'term_iri': row['Data'][3].get('VarCharValue'),
                    'span_start': row['Data'][4].get('VarCharValue'),
                    'span_end': row['Data'][5].get('VarCharValue'),
                    'creator_id': row['Data'][6].get('VarCharValue')
                }
                sample_records.append(record)

        logger.info(f"Sampled {len(sample_records)} evidence records for validation")

        from datetime import datetime
        baseline_stats = {
            "totalCount": total_count,
            "subjectCount": subject_count,
            "sampleRecords": sample_records,
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info(f"Baseline stats gathered successfully")
        return baseline_stats

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error gathering baseline stats: {str(e)}", exc_info=True)
        raise
