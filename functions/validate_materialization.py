"""
Validate Materialization Lambda

Validates analytical tables after materialization by comparing to baseline
and checking data consistency.
"""

import time
import os
import logging
import boto3
import random

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')


def execute_athena_query(query, database):
    """Execute Athena query and wait for results."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={
            'OutputLocation': f's3://{os.environ["PHEBEE_BUCKET_NAME"]}/athena-results/'
        }
    )

    query_execution_id = response['QueryExecutionId']

    # Wait for completion
    max_wait_time = 300
    poll_interval = 2
    elapsed_time = 0

    while elapsed_time < max_wait_time:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(poll_interval)
        elapsed_time += poll_interval

    if status != 'SUCCEEDED':
        error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Athena query failed: {error_msg}")

    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    return results


def lambda_handler(event, context):
    """
    Validate analytical tables after materialization.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "evidenceTable": "table",
            "bySubjectTable": "table",
            "byProjectTermTable": "table",
            "region": "aws-region",
            "baseline": {
                "distinctSubjects": 1234,
                "distinctSubjectTerms": 12345,
                "dynamoSubjectCount": 1234
            }
        }

    Output:
        {
            "valid": true/false,
            "details": {
                "subjectCountMatch": true/false,
                "termCountMatch": true/false,
                "nullCheckPassed": true/false,
                "errors": [...]
            }
        }
    """
    try:
        run_id = event['runId']
        database = event['icebergDatabase']
        by_subject_table = event['bySubjectTable']
        baseline = event['baseline']
        region = event.get('region', 'us-east-1')

        logger.info(f"Validating materialization for {database}.{by_subject_table}")
        logger.info(f"Baseline - Subjects: {baseline['distinctSubjects']:,}, Terms: {baseline['distinctSubjectTerms']:,}")

        errors = []
        details = {}

        # Validation 1: Check row count matches expected subject-term combinations
        count_query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT subject_id) as distinct_subjects
        FROM {database}.{by_subject_table}
        """

        logger.info("Checking analytical table counts...")
        results = execute_athena_query(count_query, database)

        data_row = results['ResultSet']['Rows'][1]['Data']
        total_rows = int(data_row[0]['VarCharValue'])
        distinct_subjects = int(data_row[1]['VarCharValue'])

        # Total rows should equal distinct subject-term combinations from evidence
        term_count_match = (total_rows == baseline['distinctSubjectTerms'])
        subject_count_match = (distinct_subjects == baseline['distinctSubjects'])

        details['termCountMatch'] = term_count_match
        details['subjectCountMatch'] = subject_count_match
        details['totalRows'] = total_rows
        details['distinctSubjects'] = distinct_subjects

        if not term_count_match:
            error_msg = f"Term count mismatch! Expected: {baseline['distinctSubjectTerms']:,}, Got: {total_rows:,}"
            logger.error(error_msg)
            errors.append(error_msg)

        if not subject_count_match:
            error_msg = f"Subject count mismatch! Expected: {baseline['distinctSubjects']:,}, Got: {distinct_subjects:,}"
            logger.error(error_msg)
            errors.append(error_msg)

        # Validation 2: Check for NULL values in critical fields
        null_check_query = f"""
        SELECT COUNT(*) as null_count
        FROM {database}.{by_subject_table}
        WHERE termlink_id IS NULL
           OR subject_id IS NULL
           OR term_iri IS NULL
        """

        logger.info("Checking for NULL values...")
        results = execute_athena_query(null_check_query, database)

        null_count = int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        null_check_passed = (null_count == 0)

        details['nullCheckPassed'] = null_check_passed
        details['nullCount'] = null_count

        if not null_check_passed:
            error_msg = f"Found {null_count:,} records with NULL critical fields!"
            logger.error(error_msg)
            errors.append(error_msg)

        # Validation 3: Sample verification - check a few random subjects
        # Get 10 random subject IDs
        sample_query = f"""
        SELECT DISTINCT subject_id
        FROM {database}.{by_subject_table}
        TABLESAMPLE BERNOULLI (1)
        LIMIT 10
        """

        logger.info("Sampling subjects for verification...")
        results = execute_athena_query(sample_query, database)

        sample_subjects = []
        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            if row['Data']:
                subject_id = row['Data'][0].get('VarCharValue')
                if subject_id:
                    sample_subjects.append(subject_id)

        # For each sample subject, verify term counts match between evidence and analytical
        sample_verification_passed = True
        if sample_subjects:
            sample_ids_str = "', '".join(sample_subjects[:5])  # Check first 5

            # Compare counts between evidence and analytical table
            # Note: Count distinct termlinks (not just terms) since qualifiers create separate termlinks
            comparison_query = f"""
            WITH evidence_counts AS (
                SELECT
                    subject_id,
                    COUNT(DISTINCT termlink_id) as evidence_termlink_count
                FROM {database}.{event['evidenceTable']}
                WHERE subject_id IN ('{sample_ids_str}')
                GROUP BY subject_id
            ),
            analytical_counts AS (
                SELECT
                    subject_id,
                    COUNT(*) as analytical_termlink_count
                FROM {database}.{by_subject_table}
                WHERE subject_id IN ('{sample_ids_str}')
                GROUP BY subject_id
            )
            SELECT
                COALESCE(e.subject_id, a.subject_id) as subject_id,
                COALESCE(e.evidence_termlink_count, 0) as evidence_count,
                COALESCE(a.analytical_termlink_count, 0) as analytical_count
            FROM evidence_counts e
            FULL OUTER JOIN analytical_counts a ON e.subject_id = a.subject_id
            WHERE COALESCE(e.evidence_termlink_count, 0) != COALESCE(a.analytical_termlink_count, 0)
            """

            logger.info("Verifying sample subject term counts...")
            results = execute_athena_query(comparison_query, database)

            # If any rows returned, there's a mismatch
            mismatch_count = len(results['ResultSet']['Rows']) - 1  # Subtract header

            if mismatch_count > 0:
                sample_verification_passed = False
                error_msg = f"Sample verification failed! {mismatch_count} subjects have mismatched term counts"
                logger.error(error_msg)
                errors.append(error_msg)

        details['sampleVerificationPassed'] = sample_verification_passed

        # Overall validation result
        valid = (
            term_count_match and
            subject_count_match and
            null_check_passed and
            sample_verification_passed
        )

        details['errors'] = errors

        if valid:
            logger.info("✓ Materialization validation PASSED")
        else:
            logger.error("✗ Materialization validation FAILED")
            for error in errors:
                logger.error(f"  - {error}")

        return {
            "valid": valid,
            "details": details
        }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error validating materialization: {str(e)}", exc_info=True)
        raise
