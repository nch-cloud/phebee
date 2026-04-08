"""
Gather Iceberg Baseline Stats Lambda

Queries evidence table and DynamoDB to gather baseline statistics
before materializing analytical tables.
"""

import logging
import boto3
from phebee.utils.iceberg import execute_athena_query

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    """
    Gather baseline statistics before materializing analytical tables.

    Input:
        {
            "runId": "uuid",
            "icebergDatabase": "database",
            "evidenceTable": "table",
            "dynamodbTable": "table",
            "region": "aws-region"
        }

    Output:
        {
            "distinctSubjects": 1234,
            "distinctSubjectTerms": 12345,
            "dynamoSubjectCount": 1234,
            "timestamp": "iso-timestamp"
        }
    """
    try:
        run_id = event['runId']
        database = event['icebergDatabase']
        evidence_table = event['evidenceTable']
        dynamodb_table_name = event['dynamodbTable']
        region = event.get('region', 'us-east-1')

        logger.info(f"Gathering baseline stats from {database}.{evidence_table} and DynamoDB")

        # Query evidence table for distinct subjects and subject-term-termlink combinations
        # Note: Each unique termlink_id (which includes qualifiers) creates a separate row
        # in the analytical table, so we count distinct (subject_id, termlink_id) pairs
        baseline_query = f"""
        SELECT
            COUNT(DISTINCT subject_id) as distinct_subjects,
            COUNT(DISTINCT CONCAT(subject_id, '|', term_iri, '|', termlink_id)) as distinct_termlinks
        FROM {database}.{evidence_table}
        """

        logger.info("Querying evidence table...")
        results = execute_athena_query(baseline_query, database)

        data_row = results['ResultSet']['Rows'][1]['Data']
        distinct_subjects = int(data_row[0]['VarCharValue'])
        distinct_termlinks = int(data_row[1]['VarCharValue'])

        logger.info(f"Distinct subjects: {distinct_subjects:,}")
        logger.info(f"Distinct (subject, term, termlink) combinations: {distinct_termlinks:,}")

        # Query DynamoDB for subject count
        logger.info("Querying DynamoDB for subject mappings...")
        table = dynamodb.Table(dynamodb_table_name)

        # Scan for subjects (PK starts with "SUBJECT#")
        scan_kwargs = {
            'FilterExpression': 'begins_with(PK, :pk_prefix)',
            'ExpressionAttributeValues': {
                ':pk_prefix': 'SUBJECT#'
            },
            'Select': 'COUNT'  # Only count, don't return items
        }

        dynamo_subject_count = 0
        done = False

        while not done:
            response = table.scan(**scan_kwargs)
            dynamo_subject_count += response['Count']

            if 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                done = True

        logger.info(f"DynamoDB subject mappings: {dynamo_subject_count:,}")

        from datetime import datetime
        baseline_stats = {
            "distinctSubjects": distinct_subjects,
            "distinctSubjectTerms": distinct_termlinks,  # Now counts termlinks, not just terms
            "dynamoSubjectCount": dynamo_subject_count,
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info("Baseline stats gathered successfully")
        return baseline_stats

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error gathering baseline stats: {str(e)}", exc_info=True)
        raise
