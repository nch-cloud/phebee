"""
Truncate Iceberg Table Lambda

Executes TRUNCATE TABLE command via Athena to clear an Iceberg table.
"""

import time
import os
import logging
import boto3
from phebee.utils.iceberg import get_workgroup_config_cached

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')


def lambda_handler(event, context):
    """
    Truncate an Iceberg table using Athena.

    Input:
        {
            "database": "database_name",
            "table": "table_name",
            "region": "aws-region"
        }

    Output:
        {
            "statusCode": 200,
            "database": "database_name",
            "table": "table_name",
            "queryExecutionId": "athena-query-id",
            "status": "SUCCEEDED"
        }
    """
    try:
        database = event['database']
        table = event['table']
        region = event.get('region', 'us-east-1')

        logger.info(f"Truncating table: {database}.{table}")

        # Build DELETE query (Athena doesn't support TRUNCATE for Iceberg tables)
        query = f"DELETE FROM {database}.{table}"

        # Submit query to Athena
        bucket_name = os.environ['PHEBEE_BUCKET_NAME']

        # Check workgroup configuration (cached to avoid rate limiting)
        managed, wg_cfg = get_workgroup_config_cached(athena)

        params = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": database}
        }

        if not managed:
            params["ResultConfiguration"] = {"OutputLocation": f's3://{bucket_name}/athena-results/'}

        response = athena.start_query_execution(**params)
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Started Athena query: {query_execution_id}")

        # Wait for query completion (with timeout)
        max_wait_time = 300  # 5 minutes
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
            logger.error(f"TRUNCATE TABLE failed: {error_msg}")
            raise Exception(f"TRUNCATE TABLE failed: {error_msg}")

        logger.info(f"TRUNCATE TABLE completed successfully for {database}.{table}")

        return {
            "statusCode": 200,
            "database": database,
            "table": table,
            "queryExecutionId": query_execution_id,
            "status": status
        }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error truncating table: {str(e)}", exc_info=True)
        raise
