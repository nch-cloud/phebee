import json
import boto3
import logging
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Custom resource handler for creating Iceberg ontology hierarchy table.
    """
    try:
        request_type = event['RequestType']
        properties = event['ResourceProperties']

        database = properties['Database']
        table = properties['Table']
        s3_location = properties['S3Location']
        query_results_s3 = properties['QueryResultsS3']

        if request_type == 'Create':
            create_table(database, table, s3_location, query_results_s3)
        elif request_type == 'Update':
            # For now, we'll just log updates - schema evolution can be added later
            logger.info(f"Update requested for table {database}.{table}")
        elif request_type == 'Delete':
            # Optionally delete the table - for now just log
            logger.info(f"Delete requested for table {database}.{table}")

        # Return success response
        response_data = {
            'Database': database,
            'Table': table,
            'S3Location': s3_location
        }

        send_response(event, context, 'SUCCESS', response_data)

    except Exception as e:
        logger.error(f"Error in create_ontology_hierarchy_table: {str(e)}")
        send_response(event, context, 'FAILED', {'Error': str(e)})

def create_table(database: str, table: str, s3_location: str, query_results_s3: str):
    """Create the Iceberg ontology hierarchy table."""

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table} (
        ontology_source string,
        version string,
        term_id string,
        term_iri string,
        term_label string,
        ancestor_term_ids array<string>,
        last_updated timestamp
    )
    PARTITIONED BY (ontology_source, version)
    LOCATION '{s3_location}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write.parquet.compression-codec'='snappy',
        'description'='Ontology term hierarchies for HPO, MONDO, and other ontologies'
    )
    """

    logger.info(f"Creating ontology hierarchy table with SQL: {create_table_sql}")

    try:
        # Check workgroup configuration
        wg_cfg = athena.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]
        managed_config = wg_cfg.get("ManagedQueryResultsConfiguration", {})
        managed = managed_config.get("Enabled", False) if isinstance(managed_config, dict) else False

        params = {
            "QueryString": create_table_sql,
            "QueryExecutionContext": {"Database": database}
        }

        if not managed:
            params["ResultConfiguration"] = {"OutputLocation": query_results_s3}

        response = athena.start_query_execution(**params)
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Started query execution: {query_execution_id}")

        # Wait for query to complete
        wait_for_query_completion(query_execution_id, timeout_seconds=60)

        logger.info(f"Successfully created Iceberg table {database}.{table}")

    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise

def wait_for_query_completion(query_execution_id: str, timeout_seconds: int = 60):
    """Wait for Athena query to complete."""
    import time

    sleep_interval = 3
    elapsed = 0

    while elapsed < timeout_seconds:
        try:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]

            logger.info(f"Query {query_execution_id} status: {state} (elapsed: {elapsed}s)")

            if state == "SUCCEEDED":
                return
            elif state in ["FAILED", "CANCELLED"]:
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                query_string = response["QueryExecution"]["Query"]
                logger.error(f"Query failed: {query_string}")
                logger.error(f"Failure reason: {reason}")
                raise Exception(f"Athena query {query_execution_id} failed: {reason}")

            time.sleep(sleep_interval)
            elapsed += sleep_interval

        except Exception as e:
            if 'Athena query' in str(e) and 'failed:' in str(e):
                raise  # Re-raise query failure exceptions
            logger.error(f"Error checking query status: {str(e)}")
            time.sleep(sleep_interval)
            elapsed += sleep_interval

    raise Exception(f"Athena query {query_execution_id} did not complete within {timeout_seconds} seconds.")

def send_response(event, context, response_status, response_data):
    """Send response to CloudFormation."""
    import urllib3

    response_url = event['ResponseURL']

    response_body = {
        'Status': response_status,
        'Reason': f'See CloudWatch Log Stream: {context.log_stream_name}',
        'PhysicalResourceId': f"{event['ResourceProperties']['Database']}.{event['ResourceProperties']['Table']}",
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': response_data
    }

    json_response_body = json.dumps(response_body)

    headers = {
        'content-type': '',
        'content-length': str(len(json_response_body))
    }

    http = urllib3.PoolManager()

    try:
        response = http.request('PUT', response_url, body=json_response_body, headers=headers)
        logger.info(f"CloudFormation response sent: {response.status}")
    except Exception as e:
        logger.error(f"Failed to send response to CloudFormation: {str(e)}")
