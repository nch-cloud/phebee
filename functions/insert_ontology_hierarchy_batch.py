import json
import os
import boto3
from typing import Dict, List, Any
from aws_lambda_powertools import Logger
from datetime import datetime
from phebee.utils.iceberg import _execute_athena_query

logger = Logger()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda handler to insert a single batch of ontology terms into Iceberg table.

    Designed to be invoked by a Distributed Map state that fans out batch processing.

    Expected input from Distributed Map (S3 item):
        {
            "Key": "ontology-hierarchy/batches/HPO/2024-04-26/batch-000.json"
        }

    Or direct invocation with batch data:
        {
            "ontology_source": "HPO",
            "version": "2024-04-26",
            "terms": [...]
        }

    Returns:
        {
            "batch_key": "batch-000.json",
            "terms_inserted": 100,
            "status": "success"
        }
    """
    try:
        logger.info("Inserting ontology hierarchy batch", extra={"event": event})

        # Get environment variables
        database_name = os.environ.get('ICEBERG_DATABASE')
        table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')
        bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')

        if not all([database_name, table_name, bucket_name]):
            raise ValueError("Missing required environment variables: ICEBERG_DATABASE, ICEBERG_ONTOLOGY_HIERARCHY_TABLE, PHEBEE_BUCKET_NAME")

        # Check if this is from Distributed Map (has Key field) or direct invocation
        if 'Key' in event:
            # Read batch from S3
            batch_key = event['Key']
            logger.info(f"Reading batch from s3://{bucket_name}/{batch_key}")

            response = s3.get_object(Bucket=bucket_name, Key=batch_key)
            batch_data = json.loads(response['Body'].read().decode('utf-8'))

            ontology_source = batch_data['ontology_source']
            version = batch_data['version']
            terms = batch_data['terms']
            batch_name = batch_key.split('/')[-1]
        else:
            # Direct invocation with batch data
            ontology_source = event['ontology_source']
            version = event['version']
            terms = event['terms']
            batch_name = "direct-invocation"

        if not terms:
            logger.warning("No terms in batch, skipping")
            return {
                "batch_key": batch_name,
                "terms_inserted": 0,
                "status": "skipped"
            }

        # Build INSERT query
        last_updated = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        values_list = []

        for term in terms:
            term_id = term['term_id']
            term_iri = term['term_iri']
            term_label = term['term_label'].replace("'", "''")  # Escape single quotes
            ancestor_ids = term.get('ancestor_term_ids', [])
            depth = term.get('depth', 0)

            # Format ancestor array for SQL
            if ancestor_ids:
                ancestor_array = "ARRAY[" + ", ".join([f"'{aid}'" for aid in ancestor_ids]) + "]"
            else:
                ancestor_array = "ARRAY[]"

            values_list.append(
                f"('{ontology_source}', '{version}', '{term_id}', '{term_iri}', "
                f"'{term_label}', {ancestor_array}, {depth}, TIMESTAMP '{last_updated}')"
            )

        # Execute INSERT query
        insert_query = f"""
        INSERT INTO {database_name}.{table_name}
        (ontology_source, version, term_id, term_iri, term_label, ancestor_term_ids, depth, last_updated)
        VALUES
        {', '.join(values_list)}
        """

        logger.info(f"Inserting {len(terms)} terms from {batch_name}")
        _execute_athena_query(insert_query, wait_for_completion=True)

        logger.info(f"Successfully inserted {len(terms)} terms")

        return {
            "batch_key": batch_name,
            "terms_inserted": len(terms),
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to insert ontology hierarchy batch: {str(e)}")
        raise
