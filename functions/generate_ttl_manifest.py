import json
import logging
import boto3
import time
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Generate manifest of Athena result pages for parallel N-Quads processing"""
    logger.info(event)
    
    try:
        run_id = event['run_id']
        database = event['database']
        table = event['table']
        bucket = event['bucket']
        page_size = event.get('page_size', 10000)  # Records per page
        
        athena = boto3.client('athena')
        
        # First, count total records
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM {database}.{table}
        WHERE run_id = '{run_id}'
        """
        
        query_result_location = f"s3://{bucket}/athena-results/"
        
        # Execute count query
        count_response = athena.start_query_execution(
            QueryString=count_query,
            ResultConfiguration={'OutputLocation': query_result_location},
            WorkGroup='primary'
        )
        
        count_execution_id = count_response['QueryExecutionId']
        
        # Wait for count query to complete
        while True:
            count_status = athena.get_query_execution(QueryExecutionId=count_execution_id)
            status = count_status['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Count query failed: {count_status}")
            
            time.sleep(2)
        
        # Get count result
        count_result = athena.get_query_results(QueryExecutionId=count_execution_id)
        total_count = int(count_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        logger.info(f"Total records for run_id {run_id}: {total_count}")
        
        # Calculate proper pagination
        total_pages = math.ceil(total_count / page_size)
        
        # Generate page manifests
        pages = []
        for page_num in range(total_pages):
            # Athena doesn't support OFFSET, so we'll use row_number() for pagination
            page_query = f"""
        WITH numbered_rows AS (
            SELECT subject_id, term_iri, termlink_id, qualifiers,
                   ROW_NUMBER() OVER (ORDER BY subject_id, term_iri) as row_num
            FROM {database}.{table}
            WHERE run_id = '{run_id}'
        )
        SELECT subject_id, term_iri, termlink_id, qualifiers
        FROM numbered_rows
        WHERE row_num > {page_num * page_size} AND row_num <= {(page_num + 1) * page_size}
        ORDER BY subject_id, term_iri
        """
            
            pages.append({
                'page_number': page_num,
                'query': page_query,
                'run_id': run_id,
                'offset': page_num * page_size,
                'limit': page_size
            })
        
        # Write manifest to S3
        s3 = boto3.client('s3')
        manifest = {
            'run_id': run_id,
            'total_records': total_count,
            'total_pages': total_pages,
            'page_size': page_size,
            'pages': pages
        }
        
        manifest_key = f"{run_id}/manifest.json"
        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest),
            ContentType='application/json'
        )
        
        logger.info(f"Generated {total_pages} pages for parallel processing, manifest written to s3://{bucket}/{manifest_key}")
        
        # Generate page numbers array for Step Function Map
        page_numbers = list(range(total_pages))
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'total_records': total_count,
                'total_pages': total_pages,
                'page_size': page_size,
                'manifest_location': f"s3://{bucket}/{manifest_key}",
                'page_numbers': page_numbers
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating N-Quads manifest: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id', 'unknown')
            }
        }
