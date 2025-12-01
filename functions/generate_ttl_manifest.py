import json
import logging
import boto3
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Generate manifest of Athena result pages for parallel TTL processing"""
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
        
        # For now, process all records in one page (can optimize pagination later)
        total_pages = 1
        
        # Generate single page manifest
        pages = []
        page_query = f"""
        SELECT subject_id, term_iri, termlink_id, qualifiers
        FROM {database}.{table}
        WHERE run_id = '{run_id}'
        ORDER BY subject_id, term_iri
        """
        
        pages.append({
            'page_number': 0,
            'query': page_query,
            'run_id': run_id,
            'total_records': total_count
        })
        
        logger.info(f"Generated {len(pages)} pages for parallel processing")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'total_records': total_count,
                'total_pages': total_pages,
                'page_size': page_size,
                'pages': pages
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating TTL manifest: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id', 'unknown')
            }
        }
