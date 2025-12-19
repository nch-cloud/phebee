import json
import os
import boto3
from aws_lambda_powertools import Logger, Tracer
from phebee.utils.aws import extract_body

logger = Logger()
tracer = Tracer()

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Query evidence records by run_id from Iceberg table."""
    try:
        body = extract_body(event)
        run_id = body.get("run_id")
        limit = min(body.get("limit", 1000), 10000)  # Max 10k records per page
        next_token = body.get("next_token")  # For Athena pagination
        
        if not run_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "run_id is required"})
            }
        
        # Query Iceberg table via Athena
        athena_client = boto3.client('athena')
        database = os.environ.get('ICEBERG_DATABASE', 'phebee')
        table = os.environ.get('ICEBERG_EVIDENCE_TABLE', 'evidence')
        workgroup = os.environ.get('ATHENA_WORKGROUP', 'primary')
        
        query = f"""
        SELECT 
            evidence_id,
            run_id,
            batch_id,
            evidence_type,
            subject_id,
            term_iri,
            creator,
            created_timestamp
        FROM {database}.{table}
        WHERE run_id = '{run_id}'
        ORDER BY created_timestamp
        """
        
        # Start query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup,
            ResultConfiguration={
                'OutputLocation': f's3://{os.environ["PHEBEE_BUCKET_NAME"]}/athena-results/'
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        
        if status != 'SUCCEEDED':
            error_reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            return {
                "statusCode": 500,
                "body": json.dumps({"message": f"Query failed: {error_reason}"})
            }
        
        # Get query results using Athena pagination
        # Note: For first page, MaxResults includes header row, so we need to add 1
        # For subsequent pages (with next_token), there's no header row, so use limit as-is
        # But MaxResults cannot exceed 1000, so we need to cap it
        if next_token:
            max_results = min(limit, 1000)  # No header row in subsequent pages
        else:
            max_results = min(limit + 1, 1000)  # Add 1 for header in first page
            
        get_results_params = {
            'QueryExecutionId': query_execution_id,
            'MaxResults': max_results
        }
        if next_token:
            get_results_params['NextToken'] = next_token
            
        results = athena_client.get_query_results(**get_results_params)
        
        # Parse results
        evidence_records = []
        
        if len(results['ResultSet']['Rows']) > 0:
            # For first page (no next_token), first row is header
            # For subsequent pages (with next_token), there's no header row
            if next_token:
                # No header row in subsequent pages, use predefined column order
                columns = ['evidence_id', 'run_id', 'batch_id', 'evidence_type', 'subject_id', 'term_iri', 'creator', 'created_timestamp']
                data_rows = results['ResultSet']['Rows']
            else:
                # First page has header row
                if len(results['ResultSet']['Rows']) > 1:
                    columns = [col['VarCharValue'] for col in results['ResultSet']['Rows'][0]['Data']]
                    data_rows = results['ResultSet']['Rows'][1:]
                else:
                    # Only header, no data
                    data_rows = []
            
            # Process data rows
            for row in data_rows:
                record = {}
                for i, cell in enumerate(row['Data']):
                    if i < len(columns):  # Safety check
                        value = cell.get('VarCharValue', '')
                        # Parse structured fields
                        if columns[i] == 'creator' and value:
                            try:
                                # Handle ROW format: {creator_id=value, creator_type=value, creator_name=value}
                                if value.startswith('{') and '=' in value:
                                    creator_dict = {}
                                    # Parse ROW format
                                    content = value.strip('{}')
                                    pairs = content.split(', ')
                                    for pair in pairs:
                                        if '=' in pair:
                                            key, val = pair.split('=', 1)
                                            creator_dict[key.strip()] = val.strip()
                                    record[columns[i]] = creator_dict
                                else:
                                    record[columns[i]] = json.loads(value)
                            except:
                                record[columns[i]] = value
                        else:
                            record[columns[i]] = value
                evidence_records.append(record)
        
        # Get pagination info
        response_next_token = results.get('NextToken')
        has_more = response_next_token is not None
        
        # Get total count only if this is the first page (no next_token provided)
        total_count = None
        if not next_token:
            count_query = f"""
            SELECT COUNT(*) as total_count
            FROM {database}.{table}
            WHERE run_id = '{run_id}'
            """
            
            # Execute count query
            count_response = athena_client.start_query_execution(
                QueryString=count_query,
                WorkGroup=workgroup,
                ResultConfiguration={
                    'OutputLocation': f's3://{os.environ["PHEBEE_BUCKET_NAME"]}/athena-results/'
                }
            )
            
            count_execution_id = count_response['QueryExecutionId']
            
            # Wait for count query to complete
            while True:
                count_result = athena_client.get_query_execution(QueryExecutionId=count_execution_id)
                count_status = count_result['QueryExecution']['Status']['State']
                
                if count_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
            
            if count_status == 'SUCCEEDED':
                count_results = athena_client.get_query_results(QueryExecutionId=count_execution_id)
                if len(count_results['ResultSet']['Rows']) > 1:
                    total_count = int(count_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        response_body = {
            "run_id": run_id,
            "evidence_count": len(evidence_records),
            "limit": limit,
            "has_more": has_more,
            "evidence": evidence_records
        }
        
        # Add pagination tokens
        if response_next_token:
            response_body["next_token"] = response_next_token
        if total_count is not None:
            response_body["total_count"] = total_count
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }
        
    except Exception as e:
        logger.error(f"Error querying evidence by run_id: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error querying evidence: {str(e)}"})
        }
