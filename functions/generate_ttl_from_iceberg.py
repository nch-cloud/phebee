import json
import logging
import boto3
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Generate TTL files from Athena query results for a single page"""
    logger.info(event)
    
    try:
        run_id = event['run_id']
        database = event['database']
        table = event['table']
        bucket = event['bucket']
        page_query = event['page_query']
        page_number = event['page_number']
        
        athena = boto3.client('athena')
        s3 = boto3.client('s3')
        
        query_result_location = f"s3://{bucket}/athena-results/"
        
        # Execute the page query
        response = athena.start_query_execution(
            QueryString=page_query,
            ResultConfiguration={'OutputLocation': query_result_location},
            WorkGroup='primary'
        )
        
        execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            status_response = athena.get_query_execution(QueryExecutionId=execution_id)
            status = status_response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                error_reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_reason}")
            
            time.sleep(2)
        
        # Get query results
        results = athena.get_query_results(QueryExecutionId=execution_id)
        
        # Generate TTL content
        ttl_lines = []
        ttl_lines.append("@prefix phebee: <http://ods.nationwidechildrens.org/phebee/> .")
        ttl_lines.append("@prefix hp: <http://purl.obolibrary.org/obo/> .")
        ttl_lines.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
        ttl_lines.append("")
        
        record_count = 0
        # Skip header row (index 0)
        for row in results['ResultSet']['Rows'][1:]:
            data = row['Data']
            if len(data) >= 4:
                subject_id = data[0].get('VarCharValue', '')
                term_iri = data[1].get('VarCharValue', '')
                termlink_id = data[2].get('VarCharValue', '')
                qualifiers_json = data[3].get('VarCharValue', '[]')
                
                if subject_id and term_iri and termlink_id:
                    record_count += 1
                    
                    # Parse qualifiers
                    qualifiers = []
                    if qualifiers_json and qualifiers_json != '[]':
                        try:
                            # Try JSON first
                            qualifiers = json.loads(qualifiers_json)
                        except:
                            # Parse Athena array format: [{qualifier_type=negated, qualifier_value=1}]
                            import re
                            matches = re.findall(r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}', qualifiers_json)
                            qualifiers = [{'qualifier_type': qt, 'qualifier_value': qv} for qt, qv in matches]
                    
                    # Generate URIs with full IRIs in angle brackets
                    subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
                    termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{termlink_id}>"
                    
                    # Convert term IRI to TTL format
                    if term_iri.startswith('http://purl.obolibrary.org/obo/'):
                        term_id = term_iri.replace('http://purl.obolibrary.org/obo/', 'hp:')
                    else:
                        term_id = f"<{term_iri}>"
                    
                    # Subject declaration
                    ttl_lines.append(f"{subject_uri} rdf:type phebee:Subject .")
                    
                    # TermLink declaration
                    ttl_lines.append(f"{termlink_uri} rdf:type phebee:TermLink .")
                    
                    # Subject-TermLink connection
                    ttl_lines.append(f"{subject_uri} phebee:hasTermLink {termlink_uri} .")
                    
                    # TermLink-Term connection
                    ttl_lines.append(f"{termlink_uri} phebee:hasTerm {term_id} .")
                    
                    # Add qualifiers
                    for qualifier in qualifiers:
                        qualifier_type = qualifier.get('qualifier_type', '')
                        qualifier_value = qualifier.get('qualifier_value', '')
                        
                        # Only add positive qualifiers (value = "1" or "true")
                        if qualifier_value in ['1', 'true', True]:
                            ttl_lines.append(f"{termlink_uri} phebee:hasQualifyingTerm phebee:{qualifier_type} .")
                    
                    ttl_lines.append("")  # Empty line for readability
        
        # Generate TTL content
        ttl_content = '\n'.join(ttl_lines)
        
        # Upload TTL file to S3
        ttl_key = f"{run_id}/ttl/page_{page_number:04d}_data_{record_count}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=ttl_key,
            Body=ttl_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        
        ttl_s3_path = f"s3://{bucket}/{ttl_key}"
        
        logger.info(f"Generated TTL file: {ttl_s3_path} with {record_count} records")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'page_number': page_number,
                'ttl_file': ttl_s3_path,
                'record_count': record_count,
                'ttl_prefix': f"s3://{bucket}/{run_id}/ttl/"
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating TTL: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id', 'unknown'),
                'page_number': event.get('page_number', 'unknown')
            }
        }
