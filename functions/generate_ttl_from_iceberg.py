import json
import logging
import boto3
import hashlib
from typing import List, Dict, Set

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Generate TTL files from Iceberg data in batches"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        database = event.get('database', 'phebee')
        table = event.get('table', 'evidence')
        bucket = event.get('bucket')
        
        if not all([run_id, bucket]):
            raise ValueError("run_id and bucket are required")
        
        athena = boto3.client('athena')
        s3 = boto3.client('s3')
        
        # Single query to get all data for this run
        query = f"""
        SELECT subject_id, term_iri, termlink_id, qualifiers
        FROM {database}.{table}
        WHERE run_id = '{run_id}'
        ORDER BY subject_id, term_iri
        """
        
        query_result_location = f"s3://{bucket}/athena-results/"
        
        logger.info(f"Starting Athena query for TTL generation")
        logger.info(f"Database: {database}, Table: {table}, Run ID: {run_id}")
        logger.info(f"Query: {query}")
        
        # Start the query
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': query_result_location},
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query completion
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Athena query failed: {result['QueryExecution']['Status']}")
            
            import time
            time.sleep(2)
        
        logger.info("Athena query completed, processing results")
        
        # Initialize TTL content
        ttl_lines = [
            "@prefix phebee: <http://phebee.org/> .",
            "@prefix hp: <http://purl.obolibrary.org/obo/> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            ""
        ]
        
        subjects_seen = set()
        termlinks_seen = set()
        relationships_seen = set()  # Track subject-termlink relationships
        file_count = 0
        next_token = None
        
        # Page through all results
        while True:
            # Get query results with pagination
            if next_token:
                results = athena.get_query_results(
                    QueryExecutionId=query_execution_id,
                    NextToken=next_token
                )
            else:
                results = athena.get_query_results(QueryExecutionId=query_execution_id)
            
            rows = results['ResultSet']['Rows']
            
            # Skip header row on first page
            if next_token is None:
                rows = rows[1:]
            
            if not rows:
                break
            
            # Process this page of results
            for row in rows:
                data = row['Data']
                subject_id = data[0]['VarCharValue']
                term_iri = data[1]['VarCharValue']
                termlink_id = data[2]['VarCharValue']
                
                # Parse qualifiers array (stored as JSON string)
                qualifiers_json = data[3].get('VarCharValue', '[]') if len(data) > 3 else '[]'
                
                # Extract qualifier flags
                negated = False
                family = False
                hypothetical = False
                
                try:
                    import json
                    qualifiers = json.loads(qualifiers_json) if qualifiers_json else []
                    for qualifier in qualifiers:
                        if qualifier.get('qualifier_type') == 'negated' and qualifier.get('qualifier_value') == '1':
                            negated = True
                        elif qualifier.get('qualifier_type') == 'family' and qualifier.get('qualifier_value') == '1':
                            family = True
                        elif qualifier.get('qualifier_type') == 'hypothetical' and qualifier.get('qualifier_value') == '1':
                            hypothetical = True
                except:
                    pass  # Skip if qualifiers can't be parsed
                
                subject_iri = f"phebee:{subject_id}"
                term_iri_short = term_iri.replace("http://purl.obolibrary.org/obo/", "hp:")
                termlink_iri = f"<http://phebee.org/{subject_id}/termlink/{termlink_id}>"
                
                # Subject (only once)
                if subject_id not in subjects_seen:
                    ttl_lines.append(f"{subject_iri} rdf:type phebee:Subject .")
                    subjects_seen.add(subject_id)
                
                # TermLink (only once)
                if termlink_id not in termlinks_seen:
                    ttl_lines.extend([
                        f"{termlink_iri} rdf:type phebee:TermLink ;",
                        f"    phebee:hasSubject {subject_iri} ;",
                        f"    phebee:hasTerm {term_iri_short} ."
                    ])
                    
                    # Add qualifiers
                    if negated:
                        ttl_lines.append(f"{termlink_iri} phebee:hasQualifier phebee:negated .")
                    if family:
                        ttl_lines.append(f"{termlink_iri} phebee:hasQualifier phebee:family .")
                    if hypothetical:
                        ttl_lines.append(f"{termlink_iri} phebee:hasQualifier phebee:hypothetical .")
                    
                    termlinks_seen.add(termlink_id)
                
                # Subject hasTermLink relationship (only once per subject-termlink pair)
                relationship_key = f"{subject_id}|{termlink_id}"
                if relationship_key not in relationships_seen:
                    ttl_lines.append(f"{subject_iri} phebee:hasTermLink {termlink_iri} .")
                    ttl_lines.append("")
                    relationships_seen.add(relationship_key)
            
            # Write TTL file if it gets too large (memory management)
            if len(ttl_lines) > 50000:
                ttl_content = "\n".join(ttl_lines)
                ttl_key = f"phebee/runs/{run_id}/neptune/data_{file_count:03d}.ttl"
                
                s3.put_object(
                    Bucket=bucket,
                    Key=ttl_key,
                    Body=ttl_content.encode('utf-8'),
                    ContentType='text/turtle'
                )
                
                logger.info(f"Wrote TTL file: s3://{bucket}/{ttl_key}")
                file_count += 1
                
                # Reset for next file (keep prefixes)
                ttl_lines = [
                    "@prefix phebee: <http://phebee.org/> .",
                    "@prefix hp: <http://purl.obolibrary.org/obo/> .",
                    "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
                    ""
                ]
            
            # Check for more pages
            next_token = results.get('NextToken')
            if not next_token:
                break
        
        # Write final TTL file
        if len(ttl_lines) > 4:  # More than just prefixes
            ttl_content = "\n".join(ttl_lines)
            ttl_key = f"phebee/runs/{run_id}/neptune/data_{file_count:03d}.ttl"
            
            s3.put_object(
                Bucket=bucket,
                Key=ttl_key,
                Body=ttl_content.encode('utf-8'),
                ContentType='text/turtle'
            )
            
            logger.info(f"Wrote final TTL file: s3://{bucket}/{ttl_key}")
            file_count += 1
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'ttl_files_generated': file_count,
                'ttl_prefix': f"s3://{bucket}/phebee/runs/{run_id}/neptune/"
            }
        }
        
    except Exception as e:
        logger.exception("Failed to generate TTL from Iceberg")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id')
            }
        }
