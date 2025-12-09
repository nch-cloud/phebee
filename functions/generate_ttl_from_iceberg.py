import json
import logging
import boto3
import time
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Generate N-Quads files from Athena query results for a single page"""
    logger.info(event)
    
    try:
        # Extract parameters from event
        page_number = event['page_number']
        run_id = event['run_id']
        database = event['database']
        table = event['table']
        bucket = event['bucket']
        
        # Construct manifest location from run_id and bucket
        manifest_location = f"s3://{bucket}/{run_id}/manifest.json"
        
        # Read manifest from S3
        s3 = boto3.client('s3')
        manifest_response = s3.get_object(Bucket=bucket, Key=f"{run_id}/manifest.json")
        manifest = json.loads(manifest_response['Body'].read())
        
        # Get the specific page definition
        if page_number >= len(manifest['pages']):
            raise Exception(f"Page number {page_number} out of range (total pages: {len(manifest['pages'])})")
        
        page_def = manifest['pages'][page_number]
        page_query = page_def['query']
        
        athena = boto3.client('athena')
        dynamodb = boto3.resource('dynamodb')
        
        # Get DynamoDB table name from environment
        import os
        dynamodb_table_name = os.environ.get('DYNAMODB_TABLE_NAME')
        if not dynamodb_table_name:
            raise Exception("DYNAMODB_TABLE_NAME environment variable not set")
        
        table_resource = dynamodb.Table(dynamodb_table_name)
        
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
        
        # Get query results with pagination
        all_rows = []
        next_token = None
        
        while True:
            if next_token:
                results = athena.get_query_results(QueryExecutionId=execution_id, NextToken=next_token)
            else:
                results = athena.get_query_results(QueryExecutionId=execution_id)
            
            all_rows.extend(results['ResultSet']['Rows'])
            
            # Check if there are more results
            next_token = results.get('NextToken')
            if not next_token:
                break
        
        logger.info(f"Retrieved {len(all_rows)} total rows from Athena")
        
        # Generate TTL content
        ttl_lines = []
        
        # Track subjects and projects for organizing into named graphs
        subjects_data = {}  # subject_id -> {term_links: [...], project_info: {...}}
        projects_data = {}  # project_id -> {subjects: [...]}
        
        record_count = 0
        # Skip header row (index 0)
        for row in all_rows[1:]:
            data = row['Data']
            if len(data) >= 4:
                subject_id = data[0].get('VarCharValue', '')
                term_iri = data[1].get('VarCharValue', '')
                termlink_id = data[2].get('VarCharValue', '')
                qualifiers_json = data[3].get('VarCharValue', '[]')
                
                if subject_id and term_iri and termlink_id:
                    record_count += 1
                    
                    # Get project info from DynamoDB for this subject
                    if subject_id not in subjects_data:
                        try:
                            # Query DynamoDB for project-subject mapping using correct PK/SK structure
                            response = table_resource.query(
                                KeyConditionExpression='PK = :pk',
                                ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
                            )
                            
                            project_info = None
                            if response['Items']:
                                # Parse SK: "PROJECT#{project_id}#SUBJECT#{project_subject_id}"
                                item = response['Items'][0]
                                sk_parts = item['SK'].split('#')
                                if len(sk_parts) >= 4:
                                    project_info = {
                                        'project_id': sk_parts[1],
                                        'project_subject_id': sk_parts[3]
                                    }
                            
                            subjects_data[subject_id] = {
                                'term_links': [],
                                'project_info': project_info
                            }
                            
                            # Track project
                            if project_info and project_info['project_id']:
                                project_id = project_info['project_id']
                                if project_id not in projects_data:
                                    projects_data[project_id] = {'subjects': []}
                                if subject_id not in projects_data[project_id]['subjects']:
                                    projects_data[project_id]['subjects'].append(subject_id)
                                    
                        except Exception as e:
                            logger.warning(f"Could not get project info for subject {subject_id}: {e}")
                            subjects_data[subject_id] = {
                                'term_links': [],
                                'project_info': None
                            }
                    
                    # Parse qualifiers
                    qualifiers = []
                    if qualifiers_json and qualifiers_json != '[]':
                        try:
                            qualifiers = json.loads(qualifiers_json)
                        except:
                            import re
                            matches = re.findall(r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}', qualifiers_json)
                            qualifiers = [{'qualifier_type': qt, 'qualifier_value': qv} for qt, qv in matches]
                    
                    # Store term link data
                    subjects_data[subject_id]['term_links'].append({
                        'term_iri': term_iri,
                        'termlink_id': termlink_id,
                        'qualifiers': qualifiers
                    })
        
        # Generate TTL files organized by graph
        ttl_files = {}
        
        # Subjects graph TTL
        subjects_ttl = []
        subjects_ttl.append("@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .")
        subjects_ttl.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
        subjects_ttl.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
        subjects_ttl.append("")
        
        for subject_id, subject_data in subjects_data.items():
            subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
            subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            
            # Track declared TermLinks to avoid duplicates
            declared_termlinks = set()
            
            for term_link in subject_data['term_links']:
                termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{term_link['termlink_id']}>"
                
                # Only declare TermLink once per subject
                if term_link['termlink_id'] not in declared_termlinks:
                    declared_termlinks.add(term_link['termlink_id'])
                    
                    # Convert term IRI to TTL format
                    term_iri = term_link['term_iri']
                    term_uri = f"<{term_iri}>"
                    
                    # TermLink declaration and connections
                    subjects_ttl.append(f"{termlink_uri} rdf:type phebee:TermLink .")
                    subjects_ttl.append(f"{subject_uri} phebee:hasTermLink {termlink_uri} .")
                    subjects_ttl.append(f"{termlink_uri} phebee:hasTerm {term_uri} .")
                    
                    # Add qualifiers
                    for qualifier in term_link['qualifiers']:
                        qualifier_type = qualifier.get('qualifier_type', '')
                        qualifier_value = qualifier.get('qualifier_value', '')
                        
                        if qualifier_value in ['1', 'true', True]:
                            subjects_ttl.append(f"{termlink_uri} phebee:hasQualifyingTerm phebee:{qualifier_type} .")
        
        # Upload subjects TTL
        subjects_ttl_content = '\n'.join(subjects_ttl)
        subjects_key = f"{run_id}/neptune/subjects/page_{page_number}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=subjects_key,
            Body=subjects_ttl_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        ttl_files['subjects'] = f"s3://{bucket}/{subjects_key}"
        
        # Project graphs TTL
        for project_id, project_data in projects_data.items():
            project_ttl = []
            project_ttl.append("@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .")
            project_ttl.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
            project_ttl.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
            project_ttl.append("")
            
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_uri = f"<{project_uri_base}>"
            
            # Project declaration
            project_ttl.append(f"{project_uri} rdf:type phebee:Project .")
            
            # Project-subject relationships
            for subject_id in project_data['subjects']:
                subject_data = subjects_data[subject_id]
                if subject_data['project_info']:
                    subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
                    project_subject_id = subject_data['project_info']['project_subject_id']
                    project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
                    
                    project_ttl.append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
                    project_ttl.append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
                    project_ttl.append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
                    project_ttl.append(f"{project_subject_id_iri} rdfs:label \"{project_subject_id}\" .")
            
            # Upload project TTL
            project_ttl_content = '\n'.join(project_ttl)
            project_key = f"{run_id}/neptune/projects/{project_id}/page_{page_number}.ttl"
            s3.put_object(
                Bucket=bucket,
                Key=project_key,
                Body=project_ttl_content.encode('utf-8'),
                ContentType='text/turtle'
            )
            ttl_files[f'project_{project_id}'] = f"s3://{bucket}/{project_key}"
        
        logger.info(f"Generated TTL files: {list(ttl_files.keys())} with {record_count} records")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'page_number': page_number,
                'ttl_files': ttl_files,
                'record_count': record_count,
                'ttl_prefix': f"s3://{bucket}/{run_id}/neptune/"
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating N-Quads: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id', 'unknown'),
                'page_number': event.get('page_number', 'unknown')
            }
        }
