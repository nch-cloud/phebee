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
        run_id = event['run_id']
        database = event['database']
        table = event['table']
        bucket = event['bucket']
        page_query = event['page_query']
        page_number = event['page_number']
        
        athena = boto3.client('athena')
        s3 = boto3.client('s3')
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
        
        # Get query results
        results = athena.get_query_results(QueryExecutionId=execution_id)
        
        # Generate N-Quads content (no prefixes needed)
        nq_lines = []
        
        # Track subjects and projects for organizing into named graphs
        subjects_data = {}  # subject_id -> {term_links: [...], project_info: {...}}
        projects_data = {}  # project_id -> {subjects: [...]}
        
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
        
        # Generate subjects graph (N-Quads format)
        subjects_graph = "<http://ods.nationwidechildrens.org/phebee/subjects>"
        
        for subject_id, subject_data in subjects_data.items():
            subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
            nq_lines.append(f"{subject_uri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ods.nationwidechildrens.org/phebee#Subject> {subjects_graph} .")
            
            # Track declared TermLinks to avoid duplicates
            declared_termlinks = set()
            
            for term_link in subject_data['term_links']:
                termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}/term-link/{term_link['termlink_id']}>"
                
                # Only declare TermLink once per subject
                if term_link['termlink_id'] not in declared_termlinks:
                    declared_termlinks.add(term_link['termlink_id'])
                    
                    # Convert term IRI to N-Quads format
                    term_iri = term_link['term_iri']
                    if term_iri.startswith('http://purl.obolibrary.org/obo/'):
                        term_uri = f"<{term_iri}>"
                    else:
                        term_uri = f"<{term_iri}>"
                    
                    # TermLink declaration and connections
                    nq_lines.append(f"{termlink_uri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ods.nationwidechildrens.org/phebee#TermLink> {subjects_graph} .")
                    nq_lines.append(f"{subject_uri} <http://ods.nationwidechildrens.org/phebee#hasTermLink> {termlink_uri} {subjects_graph} .")
                    nq_lines.append(f"{termlink_uri} <http://ods.nationwidechildrens.org/phebee#hasTerm> {term_uri} {subjects_graph} .")
                    
                    # Add qualifiers
                    for qualifier in term_link['qualifiers']:
                        qualifier_type = qualifier.get('qualifier_type', '')
                        qualifier_value = qualifier.get('qualifier_value', '')
                        
                        if qualifier_value in ['1', 'true', True]:
                            nq_lines.append(f"{termlink_uri} <http://ods.nationwidechildrens.org/phebee#hasQualifyingTerm> <http://ods.nationwidechildrens.org/phebee#{qualifier_type}> {subjects_graph} .")
        
        # Generate project graphs (N-Quads format)
        for project_id, project_data in projects_data.items():
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_uri = f"<{project_uri_base}>"
            project_graph = f"<{project_uri_base}>"
            
            # Project declaration
            nq_lines.append(f"{project_uri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ods.nationwidechildrens.org/phebee#Project> {project_graph} .")
            
            # Project-subject relationships
            for subject_id in project_data['subjects']:
                subject_data = subjects_data[subject_id]
                if subject_data['project_info']:
                    subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
                    project_subject_id = subject_data['project_info']['project_subject_id']
                    project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
                    
                    nq_lines.append(f"{subject_uri} <http://ods.nationwidechildrens.org/phebee#hasProjectSubjectId> {project_subject_id_iri} {project_graph} .")
                    nq_lines.append(f"{project_subject_id_iri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ods.nationwidechildrens.org/phebee#ProjectSubjectId> {project_graph} .")
                    nq_lines.append(f"{project_subject_id_iri} <http://ods.nationwidechildrens.org/phebee#hasProject> {project_uri} {project_graph} .")
                    nq_lines.append(f"{project_subject_id_iri} <http://www.w3.org/2000/01/rdf-schema#label> \"{project_subject_id}\" {project_graph} .")
        
        # Generate N-Quads content
        nq_content = '\n'.join(nq_lines)
        
        # Upload N-Quads file to S3
        nq_key = f"{run_id}/nq/page_{page_number:04d}_data_{record_count}.nq"
        s3.put_object(
            Bucket=bucket,
            Key=nq_key,
            Body=nq_content.encode('utf-8'),
            ContentType='application/n-quads'
        )
        
        nq_s3_path = f"s3://{bucket}/{nq_key}"
        
        logger.info(f"Generated N-Quads file: {nq_s3_path} with {record_count} records")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'page_number': page_number,
                'nq_file': nq_s3_path,
                'record_count': record_count,
                'nq_prefix': f"s3://{bucket}/{run_id}/nq/"
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
