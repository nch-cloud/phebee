#!/usr/bin/env python3
"""
EMR job to generate TTL files from Iceberg evidence table
Reads evidence data and generates Neptune-compatible TTL files
"""

import sys
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast
from collections import defaultdict
import uuid

def get_subject_mappings_from_s3(bucket, subjects_path, region):
    """Load subject mappings from S3 file and return as dict"""
    s3 = boto3.client('s3', region_name=region)
    
    print(f"Loading subject mappings from: s3://{bucket}/{subjects_path}")
    
    try:
        # Parse S3 path to get bucket and key
        if subjects_path.startswith('s3://'):
            # Remove s3:// prefix and split bucket/key
            path_parts = subjects_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1]
        else:
            # Assume it's just the key part
            key = subjects_path
        
        response = s3.get_object(Bucket=bucket, Key=key)
        subjects_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Convert to mapping dict format
        mapping_dict = {}
        for subject in subjects_data:
            subject_id = subject['subject_id']
            mapping_dict[subject_id] = {
                'project_id': subject['project_id'],
                'project_subject_id': subject['project_subject_id']
            }
        
        print(f"Loaded {len(mapping_dict)} subject mappings from S3")
        return mapping_dict
        
    except Exception as e:
        print(f"Error loading subject mappings from S3: {e}")
        raise
    """Load subject mappings from DynamoDB and return as dict"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table_name)
    
    # We need to scan for all SUBJECT# records to get the mappings
    # This is expensive but necessary for the broadcast approach
    print("Scanning DynamoDB for all subject mappings...")
    
    mapping_dict = {}
    
    # Scan with filter for SUBJECT# records
    response = table.scan(
        FilterExpression='begins_with(PK, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'SUBJECT#'}
    )
    
    # Process first page
    for item in response.get('Items', []):
        # PK format: SUBJECT#{subject_id}
        # SK format: PROJECT#{project_id}#SUBJECT#{project_subject_id}
        subject_id = item['PK'].replace('SUBJECT#', '')
        
        # Parse SK to get project info
        sk_parts = item['SK'].split('#')
        if len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
            project_id = sk_parts[1]
            project_subject_id = sk_parts[3]
            
            mapping_dict[subject_id] = {
                'project_id': project_id,
                'project_subject_id': project_subject_id
            }
    
    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression='begins_with(PK, :pk_prefix)',
            ExpressionAttributeValues={':pk_prefix': 'SUBJECT#'},
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        
        for item in response.get('Items', []):
            subject_id = item['PK'].replace('SUBJECT#', '')
            sk_parts = item['SK'].split('#')
            if len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
                project_id = sk_parts[1]
                project_subject_id = sk_parts[3]
                
                mapping_dict[subject_id] = {
                    'project_id': project_id,
                    'project_subject_id': project_subject_id
                }
    
    print(f"Loaded {len(mapping_dict)} subject mappings from DynamoDB")
    return mapping_dict

def write_ttl_partition(partition_iterator, broadcast_mappings, bucket, run_id, region):
    """Process a partition and write TTL files to S3"""
    import boto3
    from collections import defaultdict
    
    s3 = boto3.client('s3', region_name=region)
    partition_id = str(uuid.uuid4())[:8]  # Short unique ID for this partition
    
    subjects_ttl = []
    declared_subjects = set()  # Track declared subjects to avoid duplicates
    declared_termlinks = set()  # Track declared termlinks to avoid duplicates
    record_count = 0
    
    # Get the broadcast mappings
    subject_mappings = broadcast_mappings.value
    
    for row in partition_iterator:
        record_count += 1
        
        # Skip if we've already processed this termlink in this partition
        if row.termlink_id in declared_termlinks:
            continue
            
        declared_termlinks.add(row.termlink_id)
        
        # Generate subjects TTL - match Lambda format exactly
        subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}>"
        termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}/term-link/{row.termlink_id}>"
        term_uri = f"<{row.term_iri}>"
        
        # Subject type declaration (only once per subject)
        if row.subject_id not in declared_subjects:
            subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            declared_subjects.add(row.subject_id)
        
        # TermLink structure (same as Lambda)
        subjects_ttl.append(f"{termlink_uri} rdf:type phebee:TermLink .")
        subjects_ttl.append(f"{subject_uri} phebee:hasTermLink {termlink_uri} .")
        subjects_ttl.append(f"{termlink_uri} phebee:hasTerm {term_uri} .")
        
        # Add qualifiers if present
        qualifiers_data = getattr(row, 'qualifiers', None)
        if qualifiers_data and qualifiers_data != '[]':
            try:
                import json
                qualifiers = json.loads(qualifiers_data) if isinstance(qualifiers_data, str) else qualifiers_data
            except:
                # Fallback regex parsing for malformed JSON (same as Lambda)
                import re
                matches = re.findall(r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}', qualifiers_data)
                qualifiers = [{'qualifier_type': qt, 'qualifier_value': qv} for qt, qv in matches]
            
            for qualifier in qualifiers:
                try:
                    # Qualifiers are Spark Row objects with qualifier_type and qualifier_value fields
                    qualifier_type = qualifier.qualifier_type
                    qualifier_value = qualifier.qualifier_value
                    
                    if qualifier_value == "true":
                        subjects_ttl.append(f"{termlink_uri} phebee:hasQualifyingTerm phebee:{qualifier_type} .")
                except Exception as e:
                    # Log and skip any problematic qualifiers
                    print(f"Warning: Failed to process qualifier {qualifier}: {e}")
                    continue
    
    if record_count == 0:
        return  # Empty partition
    
    # Write subjects TTL
    if subjects_ttl:
        # Add TTL prefixes
        ttl_prefixes = [
            "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            ""
        ]
        subjects_content = '\n'.join(ttl_prefixes + subjects_ttl)
        subjects_key = f"runs/{run_id}/neptune/subjects/partition_{partition_id}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=subjects_key,
            Body=subjects_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        print(f"Wrote subjects TTL: s3://{bucket}/{subjects_key} ({len(subjects_ttl)} triples)")
    
    print(f"Partition {partition_id} processed {record_count} records")

def main():
    if len(sys.argv) != 7:
        print("Usage: generate_ttl_from_iceberg_emr.py <run_id> <database> <table> <bucket> <mapping_file> <region>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    database = sys.argv[2]
    table = sys.argv[3]
    bucket = sys.argv[4]
    mapping_file = sys.argv[5]  # S3 path to subject_mapping.json from resolve step
    region = sys.argv[6]
    
    print(f"Starting TTL generation for run_id: {run_id}")
    print(f"Reading from: {database}.{table}")
    print(f"Writing to: s3://{bucket}/runs/{run_id}/neptune/")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"GenerateTTLFromIceberg-{run_id}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()
    
    try:
        # Clean up any existing TTL files from previous runs
        print("Cleaning up existing TTL files...")
        s3 = boto3.client('s3', region_name=region)
        
        # List and delete existing TTL files for this run
        for prefix in [f"runs/{run_id}/neptune/subjects/", f"runs/{run_id}/neptune/projects/"]:
            try:
                # Handle pagination for large numbers of files
                continuation_token = None
                while True:
                    list_params = {'Bucket': bucket, 'Prefix': prefix}
                    if continuation_token:
                        list_params['ContinuationToken'] = continuation_token
                    
                    response = s3.list_objects_v2(**list_params)
                    
                    if 'Contents' in response:
                        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.ttl')]
                        if objects_to_delete:
                            # Delete in batches of 1000 (S3 limit)
                            for i in range(0, len(objects_to_delete), 1000):
                                batch = objects_to_delete[i:i+1000]
                                s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                            print(f"Deleted {len(objects_to_delete)} old TTL files from {prefix}")
                    
                    if not response.get('IsTruncated'):
                        break
                    continuation_token = response.get('NextContinuationToken')
                    
            except Exception as e:
                print(f"Warning: Could not clean up {prefix}: {e}")
        
        # Load subject mappings from S3 (from resolve step)
        print("Loading subject mappings from resolve step...")
        subject_mappings = get_subject_mappings_from_s3(bucket, mapping_file, region)
        print(f"Loaded {len(subject_mappings)} subject mappings")
        
        # Debug: Show all subjects in mapping file
        print("DEBUG: All subjects in mapping file:")
        for subject_id, mapping in subject_mappings.items():
            print(f"  {subject_id} -> {mapping['project_id']}/{mapping['project_subject_id']}")
        print("DEBUG: End of mapping file subjects")
        
        
        # Broadcast the mappings to all executors
        broadcast_mappings = spark.sparkContext.broadcast(subject_mappings)
        
        # Read evidence data from Iceberg
        print("Reading evidence data from Iceberg...")
        evidence_df = spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
        filtered_df = evidence_df.filter(f"run_id = '{run_id}'")
        
        record_count = filtered_df.count()
        print(f"Found {record_count} evidence records for run_id: {run_id}")
        
        if record_count == 0:
            print("No records found, skipping TTL generation")
            return
        
        # Generate TTL files using foreachPartition
        print("Generating TTL files...")
        filtered_df.foreachPartition(
            lambda partition: write_ttl_partition(partition, broadcast_mappings, bucket, run_id, region)
        )
        
        # Generate subject-only TTL for all mapped subjects (driver-level, runs once)
        print(f"Generating subject and project nodes for all {len(subject_mappings)} mapped subjects...")
        print("DEBUG: Creating TTL for these subjects:")
        for subject_id, mapping in subject_mappings.items():
            print(f"  Creating TTL for {subject_id} ({mapping['project_subject_id']})")
        
        all_subjects_ttl = [
            "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            ""
        ]
        
        declared_projects = set()
        
        for subject_id, mapping in subject_mappings.items():
            project_id = mapping['project_id']
            project_subject_id = mapping['project_subject_id']
            
            # Subject declaration
            subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
            all_subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            
            # Project relationships
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_uri = f"<{project_uri_base}>"
            project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
            
            # Project type declaration (only once per project)
            if project_id not in declared_projects:
                all_subjects_ttl.append(f"{project_uri} rdf:type phebee:Project .")
                declared_projects.add(project_id)
            
            # Project-subject relationships
            escaped_project_subject_id = project_subject_id.replace('"', '\\"').replace('\\', '\\\\')
            all_subjects_ttl.append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
            all_subjects_ttl.append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
            all_subjects_ttl.append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
            all_subjects_ttl.append(f"{project_subject_id_iri} rdfs:label \"{escaped_project_subject_id}\" .")
        
        # Write all-subjects TTL file to projects folder organized by project_id
        s3 = boto3.client('s3', region_name=region)
        subjects_content = '\n'.join(all_subjects_ttl)
        
        # Group subjects by project_id and write separate TTL files for each project
        projects_by_id = {}
        for subject_id, mapping in subject_mappings.items():
            project_id = mapping['project_id']
            if project_id not in projects_by_id:
                projects_by_id[project_id] = []
            projects_by_id[project_id].append((subject_id, mapping))
        
        # Write TTL file for each project
        for project_id, project_subjects in projects_by_id.items():
            project_ttl = [
                "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
                "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
                ""
            ]
            
            # Add project declaration
            project_uri = f"<http://ods.nationwidechildrens.org/phebee/projects/{project_id}>"
            project_ttl.append(f"{project_uri} rdf:type phebee:Project .")
            
            # Add subjects for this project
            for subject_id, mapping in project_subjects:
                project_subject_id = mapping['project_subject_id']
                
                # Subject declaration
                subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
                project_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
                
                # Project relationships
                project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
                project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
                
                # Project-subject relationships
                escaped_project_subject_id = project_subject_id.replace('"', '\\"').replace('\\', '\\\\')
                project_ttl.append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
                project_ttl.append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
                project_ttl.append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
                project_ttl.append(f"{project_subject_id_iri} rdfs:label \"{escaped_project_subject_id}\" .")
            
            # Write project-specific TTL file
            project_content = '\n'.join(project_ttl)
            subjects_key = f"runs/{run_id}/neptune/projects/{project_id}/all_subjects.ttl"
            s3.put_object(
                Bucket=bucket,
                Key=subjects_key,
                Body=project_content.encode('utf-8'),
                ContentType='text/turtle'
            )
            print(f"Uploaded project TTL: {subjects_key}")
        
        print(f"Generated TTL files for {len(projects_by_id)} projects")
        
        print(f"TTL generation completed for run_id: {run_id}")
        
        # Return summary information
        result = {
            'run_id': run_id,
            'record_count': record_count,
            'ttl_prefix': f"s3://{bucket}/runs/{run_id}/neptune/",
            'status': 'completed'
        }
        
        # Write result to S3 for Step Function to pick up
        s3 = boto3.client('s3', region_name=region)
        result_key = f"runs/{run_id}/ttl_generation_result.json"
        s3.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=json.dumps(result).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Result written to: s3://{bucket}/{result_key}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
