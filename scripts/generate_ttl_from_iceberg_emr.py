#!/usr/bin/env python3
"""
EMR job to generate TTL files from Iceberg evidence table
Reads evidence data and generates Neptune-compatible TTL files
"""

import sys
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from collections import defaultdict
import uuid

def get_subject_mappings(dynamodb_table_name, run_id, region):
    """Load subject mappings from DynamoDB and return as dict"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table_name)
    
    # Get subject mappings for this run
    response = table.get_item(Key={'run_id': run_id, 'data_type': 'subject_mapping'})
    
    if 'Item' not in response:
        raise Exception(f"No subject mappings found for run_id: {run_id}")
    
    subject_mappings = response['Item']['subject_mappings']
    
    # Convert to dict for fast lookup: subject_id -> project_info
    mapping_dict = {}
    for mapping in subject_mappings:
        mapping_dict[mapping['subject_id']] = {
            'project_id': mapping['project_id'],
            'project_subject_id': mapping['project_subject_id']
        }
    
    return mapping_dict

def write_ttl_partition(partition_iterator, broadcast_mappings, bucket, run_id, region):
    """Process a partition and write TTL files to S3"""
    import boto3
    from collections import defaultdict
    
    s3 = boto3.client('s3', region_name=region)
    partition_id = str(uuid.uuid4())[:8]  # Short unique ID for this partition
    
    subjects_ttl = []
    projects_ttl = defaultdict(list)
    record_count = 0
    
    # Get the broadcast mappings
    subject_mappings = broadcast_mappings.value
    
    for row in partition_iterator:
        record_count += 1
        
        # Generate subjects TTL
        subject_uri = f"http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}"
        term_uri = f"<{row.term_iri}>"
        termlink_uri = f"http://ods.nationwidechildrens.org/phebee/termlinks/{row.termlink_id}"
        
        subjects_ttl.append(f"{subject_uri} phebee:hasTerm {term_uri} .")
        subjects_ttl.append(f"{subject_uri} phebee:hasTermLink <{termlink_uri}> .")
        
        # Generate projects TTL
        if row.subject_id in subject_mappings:
            mapping = subject_mappings[row.subject_id]
            project_id = mapping['project_id']
            project_subject_id = mapping['project_subject_id']
            
            project_uri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
            
            projects_ttl[project_id].append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
            projects_ttl[project_id].append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
            projects_ttl[project_id].append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
            projects_ttl[project_id].append(f"{project_subject_id_iri} rdfs:label \"{project_subject_id}\" .")
    
    if record_count == 0:
        return  # Empty partition
    
    # Write subjects TTL
    if subjects_ttl:
        subjects_content = '\n'.join(subjects_ttl)
        subjects_key = f"runs/{run_id}/neptune/subjects/partition_{partition_id}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=subjects_key,
            Body=subjects_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        print(f"Wrote subjects TTL: s3://{bucket}/{subjects_key} ({len(subjects_ttl)} triples)")
    
    # Write projects TTL
    for project_id, project_triples in projects_ttl.items():
        if project_triples:
            project_content = '\n'.join(project_triples)
            project_key = f"runs/{run_id}/neptune/projects/{project_id}/partition_{partition_id}.ttl"
            s3.put_object(
                Bucket=bucket,
                Key=project_key,
                Body=project_content.encode('utf-8'),
                ContentType='text/turtle'
            )
            print(f"Wrote project TTL: s3://{bucket}/{project_key} ({len(project_triples)} triples)")
    
    print(f"Partition {partition_id} processed {record_count} records")

def main():
    if len(sys.argv) != 7:
        print("Usage: generate_ttl_from_iceberg_emr.py <run_id> <database> <table> <bucket> <dynamodb_table> <region>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    database = sys.argv[2]
    table = sys.argv[3]
    bucket = sys.argv[4]
    dynamodb_table = sys.argv[5]
    region = sys.argv[6]
    
    print(f"Starting TTL generation for run_id: {run_id}")
    print(f"Reading from: {database}.{table}")
    print(f"Writing to: s3://{bucket}/runs/{run_id}/neptune/")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"GenerateTTLFromIceberg-{run_id}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()
    
    try:
        # Load subject mappings from DynamoDB
        print("Loading subject mappings from DynamoDB...")
        subject_mappings = get_subject_mappings(dynamodb_table, run_id, region)
        print(f"Loaded {len(subject_mappings)} subject mappings")
        
        # Broadcast the mappings to all executors
        broadcast_mappings = spark.sparkContext.broadcast(subject_mappings)
        
        # Read evidence data from Iceberg
        print("Reading evidence data from Iceberg...")
        evidence_df = spark.read.format("iceberg").load(f"{database}.{table}")
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
