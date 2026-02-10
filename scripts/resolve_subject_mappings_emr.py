#!/usr/bin/env python3

import argparse
import json
import boto3
import uuid
from typing import Dict, Set, Tuple
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description='Resolve subject mappings using EMR Serverless')
    parser.add_argument('--run-id', required=True, help='Run ID for this bulk import')
    parser.add_argument('--input-path', required=True, help='S3 path to JSONL files')
    parser.add_argument('--mapping-file', required=True, help='S3 path for output mapping file')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name')
    parser.add_argument('--region', required=True, help='AWS region')
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"ResolveSubjectMappings-{args.run_id}") \
        .getOrCreate()
    
    try:
        # Parse mapping file S3 path
        mapping_bucket = args.mapping_file.split('/')[2]
        mapping_key = '/'.join(args.mapping_file.split('/')[3:])
        
        print(f"Processing JSONL files from: {args.input_path}")
        print(f"Output mapping file: {args.mapping_file}")
        
        # Read all JSONL files in parallel
        jsonl_path = f"{args.input_path}/*.json"
        
        try:
            df = spark.read.json(jsonl_path)
            record_count = df.count()
            print(f"Loaded DataFrame with {record_count} total records")
            
            if record_count == 0:
                raise ValueError(f"No records found in JSONL files at {jsonl_path}")
                
        except Exception as e:
            print(f"Failed to read JSONL files from {jsonl_path}: {str(e)}")
            raise ValueError(f"Invalid input path or no accessible JSONL files: {args.input_path}")
        
        # Extract unique project-subject pairs
        unique_pairs_df = df.select("project_id", "project_subject_id").distinct()
        unique_pairs_count = unique_pairs_df.count()
        
        print(f"Found {unique_pairs_count} unique subject pairs")
        
        # Collect to driver (should be manageable size)
        unique_pairs = unique_pairs_df.collect()
        
        print(f"Collected {len(unique_pairs)} pairs to driver")
        
        # Convert to set for processing (reuse Lambda logic exactly)
        project_subject_pairs = set()
        for row in unique_pairs:
            project_subject_pairs.add((row.project_id, row.project_subject_id))
        
        # Initialize AWS clients with region
        dynamodb = boto3.resource('dynamodb', region_name=args.region)
        table = dynamodb.Table(args.dynamodb_table)
        s3 = boto3.client('s3', region_name=args.region)
        
        print(f"Connected to DynamoDB table: {args.dynamodb_table}")
        
        # Resolve subject mappings (EXACT Lambda logic)
        subject_mapping = {}
        
        # Batch get existing mappings
        existing_keys = []
        for project_id, project_subject_id in project_subject_pairs:
            existing_keys.append({
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}'
            })
        
        # Process in batches of 100 (DynamoDB limit)
        for i in range(0, len(existing_keys), 100):
            batch = existing_keys[i:i+100]
            response = dynamodb.batch_get_item(
                RequestItems={
                    args.dynamodb_table: {
                        'Keys': batch
                    }
                }
            )
            
            for item in response.get('Responses', {}).get(args.dynamodb_table, []):
                project_id = item['PK'].replace('PROJECT#', '')
                project_subject_id = item['SK'].replace('SUBJECT#', '')
                subject_mapping[(project_id, project_subject_id)] = item['subject_id']
        
        # Create new mappings for missing pairs
        missing_pairs = project_subject_pairs - set(subject_mapping.keys())
        existing_count = len(subject_mapping)
        new_count = len(missing_pairs)
        
        print(f"Found {existing_count} existing subject mappings")
        print(f"Creating {new_count} new subject mappings")
        
        if missing_pairs:
            # Batch write new mappings
            with table.batch_writer() as batch:
                for project_id, project_subject_id in missing_pairs:
                    subject_id = str(uuid.uuid4())
                    subject_mapping[(project_id, project_subject_id)] = subject_id

                    # Construct full IRI (matching Lambda behavior)
                    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

                    # Write both directions (EXACT Lambda logic)
                    batch.put_item(Item={
                        'PK': f'PROJECT#{project_id}',
                        'SK': f'SUBJECT#{project_subject_id}',
                        'subject_id': subject_id
                    })

                    batch.put_item(Item={
                        'PK': f'SUBJECT#{subject_id}',
                        'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
                    })
        
        # Convert to array of mapping objects (EXACT Lambda format)
        mapping_array = [
            {
                "project_id": project_id,
                "project_subject_id": project_subject_id,
                "subject_id": subject_id
            }
            for (project_id, project_subject_id), subject_id in subject_mapping.items()
        ]
        
        # Write mapping file to S3 (use provided path)
        s3.put_object(
            Bucket=mapping_bucket,
            Key=mapping_key,
            Body=json.dumps(mapping_array).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Wrote subject mapping to {args.mapping_file}")
        
        # Log final results (no need to return complex structure)
        print(f"Subject mapping complete:")
        print(f"  - Total subjects: {len(subject_mapping)}")
        print(f"  - Existing subjects: {existing_count}")
        print(f"  - New subjects: {new_count}")
        print(f"  - Unique pairs: {len(project_subject_pairs)}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
