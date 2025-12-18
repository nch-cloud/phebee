import json
import boto3
import uuid
import os
from typing import Dict, Set, Tuple
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Resolve subject mappings for bulk import
    Input: {"run_id": "...", "input_path": "s3://bucket/prefix/"}
    Output: {"run_id": "...", "input_path": "...", "mapping_file": "s3://bucket/prefix/subject_mapping.json"}
    """
    
    run_id = event['run_id']
    input_path = event['input_path']
    
    # Parse S3 path
    bucket = input_path.split('/')[2]
    prefix = '/'.join(input_path.split('/')[3:])
    
    # Remove trailing 'jsonl/' or 'jsonl' from prefix to write mapping at run level
    if prefix.endswith('jsonl/'):
        prefix = prefix[:-6]  # Remove 'jsonl/'
    elif prefix.endswith('jsonl'):
        prefix = prefix[:-4]  # Remove 'jsonl'
    
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['PheBeeDynamoTable'])
    
    try:
        # List all JSONL files in the prefix (with pagination)
        jsonl_files = []
        continuation_token = None
        
        while True:
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            jsonl_files.extend([obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.jsonl') or obj['Key'].endswith('.json')])
            
            if not response.get('IsTruncated'):
                break
            continuation_token = response['NextContinuationToken']
        
        if not jsonl_files:
            raise ValueError(f"No JSONL files found in {input_path}")
        
        print(f"Found {len(jsonl_files)} JSONL files: {jsonl_files}")
        
        # Extract unique project-subject pairs from all files
        project_subject_pairs = set()
        for key in jsonl_files:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')
            
            for line in content.strip().split('\n'):
                if line.strip():
                    record = json.loads(line)
                    project_subject_pairs.add((record['project_id'], record['project_subject_id']))
        
        print(f"Found {len(project_subject_pairs)} unique subject pairs")
        
        # Resolve subject mappings
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
                    os.environ['PheBeeDynamoTable']: {
                        'Keys': batch
                    }
                }
            )
            
            for item in response.get('Responses', {}).get(os.environ['PheBeeDynamoTable'], []):
                project_id = item['PK'].replace('PROJECT#', '')
                project_subject_id = item['SK'].replace('SUBJECT#', '')
                subject_mapping[(project_id, project_subject_id)] = item['subject_id']
        
        # Create new mappings for missing pairs
        missing_pairs = project_subject_pairs - set(subject_mapping.keys())
        if missing_pairs:
            print(f"Creating {len(missing_pairs)} new subject mappings")
            
            # Batch write new mappings
            with table.batch_writer() as batch:
                for project_id, project_subject_id in missing_pairs:
                    subject_id = str(uuid.uuid4())
                    subject_mapping[(project_id, project_subject_id)] = subject_id
                    
                    # Write both directions
                    batch.put_item(Item={
                        'PK': f'PROJECT#{project_id}',
                        'SK': f'SUBJECT#{project_subject_id}',
                        'subject_id': subject_id
                    })
                    
                    batch.put_item(Item={
                        'PK': f'SUBJECT#{subject_id}',
                        'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
                    })
        
        # Convert to array of mapping objects for cleaner format
        mapping_array = [
            {
                "project_id": project_id,
                "project_subject_id": project_subject_id,
                "subject_id": subject_id
            }
            for (project_id, project_subject_id), subject_id in subject_mapping.items()
        ]
        
        # Write mapping file to S3
        clean_prefix = prefix.rstrip('/') + '/' if prefix else ''
        mapping_key = f"{clean_prefix}subject_mapping.json"
        s3.put_object(
            Bucket=bucket,
            Key=mapping_key,
            Body=json.dumps(mapping_array).encode('utf-8'),
            ContentType='application/json'
        )
        
        mapping_file = f"s3://{bucket}/{mapping_key}"
        print(f"Wrote subject mapping to {mapping_file}")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'input_path': input_path,
                'mapping_file': mapping_file,
                'subject_count': len(subject_mapping)
            }
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': run_id
            }
        }
