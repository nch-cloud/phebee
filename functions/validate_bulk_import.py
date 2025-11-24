import json
import logging
import boto3
from phebee.utils.aws import extract_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """Validate bulk import input and check file exists"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        input_path = event.get('input_path')
        
        if not run_id or not input_path:
            raise ValueError("run_id and input_path are required")
        
        # Parse S3 path
        if not input_path.startswith('s3://'):
            raise ValueError("input_path must be an S3 URI")
        
        path_parts = input_path[5:].split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        
        # Check if file exists
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except s3.exceptions.NoSuchKey:
            raise ValueError(f"Input file not found: {input_path}")
        
        # Get file size for validation
        response = s3.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        
        logger.info(f"Validated input file: {input_path}, size: {file_size} bytes")
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'input_path': input_path,
                'bucket': bucket,
                'key': key,
                'file_size': file_size,
                'validated': True
            }
        }
        
    except Exception as e:
        logger.exception("Validation failed")
        return {
            'statusCode': 400,
            'body': {
                'error': str(e),
                'validated': False
            }
        }
