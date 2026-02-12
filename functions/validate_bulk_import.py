import json
import logging
import boto3

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
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        # Extract project_id from path (assumes format: projects/{project_id}/runs/{run_id}/)
        project_id = None
        if 'projects/' in prefix:
            parts = prefix.split('/')
            try:
                project_idx = parts.index('projects')
                if project_idx + 1 < len(parts):
                    project_id = parts[project_idx + 1]
                    logger.info(f"Extracted project_id from path: {project_id}")
            except (ValueError, IndexError):
                logger.warning(f"Could not extract project_id from path: {prefix}")

        # List JSONL files in the prefix
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

            jsonl_files = [obj['Key'] for obj in response.get('Contents', []) if '/jsonl/' in obj['Key'] and (obj['Key'].endswith('.jsonl') or obj['Key'].endswith('.json'))]

            if not jsonl_files:
                raise ValueError(f"No JSONL files found in: {input_path}")

            # Calculate total size
            total_size = sum(obj['Size'] for obj in response.get('Contents', []) if '/jsonl/' in obj['Key'] and (obj['Key'].endswith('.jsonl') or obj['Key'].endswith('.json')))

            logger.info(f"Validated input directory: {input_path}, found {len(jsonl_files)} JSONL files, total size: {total_size} bytes")
            
        except Exception as e:
            if "NoSuchBucket" in str(e):
                raise ValueError(f"Bucket not found: {bucket}")
            raise ValueError(f"Error accessing input path: {input_path} - {str(e)}")
        
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'input_path': input_path,
                'bucket': bucket,
                'prefix': prefix,
                'project_id': project_id,
                'jsonl_files': [f"s3://{bucket}/{file_key}" for file_key in jsonl_files],
                'total_size': total_size,
                'validated': True
            }
        }
        
    except Exception as e:
        logger.exception("Validation failed")
        raise Exception(f"Validation failed: {str(e)}")
