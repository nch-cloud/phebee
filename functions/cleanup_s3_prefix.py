import boto3
from aws_lambda_powertools import Logger

logger = Logger()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Delete all objects under an S3 prefix.

    Expected input:
        {
            "bucket": "phebee-bucket",
            "prefix": "ontology-hierarchy/batches/HPO/2024-04-26"
        }

    Returns:
        {
            "deleted_count": 170,
            "status": "success"
        }
    """
    try:
        bucket = event['bucket']
        prefix = event['prefix']

        logger.info(f"Cleaning up s3://{bucket}/{prefix}/")

        # List all objects with the prefix
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        deleted_count = 0
        objects_to_delete = []

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                objects_to_delete.append({'Key': obj['Key']})

                # Delete in batches of 1000 (S3 limit)
                if len(objects_to_delete) >= 1000:
                    s3.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(objects_to_delete)
                    objects_to_delete = []

        # Delete remaining objects
        if objects_to_delete:
            s3.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            deleted_count += len(objects_to_delete)

        logger.info(f"Deleted {deleted_count} objects from s3://{bucket}/{prefix}/")

        return {
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to cleanup S3 prefix: {str(e)}")
        raise
