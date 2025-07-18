import os
import uuid
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["PheBeeBucketName"]
EXPIRATION_SECONDS = 900  # 15 minutes

def lambda_handler(event, context):
    job_id = str(uuid.uuid4())
    object_key = f"bulk-upload/input/{job_id}.json"

    try:
        signed_url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": BUCKET_NAME, "Key": object_key},
            ExpiresIn=EXPIRATION_SECONDS,
            HttpMethod="PUT",
        )

        logger.info("Generated signed upload URL for job_id=%s, key=%s", job_id, object_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "job_id": job_id,
                "upload_url": signed_url,
                "s3_key": object_key,
                "expires_in": EXPIRATION_SECONDS,
            }),
        }

    except Exception as e:
        logger.error("Error generating signed URL: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to generate signed URL"}),
        }