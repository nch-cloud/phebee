import os, uuid, boto3, json, logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["PheBeeBucketName"]
EXPIRATION_SECONDS = 900  # 15 minutes

def _get_body(event):
    try:
        return json.loads(event.get("body") or "{}")
    except Exception:
        return {}

def lambda_handler(event, context):
    body = _get_body(event)
    run_id = body.get("run_id")
    batch_id = body.get("batch_id")

    if run_id is None or batch_id is None:
        return {"statusCode": 400, "body": json.dumps({"error": "run_id and batch_id are required"})}

    try:
        batch_no = int(batch_id)
    except ValueError:
        return {"statusCode": 400, "body": json.dumps({"error": "batch_id must be an integer"})}

    object_key = f"phebee/runs/{run_id}/input/batch-{batch_no:05d}.json"

    try:
        signed_url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": BUCKET_NAME, "Key": object_key},
            ExpiresIn=EXPIRATION_SECONDS,
            HttpMethod="PUT",
        )
        logger.info("Presigned URL for run_id=%s batch_id=%s key=%s", run_id, batch_id, object_key)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "run_id": run_id,
                "batch_id": batch_no,
                "upload_url": signed_url,
                "s3_key": object_key,
                "expires_in": EXPIRATION_SECONDS,
            }),
        }
    except Exception as e:
        logger.exception("Error generating signed URL")
        return {"statusCode": 500, "body": json.dumps({"error": "Failed to generate signed URL"})}