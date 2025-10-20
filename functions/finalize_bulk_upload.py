import os, json, logging, boto3
from phebee.utils.neptune import start_load

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME   = os.environ["PheBeeBucketName"]
REGION        = os.environ["Region"]
LOADER_ROLE_ARN = os.environ["LoaderRoleArn"]

def _list_keys(prefix: str):
    keys = []
    token = None
    while True:
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, ContinuationToken=token) if token else \
               s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        for it in resp.get("Contents", []):
            keys.append(it["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        body = {}

    run_id = body.get("run_id")
    expected_batches = body.get("expected_batches")  # optional int

    if not run_id:
        return {"statusCode": 400, "body": json.dumps({"error": "run_id is required"})}

    base = f"phebee/runs/{run_id}"
    data_prefix = f"{base}/data/"
    prov_prefix = f"{base}/prov/"
    input_prefix = f"{base}/input/"

    inputs = _list_keys(input_prefix)
    datas  = _list_keys(data_prefix)
    provs  = _list_keys(prov_prefix)

    # Basic sanity checks (optional but helpful)
    if expected_batches is not None and len(inputs) != int(expected_batches):
        return {"statusCode": 400, "body": json.dumps({
            "error": "input count mismatch",
            "inputs_found": len(inputs),
            "expected_batches": expected_batches
        })}

    if len(datas) == 0 or len(provs) == 0:
        return {"statusCode": 400, "body": json.dumps({"error": "no outputs to load", "data_files": len(datas), "prov_files": len(provs)})}

    # Start domain (Turtle) load from prefix
    domain_params = {
        "source": f"s3://{BUCKET_NAME}/{data_prefix}",
        "format": "turtle",
        "iamRoleArn": LOADER_ROLE_ARN,
        "region": REGION,
        "failOnError": "TRUE",
        "queueRequest": "TRUE",
        "parserConfiguration": {
            "baseUri": "http://ods.nationwidechildrens.org/phebee",
            "namedGraphUri": "http://ods.nationwidechildrens.org/phebee/subjects"
        },
        "mode": "AUTO",
        "parallelism": "OVERSUBSCRIBE",
    }
    resp_domain = start_load(domain_params)

    # Start provenance (N-Quads) load from prefix
    prov_params = {
        "source": f"s3://{BUCKET_NAME}/{prov_prefix}",
        "format": "nquads",
        "iamRoleArn": LOADER_ROLE_ARN,
        "region": REGION,
        "failOnError": "TRUE",
        "queueRequest": "TRUE",
        "mode": "AUTO",
        "parallelism": "OVERSUBSCRIBE",
    }
    resp_prov = start_load(prov_params)

    logger.info(resp_domain)
    logger.info(resp_prov)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Batch loads started",
            "run_id": run_id,
            "domain_load_id": resp_domain.get("payload", {}).get("loadId"),
            "prov_load_id": resp_prov.get("payload", {}).get("loadId"),
            "domain_files": len(datas),
            "prov_files": len(provs),
            "input_files": len(inputs),
        }),
    }