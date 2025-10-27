import os
import json
import logging
import boto3

from phebee.utils.neptune import start_load

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")

BUCKET_NAME     = os.environ["PheBeeBucketName"]
REGION          = os.environ["Region"]
LOADER_ROLE_ARN = os.environ["LoaderRoleArn"]
BULK_LOAD_MONITOR_SFN_ARN = os.environ.get("BulkLoadMonitorSFNArn")


def _list_keys(prefix: str, suffix: str | None = None) -> list[str]:
    """
    List S3 keys under a prefix. Optionally filter by suffix.
    Handles pagination.
    """
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": BUCKET_NAME, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for it in resp.get("Contents", []):
            k = it["Key"]
            if suffix is None or k.endswith(suffix):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def _stem_from_filename(filename: str) -> str:
    """
    Convert a filename to its batch stem:
      batch-00001.json     -> batch-00001
      batch-00001.ttl.gz   -> batch-00001
      batch-00001.nq.gz    -> batch-00001
    """
    if filename.endswith(".json"):
        return filename[:-5]
    if filename.endswith(".ttl.gz"):
        return filename[:-7]
    if filename.endswith(".nq.gz"):
        return filename[:-6]
    # Fallback: drop everything after first dot if any
    return filename.split(".", 1)[0]


def lambda_handler(event, context):
    # Parse body
    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        body = {}

    run_id = body.get("run_id")
    expected_batches = body.get("expected_batches")  # optional int

    # Optional knobs (defaults chosen to match prior behavior)
    mode          = (body.get("mode") or "AUTO").upper()  # "AUTO" or "RESUME"
    parallelism   = (body.get("parallelism") or "OVERSUBSCRIBE").upper()
    fail_on_error = "TRUE" if str(body.get("fail_on_error", "TRUE")).upper() == "TRUE" else "FALSE"
    queue_request = "TRUE" if str(body.get("queue_request", "TRUE")).upper() == "TRUE" else "FALSE"

    if not run_id:
        return {"statusCode": 400, "body": json.dumps({"error": "run_id is required"})}

    base         = f"phebee/runs/{run_id}"
    input_prefix = f"{base}/input/"
    data_prefix  = f"{base}/data/"
    prov_prefix  = f"{base}/prov/"

    # List keys with strict suffix filters
    input_keys = _list_keys(input_prefix, suffix=".json")
    data_keys  = _list_keys(data_prefix,  suffix=".ttl.gz")
    prov_keys  = _list_keys(prov_prefix,  suffix=".nq.gz")

    # Early validation
    if expected_batches is not None and len(input_keys) != int(expected_batches):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "input count mismatch",
                "inputs_found": len(input_keys),
                "expected_batches": int(expected_batches),
            }),
        }

    if len(data_keys) == 0 or len(prov_keys) == 0:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "no outputs to load",
                "data_files": len(data_keys),
                "prov_files": len(prov_keys),
            }),
        }

    # Build stem sets and detect missing/extra outputs
    input_stems = { _stem_from_filename(k.split("/")[-1]) for k in input_keys }
    data_stems  = { _stem_from_filename(k.split("/")[-1]) for k in data_keys }
    prov_stems  = { _stem_from_filename(k.split("/")[-1]) for k in prov_keys }

    missing_data = sorted(input_stems - data_stems)
    missing_prov = sorted(input_stems - prov_stems)
    extra_data   = sorted(data_stems - input_stems)
    extra_prov   = sorted(prov_stems - input_stems)

    if missing_data or missing_prov:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "some batches not produced",
                "missing_data_batches": missing_data,
                "missing_prov_batches": missing_prov,
            }),
        }

    # Not a hard error, but useful to report if extras exist
    if extra_data or extra_prov:
        logger.warning("Extras found: data=%s prov=%s", extra_data, extra_prov)

    # Start domain (Turtle) bulk load from prefix
    domain_params = {
        "source": f"s3://{BUCKET_NAME}/{data_prefix}",
        "format": "turtle",
        "iamRoleArn": LOADER_ROLE_ARN,
        "region": REGION,
        "failOnError": fail_on_error,
        "queueRequest": queue_request,
        "parserConfiguration": {
            "baseUri": "http://ods.nationwidechildrens.org/phebee",
            "namedGraphUri": "http://ods.nationwidechildrens.org/phebee/subjects",
        },
        "mode": mode,                 # "AUTO" or "RESUME"
        "parallelism": parallelism,   # e.g., "OVERSUBSCRIBE"
    }
    resp_domain = start_load(domain_params)

    # Start provenance (N-Quads) bulk load from prefix
    prov_params = {
        "source": f"s3://{BUCKET_NAME}/{prov_prefix}",
        "format": "nquads",
        "iamRoleArn": LOADER_ROLE_ARN,
        "region": REGION,
        "failOnError": fail_on_error,
        "queueRequest": queue_request,
        "mode": mode,
        "parallelism": parallelism,
    }
    resp_prov = start_load(prov_params)

    logger.info(resp_domain)
    logger.info(resp_prov)

    domain_load_id = resp_domain.get("payload", {}).get("loadId")
    prov_load_id = resp_prov.get("payload", {}).get("loadId")

    # Start Step Function to monitor load completion and fire events
    if BULK_LOAD_MONITOR_SFN_ARN and domain_load_id and prov_load_id:
        try:
            stepfunctions.start_execution(
                stateMachineArn=BULK_LOAD_MONITOR_SFN_ARN,
                input=json.dumps({
                    "run_id": run_id,
                    "domain_load_id": domain_load_id,
                    "prov_load_id": prov_load_id
                })
            )
            logger.info(f"Started bulk load monitor for run_id: {run_id}")
        except Exception as e:
            logger.error(f"Failed to start bulk load monitor: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Batch loads started",
            "run_id": run_id,
            "mode": mode,
            "parallelism": parallelism,
            "domain_load_id": domain_load_id,
            "prov_load_id": prov_load_id,
            "counts": {
                "inputs": len(input_keys),
                "data":   len(data_keys),
                "prov":   len(prov_keys),
            },
            "extras": {
                "data_without_input": extra_data,
                "prov_without_input": extra_prov,
            },
        }),
    }