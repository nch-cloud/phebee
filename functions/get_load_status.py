from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.neptune import get_load_status

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    load_job_id = event.get("load_job_id")

    response = get_load_status(load_job_id)

    logger.info(response)

    if not response["status"] == "200 OK":
        raise (Exception("Failed to get load job status"))

    return response
