from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.neptune import start_load

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    source = event.get("source")
    role = event.get("role")
    region = event.get("region")
    format = event.get("format")
    graph_name = event.get("graph_name")

    load_params = {
        "source": source,
        "format": format,
        "iamRoleArn": role,
        "region": region,
        "failOnError": "TRUE",
        "updateSingleCardinalityProperties": "TRUE",
        "queueRequest": "TRUE",
        "parserConfiguration": {
            "baseUri": "http://ods.nationwidechildrens.org/phebee",
            "namedGraphUri": f"http://ods.nationwidechildrens.org/phebee/{graph_name}",
        },
    }

    response = start_load(load_params)

    logger.info(response)

    if not response["status"] == "200 OK":
        raise (Exception("Failed to load RDF/XML data"))

    return response
