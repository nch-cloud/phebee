import requests
import json
import os
from aws_lambda_powertools import Metrics, Logger, Tracer
import boto3
from urllib.parse import urlparse
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from types import SimpleNamespace
from time import sleep
from .aws import exponential_backoff

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def execute_update(query: str):
    logger.info(query)

    response = make_signed_request("POST", "sparqlupdate", query)

    logger.info(response)

    return json.loads(response)


def execute_query(query: str):
    logger.info(query)

    response = make_signed_request("POST", "sparql", query)

    logger.info(response)

    return json.loads(response)


def start_load(load_params: dict):
    logger.info("Starting Neptune load...")
    logger.info(load_params)

    response = make_signed_request("POST", "loader", json.dumps(load_params))

    logger.info(response)

    return json.loads(response)


def get_load_status(load_job_id: str):
    logger.info(f"Getting load status: {load_job_id}")

    response = make_signed_request("GET", "loader", load_job_id)

    logger.info(response)

    return json.loads(response)


def reset_neptune_database():
    print("Resetting database...")

    response = make_signed_request(
        "POST", "system", {"action": "initiateDatabaseReset"}
    )

    logger.info(response)

    if json.loads(response)["status"] != "200 OK":
        raise Exception("Failed to initiate database reset.")

    payload = json.loads(response)["payload"]
    reset_token = payload["token"]

    print(f"Reset token: {reset_token}")

    response = make_signed_request(
        "POST", "system", {"action": "performDatabaseReset", "token": reset_token}
    )

    if json.loads(response)["status"] != "200 OK":
        raise Exception("Failed to perform database reset.")

    wait_for_cluster_availability()
    logger.info("Cluster is available.")

    wait_for_sparql_availability()
    logger.info("SPARQL endpoint is available.")


def wait_for_cluster_availability():
    max_status_retries = 20
    initial_delay = 10
    max_sleep_duration = 60

    status_attempts = 0
    status = get_cluster_status()

    while status != "available" and status_attempts < max_status_retries:
        sleep_duration = exponential_backoff(
            status_attempts, initial_delay, max_sleep_duration
        )
        sleep(sleep_duration)
        status_attempts += 1
        status = get_cluster_status()
        print(f"Attempt {status_attempts}: Cluster status is '{status}'")

    if status_attempts >= max_status_retries:
        raise TimeoutError("Exceeded maximum attempts to retrieve cluster status.")


def wait_for_sparql_availability():
    print("Waiting for SPARQL availability...")
    max_sparql_retries = 12
    sparql_retries = 0
    initial_delay = 10
    max_sleep_duration = 60

    while sparql_retries < max_sparql_retries:
        sparql_retries += 1
        print(f"Checking SPARQL availability (attempt {sparql_retries})...")
        if check_sparql_availability():
            return
        if sparql_retries < max_sparql_retries - 1:
            sleep_duration = exponential_backoff(
                sparql_retries, initial_delay, max_sleep_duration
            )
            sleep(sleep_duration)
    raise Exception(
        "Exceeded maximum number of retries to connect to Neptune SPARQL endpoint."
    )


def check_sparql_availability():
    # Check SPARQL endpoint
    sparql_query = "SELECT * WHERE {?s ?p ?o} LIMIT 1"
    try:
        print("Executing query...")
        response = execute_query(sparql_query)
        print(response)

        print("Neptune SPARQL endpoint is available.")
        return True
    except Exception as e:
        print(f"Error connecting to Neptune SPARQL endpoint: {e}")
        return False


def get_cluster_status():
    logger.info("Getting database status...")

    region = os.environ["Region"]
    cluster_identifier = os.environ["NeptuneClusterIdentifier"]

    client = boto3.client("neptune", region_name=region)
    response = client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)

    cluster_status = response["DBClusters"][0]["Status"]
    print(f"Cluster Status: {cluster_status}")

    return cluster_status


def make_signed_request(method, query_type, query):
    service = "neptune-db"

    neptune_endpoint = os.environ["NeptuneEndpoint"]
    neptune_host = urlparse(neptune_endpoint).hostname

    print()
    print("+++++ USER INPUT +++++")
    print("host = " + neptune_host)
    print("method = " + method)
    print("query_type = " + query_type)
    if isinstance(query, str):
        print("query = " + query)
    else:
        print("query (json) = " + json.dumps(query))

    # validate input
    validate_input(method, query_type)

    # get canonical_uri and payload
    canonical_uri, payload = get_canonical_uri_and_payload(query_type, query, method)

    # assign payload to data or params
    data = payload if method == "POST" else None
    params = payload if method == "GET" else None

    # create request URL
    request_url = neptune_endpoint + canonical_uri

    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    region = os.environ["Region"]

    # create and sign request
    creds = SimpleNamespace(
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
        token=credentials.token,
        region=region,
    )

    request = AWSRequest(method=method, url=request_url, data=data, params=params)
    SigV4Auth(creds, service, region).add_auth(request)

    r = None

    # ************* SEND THE REQUEST *************
    if method == "GET":
        print("++++ BEGIN GET REQUEST +++++")
        print("Request URL = " + request_url)
        print("params = " + json.dumps(params))
        r = requests.get(
            request_url, headers=request.headers, verify=False, params=params
        )

    elif method == "POST":
        print("\n+++++ BEGIN POST REQUEST +++++")
        print("Request URL = " + request_url)
        if query_type == "loader":
            request.headers["Content-type"] = "application/json"
        elif query_type == "sparql" or query_type == "sparqlupdate":
            request.headers["Content-type"] = "application/x-www-form-urlencoded"
        r = requests.post(request_url, headers=request.headers, verify=False, data=data)

    else:
        print('Request method is neither "GET" nor "POST", something is wrong here.')

    if r is not None:
        print("+++++ RESPONSE +++++")
        print("Response code: %d\n" % r.status_code)
        print(r)

        if not r.status_code == 200:
            raise (Exception(f"Query failed with status {r.status_code}: {r.text}"))

        response = r.text
        r.close()
        print(response)

        return response


def validate_input(method, query_type):
    # Supporting GET and POST for now:
    if method != "GET" and method != "POST":
        raise (
            Exception(
                'First parameter must be "GET" or "POST", but is "' + method + '".'
            )
        )

    # SPARQL UPDATE requires POST
    if method == "GET" and query_type == "sparqlupdate":
        raise (
            Exception("SPARQL UPDATE is not supported in GET mode. Please choose POST.")
        )


def get_canonical_uri_and_payload(query_type, query, method):
    # Set the stack and payload depending on query_type.
    if query_type == "sparql":
        canonical_uri = "/sparql/"
        if method == "POST":
            from urllib.parse import urlencode
            payload = urlencode({"query": query})
        else:
            payload = {"query": query}

    elif query_type == "sparqlupdate":
        canonical_uri = "/sparql/"
        if method == "POST":
            from urllib.parse import urlencode
            payload = urlencode({"update": query})
        else:
            payload = {"update": query}

    elif query_type == "gremlin":
        canonical_uri = "/gremlin/"
        payload = {"gremlin": query}
        if method == "POST":
            payload = json.dumps(payload)

    elif query_type == "openCypher":
        canonical_uri = "/openCypher/"
        payload = {"query": query}

    elif query_type == "loader":
        if method == "POST":
            # Calling the loader with POST initiates a load job with the specified parameters
            canonical_uri = "/loader/"
            payload = query
        else:
            # Calling the loader with GET returns the status of a specific job
            canonical_uri = f"/loader/{query}"
            payload = {}

    elif query_type == "status":
        canonical_uri = "/status"
        payload = {}

    elif query_type == "system":
        canonical_uri = "/system"
        payload = query

    elif query_type == "gremlin/status":
        canonical_uri = "/gremlin/status/"
        payload = {}

    elif query_type == "openCypher/status":
        canonical_uri = "/openCypher/status/"
        payload = {}

    elif query_type == "sparql/status":
        canonical_uri = "/sparql/status/"
        payload = {}

    else:
        raise (
            Exception(
                'Third parameter should be from ["gremlin", "sparql", "sparqlupdate", "loader", "status", "system"] but is "'
                + query_type
                + '".'
            )
        )

    ## return output as tuple
    return canonical_uri, payload
