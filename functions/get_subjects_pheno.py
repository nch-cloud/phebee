import json
import gzip
import base64
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.iceberg import query_subjects_by_project
from phebee.utils.aws import parse_s3_path, get_client, extract_body
from phebee.utils.dynamodb import get_current_term_source_version
from phebee.utils.phenopackets import subjects_to_phenopackets, zip_phenopackets
from phebee.constants import PHEBEE

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = get_client("s3")


def lambda_handler(event, context):
    logger.info("Event: %s", event)

    body = extract_body(event)

    project_id = body.get("project_id")
    if not project_id:
        raise ValueError("Missing required parameter 'project_id'.")

    project_iri = f"{PHEBEE}/projects/{project_id}"

    output_type = body.get("output_type", "json")

    term_iri = body.get("term_iri")
    term_source = body.get("term_source", "hpo")
    term_source_version = body.get(
        "term_source_version", get_current_term_source_version(term_source)
    )

    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    project_subject_ids = body.get("project_subject_ids")

    # Qualifier filtering parameter
    include_qualified = body.get("include_qualified", True)

    # Term hierarchy parameter
    include_child_terms = body.get("include_child_terms", True)

    # Validate against root/universal terms that would match all subjects
    # These queries are inefficient and should use unfiltered project query instead
    ROOT_TERMS = {
        "http://purl.obolibrary.org/obo/HP_0000001",  # All (root of HPO)
        "http://purl.obolibrary.org/obo/HP_0000118",  # Phenotypic abnormality (matches nearly all)
    }

    if term_iri and include_child_terms and term_iri in ROOT_TERMS:
        raise ValueError(
            f"Query term '{term_iri}' with include_child_terms=true would match all subjects. "
            f"Use an unfiltered query (omit term_iri) or set include_child_terms=false instead."
        )
    
    # Pagination parameters
    limit = body.get("limit", 1000)
    cursor = body.get("cursor")

    # Convert term IRI to term ID if provided
    term_id = None
    if term_iri:
        if '/obo/' in term_iri:
            term_id = term_iri.split('/obo/')[-1].replace('_', ':')
        else:
            term_id = term_iri.split('/')[-1]

    # Query Iceberg analytical tables directly
    offset = int(cursor) if cursor else 0
    result = query_subjects_by_project(
        project_id=project_id,
        term_id=term_id,
        term_source=term_source,
        include_child_terms=include_child_terms,
        include_qualified=include_qualified,
        project_subject_ids=project_subject_ids,
        limit=limit,
        offset=offset
    )
    
    # Extract subjects and pagination info
    subject_data = result["subjects"]
    pagination = result["pagination"]

    if "phenopacket" in output_type:
        subject_data = subjects_to_phenopackets(subject_data, project_iri, hpo_version, mondo_version)

    if output_type == "phenopacket_zip":
        s3_content = zip_phenopackets(subject_data)
    else:
        s3_content = json.dumps({"subjects": subject_data, "pagination": pagination}, indent=2)

    if "output_s3_path" in body:
        bucket, key = parse_s3_path(body["output_s3_path"])
        s3_client.put_object(Bucket=bucket, Key=key, Body=s3_content)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"s3_path": f"s3://{bucket}/{key}", "n_subjects": len(subject_data), "pagination": pagination}
            ),
        }

    # Compress response for direct API calls
    response_data = {"body": subject_data, "n_subjects": len(subject_data), "pagination": pagination}
    json_str = json.dumps(response_data)
    
    # Compress the JSON response
    compressed_data = gzip.compress(json_str.encode('utf-8'))
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip"
        },
        "body": base64.b64encode(compressed_data).decode('utf-8'),
        "isBase64Encoded": True
    }
