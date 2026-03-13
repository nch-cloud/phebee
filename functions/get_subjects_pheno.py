import json
import gzip
import base64
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.iceberg import query_subjects_by_project
from phebee.utils.aws import parse_s3_path, get_client, extract_body
from phebee.utils.dynamodb import get_current_term_source_version
from phebee.utils.phenopackets import subjects_to_phenopackets, zip_phenopackets
from phebee.utils.monarch import get_associated_terms
from phebee.constants import PHEBEE

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = get_client("s3")


def lambda_handler(event, context):
    """
    Query subjects in a project with their phenotypes.

    IMPORTANT: This endpoint queries the materialized subject_terms_by_project_term table,
    which only includes subjects that have evidence/phenotypes. Subjects without any
    evidence will NOT appear in results. This is by design for performance - the
    materialized table enables efficient term-based queries and subject retrieval.
    """
    logger.info("Event: %s", event)

    body = extract_body(event)

    project_id = body.get("project_id")
    if not project_id:
        raise ValueError("Missing required parameter 'project_id'.")

    project_iri = f"{PHEBEE}/projects/{project_id}"

    output_type = body.get("output_type", "json")

    term_iri = body.get("term_iri")
    term_source = body.get("term_source", "hpo")
    # Normalize term_source to lowercase for consistency
    if term_source:
        term_source = term_source.lower()
    # Get user-specified version (if provided) or use latest from DynamoDB
    term_source_version = body.get("term_source_version")

    # Fetch latest versions from DynamoDB (used for phenopackets and as default for queries)
    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    # If user didn't specify version, use the latest from DynamoDB based on term_source
    # This avoids redundant Athena queries to discover the latest version
    if not term_source_version:
        if term_source == "hpo":
            term_source_version = hpo_version
        elif term_source == "mondo":
            term_source_version = mondo_version

    project_subject_ids = body.get("project_subject_ids")

    # Qualifier filtering parameter
    include_qualified = body.get("include_qualified", True)

    # Term hierarchy parameter
    include_child_terms = body.get("include_child_terms", True)

    # Monarch association parameter
    term_association_source_entity = body.get("term_association_source_entity")

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

    # Validate mutual exclusivity of term_iri and term_association_source_entity
    if term_iri and term_association_source_entity:
        raise ValueError(
            "Parameters 'term_iri' and 'term_association_source_entity' are mutually exclusive. "
            "Provide only one of these parameters."
        )

    # Validate that term_association_source_entity cannot be used with include_child_terms
    # Monarch associations are already semantically meaningful at their given specificity level.
    # Expanding to descendants would include unrelated "cousin" terms that don't apply to the entity.
    if term_association_source_entity and include_child_terms:
        raise ValueError(
            "Parameter 'include_child_terms' must be false when using 'term_association_source_entity'. "
            "Monarch associations are already provided at appropriate hierarchical levels. "
            "Expanding terms to include descendants would incorrectly match subjects with unrelated phenotypes."
        )

    # Pagination parameters
    limit = body.get("limit", 1000)
    cursor = body.get("cursor")

    # Convert term IRI to term ID if provided, OR fetch associated terms from Monarch
    term_ids = None

    if term_iri:
        # Single term query (existing behavior)
        if '/obo/' in term_iri:
            term_id = term_iri.split('/obo/')[-1].replace('_', ':')
        else:
            term_id = term_iri.split('/')[-1]
        term_ids = [term_id]

    elif term_association_source_entity:
        # Monarch association query (new behavior)
        logger.info(f"Fetching associated terms from Monarch for entity: {term_association_source_entity}")
        try:
            term_ids = get_associated_terms(term_association_source_entity)
            logger.info(f"Found {len(term_ids)} associated terms from Monarch")

            if not term_ids:
                logger.warning(f"No associated terms found for entity {term_association_source_entity}")
                # Return empty result early
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "subjects": [],
                        "n_subjects": 0,
                        "pagination": {
                            "limit": limit,
                            "cursor": None,
                            "next_cursor": None,
                            "has_more": False,
                            "total_count": 0
                        }
                    })
                }

            # Filter out root/universal terms that would cause excessive expansion
            # These terms have massive subtrees and match most subjects
            ROOT_TERM_IDS = {'HP:0000001', 'HP:0000118'}  # CURIE format
            original_count = len(term_ids)
            term_ids = [tid for tid in term_ids if tid not in ROOT_TERM_IDS]

            if len(term_ids) < original_count:
                logger.warning(f"Filtered out {original_count - len(term_ids)} root terms from Monarch results to prevent excessive expansion")

            if not term_ids:
                logger.warning(f"All Monarch terms were root terms that would match all subjects, returning empty results")
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "subjects": [],
                        "n_subjects": 0,
                        "pagination": {
                            "limit": limit,
                            "cursor": None,
                            "next_cursor": None,
                            "has_more": False,
                            "total_count": 0
                        }
                    })
                }

        except Exception as e:
            logger.error(f"Error fetching associated terms from Monarch: {e}")
            raise ValueError(f"Failed to fetch associated terms from Monarch for entity {term_association_source_entity}: {str(e)}")

    # Query Iceberg analytical tables directly
    # Note: This queries subject_terms_by_project_term, which only includes subjects
    # with evidence. Subjects without any phenotypes/evidence will not appear in results.
    offset = int(cursor) if cursor else 0
    result = query_subjects_by_project(
        project_id=project_id,
        term_ids=term_ids,  # Changed from term_id (single) to term_ids (list)
        term_source=term_source,
        term_source_version=term_source_version,
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
