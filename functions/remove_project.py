from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import _execute_athena_query
import json
import os

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")

    if not project_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "project_id is required"}),
            "headers": {"Content-Type": "application/json"},
        }

    sparql = f"""
        PREFIX phebee: <{PHEBEE}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        CLEAR GRAPH <{PHEBEE}/projects/{project_id}>
    """

    try:
        # 1. Clear Neptune named graph for this project
        execute_update(sparql)
        logger.info(f"Cleared Neptune graph for project {project_id}")

        # 2. Remove project data from Iceberg by_project_term analytical table
        try:
            database_name = os.environ.get('ICEBERG_DATABASE')
            by_project_term_table = os.environ.get('ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE')

            if database_name and by_project_term_table:
                delete_query = f"""
                DELETE FROM {database_name}.{by_project_term_table}
                WHERE project_id = '{project_id}'
                """
                _execute_athena_query(delete_query)
                logger.info(f"Removed Iceberg analytical data for project {project_id}")
            else:
                logger.warning("Iceberg environment variables not set, skipping analytical table cleanup")
        except Exception as iceberg_error:
            # Log but don't fail the overall operation if Iceberg cleanup fails
            logger.error(f"Failed to clean up Iceberg data for project {project_id}: {iceberg_error}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Project {project_id} successfully removed."}),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        logger.exception("Failed to remove project")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to remove project: {str(e)}"}),
            "headers": {"Content-Type": "application/json"},
        }