import json
import logging
from aws_lambda_powertools import Logger
from phebee.utils.iceberg import materialize_project

logger = Logger()

def lambda_handler(event, context):
    """
    Lambda handler to materialize subject-terms analytical tables for a project.

    This is typically invoked as part of the bulk import state machine after
    Neptune load completes successfully.

    Expected input:
        {
            "project_id": "project-uuid",
            "batch_size": 100  # optional
        }

    Returns:
        {
            "project_id": "project-uuid",
            "subjects_processed": 150,
            "terms_materialized": 450,
            "status": "success"
        }
    """
    try:
        logger.info("Materializing project subject-terms", extra={"event": event})

        # Extract parameters
        project_id = event.get('project_id')
        batch_size = event.get('batch_size', 100)

        if not project_id or project_id == 'null' or project_id == 'None':
            error_msg = "Missing or invalid project_id - cannot materialize subject-terms"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Starting materialization for project {project_id}")

        # Call materialization function
        stats = materialize_project(
            project_id=project_id,
            batch_size=batch_size
        )

        logger.info(f"Materialization completed for project {project_id}", extra={"stats": stats})

        # Return success response
        return {
            "statusCode": 200,
            "project_id": project_id,
            "subjects_processed": stats['subjects_processed'],
            "terms_materialized": stats['terms_materialized'],
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to materialize project: {str(e)}")

        # Return error response that Step Functions can handle
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "project_id": event.get('project_id')
            }),
            "status": "failed",
            "error": str(e)
        }
