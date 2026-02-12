import os
from aws_lambda_powertools import Logger
from phebee.utils.iceberg import _execute_athena_query

logger = Logger()

def lambda_handler(event, context):
    """
    Delete ontology hierarchy data for a specific version.

    Expected input:
        {
            "ontology_source": "HPO",
            "version": "2024-04-26"
        }

    Returns:
        {
            "ontology_source": "HPO",
            "version": "2024-04-26",
            "status": "success"
        }
    """
    try:
        ontology_source = event['ontology_source']
        version = event['version']

        database_name = os.environ.get('ICEBERG_DATABASE')
        table_name = os.environ.get('ICEBERG_ONTOLOGY_HIERARCHY_TABLE')

        if not database_name or not table_name:
            raise ValueError("ICEBERG_DATABASE and ICEBERG_ONTOLOGY_HIERARCHY_TABLE environment variables are required")

        logger.info(f"Deleting {ontology_source} version {version} from ontology hierarchy table")

        delete_query = f"""
        DELETE FROM {database_name}.{table_name}
        WHERE ontology_source = '{ontology_source}'
          AND version = '{version}'
        """

        _execute_athena_query(delete_query, wait_for_completion=True)

        logger.info(f"Successfully deleted {ontology_source} version {version}")

        return {
            "ontology_source": ontology_source,
            "version": version,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete ontology hierarchy partition: {str(e)}")
        raise
