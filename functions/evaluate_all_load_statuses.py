"""
Evaluate All Load Statuses Lambda

Evaluates the status of multiple Neptune bulk loads to determine if all are complete,
any have failed, or loads are still in progress.
"""

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Evaluate multiple Neptune bulk load statuses.

    Input:
        {
            "statuses": [
                {
                    "Payload": {
                        "statusCode": 200,
                        "body": {
                            "load_id": "...",
                            "status": "LOAD_COMPLETED" | "LOAD_IN_PROGRESS" | "LOAD_FAILED"
                        }
                    }
                },
                ...
            ]
        }

    Output:
        {
            "allComplete": true/false,
            "anyFailed": true/false,
            "anyInProgress": true/false,
            "totalLoads": 5,
            "completed": 3,
            "failed": 0,
            "inProgress": 2,
            "details": [...]
        }
    """
    try:
        statuses = event.get('statuses', [])

        if not statuses:
            logger.warning("No statuses provided")
            return {
                "allComplete": True,
                "anyFailed": False,
                "anyInProgress": False,
                "totalLoads": 0,
                "completed": 0,
                "failed": 0,
                "inProgress": 0,
                "details": []
            }

        total_loads = len(statuses)
        completed_count = 0
        failed_count = 0
        in_progress_count = 0
        details = []

        for status_response in statuses:
            try:
                payload = status_response.get('Payload', {})
                body = payload.get('body', {})
                load_id = body.get('load_id', 'unknown')
                status = body.get('status', 'UNKNOWN')

                details.append({
                    'load_id': load_id,
                    'status': status
                })

                if status == 'LOAD_COMPLETED':
                    completed_count += 1
                elif status == 'LOAD_FAILED':
                    failed_count += 1
                    logger.error(f"Load {load_id} failed")
                else:
                    in_progress_count += 1

            except Exception as e:
                logger.error(f"Error parsing status response: {str(e)}", exc_info=True)
                failed_count += 1

        all_complete = (completed_count == total_loads)
        any_failed = (failed_count > 0)
        any_in_progress = (in_progress_count > 0)

        logger.info(f"Load status summary: {completed_count}/{total_loads} complete, "
                   f"{failed_count} failed, {in_progress_count} in progress")

        return {
            "allComplete": all_complete,
            "anyFailed": any_failed,
            "anyInProgress": any_in_progress,
            "totalLoads": total_loads,
            "completed": completed_count,
            "failed": failed_count,
            "inProgress": in_progress_count,
            "details": details
        }

    except Exception as e:
        logger.error(f"Error evaluating load statuses: {str(e)}", exc_info=True)
        raise
