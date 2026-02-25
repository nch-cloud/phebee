import json
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import get_evidence_record, delete_evidence_record, count_evidence_by_termlink
from phebee.utils.sparql import delete_term_link

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)
        evidence_id = body.get("evidence_id")

        if not evidence_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "evidence_id is required"}),
                "headers": {"Content-Type": "application/json"}
            }

        # Get the evidence record to extract termlink_id before deleting
        evidence = get_evidence_record(evidence_id)

        if not evidence:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Evidence not found"}),
                "headers": {"Content-Type": "application/json"}
            }

        termlink_id = evidence.get("termlink_id")
        subject_id = evidence.get("subject_id")

        logger.info(f"Deleting evidence {evidence_id} with termlink {termlink_id}")

        # Delete the evidence record from Iceberg
        success = delete_evidence_record(evidence_id)

        if not success:
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Failed to delete evidence"}),
                "headers": {"Content-Type": "application/json"}
            }

        # Check if this was the last evidence for this termlink
        # If so, delete the term link from Neptune
        # Also update subject-terms analytical tables
        if termlink_id:
            try:
                evidence_count = count_evidence_by_termlink(termlink_id)
                logger.info(f"Remaining evidence count for termlink {termlink_id}: {evidence_count}")

                # Update subject-terms analytical tables incrementally
                try:
                    from phebee.utils.iceberg import remove_subject_terms_for_evidence

                    remove_subject_terms_for_evidence(
                        subject_id=subject_id,
                        termlink_id=termlink_id,
                        evidence_count_remaining=evidence_count
                    )
                    logger.info(f"Updated subject-terms analytical tables for termlink {termlink_id}")
                except Exception as e:
                    # Log but don't fail the evidence deletion if analytical table update fails
                    logger.error(f"Failed to update subject-terms analytical tables: {e}")

                if evidence_count == 0:
                    # This was the last evidence - delete term link from Neptune
                    logger.info(f"Deleting term link from Neptune for termlink {termlink_id}")

                    # Construct termlink IRI from subject_id and termlink_id
                    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
                    termlink_iri = f"{subject_iri}/term-link/{termlink_id}"

                    delete_term_link(termlink_iri)
                    logger.info(f"Term link deleted: {termlink_iri}")
            except Exception as e:
                # Log but don't fail the evidence deletion if term link deletion fails
                logger.error(f"Failed to delete term link from Neptune: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Evidence deleted successfully"}),
            "headers": {"Content-Type": "application/json"}
        }

    except Exception as e:
        logger.exception("Failed to delete evidence")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Internal server error: {str(e)}"}),
            "headers": {"Content-Type": "application/json"}
        }
