import json
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import create_evidence_record

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)

        # Required fields
        subject_id = body.get("subject_id")
        term_iri = body.get("term_iri")
        creator_id = body.get("creator_id")
        creator_name = body.get("creator_name")
        creator_type = body.get("creator_type", "human")  # Default to human
        evidence_type = body.get("evidence_type", "manual_annotation")

        # Optional fields
        run_id = body.get("run_id")
        batch_id = body.get("batch_id")
        encounter_id = body.get("encounter_id")
        clinical_note_id = body.get("clinical_note_id")
        span_start = body.get("span_start")
        span_end = body.get("span_end")
        qualifiers = body.get("qualifiers", [])
        term_source = body.get("term_source")
        
        # Clinical note context fields
        note_timestamp = body.get("note_timestamp")
        provider_type = body.get("provider_type")
        author_specialty = body.get("author_specialty")
        note_type = body.get("note_type")

        if not subject_id or not term_iri or not creator_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Missing required fields: subject_id, term_iri, and creator_id are required."
                })
            }

        logger.info("Creating evidence for subject %s, term: %s", subject_id, term_iri)

        # Build creator object
        creator = {
            "creator_id": creator_id,
            "creator_type": creator_type or "human"
        }
        if creator_name:
            creator["creator_name"] = creator_name

        # Generate timestamp
        from datetime import datetime
        created_timestamp = datetime.utcnow().isoformat()

        evidence_id = create_evidence_record(
            subject_id=subject_id,
            term_iri=term_iri,
            creator_id=creator_id,
            creator_name=creator_name,
            creator_type=creator_type,
            evidence_type=evidence_type,
            run_id=run_id,
            batch_id=batch_id,
            encounter_id=encounter_id,
            clinical_note_id=clinical_note_id,
            span_start=span_start,
            span_end=span_end,
            qualifiers=qualifiers,
            note_timestamp=note_timestamp,
            provider_type=provider_type,
            author_specialty=author_specialty,
            note_type=note_type,
            term_source=term_source
        )

        # Return the complete evidence record
        evidence_record = {
            "evidence_id": evidence_id,
            "run_id": run_id,
            "batch_id": batch_id,
            "evidence_type": evidence_type,
            "subject_id": subject_id,
            "term_iri": term_iri,
            "creator": creator,
            "created_timestamp": created_timestamp
        }
        
        return {
            "statusCode": 201,
            "body": json.dumps(evidence_record),
            "headers": {"Content-Type": "application/json"}
        }

    except Exception as e:
        logger.exception("Failed to create evidence")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Internal server error: {str(e)}"}),
            "headers": {"Content-Type": "application/json"}
        }
