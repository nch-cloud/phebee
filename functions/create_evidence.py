import json
import os
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import create_evidence_record, count_evidence_by_termlink
from phebee.utils.hash import generate_termlink_hash
from phebee.utils.sparql import create_term_link

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

        # Generate timestamps (use same base time for consistency)
        from datetime import datetime
        now = datetime.utcnow()
        created_timestamp = now.isoformat()
        created_date = now.date().isoformat()

        # Compute termlink_id using the same logic as create_evidence_record
        subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        termlink_id = generate_termlink_hash(subject_iri, term_iri, qualifiers or [])

        # Create evidence record in Iceberg
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

        # Create the term link in Neptune (idempotent - will not recreate if it exists)
        try:
            logger.info(f"Ensuring term link exists in Neptune for termlink {termlink_id}")
            creator_iri = f"http://ods.nationwidechildrens.org/phebee/creators/{creator_id}"

            # Convert qualifier strings to IRIs if needed
            qualifier_iris = []
            for q in (qualifiers or []):
                if q.startswith('http://') or q.startswith('https://'):
                    qualifier_iris.append(q)
                else:
                    # Assume qualifier is a short name, convert to IRI
                    qualifier_iris.append(f"http://ods.nationwidechildrens.org/phebee/qualifier/{q}")

            result = create_term_link(
                subject_iri=subject_iri,
                term_iri=term_iri,
                creator_iri=creator_iri,
                qualifiers=qualifier_iris
            )
            if result.get("created"):
                logger.info(f"Created new term link in Neptune: {result.get('termlink_iri')}")
            else:
                logger.info(f"Term link already exists in Neptune: {result.get('termlink_iri')}")
        except Exception as e:
            # Log but don't fail the evidence creation if term link creation fails
            logger.error(f"Failed to ensure term link in Neptune: {e}")

        # Update subject-terms analytical tables incrementally
        try:
            from phebee.utils.iceberg import update_subject_terms_for_evidence

            update_subject_terms_for_evidence(
                subject_id=subject_id,
                term_iri=term_iri,
                termlink_id=termlink_id,
                qualifiers=qualifiers or [],
                created_date=created_date
            )
            logger.info(f"Updated subject-terms analytical tables for termlink {termlink_id}")
        except Exception as e:
            # Log but don't fail the evidence creation if analytical table update fails
            logger.error(f"Failed to update subject-terms analytical tables: {e}")

        # Return the complete evidence record
        evidence_record = {
            "evidence_id": evidence_id,
            "run_id": run_id,
            "batch_id": batch_id,
            "evidence_type": evidence_type,
            "subject_id": subject_id,
            "term_iri": term_iri,
            "termlink_id": termlink_id,
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
