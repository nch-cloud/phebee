"""Process a single phenopacket: create payloads for create_subject and create_evidence calls."""

from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import escape_string
import uuid

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info("Event: ", event)

    phenopacket = event["Phenopacket"]
    project_id = phenopacket["project_id"]
    project_subject_id = escape_string(phenopacket["id"])
    subject_payload = {
        "project_id": project_id,
        "project_subject_id": project_subject_id,
    }

    evidence_payload = create_evidence_payload(
        project_id, project_subject_id, phenopacket
    )

    return {
        "statusCode": 200,
        "subject_payload": subject_payload,
        "evidence_payload": evidence_payload,
    }


def create_evidence_payload(project_id, project_subject_id, phenopacket):
    namespace_to_iri_prefix = create_iri_map(phenopacket)

    # Extract creator from phenopacket metadata
    creator_id = phenopacket.get('metaData', {}).get('createdBy', 'phenopacket-importer')

    # Use temporary identifier - will be replaced with UUID in prepare_evidence_payload
    subject_id = f"{project_id}#{project_subject_id}"

    evidence_records = []

    for feature in phenopacket["phenotypicFeatures"]:
        term_iri = to_full_iri(namespace_to_iri_prefix, feature["type"]["id"])

        # Map excluded field to negated qualifier
        qualifiers = []
        if feature.get("excluded", False):
            qualifiers.append("negated")

        # Note: Phenopacket modifiers are not currently mapped to PheBee qualifiers
        # This could be added in the future if needed

        evidence_records.append(
            {
                "subject_id": subject_id,  # Temporary - will be replaced with UUID
                "term_iri": term_iri,
                "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000311",  # Imported information
                "creator_id": creator_id,
                "creator_type": "system",  # Phenopackets are imported, not manually annotated
                "qualifiers": qualifiers,
            }
        )

    return evidence_records


def create_iri_map(phenopacket):
    namespace_to_iri_prefix = {}
    for resource in phenopacket["metaData"]["resources"]:
        namespace_to_iri_prefix[resource["namespacePrefix"]] = resource["iriPrefix"]
    return namespace_to_iri_prefix


def to_full_iri(namespace_to_iri_prefix, term_iri):
    for namespace_prefix, iri_prefix in namespace_to_iri_prefix.items():
        if term_iri.startswith(namespace_prefix):
            return term_iri.replace(namespace_prefix + ":", iri_prefix)
    return term_iri
