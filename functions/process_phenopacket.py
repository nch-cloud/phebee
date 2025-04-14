""" Process a single phenopacket: create payloads for create_subject and create_subject_term_link calls.
"""

from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import escape_string

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
    evidence_payload = {"evidence_list": create_evidence_list(project_id, phenopacket)}

    return {
        "statusCode": 200,
        "subject_payload": subject_payload,
        "evidence_payload": evidence_payload,
    }


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


def create_evidence_list(project_id, phenopacket):
    namespace_to_iri_prefix = create_iri_map(phenopacket)
    evidence_list = []

    for feature in phenopacket["phenotypicFeatures"]:
        evidence = {
            "excluded": str(feature["excluded"]) if "excluded" in feature else "False",
            "creator": phenopacket["metaData"]["createdBy"],
            "evidence_created": phenopacket["metaData"]["created"],
            "evidence_type": "http://purl.obolibrary.org/obo/ECO_0000218",
            "assertion_method": "http://purl.obolibrary.org/obo/ECO_0000218",
        }

        evidence_list.append(
            {
                "project_id": project_id,
                "project_subject_id": escape_string(phenopacket["id"]),
                "term_iri": to_full_iri(namespace_to_iri_prefix, feature["type"]["id"]),
                "evidence": evidence,
            }
        )

    return evidence_list
