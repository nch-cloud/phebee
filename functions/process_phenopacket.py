"""Process a single phenopacket: create payloads for create_subject and create_term_link calls."""

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

    term_links_payload = create_term_links_payload(
        project_id, project_subject_id, phenopacket
    )

    return {
        "statusCode": 200,
        "subject_payload": subject_payload,
        "term_links_payload": term_links_payload,
    }


def create_term_links_payload(project_id, project_subject_id, phenopacket):
    namespace_to_iri_prefix = create_iri_map(phenopacket)
    creator_iri = (
        "http://ods.nationwidechildrens.org/phebee/creator/phenopacket-importer"
    )

    subject_iri = (
        f"http://ods.nationwidechildrens.org/phebee/subjects/{project_subject_id}"
    )

    term_links = []

    for feature in phenopacket["phenotypicFeatures"]:
        term_iri = to_full_iri(namespace_to_iri_prefix, feature["type"]["id"])

        # TODO: create an evidence node and link to it

        term_links.append(
            {
                "source_node_iri": subject_iri,
                "term_iri": term_iri,
                "creator_iri": creator_iri,
                "evidence_iris": [],
            }
        )

    return term_links


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
