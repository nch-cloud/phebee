import os
import json
import logging
import boto3
import hashlib
from typing import List, Optional, Literal
from pydantic import BaseModel, ValidationError
from rdflib import Graph, URIRef, Namespace, RDF, Literal as RdfLiteral
from rdflib.namespace import DCTERMS, XSD

from phebee.constants import PHEBEE
from phebee.utils.neptune import start_load
from phebee.utils.sparql import (
    get_subject,
    project_exists,
    term_link_exists,
    create_subject,
    link_subject_to_project,
    node_exists,
    create_creator
)
from phebee.utils.aws import get_current_timestamp, extract_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
neptune = boto3.client("neptunedata")

BUCKET_NAME = os.environ["PheBeeBucketName"]
REGION = os.environ["Region"]
LOADER_ROLE_ARN = os.environ["LoaderRoleArn"]

PHEBEE_NS = Namespace("http://ods.nationwidechildrens.org/phebee#")
OBO = "http://purl.obolibrary.org/obo/"

# -------------------
# Pydantic Models
# -------------------

class ClinicalNoteEvidence(BaseModel):
    type: Literal["clinical_note"]
    clinical_note_id: str
    encounter_iri: str
    creator_id: str
    creator_type: str
    creator_name: Optional[str] = None
    creator_version: Optional[str] = None
    note_timestamp: Optional[str] = None

class TermLinkInput(BaseModel):
    project_id: str
    project_subject_id: str
    term_iri: str
    creator_id: str
    creator_type: str
    creator_name: Optional[str] = None
    creator_version: Optional[str] = None
    evidence: List[ClinicalNoteEvidence] = []

# -------------------
# Utility Functions
# -------------------

def get_or_create_subject(project_id: str, project_subject_id: str) -> str:
    if not project_exists(project_id):
        raise ValueError(f"Project ID not found: {project_id}")

    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    subject = get_subject(project_subject_iri)
    if subject:
        subject_iri = subject["subject_iri"]
    else:
        subject_iri = create_subject(project_id, project_subject_id)

    link_subject_to_project(subject_iri, project_id, project_subject_id)
    return subject_iri

def get_term_link_iri(source_node_iri: str, term_iri: str) -> str:
    key = f"{source_node_iri}|{term_iri}"
    digest = hashlib.sha256(key.encode()).hexdigest()
    return f"{source_node_iri}/term-link/{digest}"

def generate_rdf(entries: List[TermLinkInput]) -> str:
    g = Graph()
    g.bind("phebee", PHEBEE_NS)
    g.bind("obo", OBO)
    g.bind("dc", DCTERMS)

    # Keep track of what creators we've already verified exist.
    # Bulk uploads are likely to use the same creator repeatedly, so this cuts down on unnecessary Neptune checks.
    creator_exists_cache = []

    for entry in entries:
        subject_iri = URIRef(get_or_create_subject(entry.project_id, entry.project_subject_id))
        g.add((subject_iri, RDF.type, PHEBEE_NS.Subject))

        term_iri = URIRef(entry.term_iri)
        term_link_iri_str = get_term_link_iri(str(subject_iri), str(term_iri))

        if term_link_exists(str(subject_iri), str(term_iri))["link_exists"]:
            logger.info(f"Skipping existing TermLink: {term_link_iri_str}")
            continue

        term_link_iri = URIRef(term_link_iri_str)

        g.add((term_link_iri, RDF.type, PHEBEE_NS.TermLink))
        g.add((term_link_iri, PHEBEE_NS.sourceNode, subject_iri))
        g.add((term_link_iri, PHEBEE_NS.hasTerm, term_iri))
        g.add((subject_iri, PHEBEE_NS.hasTermLink, term_link_iri))

        creator_iri = URIRef(f"{PHEBEE}/creator/{entry.creator_id}")
        # Check if creator already exists.  If not, we need to create it.
        logger.info(f"Checking if creator exists: {creator_iri}")
        if creator_iri not in creator_exists_cache and not node_exists(creator_iri):
            logger.info(f"Creating {entry.creator_type} creator: {entry.creator_id}")
            create_creator(entry.creator_id, entry.creator_type, entry.creator_name, entry.creator_version)
        else:
            logger.info("Creator already exists.")
        creator_exists_cache.append(creator_iri)

        g.add((term_link_iri, PHEBEE_NS.creator, creator_iri))
        g.add((term_link_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))

        for evidence in entry.evidence:
            creator_iri = URIRef(f"{PHEBEE}/creator/{evidence.creator_id}")
            # Check if creator already exists.  If not, we need to create it.
            logger.info(f"Checking if creator exists: {creator_iri}")
            if creator_iri not in creator_exists_cache and not node_exists(creator_iri):
                logger.info(f"Creating {evidence.creator_type} creator: {evidence.creator_id}")
                create_creator(evidence.creator_id, evidence.creator_type, evidence.creator_name, evidence.creator_version)
            else:
                logger.info("Creator already exists.")
            creator_exists_cache.append(creator_iri)
            
            if evidence.type == "clinical_note":
                note_iri = URIRef(f"{evidence.encounter_iri}/note/{evidence.clinical_note_id}")
                g.add((note_iri, RDF.type, PHEBEE_NS.ClinicalNote))
                g.add((note_iri, PHEBEE_NS.clinicalNoteId, RdfLiteral(evidence.clinical_note_id)))
                g.add((note_iri, PHEBEE_NS.hasEncounter, URIRef(evidence.encounter_iri)))
                g.add((note_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
                if evidence.note_timestamp:
                    g.add((note_iri, PHEBEE_NS.noteTimestamp, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))
                g.add((term_link_iri, PHEBEE_NS.hasEvidence, note_iri))
                g.add((note_iri, PHEBEE_NS.creator, creator_iri))

    graph_ttl = g.serialize(format="turtle")

    logger.info(graph_ttl)

    return graph_ttl

# -------------------
# Lambda Handler
# -------------------

def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)
        s3_key = body.get("s3_key")
        if not s3_key:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 's3_key'"})}

        logger.info(f"Reading JSON from: s3://{BUCKET_NAME}/{s3_key}")
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        raw_data = obj["Body"].read().decode("utf-8")

        try:
            data = json.loads(raw_data)
            if not isinstance(data, list):
                raise ValueError("Expected a list of term link entries")
            validated = [TermLinkInput(**d) for d in data]
        except (ValidationError, ValueError) as ve:
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON", "details": str(ve)})}

        turtle = generate_rdf(validated)
        ttl_key = s3_key.replace("input/", "rdf/").replace(".json", ".ttl")
        s3.put_object(Bucket=BUCKET_NAME, Key=ttl_key, Body=turtle.encode("utf-8"))
        logger.info(f"Uploaded RDF to s3://{BUCKET_NAME}/{ttl_key}")

        s3_uri = f"s3://{BUCKET_NAME}/{ttl_key}"

        load_params = {
            "source": s3_uri,
            "format": "turtle",
            "iamRoleArn": LOADER_ROLE_ARN,
            "region": REGION,
            "failOnError": "TRUE",
            "updateSingleCardinalityProperties": "TRUE",
            "queueRequest": "TRUE",
            "parserConfiguration": {
                "baseUri": "http://ods.nationwidechildrens.org/phebee",
                "namedGraphUri": "http://ods.nationwidechildrens.org/phebee/subjects"
            }
        }

        response = start_load(load_params)

        logger.info(response)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Bulk load started",
                "load_id": response.get("payload")["loadId"],
                "status": response.get("status")
            }),
        }

    except Exception as e:
        logger.exception("Bulk upload failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Bulk upload failed", "details": str(e)})
        }
