import os
import json
import logging
import boto3
import hashlib
from typing import List, Optional, Literal
from pydantic import BaseModel, ValidationError
from rdflib import Graph, URIRef, Namespace, RDF, Literal as RdfLiteral
from rdflib.namespace import DCTERMS, XSD
import uuid

from phebee.constants import PHEBEE
from phebee.utils.neptune import start_load
from phebee.utils.sparql import (
    get_subject,
    project_exists,
    term_link_exists,
    create_subject,
    link_subject_to_project,
    node_exists,
    create_creator,
    infer_evidence_type,
    infer_assertion_type
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
    encounter_id: str
    evidence_creator_id: str
    evidence_creator_type: str
    evidence_creator_name: Optional[str] = None
    evidence_creator_version: Optional[str] = None
    note_timestamp: Optional[str] = None
    note_type: Optional[str] = None
    author_prov_type: Optional[str] = None
    author_specialty: Optional[str] = None
    span_start: Optional[int] = None
    span_end: Optional[int] = None
    qualifying_terms: Optional[List[str]] = None  # List of qualifying term names (e.g., "negated", "hypothetical")
    contexts: Optional[dict] = None  # Context flags from JSON input (e.g., {"negated": 1, "family": 0, "hypothetical": 0})

class TermLinkInput(BaseModel):
    project_id: str
    project_subject_id: str
    term_iri: str
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
    g.bind("dcterms", DCTERMS)  # Changed from "dc" to "dcterms" for consistency

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
        g.add((term_link_iri, PHEBEE_NS.hasTerm, term_iri))
        g.add((term_link_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))

        for evidence in entry.evidence:
            evidence_creator_iri = create_or_find_creator(evidence.evidence_creator_id, evidence.evidence_creator_version, evidence.evidence_creator_name, evidence.evidence_creator_type, creator_exists_cache)
            
            if evidence.type == "clinical_note":
                encounter_iri = URIRef(f"{subject_iri}/encounter/{evidence.encounter_id}")
                note_iri = URIRef(f"{encounter_iri}/note/{evidence.clinical_note_id}")
                
                # Create the encounter node
                g.add((encounter_iri, RDF.type, PHEBEE_NS.Encounter))
                g.add((encounter_iri, PHEBEE_NS.encounterId, RdfLiteral(evidence.encounter_id)))
                g.add((encounter_iri, PHEBEE_NS.hasSubject, subject_iri))
                g.add((encounter_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
                
                # Add clinical note properties
                g.add((note_iri, RDF.type, PHEBEE_NS.ClinicalNote))
                g.add((note_iri, PHEBEE_NS.clinicalNoteId, RdfLiteral(evidence.clinical_note_id)))
                g.add((note_iri, PHEBEE_NS.hasEncounter, URIRef(encounter_iri)))
                g.add((note_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
                if evidence.note_timestamp:
                    g.add((note_iri, PHEBEE_NS.noteTimestamp, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))
                if evidence.author_prov_type:
                    g.add((note_iri, PHEBEE_NS.providerType, RdfLiteral(evidence.author_prov_type)))
                if evidence.author_specialty:
                    g.add((note_iri, PHEBEE_NS.authorSpecialty, RdfLiteral(evidence.author_specialty)))
                
                # Connect the note to the term link
                g.add((note_iri, PHEBEE_NS.hasTermLink, term_link_iri))

                # Create a TextAnnotation node
                annotation_id = str(uuid.uuid4())
                annotation_iri = URIRef(f"{note_iri}/annotation/{annotation_id}")
                
                # Add TextAnnotation properties
                g.add((annotation_iri, RDF.type, PHEBEE_NS.TextAnnotation))
                g.add((annotation_iri, PHEBEE_NS.textSource, note_iri))
                g.add((annotation_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
                g.add((annotation_iri, PHEBEE_NS.creator, evidence_creator_iri))
                
                # Add evidence type and assertion type based on creator type
                creator_type = f"http://ods.nationwidechildrens.org/phebee#{evidence.evidence_creator_type.capitalize()}Creator"
                text_source_type = "http://ods.nationwidechildrens.org/phebee#ClinicalNote"
                evidence_type_iri = infer_evidence_type(creator_type, text_source_type)
                assertion_type_iri = infer_assertion_type(creator_type)
                g.add((annotation_iri, PHEBEE_NS.evidenceType, URIRef(evidence_type_iri)))
                g.add((annotation_iri, PHEBEE_NS.assertionType, URIRef(assertion_type_iri)))
                
                # Add span information if provided
                if evidence.span_start is not None:
                    g.add((annotation_iri, PHEBEE_NS.spanStart, RdfLiteral(evidence.span_start, datatype=XSD.integer)))
                if evidence.span_end is not None:
                    g.add((annotation_iri, PHEBEE_NS.spanEnd, RdfLiteral(evidence.span_end, datatype=XSD.integer)))
                
                # Link the TextAnnotation to the term
                g.add((annotation_iri, PHEBEE_NS.hasTerm, term_iri))
                
                # Connect the TermLink to the TextAnnotation as evidence
                g.add((term_link_iri, PHEBEE_NS.hasEvidence, annotation_iri))
                
                # Add qualifying terms if provided
                if evidence.qualifying_terms:
                    for qualifier in evidence.qualifying_terms:
                        # Create a fully qualified IRI using the phebee namespace with "qualifier" in the path
                        qualifier_iri = URIRef(f"{PHEBEE}/qualifier/{qualifier}")
                        # Add the qualifying term to the term link
                        g.add((term_link_iri, PHEBEE_NS.hasQualifyingTerm, qualifier_iri))
                
                # Add context flags as qualifying terms if they're set to 1
                if evidence.contexts:
                    for context_type, value in evidence.contexts.items():
                        if value == 1:
                            qualifier_iri = URIRef(f"{PHEBEE}/qualifier/{context_type}")
                            g.add((term_link_iri, PHEBEE_NS.hasQualifyingTerm, qualifier_iri))
                

    graph_ttl = g.serialize(format="turtle", prefixes={"phebee": PHEBEE_NS, "obo": OBO, "dcterms": DCTERMS})

    logger.info(graph_ttl)

    return graph_ttl

def create_or_find_creator(creator_id, creator_version, creator_name, creator_type, creator_exists_cache):
    creator_iri = URIRef(f"{PHEBEE}/creator/{creator_id}")

    # Check if creator already exists.  If not, we need to create it.
    logger.info(f"Checking if evidence creator exists: {creator_iri}")
    if creator_iri not in creator_exists_cache and not node_exists(creator_iri):
        logger.info(f"Creating {creator_type} creator: {creator_id}")
        create_creator(creator_id, creator_type, creator_name, creator_version)
    else:
        logger.info("Creator already exists.")
    creator_exists_cache.append(creator_iri)

    return creator_iri

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
