import os
import json
import logging
import boto3
import hashlib
import time
from typing import List, Optional, Literal
from pydantic import BaseModel, ValidationError
from rdflib import Graph, URIRef, Namespace, RDF, Literal as RdfLiteral
from rdflib.namespace import DCTERMS, XSD
import uuid
from urllib.parse import quote

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
    infer_assertion_type,
    generate_termlink_hash,
    check_existing_term_links,
    check_existing_encounters,
    check_existing_clinical_notes
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

def get_term_link_iri(source_node_iri: str, term_iri: str, qualifiers=None) -> str:
    """
    Generate a deterministic term link IRI based on source node, term, and qualifiers.
    The source node can be a subject, encounter, or clinical note.
    
    Args:
        source_node_iri (str): The IRI of the source node (subject, encounter, or clinical note)
        term_iri (str): The IRI of the term
        qualifiers (list): List of qualifier IRIs
        
    Returns:
        str: A deterministic term link IRI
    """
    termlink_hash = generate_termlink_hash(source_node_iri, term_iri, qualifiers)
    return f"{source_node_iri}/term-link/{termlink_hash}"

def generate_rdf(entries: List[TermLinkInput]) -> str:
    start_total_time = time.time()
    g = Graph()
    g.bind("phebee", PHEBEE_NS)
    g.bind("obo", OBO)
    g.bind("dcterms", DCTERMS)  # Changed from "dc" to "dcterms" for consistency

    # Keep track of what creators we've already verified exist.
    # Bulk uploads are likely to use the same creator repeatedly, so this cuts down on unnecessary Neptune checks.
    creator_exists_cache = []
    
    # Pre-compute all potential term link IRIs and build mapping
    start_precompute_time = time.time()
    potential_termlinks = []
    termlink_to_annotation_map = {}
    
    # Pre-compute all potential encounter IRIs
    potential_encounters = set()  # Use a set to avoid duplicates
    
    # Pre-compute all potential clinical note IRIs
    potential_notes = set()  # Use a set to avoid duplicates
    
    for entry in entries:
        subject_iri = get_or_create_subject(entry.project_id, entry.project_subject_id)
        
        for evidence in entry.evidence:
            # Extract qualifiers from context
            qualifiers = []
            if evidence.contexts:
                for context_type, value in evidence.contexts.items():
                    if value == 1:
                        qualifiers.append(f"{PHEBEE}/qualifier/{context_type}")
            
            # Add explicit qualifying terms if provided
            if evidence.qualifying_terms:
                for qualifier in evidence.qualifying_terms:
                    qualifier_iri = f"{PHEBEE}/qualifier/{qualifier}"
                    if qualifier_iri not in qualifiers:
                        qualifiers.append(qualifier_iri)
            
            # Determine the appropriate source node based on evidence type
            if evidence.type == "clinical_note":
                encounter_iri = f"{subject_iri}/encounter/{evidence.encounter_id}"
                note_iri = f"{encounter_iri}/note/{evidence.clinical_note_id}"
                source_node_iri = note_iri  # Use the clinical note as the source node
                
                # Add to potential encounters and notes
                potential_encounters.add(encounter_iri)
                potential_notes.add(note_iri)
            else:
                # Default to subject if not a clinical note
                source_node_iri = subject_iri
            
            # Generate the term link IRI
            termlink_iri = get_term_link_iri(source_node_iri, entry.term_iri, qualifiers)
            potential_termlinks.append(termlink_iri)
            
            # Group annotations by term link IRI
            if termlink_iri not in termlink_to_annotation_map:
                termlink_to_annotation_map[termlink_iri] = {
                    'source_node_iri': source_node_iri,
                    'term_iri': entry.term_iri,
                    'qualifiers': qualifiers,
                    'evidence': []
                }
            
            termlink_to_annotation_map[termlink_iri]['evidence'].append(evidence)
    
    precompute_duration = time.time() - start_precompute_time
    logger.info("Pre-computation completed in %.2f seconds. Generated %s potential term links, %s potential encounters, and %s potential clinical notes.", 
                precompute_duration, len(potential_termlinks), len(potential_encounters), len(potential_notes))
    
    # Log qualifier statistics
    qualifier_counts = {}
    for termlink_iri, group in termlink_to_annotation_map.items():
        for qualifier in group['qualifiers']:
            qualifier_counts[qualifier] = qualifier_counts.get(qualifier, 0) + 1
    
    if qualifier_counts:
        logger.info("Qualifier distribution: %s", qualifier_counts)
    
    # Check which term links already exist
    start_check_time = time.time()
    try:
        existing_termlinks = check_existing_term_links(potential_termlinks)
        check_duration = time.time() - start_check_time
        logger.info("Term link existence check completed in %.2f seconds. Found %s existing term links out of %s potential links.", 
                   check_duration, len(existing_termlinks), len(potential_termlinks))
    except Exception as e:
        logger.error("Critical error checking existing term links after %.2f seconds: %s", 
                  time.time() - start_check_time, str(e))
        # Re-raise the exception to fail the process
        raise Exception(f"Failed to check existing term links: {str(e)}") from e
    
    # Check which encounters already exist
    start_encounter_check_time = time.time()
    try:
        existing_encounters = check_existing_encounters(list(potential_encounters))
        encounter_check_duration = time.time() - start_encounter_check_time
        logger.info("Encounter existence check completed in %.2f seconds. Found %s existing encounters out of %s potential encounters.", 
                   encounter_check_duration, len(existing_encounters), len(potential_encounters))
    except Exception as e:
        logger.error("Critical error checking existing encounters after %.2f seconds: %s", 
                  time.time() - start_encounter_check_time, str(e))
        # Re-raise the exception to fail the process
        raise Exception(f"Failed to check existing encounters: {str(e)}") from e
    
    # Check which clinical notes already exist
    start_note_check_time = time.time()
    try:
        existing_notes = check_existing_clinical_notes(list(potential_notes))
        note_check_duration = time.time() - start_note_check_time
        logger.info("Clinical note existence check completed in %.2f seconds. Found %s existing clinical notes out of %s potential notes.", 
                   note_check_duration, len(existing_notes), len(potential_notes))
    except Exception as e:
        logger.error("Critical error checking existing clinical notes after %.2f seconds: %s", 
                  time.time() - start_note_check_time, str(e))
        # Re-raise the exception to fail the process
        raise Exception(f"Failed to check existing clinical notes: {str(e)}") from e

    # Start RDF generation
    start_rdf_time = time.time()
    new_links_count = 0
    skipped_links_count = 0
    new_encounters_count = 0
    skipped_encounters_count = 0
    new_notes_count = 0
    skipped_notes_count = 0
    
    # Track encounters and notes we've already processed in this batch
    encountered_iris = set()
    note_iris = set()
    
    for termlink_iri, group in termlink_to_annotation_map.items():
        source_node_iri = URIRef(group['source_node_iri'])
        term_iri = URIRef(group['term_iri'])
        qualifiers = group['qualifiers']
        
        # Ensure the source node exists
        # If it's a subject, add the type
        if "/subjects/" in str(source_node_iri) and "/encounter/" not in str(source_node_iri):
            g.add((source_node_iri, RDF.type, PHEBEE_NS.Subject))
        
        # Skip if this term link already exists
        if termlink_iri in existing_termlinks:
            logger.debug("Skipping existing TermLink: %s", termlink_iri)
            skipped_links_count += 1
            continue
        
        new_links_count += 1
        term_link_iri = URIRef(termlink_iri)
        
        g.add((term_link_iri, RDF.type, PHEBEE_NS.TermLink))
        g.add((term_link_iri, PHEBEE_NS.sourceNode, source_node_iri))
        g.add((term_link_iri, PHEBEE_NS.hasTerm, term_iri))
        g.add((term_link_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
        
        # Connect the source node to the term link
        g.add((source_node_iri, PHEBEE_NS.hasTermLink, term_link_iri))
        
        # Add qualifier triples if any
        for qualifier in qualifiers:
            g.add((term_link_iri, PHEBEE_NS.hasQualifyingTerm, URIRef(qualifier)))

        for evidence in group['evidence']:
            evidence_creator_iri = create_or_find_creator(evidence.evidence_creator_id, evidence.evidence_creator_version, evidence.evidence_creator_name, evidence.evidence_creator_type, creator_exists_cache)
            
            if evidence.type == "clinical_note":
                encounter_iri_str = f"{subject_iri}/encounter/{evidence.encounter_id}"
                encounter_iri = URIRef(encounter_iri_str)
                note_iri_str = f"{encounter_iri_str}/note/{evidence.clinical_note_id}"
                note_iri = URIRef(note_iri_str)
                
                # Only create the encounter if it doesn't already exist in the database
                # AND we haven't already created it in this batch
                if encounter_iri_str not in existing_encounters and encounter_iri_str not in encountered_iris:
                    # Create the encounter node
                    g.add((encounter_iri, RDF.type, PHEBEE_NS.Encounter))
                    g.add((encounter_iri, PHEBEE_NS.encounterId, RdfLiteral(evidence.encounter_id)))
                    g.add((encounter_iri, PHEBEE_NS.hasSubject, subject_iri))
                    g.add((encounter_iri, DCTERMS.created, RdfLiteral(get_current_timestamp(), datatype=XSD.dateTime)))
                    new_encounters_count += 1
                    logger.debug("Created new encounter: %s", encounter_iri_str)
                    
                    # Add to our tracking set
                    encountered_iris.add(encounter_iri_str)
                else:
                    skipped_encounters_count += 1
                    logger.debug("Skipping existing encounter: %s", encounter_iri_str)
                
                # Only create the clinical note if it doesn't already exist in the database
                # AND we haven't already created it in this batch
                if note_iri_str not in existing_notes and note_iri_str not in note_iris:
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
                    
                    new_notes_count += 1
                    logger.debug("Created new clinical note: %s", note_iri_str)
                    
                    # Add to our tracking set
                    note_iris.add(note_iri_str)
                else:
                    skipped_notes_count += 1
                    logger.debug("Skipping existing clinical note: %s", note_iri_str)
                
                # Always connect the note to the term link, even if the note already exists
                # But only if the note is not already the source node (to avoid duplicate hasTermLink properties)
                if str(source_node_iri) != note_iri_str:
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

    rdf_duration = time.time() - start_rdf_time
    triple_count = len(g)
    logger.info("RDF generation completed in %.2f seconds. Generated %s triples for %s new term links, %s new encounters, and %s new clinical notes. Skipped %s existing links, %s existing encounters, and %s existing clinical notes.", 
               rdf_duration, triple_count, new_links_count, new_encounters_count, new_notes_count, skipped_links_count, skipped_encounters_count, skipped_notes_count)

    graph_ttl = g.serialize(format="turtle", prefixes={"phebee": PHEBEE_NS, "obo": OBO, "dcterms": DCTERMS})
    
    total_duration = time.time() - start_total_time
    logger.info("Total RDF generation process completed in %.2f seconds", total_duration)

    return graph_ttl

def create_or_find_creator(creator_id, creator_version, creator_name, creator_type, creator_exists_cache):
    # Create the creator IRI
    creator_id_safe = quote(creator_id, safe="")
    
    # Include version in the creator IRI for automated creators using /version/ path
    if creator_type == "automated" and creator_version:
        version_safe = quote(creator_version, safe="")
        creator_iri = URIRef(f"{PHEBEE}/creator/{creator_id_safe}/version/{version_safe}")
    else:
        creator_iri = URIRef(f"{PHEBEE}/creator/{creator_id_safe}")

    # Check if creator already exists.  If not, we need to create it.
    logger.info("Checking if evidence creator exists: %s", creator_iri)
    if creator_iri not in creator_exists_cache and not node_exists(creator_iri):
        logger.info("Creating %s creator: %s", creator_type, creator_id)
        created_iri = create_creator(creator_id, creator_type, creator_name, creator_version)
        # Verify the IRIs match
        if str(creator_iri) != created_iri:
            logger.warning("Created creator IRI %s doesn't match expected IRI %s", created_iri, creator_iri)
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

        logger.info("Reading JSON from: s3://%s/%s", BUCKET_NAME, s3_key)
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
        logger.info("Uploaded RDF to s3://%s/%s", BUCKET_NAME, ttl_key)

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
