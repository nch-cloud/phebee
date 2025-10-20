import os
import json
import logging
import boto3
import time
from typing import List, Optional, Literal
from pydantic import BaseModel, ValidationError
from rdflib import Graph, URIRef, Namespace, RDF, Literal as RdfLiteral
from rdflib.namespace import DCTERMS, XSD
from urllib.parse import quote

from phebee.constants import PHEBEE
from phebee.utils.neptune import start_load
from phebee.utils.sparql import (
    get_subject,
    project_exists,
    create_subject,
    link_subject_to_project,
    infer_evidence_type,
    infer_assertion_type,
    generate_termlink_hash,
    stable_text_annotation_iri,
    build_qualifier_iris
)
from phebee.utils.aws import extract_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
neptune = boto3.client("neptunedata")

BUCKET_NAME = os.environ["PheBeeBucketName"]
REGION = os.environ["Region"]
LOADER_ROLE_ARN = os.environ["LoaderRoleArn"]

PHEBEE_NS = Namespace("http://ods.nationwidechildrens.org/phebee#")
OBO = Namespace("http://purl.obolibrary.org/obo/")

CREATOR_CLASS = {
    "automated": f"{str(PHEBEE_NS)}AutomatedCreator",
    "human": f"{str(PHEBEE_NS)}HumanCreator",
}

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
    contexts: Optional[dict] = None  # Context flags from JSON input (e.g., {"negated": 1, "family": 0, "hypothetical": 0})

class TermLinkInput(BaseModel):
    project_id: str
    project_subject_id: str
    term_iri: str
    evidence: List[ClinicalNoteEvidence] = Field(default_factory=list)

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
    g.bind("dcterms", DCTERMS)
    g.bind("xsd", XSD)

    # Precompute & group by TermLink
    start_precompute_time = time.time()
    termlink_to_annotation_map: dict[str, dict] = {}
    emitted_creators: set[str] = set()

    for entry in entries:
        subject_iri = get_or_create_subject(entry.project_id, entry.project_subject_id)  # str

        for evidence in entry.evidence:
            # Canonical qualifiers from contexts (lowercased, escaped, deduped, sorted)
            qualifiers = build_qualifier_iris(evidence.contexts)  # contexts-only helper

            # Determine source node
            if evidence.type == "clinical_note":
                encounter_iri = f"{subject_iri}/encounter/{evidence.encounter_id}"
                note_iri = f"{encounter_iri}/note/{evidence.clinical_note_id}"
                source_node_iri = note_iri
            else:
                source_node_iri = subject_iri

            # TermLink IRI must use the same canonical qualifiers
            termlink_iri = get_term_link_iri(source_node_iri, entry.term_iri, qualifiers)
            
            if termlink_iri not in termlink_to_annotation_map:
                termlink_to_annotation_map[termlink_iri] = {
                    "source_node_iri": source_node_iri,   # str
                    "subject_iri": subject_iri,           # str (to avoid outer-scope capture)
                    "term_iri": entry.term_iri,           # str
                    "qualifiers": qualifiers,             # canonical list
                    "evidence": [],
                }

            termlink_to_annotation_map[termlink_iri]["evidence"].append(evidence)

    precompute_duration = time.time() - start_precompute_time
    logger.info(
        "Pre-computation completed in %.2f seconds.",
        precompute_duration
    )

    # Qualifier stats (optional)
    qualifier_counts: dict[str, int] = {}
    for _, group in termlink_to_annotation_map.items():
        for q in group["qualifiers"]:
            qualifier_counts[q] = qualifier_counts.get(q, 0) + 1
    if qualifier_counts:
        logger.info("Qualifier distribution: %s", qualifier_counts)

    # RDF generation
    start_rdf_time = time.time()
    new_links_count = 0
    new_encounters_count = 0
    new_notes_count = 0

    encountered_iris: set[str] = set()
    note_iris: set[str] = set()

    for termlink_iri, group in termlink_to_annotation_map.items():
        source_node_iri_ref = URIRef(group["source_node_iri"])
        term_iri_ref = URIRef(group["term_iri"])
        subject_iri_ref = URIRef(group["subject_iri"])
        qualifiers = group["qualifiers"]  # canonical list

        # Ensure Subject type if the source is a bare subject
        if "/subjects/" in str(source_node_iri_ref) and "/encounter/" not in str(source_node_iri_ref):
            g.add((source_node_iri_ref, RDF.type, PHEBEE_NS.Subject))

        new_links_count += 1
        term_link_ref = URIRef(termlink_iri)

        g.add((term_link_ref, RDF.type, PHEBEE_NS.TermLink))
        g.add((term_link_ref, PHEBEE_NS.sourceNode, source_node_iri_ref))
        g.add((term_link_ref, PHEBEE_NS.hasTerm, term_iri_ref))
        
        earliest = min(
            (ev.note_timestamp for ev in group["evidence"] if getattr(ev, "note_timestamp", None)),
            default=None
        )
        if earliest:
            g.add((term_link_ref, DCTERMS.created, RdfLiteral(earliest, datatype=XSD.dateTime)))

        # Back-link for convenience
        g.add((source_node_iri_ref, PHEBEE_NS.hasTermLink, term_link_ref))

        # Qualifier triples on the TermLink
        for q in qualifiers:
            g.add((term_link_ref, PHEBEE_NS.hasQualifyingTerm, URIRef(q)))

        for evidence in group["evidence"]:
            creator_iri = build_creator_iri(
                evidence.evidence_creator_id,
                evidence.evidence_creator_type,          # pass raw; build_creator_iri normalizes type internally
                evidence.evidence_creator_version,       # pass raw; IRIs preserve case
            )

            normalized_type = (evidence.evidence_creator_type or "").strip().lower()
            creator_class_iri = URIRef(CREATOR_CLASS.get(normalized_type, f"{str(PHEBEE_NS)}Creator"))

            # Emit creator node once per batch (no reads)
            emit_creator_once(
                g,
                creator_iri,
                creator_class_iri,
                creator_name=evidence.evidence_creator_name,
                creator_version=evidence.evidence_creator_version,
                creator_id=evidence.evidence_creator_id,
                cache=emitted_creators,
            )

            if evidence.type == "clinical_note":
                subj_str = str(subject_iri_ref)
                encounter_iri_str = f"{subj_str}/encounter/{evidence.encounter_id}"
                note_iri_str = f"{encounter_iri_str}/note/{evidence.clinical_note_id}"
                encounter_ref = URIRef(encounter_iri_str)
                note_ref = URIRef(note_iri_str)

                # Encounter
                if encounter_iri_str not in encountered_iris:
                    g.add((encounter_ref, RDF.type, PHEBEE_NS.Encounter))
                    g.add((encounter_ref, PHEBEE_NS.encounterId, RdfLiteral(evidence.encounter_id)))
                    g.add((encounter_ref, PHEBEE_NS.hasSubject, subject_iri_ref))
                    encountered_iris.add(encounter_iri_str)
                    new_encounters_count += 1
                # else: already emitted in this batch

                # Note
                if note_iri_str not in note_iris:
                    g.add((note_ref, RDF.type, PHEBEE_NS.ClinicalNote))
                    g.add((note_ref, PHEBEE_NS.clinicalNoteId, RdfLiteral(evidence.clinical_note_id)))
                    g.add((note_ref, PHEBEE_NS.hasEncounter, encounter_ref))
                    if evidence.note_timestamp:
                        g.add((note_ref, DCTERMS.created, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))
                        g.add((note_ref, PHEBEE_NS.noteTimestamp, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))
                    if evidence.author_prov_type:
                        g.add((note_ref, PHEBEE_NS.providerType, RdfLiteral(evidence.author_prov_type)))
                    if evidence.author_specialty:
                        g.add((note_ref, PHEBEE_NS.authorSpecialty, RdfLiteral(evidence.author_specialty)))
                    note_iris.add(note_iri_str)
                    new_notes_count += 1

                # Only add hasTermLink from note if it's not the source node
                if str(source_node_iri_ref) != note_iri_str:
                    g.add((note_ref, PHEBEE_NS.hasTermLink, term_link_ref))

                # ---- TextAnnotation (deterministic IRI) ----
                annotation_iri_str = stable_text_annotation_iri(
                    text_source_iri=note_iri_str,
                    term_iri=str(term_iri_ref),
                    creator_iri=str(creator_iri),
                    span_start=evidence.span_start,
                    span_end=evidence.span_end,
                    qualifier_iris=qualifiers,
                )
                annotation_ref = URIRef(annotation_iri_str)

                g.add((annotation_ref, RDF.type, PHEBEE_NS.TextAnnotation))
                g.add((annotation_ref, PHEBEE_NS.textSource, note_ref))
                g.add((annotation_ref, PHEBEE_NS.creator, creator_iri))

                # Domain-time created for the annotation if available
                if evidence.note_timestamp:
                    g.add((annotation_ref, DCTERMS.created, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))

                # Evidence/assertion types
                creator_type_iri = CREATOR_CLASS.get(normalized_type, f"{str(PHEBEE_NS)}Creator")
                text_source_type_iri = f"{str(PHEBEE_NS)}ClinicalNote"
                evidence_type_iri = infer_evidence_type(creator_type_iri, text_source_type_iri)
                assertion_type_iri = infer_assertion_type(creator_type_iri)
                g.add((annotation_ref, PHEBEE_NS.evidenceType, URIRef(evidence_type_iri)))
                g.add((annotation_ref, PHEBEE_NS.assertionType, URIRef(assertion_type_iri)))

                # Spans
                if evidence.span_start is not None:
                    g.add((annotation_ref, PHEBEE_NS.spanStart, RdfLiteral(evidence.span_start, datatype=XSD.integer)))
                if evidence.span_end is not None:
                    g.add((annotation_ref, PHEBEE_NS.spanEnd, RdfLiteral(evidence.span_end, datatype=XSD.integer)))

                # Term relationship + evidence link
                g.add((annotation_ref, PHEBEE_NS.hasTerm, term_iri_ref))
                g.add((term_link_ref, PHEBEE_NS.hasEvidence, annotation_ref))

    rdf_duration = time.time() - start_rdf_time
    triple_count = len(g)
    logger.info(
        "RDF generation completed in %.2f seconds. Generated %s triples for %s new term links, %s new encounters, and %s new clinical notes.",
        rdf_duration, triple_count, new_links_count, new_encounters_count, new_notes_count
    )

    graph_ttl = g.serialize(format="turtle", prefixes={"phebee": PHEBEE_NS, "obo": OBO, "dcterms": DCTERMS, "xsd": XSD})
    total_duration = time.time() - start_total_time
    logger.info("Total RDF generation process completed in %.2f seconds", total_duration)
    return graph_ttl

def build_creator_iri(creator_id: str, creator_type: str, creator_version: str | None) -> URIRef:
    ctype = (creator_type or "").strip().lower()
    cid = quote((creator_id or "").strip(), safe="")
    if ctype == "automated" and creator_version:
        ver = quote(str(creator_version).strip(), safe="")
        return URIRef(f"{PHEBEE}/creator/{cid}/version/{ver}")
    return URIRef(f"{PHEBEE}/creator/{cid}")

def emit_creator_once(
    g: Graph,
    creator_iri: URIRef,
    creator_class_iri: URIRef,
    creator_name: str | None = None,
    creator_version: str | None = None,
    creator_id: str | None = None,
    cache: set[str] = None,
):
    key = str(creator_iri)
    if cache is None:
        raise ValueError("creator cache must be provided")
    if key in cache:
        return
    g.add((creator_iri, RDF.type, creator_class_iri))
    if creator_name:
        g.add((creator_iri, PHEBEE_NS.creatorName, RdfLiteral(creator_name)))
    if creator_version:
        g.add((creator_iri, PHEBEE_NS.creatorVersion, RdfLiteral(creator_version)))
    if creator_id:
        g.add((creator_iri, PHEBEE_NS.creatorId, RdfLiteral(creator_id)))
    cache.add(key)

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
            "queueRequest": "TRUE",
            "parserConfiguration": {
                "baseUri": "http://ods.nationwidechildrens.org/phebee",
                "namedGraphUri": "http://ods.nationwidechildrens.org/phebee/subjects"
            },
            "mode": "AUTO",
            "parallelism": "OVERSUBSCRIBE"
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
