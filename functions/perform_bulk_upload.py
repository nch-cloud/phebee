import os
import io
import json
import gzip
import time
import uuid
import logging
from datetime import datetime
from typing import List, Optional, Literal, Sequence, Tuple, Dict, Set

import boto3
from pydantic import BaseModel, Field, ValidationError
from rdflib import (
    Graph,
    ConjunctiveGraph,
    URIRef,
    Namespace,
    RDF,
    Literal as RdfLiteral,
)
from rdflib.namespace import DCTERMS, XSD
from urllib.parse import quote

from phebee.constants import PHEBEE
from phebee.utils.sparql import (
    infer_evidence_type,
    infer_assertion_type,
    generate_termlink_hash,
    stable_text_annotation_iri,
    build_qualifier_iris,
)
from phebee.utils.aws import extract_body
from phebee.utils.dynamodb import resolve_subjects  # <-- NEW

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["PheBeeBucketName"]

PHEBEE_NS = Namespace("http://ods.nationwidechildrens.org/phebee#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
PROV = Namespace("http://www.w3.org/ns/prov#")

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
    contexts: Optional[dict] = None  # e.g., {"negated": 1, "family": 0, "hypothetical": 0}

class TermLinkInput(BaseModel):
    project_id: str
    project_subject_id: str
    term_iri: str
    evidence: List[ClinicalNoteEvidence] = Field(default_factory=list)

# -------------------
# Helpers
# -------------------

def _put_gzip_text(bucket: str, key: str, text: str, content_type: str):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(text.encode("utf-8"))
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentEncoding="gzip",
        ContentType=content_type,
    )

def _put_gzip_bytes(bucket: str, key: str, raw_bytes: bytes, content_type: str):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(raw_bytes)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentEncoding="gzip",
        ContentType=content_type,
    )

def build_creator_iri(creator_id: str, creator_type: str, creator_version: Optional[str]) -> URIRef:
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
    creator_name: Optional[str] = None,
    creator_version: Optional[str] = None,
    creator_id: Optional[str] = None,
    cache: Optional[set] = None,
):
    if cache is None:
        raise ValueError("creator cache must be provided")
    key = str(creator_iri)
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

def get_term_link_iri(source_node_iri: str, term_iri: str, qualifiers=None) -> str:
    """
    Deterministic TermLink IRI from: source node + term + qualifiers
    """
    termlink_hash = generate_termlink_hash(source_node_iri, term_iri, qualifiers)
    return f"{source_node_iri}/term-link/{termlink_hash}"

# -------------------
# RDF Generation (Domain) + Created Entity Manifest (for PROV)
# -------------------

def generate_rdf(
    entries: List[TermLinkInput],
    subject_map: Dict[Tuple[str, str], str],  # (project_id, project_subject_id) -> subject_iri
) -> tuple[str, dict, Set[Tuple[str, str]]]:
    """
    Returns:
      - graph_ttl (str): Domain triples (Turtle)
      - created_manifest (dict): entity IRIs + domain timestamps for provenance
      - pairs_seen (set): {(project_id, project_subject_id)} used by this shard (for project-link triples)
    """
    start_total_time = time.time()
    g = Graph()
    g.bind("phebee", PHEBEE_NS)
    g.bind("obo", OBO)
    g.bind("dcterms", DCTERMS)
    g.bind("xsd", XSD)

    start_precompute_time = time.time()
    termlink_to_annotation_map: dict[str, dict] = {}
    emitted_creators: set[str] = set()
    pairs_seen: Set[Tuple[str, str]] = set()

    # Track created entities (for provenance)
    created_termlinks: set[str] = set()
    created_encounters: set[str] = set()
    created_notes: set[str] = set()
    created_annotations: set[str] = set()

    # Domain timestamps we can carry to PROV
    termlink_earliest_ts: dict[str, Optional[str]] = {}
    note_ts: dict[str, Optional[str]] = {}
    annotation_ts: dict[str, Optional[str]] = {}

    for entry in entries:
        key = (entry.project_id, entry.project_subject_id)
        subject_iri = subject_map.get(key)
        if not subject_iri:
            raise ValueError(f"Subject resolution missing for {key}")
        pairs_seen.add(key)

        for evidence in entry.evidence:
            qualifiers = build_qualifier_iris(evidence.contexts)  # canonical list

            # Source node selection
            if evidence.type == "clinical_note":
                encounter_iri = f"{subject_iri}/encounter/{evidence.encounter_id}"
                note_iri = f"{encounter_iri}/note/{evidence.clinical_note_id}"
                source_node_iri = note_iri
            else:
                source_node_iri = subject_iri

            termlink_iri = get_term_link_iri(source_node_iri, entry.term_iri, qualifiers)

            if termlink_iri not in termlink_to_annotation_map:
                termlink_to_annotation_map[termlink_iri] = {
                    "source_node_iri": source_node_iri,
                    "subject_iri": subject_iri,
                    "term_iri": entry.term_iri,
                    "qualifiers": qualifiers,
                    "evidence": [],
                }

            termlink_to_annotation_map[termlink_iri]["evidence"].append(evidence)

    precompute_duration = time.time() - start_precompute_time
    logger.info("Pre-computation completed in %.2f seconds.", precompute_duration)

    # Optional qualifier stats
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
        qualifiers = group["qualifiers"]

        # If the source is a bare subject, ensure it's typed
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
            # Deterministic: domain time only
            g.add((term_link_ref, DCTERMS.created, RdfLiteral(earliest, datatype=XSD.dateTime)))
        created_termlinks.add(termlink_iri)
        termlink_earliest_ts[termlink_iri] = earliest

        # Convenience backlink
        g.add((source_node_iri_ref, PHEBEE_NS.hasTermLink, term_link_ref))

        # Qualifiers
        for q in qualifiers:
            g.add((term_link_ref, PHEBEE_NS.hasQualifyingTerm, URIRef(q)))

        for evidence in group["evidence"]:
            # Creator node (emit once per batch)
            creator_iri = build_creator_iri(
                evidence.evidence_creator_id,
                evidence.evidence_creator_type,
                evidence.evidence_creator_version,
            )
            normalized_type = (evidence.evidence_creator_type or "").strip().lower()
            creator_class_iri = URIRef(CREATOR_CLASS.get(normalized_type, f"{str(PHEBEE_NS)}Creator"))

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

                # Encounter (no created timestamp; no stable domain time)
                if encounter_iri_str not in encountered_iris:
                    g.add((encounter_ref, RDF.type, PHEBEE_NS.Encounter))
                    g.add((encounter_ref, PHEBEE_NS.encounterId, RdfLiteral(evidence.encounter_id)))
                    g.add((encounter_ref, PHEBEE_NS.hasSubject, subject_iri_ref))
                    encountered_iris.add(encounter_iri_str)
                    new_encounters_count += 1
                    created_encounters.add(encounter_iri_str)

                # ClinicalNote (created = note_timestamp if provided)
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
                    created_notes.add(note_iri_str)
                    note_ts[note_iri_str] = evidence.note_timestamp

                # Only add hasTermLink from note if it's not the source node
                if str(source_node_iri_ref) != note_iri_str:
                    g.add((note_ref, PHEBEE_NS.hasTermLink, term_link_ref))

                # ---- TextAnnotation (deterministic IRI, no timestamp in hash) ----
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

                # Domain-created = note timestamp (deterministic)
                if evidence.note_timestamp:
                    g.add((annotation_ref, DCTERMS.created, RdfLiteral(evidence.note_timestamp, datatype=XSD.dateTime)))

                # Evidence/assertion types (ECO)
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

                created_annotations.add(annotation_iri_str)
                annotation_ts[annotation_iri_str] = evidence.note_timestamp

    rdf_duration = time.time() - start_rdf_time
    triple_count = len(g)
    logger.info(
        "RDF generation completed in %.2f seconds. Triples=%s, links=%s, encounters=%s, notes=%s",
        rdf_duration, triple_count, new_links_count, new_encounters_count, new_notes_count
    )

    # ensure every resolved subject is explicitly typed once
    for key in pairs_seen:
        g.add((URIRef(subject_map[key]), RDF.type, PHEBEE_NS.Subject))

    graph_ttl = g.serialize(format="turtle", prefixes={"phebee": PHEBEE_NS, "obo": OBO, "dcterms": DCTERMS, "xsd": XSD})
    total_duration = time.time() - start_total_time
    logger.info("Total RDF generation process completed in %.2f seconds", total_duration)

    created_manifest = {
        "termlinks": [(iri, termlink_earliest_ts.get(iri)) for iri in created_termlinks],
        "encounters": [(iri, None) for iri in created_encounters],
        "notes": [(iri, note_ts.get(iri)) for iri in created_notes],
        "annotations": [(iri, annotation_ts.get(iri)) for iri in created_annotations],
    }
    return graph_ttl, created_manifest, pairs_seen

# -------------------
# Lambda Handler
# -------------------

def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)

        run_id = body.get("run_id")
        batch_id = body.get("batch_id")
        agent_iri = body.get("agent_iri")  # optional prov:Agent
        if run_id is None or batch_id is None:
            return {"statusCode": 400, "body": json.dumps({"error": "run_id and batch_id are required"})}

        try:
            batch_no = int(batch_id)
        except ValueError:
            return {"statusCode": 400, "body": json.dumps({"error": "batch_id must be an integer"})}

        # Input shard location (deterministic)
        s3_key = f"phebee/runs/{run_id}/input/batch-{batch_no:05d}.json"

        # Read shard
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

        # ---- DDB subject resolution (one call per shard) ----
        pairs: Set[Tuple[str, str]] = {(e.project_id, e.project_subject_id) for e in validated}
        logger.info("Resolving %d (project_id, project_subject_id) pairs via DynamoDB", len(pairs))
        subject_map = resolve_subjects(pairs)  # {(project_id, project_subject_id) -> subject_iri}
        logger.info("Resolved %d subjects", len(subject_map))

        # Provenance identifiers
        activity_iri = f"{PHEBEE}/activity/run/{run_id}/batch/{batch_no:05d}"
        # One named graph per run to group all batch activities for that run
        run_prov_graph_iri = f"{PHEBEE}/provenance/run/{run_id}"
        started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # Generate domain RDF and manifest of created entities
        turtle, created, pairs_seen = generate_rdf(validated, subject_map)

        # Upload compressed domain TTL
        ttl_key = f"phebee/runs/{run_id}/data/batch-{batch_no:05d}.ttl.gz"
        _put_gzip_text(BUCKET_NAME, ttl_key, turtle, "text/turtle")

        # Build N-Quads containing:
        #   (a) PROV activity in the run-scoped graph
        #   (b) Project-link triples in each project's graph
        ds = ConjunctiveGraph()

        # (a) Run provenance
        prov_ctx = ds.get_context(URIRef(run_prov_graph_iri))
        act = URIRef(activity_iri)

        prov_ctx.add((act, RDF.type, PROV.Activity))
        prov_ctx.add((act, PROV.startedAtTime, RdfLiteral(started_at, datatype=XSD.dateTime)))

        if agent_iri:
            ag_ref = URIRef(agent_iri)
            prov_ctx.add((ag_ref, RDF.type, PROV.Agent))
            prov_ctx.add((act, PROV.wasAssociatedWith, ag_ref))

        inp = URIRef(f"s3://{BUCKET_NAME}/{s3_key}")
        out = URIRef(f"s3://{BUCKET_NAME}/{ttl_key}")
        prov_ctx.add((inp, RDF.type, PROV.Entity))
        prov_ctx.add((out, RDF.type, PROV.Entity))
        prov_ctx.add((act, PROV.used, inp))
        prov_ctx.add((act, PROV.generated, out))

        # Per-entity provenance (domain timestamps only)
        def _gen(eiri: str, ts: Optional[str]):
            ent = URIRef(eiri)
            prov_ctx.add((ent, PROV.wasGeneratedBy, act))
            if ts:
                prov_ctx.add((ent, PROV.generatedAtTime, RdfLiteral(ts, datatype=XSD.dateTime)))

        for eiri, ts in created.get("termlinks", []): _gen(eiri, ts)
        for eiri, ts in created.get("notes", []): _gen(eiri, ts)
        for eiri, ts in created.get("annotations", []): _gen(eiri, ts)
        for eiri, _  in created.get("encounters", []): _gen(eiri, None)

        ended_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        prov_ctx.add((act, PROV.endedAtTime, RdfLiteral(ended_at, datatype=XSD.dateTime)))

        # (b) Project-link triples (in each project graph)
        for (project_id, project_subject_id) in pairs_seen:
            subject_iri = subject_map[(project_id, project_subject_id)]
            project_graph_iri = f"{PHEBEE}/projects/{project_id}"
            project_ctx = ds.get_context(URIRef(project_graph_iri))

            subject_ref = URIRef(subject_iri)
            project_graph_ref = URIRef(project_graph_iri)
            project_subject_iri = f"{project_graph_iri}/{project_subject_id}"
            project_subject_ref = URIRef(project_subject_iri)

            # Subject ↔ ProjectSubjectId link (no timestamps)
            project_ctx.add((subject_ref, PHEBEE_NS.hasProjectSubjectId, project_subject_ref))
            project_ctx.add((project_subject_ref, RDF.type, PHEBEE_NS.ProjectSubjectId))
            project_ctx.add((project_subject_ref, PHEBEE_NS.hasProject, project_graph_ref))

        nq_bytes = ds.serialize(format="nquads")

        # Upload compressed N-Quads
        nq_key = f"phebee/runs/{run_id}/prov/batch-{batch_no:05d}.nq.gz"
        _put_gzip_bytes(BUCKET_NAME, nq_key, nq_bytes, "application/n-quads")

        logger.info("Wrote data=%s prov=%s", ttl_key, nq_key)

        # IMPORTANT: do NOT start loaders here; finalizer will.
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Shard processed",
                "run_id": run_id,
                "batch_id": batch_no,
                "data_key": ttl_key,
                "prov_key": nq_key,
                "prov_graph": run_prov_graph_iri,
                "activity": activity_iri,
            }),
        }

    except Exception as e:
        logger.exception("Shard processing failed")
        return {"statusCode": 500, "body": json.dumps({"error": "Shard processing failed", "details": str(e)})}