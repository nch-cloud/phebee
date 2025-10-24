"""
SPARQL Query Conventions for PheBee Graph

Prefix Usage:
- PREFIX rdf:    http://www.w3.org/1999/02/22-rdf-syntax-ns#
- PREFIX rdfs:   http://www.w3.org/2000/01/rdf-schema#
- PREFIX dcterms: http://purl.org/dc/terms/
- PREFIX xsd:    http://www.w3.org/2001/XMLSchema#
- PREFIX phebee: http://ods.nationwidechildrens.org/phebee#

Property Naming:
- All custom properties from the PheBee ontology use camelCase (e.g., phebee:hasTerm, phebee:noteTimestamp).
- Extracted property keys in Python are normalized to lowercase via `split_predicate()` to avoid case mismatches.
- Known prefixes are included in all SPARQL queries, even if not immediately used, for readability and future-proofing.

Query Structure:
- Prefer `INSERT DATA`, `SELECT`, `DELETE WHERE` syntax blocks with consistent indentation.
- Use FROM clauses for graph-specific queries (e.g., subjects, HPO, MONDO).
- Optional clauses use `OPTIONAL { ... }` syntax for safe retrieval of uncertain data.

Utilities:
- `split_predicate(pred)` extracts the property name from a full IRI and lowercases it.
- Use `get_current_timestamp()` for consistent xsd:dateTime values.
"""

import re
import uuid
import hashlib
import time
from collections import defaultdict
from typing import List, Optional, Sequence
from aws_lambda_powertools import Metrics, Logger, Tracer
from datetime import datetime
from urllib.parse import quote
from collections import defaultdict
from phebee.constants import SPARQL_SEPARATOR, PHEBEE
from .neptune import execute_query, execute_update
from .aws import get_current_timestamp

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def node_exists(iri: str) -> bool:
    sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        ASK WHERE {{
            <{iri}> ?p ?o .
        }}
    """

    result = execute_query(sparql)

    logger.info(result)

    return result["boolean"]


def triple_exists(subject: str, predicate: str, object: str) -> bool:
    sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        ASK WHERE {{
            <{subject}> <{predicate}> <{object}> .
        }}
    """

    result = execute_query(sparql)

    logger.info(result)

    return result["boolean"]


def project_exists(project_id: str) -> bool:
    project_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    ASK WHERE {{
        GRAPH <{project_iri}> {{
            <{project_iri}> rdf:type phebee:Project .
        }}
    }}
    """
    result = execute_query(sparql)
    return result.get("boolean", False)


def create_project(project_id: str, project_label: str) -> bool:
    project_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"

    if project_exists(project_id):
        return False

    # Insert if not
    sparql_insert = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    INSERT DATA {{
        GRAPH <{project_iri}> {{
            <{project_iri}> rdf:type phebee:Project ;
                            rdfs:label "{project_label}" ;
                            phebee:projectId "{project_id}" .
        }}
    }}
    """
    execute_update(sparql_insert)
    return True


def get_subject(project_subject_iri: str) -> dict:
    # Get project node with IRI matching project_id
    # Get project-subject id nodes pointing at project node
    # Create a project-subject id IRI matching our project's namespace and provided project_subject_iri
    # Find the subject node connected to the created project-subject id
    sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

        SELECT ?subject
        WHERE {{
            ?subject phebee:hasProjectSubjectId <{project_subject_iri}> .
        }}
    """

    result = execute_query(sparql)

    logger.info(result)

    # If bindings is empty, we didn't find a matching subject
    if len(result["results"]["bindings"]) == 0:
        return None
    else:
        binding = result["results"]["bindings"][0]

        subject_iri = binding["subject"]["value"]

        return {
            "subject_iri": subject_iri,
            # Yes, we passsed this value in, but this keeps the return format consistent with get_subjects
            "project_subject_iri": project_subject_iri,
        }


def subject_exists(subject_iri: str) -> bool:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    ASK WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> rdf:type phebee:Subject .
        }}
    }}
    """
    result = execute_query(sparql)
    return result.get("boolean", False)


def create_subject(project_id: str, project_subject_id: str) -> str:
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{uuid.uuid4()}"
    project_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
    project_subject_iri = f"{project_iri}/{project_subject_id}"
    timestamp = get_current_timestamp()

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> rdf:type phebee:Subject .
        }}
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                     phebee:hasProject <{project_iri}> ;
                                     dcterms:created \"{timestamp}\"^^xsd:dateTime .
        }}
    }}
    """
    execute_update(sparql)
    return subject_iri


def link_subject_to_project(
    subject_iri: str, project_id: str, project_subject_id: str
) -> None:
    project_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
    project_subject_iri = f"{project_iri}/{project_subject_id}"
    timestamp = get_current_timestamp()

    sparql = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                     phebee:hasProject <{project_iri}> ;
                                     dcterms:created \"{timestamp}\"^^xsd:dateTime .
        }}
    }}
    """
    execute_update(sparql)


def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_subjects(
    project_iri: str,
    hpo_version: str,
    mondo_version: str,
    limit: int = 200,
    cursor: str = None,
    term_iri: str = None,
    term_source: str = None,
    term_source_version: str = None,
    project_subject_ids: list[str] = None,
) -> dict:
    # Build optional clauses
    project_subject_ids_clause = ""
    if project_subject_ids:
        iri_list = " ".join(f"<{project_iri}/{psid}>" for psid in project_subject_ids)
        project_subject_ids_clause = f"VALUES ?projectSubjectIRI {{ {iri_list} }}"

    cursor_clause = ""
    if cursor:
        cursor_clause = f'FILTER(STR(?subjectIRI) > "{cursor}")'

    # Phase 1: Get paginated subject IRIs with optional term filtering
    term_filter_clause = ""
    term_graphs = ""
    if term_iri:
        term_graphs = f"FROM <http://ods.nationwidechildrens.org/phebee/{term_source}~{term_source_version}>"
        term_filter_clause = f"""
        # Find subjects with term links (direct or via clinical notes)
        ?termlink rdf:type phebee:TermLink ;
                  phebee:sourceNode ?srcNode ;
                  phebee:hasTerm ?term .
        ?srcNode (phebee:hasEncounter/phebee:hasSubject)? ?subjectIRI .
        ?term rdfs:subClassOf* <{term_iri}> .
        """

    subjects_query = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT DISTINCT ?subjectIRI ?projectSubjectIRI
    {term_graphs}
    FROM <{project_iri}>
    FROM <http://ods.nationwidechildrens.org/phebee/subjects>
    WHERE {{
        # Core subject-project relationship
        ?projectSubjectIRI phebee:hasProject <{project_iri}> .
        ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectIRI .
        {project_subject_ids_clause}
        
        # Term filtering (only if term_iri provided)
        {term_filter_clause}
        
        # Cursor-based filtering
        {cursor_clause}
    }}
    ORDER BY ?subjectIRI
    LIMIT {limit + 1}
    """

    subjects_result = execute_query(subjects_query)
    logger.info("Phase 1: Subject query returned %d subjects", len(subjects_result["results"]["bindings"]))
    
    # Process subjects and determine pagination
    subject_bindings = subjects_result["results"]["bindings"]
    has_more = len(subject_bindings) > limit
    if has_more:
        subject_bindings = subject_bindings[:limit]  # Remove extra record
        next_cursor = subject_bindings[-1]["subjectIRI"]["value"] if subject_bindings else None
    else:
        next_cursor = None

    # If no subjects found, return empty result
    if not subject_bindings:
        return {
            "subjects": [],
            "pagination": {
                "limit": limit,
                "has_more": False,
                "next_cursor": None,
                "cursor": cursor
            }
        }

    # Phase 2: Get term links for the paginated subjects
    subject_iris = [binding["subjectIRI"]["value"] for binding in subject_bindings]
    subject_values = " ".join(f"<{iri}>" for iri in subject_iris)
    
    termlinks_query = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?subjectIRI ?termlink ?term 
           (GROUP_CONCAT(DISTINCT ?qualifier; separator="|") AS ?qualifiers_concat)
           (COUNT(DISTINCT ?evidence) AS ?evidence_count)
    FROM <http://ods.nationwidechildrens.org/phebee/subjects>
    FROM <http://ods.nationwidechildrens.org/phebee/hpo~{hpo_version}>
    FROM <http://ods.nationwidechildrens.org/phebee/mondo~{mondo_version}>
    WHERE {{
        VALUES ?subjectIRI {{ {subject_values} }}
        
        {{
            # Pattern 1: Direct subject → termlink
            ?termlink rdf:type phebee:TermLink ;
                      phebee:sourceNode ?subjectIRI ;
                      phebee:hasTerm ?term .
        }}
        UNION
        {{
            # Pattern 2: Subject → encounter → clinical note → termlink
            ?encounter phebee:hasSubject ?subjectIRI .
            ?clinicalNote phebee:hasEncounter ?encounter .
            ?termlink rdf:type phebee:TermLink ;
                      phebee:sourceNode ?clinicalNote ;
                      phebee:hasTerm ?term .
        }}
        
        # Optional qualifiers
        OPTIONAL {{
            ?termlink phebee:hasQualifyingTerm ?qualifier .
        }}
        
        # Optional evidence for counting
        OPTIONAL {{
            ?termlink phebee:hasEvidence ?evidence .
        }}
    }}
    GROUP BY ?subjectIRI ?termlink ?term
    """

    termlinks_result = execute_query(termlinks_query)
    logger.info("Phase 2: Term links query returned %d bindings", len(termlinks_result["results"]["bindings"]))
    
    # Build subjects map from Phase 1 results
    subjects_map = {}
    for binding in subject_bindings:
        subject_iri = binding["subjectIRI"]["value"]
        project_subject_iri = binding["projectSubjectIRI"]["value"]
        subjects_map[subject_iri] = {
            "subject_iri": subject_iri,
            "project_subject_iri": project_subject_iri,
            "project_subject_id": project_subject_iri.split("/")[-1],
            "term_links": {}
        }
    
    # Add term links from Phase 2 results
    for binding in termlinks_result["results"]["bindings"]:
        subject_iri = binding["subjectIRI"]["value"]
        termlink_iri = binding["termlink"]["value"]
        term_iri_val = binding["term"]["value"]
        qualifiers_concat = binding.get("qualifiers_concat", {}).get("value", "")
        evidence_count = int(binding.get("evidence_count", {}).get("value", "0"))
        
        if subject_iri in subjects_map:
            # Parse concatenated qualifiers
            qualifiers = set()
            if qualifiers_concat and qualifiers_concat.strip():
                # Split by separator and filter out empty strings
                qualifier_list = [q.strip() for q in qualifiers_concat.split("|") if q.strip()]
                qualifiers.update(qualifier_list)
            
            subjects_map[subject_iri]["term_links"][termlink_iri] = {
                "term_iri": term_iri_val,
                "qualifiers": qualifiers,
                "evidence_count": evidence_count
            }
    
    # Convert to final format
    subjects = []
    for subject in subjects_map.values():
        # Group term_links by term_iri
        term_groups = {}
        for termlink_iri, term_link in subject["term_links"].items():
            term_iri = term_link["term_iri"]
            if term_iri not in term_groups:
                term_groups[term_iri] = {
                    "term_iri": term_iri,
                    "qualifiers": set(),
                    "evidence_count": 0
                }
            # Merge qualifiers and sum evidence counts
            term_groups[term_iri]["qualifiers"].update(term_link["qualifiers"])
            term_groups[term_iri]["evidence_count"] += term_link["evidence_count"]
        
        # Convert to final format
        term_links = []
        for term_group in term_groups.values():
            term_links.append({
                "term_iri": term_group["term_iri"],
                "qualifiers": sorted(list(term_group["qualifiers"])),
                "evidence_count": term_group["evidence_count"]
            })
        
        subject["term_links"] = sorted(term_links, key=lambda x: x["term_iri"])
        subjects.append(subject)
    
    # Sort by subject IRI to maintain consistent ordering
    subjects = sorted(subjects, key=lambda x: x["subject_iri"])
    
    return {
        "subjects": subjects,
        "pagination": {
            "limit": limit,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "cursor": cursor
        }
    }


def dump_graph_contents(
    graph_iri: str, limit: int = 100, object_iri: str = None
) -> list[dict]:
    if object_iri:
        sparql = f"""
        SELECT ?s ?p ?o
        FROM <{graph_iri}>
        WHERE {{
            BIND (<{object_iri}> AS ?o)
            ?s ?p ?o
        }}
        LIMIT {limit}
        """
    else:
        sparql = f"""
        SELECT ?s ?p ?o
        FROM <{graph_iri}>
        WHERE {{
            ?s ?p ?o
        }}
        LIMIT {limit}
        """

    result = execute_query(sparql)
    return [
        {"s": row["s"]["value"], "p": row["p"]["value"], "o": row["o"]["value"]}
        for row in result["results"]["bindings"]
    ]


def get_creator_info(creator_iri: str) -> dict:
    sparql = f"""
    PREFIX dcterms: <http://purl.org/dc/terms/>
    SELECT ?p ?o WHERE {{
        <{creator_iri}> ?p ?o .
    }}
    """
    result = execute_query(sparql)
    creator = {"iri": creator_iri}

    for row in result["results"]["bindings"]:
        pred = row["p"]["value"]
        obj = row["o"]["value"]
        key = split_predicate(pred)
        creator[key] = obj

    return creator


def split_predicate(pred: str):
    return camel_to_snake(
        (pred.split("#")[-1] if "#" in pred else pred.split("/")[-1])
    ).lower()


def get_term_links_with_evidence(
    source_node_iri: str,
    hpo_version: str | None = None,
    mondo_version: str | None = None,
) -> list[dict]:
    """
    Returns:
      [
        {
          "termlink_iri": str,
          "term_iri": str,
          "term_label": Optional[str],
          "evidence": [
            {
              "evidence_iri": str,
              "evidence_class": Optional[str],        # e.g., phebee:TextAnnotation (IRI)
              "evidence_type": Optional[str],         # ECO IRI
              "assertion_type": Optional[str],        # ECO IRI
              "created": Optional[str],               # xsd:dateTime (ISO)
              # TextAnnotation-specific (when applicable)
              "span_start": Optional[int],
              "span_end": Optional[int],
              "text_source": Optional[str],           # IRI of ClinicalNote/TextSource
              "metadata": Optional[dict | str],       # JSON-decoded when possible
            },
            ...
          ],
        },
        ...
      ]
    """
    from json import loads as json_loads

    def _safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    def _maybe_json(s: str):
        try:
            return json_loads(s)
        except Exception:
            return s

    # -------------------------------
    # 1) Lean main query: link/term/evidence/qualifiers
    # -------------------------------
    sparql_links = f"""
    PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    SELECT ?link ?term ?evidence ?qualifier
    WHERE {{
      VALUES ?subject {{ <{source_node_iri}> }}

      GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
        ?link a phebee:TermLink ;
              phebee:hasTerm ?term ;
              phebee:sourceNode ?srcNode .
        ?srcNode phebee:hasEncounter/phebee:hasSubject ?subject .

        OPTIONAL {{
          ?link phebee:hasEvidence ?evidence .
          # Guard: don't treat nested TermLinks as evidence
          MINUS {{ ?evidence a phebee:TermLink }}
        }}
        
        OPTIONAL {{
          ?link phebee:hasQualifyingTerm ?qualifier .
        }}
      }}
    }}
    """

    res = execute_query(sparql_links)

    links: dict[str, dict] = {}
    distinct_terms: set[str] = set()
    distinct_evidence: set[str] = set()

    for row in res["results"]["bindings"]:
        link_iri = row["link"]["value"]
        term_iri = row["term"]["value"]
        evidence_iri = row.get("evidence", {}).get("value")
        qualifier_iri = row.get("qualifier", {}).get("value")

        link_obj = links.setdefault(
            link_iri,
            {"termlink_iri": link_iri, "term_iri": term_iri, "term_label": None, "evidence": [], "qualifiers": set()},
        )
        distinct_terms.add(term_iri)

        if evidence_iri:
            if evidence_iri not in distinct_evidence:
                distinct_evidence.add(evidence_iri)
            # We'll fill details after the second query, but keep placeholder list membership now
            link_obj["evidence"].append({"evidence_iri": evidence_iri})
            
        if qualifier_iri:
            link_obj["qualifiers"].add(qualifier_iri)

    # -------------------------------
    # 2) Bounded evidence-details query (VALUES over distinct_evidence)
    #     - chooses a "specific class" if present (prefers non-abstract over phebee:Evidence)
    # -------------------------------
    evidence_details: dict[str, dict] = {}
    if distinct_evidence:
        # If this set could be very large, chunk it (Neptune handles a few hundred–couple thousand fine).
        ev_list = list(distinct_evidence)
        CHUNK = 500
        for i in range(0, len(ev_list), CHUNK):
            chunk = ev_list[i : i + CHUNK]
            vals = " ".join(f"<{e}>" for e in chunk)

            sparql_ev = f"""
            PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dcterms: <http://purl.org/dc/terms/>
            PREFIX phebee:  <http://ods.nationwidechildrens.org/phebee#>

            SELECT ?evidence ?evidence_class
                   (SAMPLE(?etype)   AS ?evidence_type)
                   (SAMPLE(?atype)   AS ?assertion_type)
                   (SAMPLE(?created) AS ?created)
                   (SAMPLE(?sStart)  AS ?span_start)
                   (SAMPLE(?sEnd)    AS ?span_end)
                   (SAMPLE(?tsrc)    AS ?text_source)
                   (SAMPLE(?meta)    AS ?metadata)
            WHERE {{
              VALUES ?evidence {{ {vals} }}
              GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                # Prefer a concrete subclass over the abstract phebee:Evidence
                OPTIONAL {{ ?evidence a ?t_specific . FILTER(?t_specific != phebee:Evidence) }}
                BIND(COALESCE(?t_specific, phebee:Evidence) AS ?evidence_class)

                OPTIONAL {{ ?evidence phebee:evidenceType  ?etype }}
                OPTIONAL {{ ?evidence phebee:assertionType ?atype }}
                OPTIONAL {{ ?evidence dcterms:created      ?created }}

                # TextAnnotation-only fields (harmless if it isn't TA)
                OPTIONAL {{ ?evidence phebee:spanStart ?sStart }}
                OPTIONAL {{ ?evidence phebee:spanEnd   ?sEnd   }}
                OPTIONAL {{ ?evidence phebee:textSource ?tsrc  }}
                OPTIONAL {{ ?evidence phebee:metadata  ?meta   }}
              }}
            }}
            GROUP BY ?evidence ?evidence_class
            """

            ev_res = execute_query(sparql_ev)
            for b in ev_res["results"]["bindings"]:
                ev_iri = b["evidence"]["value"]
                evidence_details[ev_iri] = {
                    "evidence_iri": ev_iri,
                    "evidence_class": b.get("evidence_class", {}).get("value"),
                    "evidence_type": b.get("evidence_type", {}).get("value"),
                    "assertion_type": b.get("assertion_type", {}).get("value"),
                    "created": b.get("created", {}).get("value"),
                    "span_start": _safe_int(b.get("span_start", {}).get("value")),
                    "span_end": _safe_int(b.get("span_end", {}).get("value")),
                    "text_source": b.get("text_source", {}).get("value"),
                    "metadata": _maybe_json(b.get("metadata", {}).get("value")) if b.get("metadata") else None,
                }

    # Attach evidence details to each link (dedup & stable order)
    for link_obj in links.values():
        seen = set()
        detailed = []
        for ev in link_obj["evidence"]:
            iri = ev["evidence_iri"]
            if iri in seen:
                continue
            seen.add(iri)
            detailed.append(evidence_details.get(iri, {"evidence_iri": iri}))
        # sort for stability (by IRI)
        link_obj["evidence"] = sorted(detailed, key=lambda x: x["evidence_iri"])

    # -------------------------------
    # 3) Optional labels for distinct terms (small, bounded UNION)
    # -------------------------------
    if hpo_version and mondo_version and distinct_terms:
        term_vals = " ".join(f"<{t}>" for t in distinct_terms)
        sparql_labels = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?term (SAMPLE(?lbl) AS ?term_label)
        WHERE {{
          VALUES ?term {{ {term_vals} }}
          {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/hpo~{hpo_version}> {{
              ?term rdfs:label ?lbl .
              FILTER(LANGMATCHES(LANG(?lbl),'en'))
            }}
          }}
          UNION
          {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/mondo~{mondo_version}> {{
              ?term rdfs:label ?lbl .
              FILTER(LANGMATCHES(LANG(?lbl),'en'))
            }}
          }}
        }}
        GROUP BY ?term
        """
        lab_res = execute_query(sparql_labels)
        label_map = {b["term"]["value"]: b["term_label"]["value"] for b in lab_res["results"]["bindings"] if "term_label" in b}
        for obj in links.values():
            obj["term_label"] = label_map.get(obj["term_iri"])

    # Convert qualifiers sets to sorted lists for consistent output
    for obj in links.values():
        obj["qualifiers"] = sorted(list(obj["qualifiers"]))

    return list(links.values())


def term_link_exists(source_node_iri: str, term_iri: str) -> dict:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    SELECT ?link WHERE {{
        ?link rdf:type phebee:TermLink ;
              phebee:sourceNode <{source_node_iri}> ;
              phebee:hasTerm <{term_iri}> .
    }}
    """

    result = execute_query(sparql)

    if not result["results"]["bindings"]:
        return {"link_exists": False}
    else:
        link_iri = result["results"]["bindings"][0]["link"]["value"]
        return {"link_exists": True, "link_iri": link_iri}


def create_encounter(subject_iri: str, encounter_id: str):
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    now_iso = get_current_timestamp()
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{encounter_iri}> rdf:type phebee:Encounter ;
                            phebee:encounterId "{encounter_id}" ;
                            dcterms:created "{now_iso}" ;
                            phebee:hasSubject <{subject_iri}> .
        }}
    }}
    """
    execute_update(sparql)


def get_encounter(subject_iri: str, encounter_id: str) -> dict:
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?p ?o WHERE {{
        <{encounter_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    bindings = results["results"]["bindings"]
    for binding in bindings:
        predicate = binding["p"]["value"]
        obj = binding["o"]["value"]

        # Extract unprefixed name from IRI (e.g. ...#encounterType → encounterType)
        key = predicate.split("#")[-1] if "#" in predicate else predicate.split("/")[-1]
        properties[key] = obj

    if len(bindings) > 0:
        return flatten_response(
            {
                "encounter_iri": encounter_iri,
                "subject_iri": subject_iri,
                "encounter_id": encounter_id,
            },
            properties,
        )
    else:
        return None


def delete_encounter(subject_iri: str, encounter_id: str):
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    sparql = f"""
    DELETE WHERE {{
        <{encounter_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{encounter_iri}> .
    }}
    """
    execute_update(sparql)


def create_clinical_note(
    encounter_iri: str,
    clinical_note_id: str,
    note_timestamp: str = None,
    provider_type: str = None,
    author_specialty: str = None,
):
    clinical_note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    now_iso = get_current_timestamp()

    triples = [
        f"<{clinical_note_iri}> rdf:type phebee:ClinicalNote",
        f'<{clinical_note_iri}> phebee:clinicalNoteId "{clinical_note_id}"',
        f"<{clinical_note_iri}> phebee:hasEncounter <{encounter_iri}>",
        f'<{clinical_note_iri}> dcterms:created "{now_iso}"^^xsd:dateTime',
    ]

    if note_timestamp:
        triples.append(
            f'<{clinical_note_iri}> phebee:noteTimestamp "{note_timestamp}"^^xsd:dateTime'
        )
        
    if provider_type:
        triples.append(
            f'<{clinical_note_iri}> phebee:providerType "{provider_type}"'
        )
        
    if author_specialty:
        triples.append(
            f'<{clinical_note_iri}> phebee:authorSpecialty "{author_specialty}"'
        )

    triples_block = " .\n        ".join(triples)

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block} .
        }}
    }}
    """

    execute_update(sparql)


def get_clinical_note(encounter_iri: str, clinical_note_id: str) -> dict:
    clinical_note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?p ?o WHERE {{
        <{clinical_note_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    bindings = results["results"]["bindings"]
    for binding in bindings:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = split_predicate(pred)
        
        if key == "has_term_link":
            # Always make has_term_link an array
            if key not in properties:
                properties[key] = []
            properties[key].append(obj)
        else:
            properties[key] = obj

    if len(bindings) > 0:
        return flatten_response(
            {
                "clinical_note_iri": clinical_note_iri,
                "encounter_iri": encounter_iri,
            },
            properties,
        )
    else:
        return None


def delete_clinical_note(encounter_iri: str, clinical_note_id: str):
    clinical_note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    sparql = f"""
    DELETE WHERE {{
        <{clinical_note_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{clinical_note_iri}> .
    }}
    """
    execute_update(sparql)


def create_creator(
    creator_id: str, creator_type: str, name: str = None, version: str = None
):
    now_iso = get_current_timestamp()
    
    # Create the creator IRI
    creator_id_safe = quote(creator_id, safe="")
    
    # Include version in the creator IRI for automated creators using /version/ path
    if creator_type == "automated" and version:
        version_safe = quote(version, safe="")
        creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}/version/{version_safe}"
    else:
        creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"

    if creator_type == "human":
        rdf_type = "phebee:HumanCreator"
    elif creator_type == "automated":
        rdf_type = "phebee:AutomatedCreator"
    else:
        raise ValueError("Invalid creator_type. Must be 'human' or 'automated'.")

    triples = [
        f'<{creator_iri}> phebee:creatorId "{creator_id}"',
        f"<{creator_iri}> rdf:type {rdf_type}",
        f'<{creator_iri}> dcterms:created "{now_iso}"^^xsd:dateTime',
    ]

    if name:
        triples.append(f'<{creator_iri}> dcterms:title "{name}"')
    if creator_type == "automated":
        if not version:
            raise ValueError("version is required for automated creators")
        triples.append(f'<{creator_iri}> dcterms:hasVersion "{version}"')

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block}
        }}
    }}
    """
    execute_update(sparql)
    
    return creator_iri


def get_creator(creator_id: str) -> dict:
    """
    Get creator by ID. This function supports both direct creator_id lookup
    and full creator_iri lookup.
    
    Args:
        creator_id (str): Either the creator ID or the full creator IRI
        
    Returns:
        dict: Creator information or None if not found
    """
    # Check if the input is a full IRI
    if creator_id.startswith("http://"):
        creator_iri = creator_id
    else:
        # Try to find the creator by ID (might be multiple if versioned)
        sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        
        SELECT ?creator WHERE {{
            ?creator phebee:creatorId "{creator_id}" .
        }}
        LIMIT 1
        """
        
        result = execute_query(sparql)
        bindings = result.get("results", {}).get("bindings", [])
        
        if not bindings:
            # Fall back to the old method if no results
            creator_id_safe = quote(creator_id, safe="")
            creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"
        else:
            creator_iri = bindings[0]["creator"]["value"]

    # Get creator properties
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?p ?o WHERE {{
        <{creator_iri}> ?p ?o .
    }}
    """

    results = execute_query(sparql)
    properties = {}
    bindings = results["results"]["bindings"]
    for binding in bindings:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = split_predicate(pred)
        properties[key] = obj

    if len(bindings) > 0:
        return flatten_response(
            {"creator_iri": creator_iri}, properties
        )
    else:
        return None


def delete_creator(creator_id_or_iri: str):
    """
    Delete a creator by ID or IRI.
    
    Args:
        creator_id_or_iri (str): Either the creator ID or the full creator IRI
    """
    # Check if the input is a full IRI
    if creator_id_or_iri.startswith("http://"):
        creator_iri = creator_id_or_iri
    else:
        # Try to find the creator by ID (might be multiple if versioned)
        sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        
        SELECT ?creator WHERE {{
            ?creator phebee:creatorId "{creator_id_or_iri}" .
        }}
        LIMIT 1
        """
        
        result = execute_query(sparql)
        bindings = result.get("results", {}).get("bindings", [])
        
        if not bindings:
            # Fall back to the old method if no results
            creator_id_safe = quote(creator_id_or_iri, safe="")
            creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"
        else:
            creator_iri = bindings[0]["creator"]["value"]
    
    sparql = f"""
    DELETE WHERE {{
        <{creator_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{creator_iri}> .
    }}
    """
    execute_update(sparql)
    
    return creator_iri

QUALIFIER_BASE = f"{PHEBEE}/qualifier"

def build_qualifier_iris(contexts: Optional[dict]) -> list[str]:
    """
    Turn contexts dict into canonical qualifier IRIs.
    Returns a sorted, de-duplicated list of IRIs (stable across runs).
    """
    iris = set()

    # contexts: {"negated": 1, "family": 0, ...}
    if contexts:
        for k, v in contexts.items():
            if v in (1, True, "1", "true", "True"):
                slug = quote(str(k).strip().lower(), safe="")
                iris.add(f"{QUALIFIER_BASE}/{slug}")

    return sorted(iris)

def _sparql_escape_literal(s: str) -> str:
    """Minimal escaping for a SPARQL string literal."""
    return (
        s.replace("\\", "\\\\")
         .replace('"', '\\"')
         .replace("\r", "\\r")
         .replace("\n", "\\n")
    )

# ---- Deterministic TextAnnotation IDs ----
ANNOTATION_HASH_VERSION = "v1"

def stable_text_annotation_iri(
    text_source_iri: str,
    term_iri: str,
    creator_iri: str,
    span_start: Optional[int],
    span_end: Optional[int],
    qualifier_iris: Optional[Sequence[str]],  # may be unsorted/duplicated
) -> str:
    """
    v1 inputs (frozen): text_source_iri, term_iri, creator_iri, span_start, span_end,
    qualifier_iris (order-insensitive; duplicates ignored). No timestamps.
    """
    import hashlib

    # Defensive canonicalization so caller order/dupes don't matter
    qlist = tuple(sorted(set(qualifier_iris or ())))  # deterministic, hashable

    key = "|".join([
        text_source_iri,
        term_iri,
        creator_iri,
        str(span_start or ""),
        str(span_end or ""),
        ",".join(qlist),
    ])
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return f"{text_source_iri}/annotation/{ANNOTATION_HASH_VERSION}/{digest}"

def create_text_annotation(
    text_source_iri: str,
    span_start: Optional[int] = None,
    span_end: Optional[int] = None,
    creator_iri: Optional[str] = None,
    term_iri: Optional[str] = None,
    metadata: Optional[str] = None,
    contexts: Optional[dict] = None,
    note_timestamp: Optional[str] = None
) -> str:
    if not (creator_iri and term_iri):
        raise ValueError("creator_iri and term_iri are required")

    # Deterministic TextAnnotation IRI (v1)
    qualifiers = build_qualifier_iris(contexts)

    annotation_iri = stable_text_annotation_iri(
        text_source_iri,
        term_iri,
        creator_iri,
        span_start,
        span_end,
        qualifiers
    )

    # Lookup rdf:type of the source and creator
    creator_type = get_rdf_type(creator_iri) if creator_iri else None
    text_source_type = get_rdf_type(text_source_iri)

    # Infer ECO terms
    evidence_type_iri = (
        infer_evidence_type(creator_type, text_source_type)
        if creator_type and text_source_type
        else "http://purl.obolibrary.org/obo/ECO_0000000"
    )
    assertion_type_iri = (
        infer_assertion_type(creator_type)
        if creator_type
        else "http://purl.obolibrary.org/obo/ECO_0000217"
    )

    triples = [
        f"<{annotation_iri}> rdf:type phebee:TextAnnotation",
        f"<{annotation_iri}> phebee:textSource <{text_source_iri}>",        
        f"<{annotation_iri}> phebee:evidenceType <{evidence_type_iri}>",
        f"<{annotation_iri}> phebee:assertionType <{assertion_type_iri}>",
    ]

    if note_timestamp is not None:
        triples.append(
            f'<{annotation_iri}> dcterms:created "{note_timestamp}"^^xsd:dateTime',
        )

    if span_start is not None:
        triples.append(
            f'<{annotation_iri}> phebee:spanStart "{span_start}"^^xsd:integer'
        )
    if span_end is not None:
        triples.append(f'<{annotation_iri}> phebee:spanEnd "{span_end}"^^xsd:integer')
    if creator_iri:
        triples.append(f"<{annotation_iri}> phebee:creator <{creator_iri}>")
    if term_iri:
        triples.append(f"<{annotation_iri}> phebee:term <{term_iri}>")
    if metadata:
        triples.append(f'<{annotation_iri}> phebee:metadata "{_sparql_escape_literal(metadata)}"')

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block}
        }}
    }}
    """
    execute_update(sparql)
    return annotation_iri


def get_rdf_type(node_iri: str) -> str:
    """
    Returns the rdf:type of the given node IRI, or None if not found.
    Assumes the node has only one rdf:type.
    """
    sparql = f"""
    SELECT ?type WHERE {{
        <{node_iri}> rdf:type ?type .
    }} LIMIT 1
    """
    results = execute_query(sparql)
    bindings = results.get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["type"]["value"]
    return None


def infer_evidence_type(creator_type: str, text_source_type: str) -> str:
    """
    Returns an ECO evidenceType IRI based on the type of creator and source.

    Parameters:
        creator_type (str): RDF type of the creator (e.g., 'phebee:AutomatedCreator').
        text_source_type (str): RDF type of the text source (e.g., 'phebee:ClinicalNote').

    Returns:
        str: ECO term IRI indicating the evidence type.
    """
    # Case: Automatically generated from a clinical note
    if creator_type == "http://ods.nationwidechildrens.org/phebee#AutomatedCreator":
        if text_source_type == "http://ods.nationwidechildrens.org/phebee#ClinicalNote":
            return "http://purl.obolibrary.org/obo/ECO_0006162"  # medical practitioner statement used in automatic assertion

    # Case: Human curated from a clinical note
    elif creator_type == "http://ods.nationwidechildrens.org/phebee#HumanCreator":
        if text_source_type == "http://ods.nationwidechildrens.org/phebee#ClinicalNote":
            return "http://purl.obolibrary.org/obo/ECO_0006161"  # medical practitioner statement evidence used in manual assertion

    # Fallback: Generic evidence (unspecified)
    return "http://purl.obolibrary.org/obo/ECO_0000000"


def infer_assertion_type(creator_type: str) -> str:
    """
    Returns an ECO assertionType IRI based on the type of creator.

    Parameters:
        creator_type (str): RDF type of the creator (e.g., 'phebee:AutomatedCreator').

    Returns:
        str: ECO term IRI indicating the assertion type.
    """
    # Case: Automated assertion
    if creator_type == "http://ods.nationwidechildrens.org/phebee#AutomatedCreator":
        return "http://purl.obolibrary.org/obo/ECO_0000203"  # automatic assertion

    # Case: Manual assertion
    elif creator_type == "http://ods.nationwidechildrens.org/phebee#HumanCreator":
        return "http://purl.obolibrary.org/obo/ECO_0000218"  # manual assertion

    # Fallback: Generic assertion evidence
    return "http://purl.obolibrary.org/obo/ECO_0000217"


def get_text_annotation(annotation_iri: str) -> dict:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?p ?o WHERE {{
        <{annotation_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    bindings = results["results"]["bindings"]
    for binding in bindings:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = split_predicate(pred)
        properties[key] = obj

    if len(bindings) > 0:
        return flatten_response({"annotation_iri": annotation_iri}, properties)
    else:
        return None


def delete_text_annotation(annotation_iri: str):
    sparql = f"""
    DELETE WHERE {{
        <{annotation_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{annotation_iri}> .
    }}
    """
    execute_update(sparql)


def create_term_link(
    source_node_iri: str, term_iri: str, creator_iri: str, evidence_iris: list[str], qualifiers=None
) -> dict:
    """
    Create a term link between a source node (subject, encounter, or clinical note) and a term.
    
    Args:
        source_node_iri (str): The IRI of the source node (subject, encounter, or clinical note)
        term_iri (str): The IRI of the term
        creator_iri (str): The IRI of the creator
        evidence_iris (list[str]): List of evidence IRIs
        qualifiers (list): List of qualifier IRIs
        
    Returns:
        dict: Dictionary with the term link IRI and whether it was created
    """
    # Generate deterministic hash based on source node, term, and qualifiers
    termlink_hash = generate_termlink_hash(source_node_iri, term_iri, qualifiers)
    termlink_iri = f"{source_node_iri}/term-link/{termlink_hash}"
    
    # Check if the term link already exists
    link_exists = node_exists(termlink_iri)
    
    if link_exists:
        logger.info("Term link already exists: %s", termlink_iri)
        
        # If evidence_iris is provided, attach them to the existing term link
        if evidence_iris:
            for evidence_iri in evidence_iris:
                attach_evidence_to_term_link(termlink_iri, evidence_iri)
                    
        return {"termlink_iri": termlink_iri, "created": False}
    
    # Create the term link if it doesn't exist
    created = get_current_timestamp()

    triples = [
        f"<{termlink_iri}> rdf:type phebee:TermLink",
        f"<{termlink_iri}> phebee:sourceNode <{source_node_iri}>",
        f"<{termlink_iri}> phebee:hasTerm <{term_iri}>",
        f"<{termlink_iri}> phebee:creator <{creator_iri}>",
        f'<{termlink_iri}> dcterms:created "{created}"^^xsd:dateTime',
        f"<{source_node_iri}> phebee:hasTermLink <{termlink_iri}>",
    ]

    if evidence_iris:
        for evidence_iri in evidence_iris:
            triples.append(f"<{termlink_iri}> phebee:hasEvidence <{evidence_iri}>")
            
    # Add qualifier triples if any
    if qualifiers:
        for qualifier in qualifiers:
            triples.append(f"<{termlink_iri}> phebee:hasQualifyingTerm <{qualifier}>")

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block}
        }}
    }}
    """
    execute_update(sparql)
    return {"termlink_iri": termlink_iri, "created": True}


def get_term_link(termlink_iri: str) -> dict:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT ?p ?o WHERE {{
        <{termlink_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    bindings = results["results"]["bindings"]
    for binding in bindings:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = split_predicate(pred)
        properties.setdefault(key, []).append(obj)

    if len(bindings) > 0:
        return flatten_response({"termlink_iri": termlink_iri}, properties)
    else:
        return None


def attach_evidence_to_term_link(termlink_iri: str, evidence_iri: str) -> None:
    """
    Attach evidence to a term link if it's not already attached.
    
    Args:
        termlink_iri (str): The IRI of the term link
        evidence_iri (str): The IRI of the evidence to attach
    """
    # Check if the evidence is already attached
    if triple_exists(termlink_iri, "http://ods.nationwidechildrens.org/phebee#hasEvidence", evidence_iri):
        logger.debug("Evidence %s already attached to %s", evidence_iri, termlink_iri)
        return
        
    # Attach the evidence
    sparql = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    
    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{termlink_iri}> phebee:hasEvidence <{evidence_iri}> .
        }}
    }}
    """
    execute_update(sparql)
    logger.info("Attached evidence %s to %s", evidence_iri, termlink_iri)


def flatten_response(fixed: dict, properties: dict) -> dict:
    overlap = fixed.keys() & properties.keys()
    if overlap:
        raise ValueError(f"Property keys conflict with fixed keys: {overlap}")
    return {**fixed, **properties}


def delete_term_link(termlink_iri: str):
    sparql = f"""
    DELETE WHERE {{
        <{termlink_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{termlink_iri}> .
    }}
    """
    execute_update(sparql)


def generate_termlink_hash(source_node_iri: str, term_iri: str, qualifiers=None):
    """
    Generate a deterministic hash for a term link based on its components.
    
    Args:
        source_node_iri (str): The IRI of the source node (subject, encounter, or clinical note)
        term_iri (str): The IRI of the term being linked
        qualifiers (list): List of qualifier IRIs, e.g., negated, hypothetical
        
    Returns:
        str: A deterministic hash that can be used as part of the term link IRI
    """
    # Sort qualifiers to ensure consistent ordering
    sorted_qualifiers = sorted(qualifiers) if qualifiers else []
    
    # Create a composite key
    key_parts = [source_node_iri, term_iri] + sorted_qualifiers
    key_string = '|'.join(key_parts)
    
    # Generate a deterministic hash
    return hashlib.sha256(key_string.encode()).hexdigest()


def check_existing_term_links(termlink_iris, batch_size=1000):
    """
    Check which term links from a list already exist in the database.
    Uses batching to handle large numbers of IRIs efficiently.
    
    Args:
        termlink_iris (list): List of term link IRIs to check
        batch_size (int): Maximum number of IRIs to check in a single query
        
    Returns:
        set: Set of existing term link IRIs
    """
    if not termlink_iris:
        return set()
    
    existing = set()
    batch_count = (len(termlink_iris) + batch_size - 1) // batch_size  # Ceiling division
    logger.info("Checking %s term links in %s batches of up to %s each", len(termlink_iris), batch_count, batch_size)
    
    # Process in batches to avoid query size limitations
    for i in range(0, len(termlink_iris), batch_size):
        batch_start_time = time.time()
        batch = termlink_iris[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        # Convert batch to VALUES clause
        values_clause = " ".join(f"<{iri}>" for iri in batch)
        
        sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        
        SELECT ?termlink WHERE {{
            VALUES ?termlink {{ {values_clause} }}
            ?termlink a phebee:TermLink .
        }}
        """
        
        # Use POST method to avoid URL length limitations
        from .neptune import execute_query_post
        result = execute_query_post(sparql)
        
        # Extract existing IRIs from results
        for binding in result["results"]["bindings"]:
            existing.add(binding["termlink"]["value"])
        
        batch_duration = time.time() - batch_start_time
        logger.info("Batch %s/%s processed in %.2f seconds. Found %s existing term links.", batch_num, batch_count, batch_duration, len(result['results']['bindings']))
    
    return existing


def check_existing_encounters(encounter_iris, batch_size=1000):
    """
    Check which encounters from a list already exist in the database.
    Uses batching to handle large numbers of IRIs efficiently.
    
    Args:
        encounter_iris (list): List of encounter IRIs to check
        batch_size (int): Maximum number of IRIs to check in a single query
        
    Returns:
        set: Set of existing encounter IRIs
    """
    if not encounter_iris:
        return set()
    
    existing = set()
    batch_count = (len(encounter_iris) + batch_size - 1) // batch_size  # Ceiling division
    logger.info("Checking %s encounters in %s batches of up to %s each", len(encounter_iris), batch_count, batch_size)
    
    # Process in batches to avoid query size limitations
    for i in range(0, len(encounter_iris), batch_size):
        batch_start_time = time.time()
        batch = encounter_iris[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        # Convert batch to VALUES clause
        values_clause = " ".join(f"<{iri}>" for iri in batch)
        
        sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        
        SELECT ?encounter WHERE {{
            VALUES ?encounter {{ {values_clause} }}
            ?encounter a phebee:Encounter .
        }}
        """
        
        # Use POST method to avoid URL length limitations
        from .neptune import execute_query_post
        result = execute_query_post(sparql)
        
        # Extract existing IRIs from results
        for binding in result["results"]["bindings"]:
            existing.add(binding["encounter"]["value"])
        
        batch_duration = time.time() - batch_start_time
        logger.info("Batch %s/%s processed in %.2f seconds. Found %s existing encounters.", batch_num, batch_count, batch_duration, len(result['results']['bindings']))
    
    return existing


def flatten_sparql_results(sparql_json, include_datatype=False, group_subjects=False):
    """
    Flattens the SPARQL JSON result format. Optionally, groups results by subject if group_subjects is True.

    Parameters:
    - sparql_json: The JSON object returned from a SPARQL query.
    - include_datatype: If True, include the datatypes if available.
    - group_subjects: If True, group results by subject at the top level.

    """
    simplified_results = []
    grouped_results = defaultdict(
        list
    )  # Dictionary to group by subject if group_subjects=True

    # Extract the variable names
    variables = sparql_json.get("head", {}).get("vars", [])

    # Iterate over the results
    for result in sparql_json.get("results", {}).get("bindings", []):
        flat_result = {}

        for var in variables:
            if var in result:
                value = result[var].get("value")
                datatype = result[var].get("datatype")
                if include_datatype and datatype:
                    flat_result[f"{var}_datatype"] = datatype
                flat_result[var] = value
            else:
                flat_result[var] = None  # If the variable is not bound, use None

        # If group_subjects is True, group by subjectIRI or projectSubjectId
        if group_subjects:
            subject_key = flat_result.get(
                "subjectIRI"
            )  # You can change to 'projectSubjectId' if needed
            grouped_results[subject_key].append(flat_result)
        else:
            simplified_results.append(flat_result)

    # Return results based on the group_subjects flag
    if group_subjects:
        return dict(grouped_results)
    else:
        return simplified_results
def check_existing_clinical_notes(note_iris, batch_size=1000):
    """
    Check which clinical notes from a list already exist in the database.
    Uses batching to handle large numbers of IRIs efficiently.
    
    Args:
        note_iris (list): List of clinical note IRIs to check
        batch_size (int): Maximum number of IRIs to check in a single query
        
    Returns:
        set: Set of existing clinical note IRIs
    """
    if not note_iris:
        return set()
    
    existing = set()
    batch_count = (len(note_iris) + batch_size - 1) // batch_size  # Ceiling division
    logger.info("Checking %s clinical notes in %s batches of up to %s each", len(note_iris), batch_count, batch_size)
    
    # Process in batches to avoid query size limitations
    for i in range(0, len(note_iris), batch_size):
        batch_start_time = time.time()
        batch = note_iris[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        # Convert batch to VALUES clause
        values_clause = " ".join(f"<{iri}>" for iri in batch)
        
        sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        
        SELECT ?note WHERE {{
            VALUES ?note {{ {values_clause} }}
            ?note a phebee:ClinicalNote .
        }}
        """
        
        # Use POST method to avoid URL length limitations
        from .neptune import execute_query_post
        result = execute_query_post(sparql)
        
        # Extract existing IRIs from results
        for binding in result["results"]["bindings"]:
            existing.add(binding["note"]["value"])
        
        batch_duration = time.time() - batch_start_time
        logger.info("Batch %s/%s processed in %.2f seconds. Found %s existing clinical notes.", batch_num, batch_count, batch_duration, len(result['results']['bindings']))
    
    return existing
