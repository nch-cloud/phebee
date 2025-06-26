"""
SPARQL Query Conventions for PheBee Graph

Prefix Usage:
- PREFIX rdf:    http://www.w3.org/1999/02/22-rdf-syntax-ns#
- PREFIX rdfs:   http://www.w3.org/2000/01/rdf-schema#
- PREFIX dc:     http://purl.org/dc/terms/
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
from collections import defaultdict
from typing import List
from aws_lambda_powertools import Metrics, Logger, Tracer
from datetime import datetime
from urllib.parse import quote
from collections import defaultdict
from phebee.constants import SPARQL_SEPARATOR
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
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> rdf:type phebee:Subject .
        }}
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                     phebee:hasProject <{project_iri}> ;
                                     dc:created \"{timestamp}\"^^xsd:dateTime .
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
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                     phebee:hasProject <{project_iri}> ;
                                     dc:created \"{timestamp}\"^^xsd:dateTime .
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
    term_iri: str = None,
    term_source: str = None,
    term_source_version: str = None,
    project_subject_ids: list[str] = None,
) -> list[dict]:
    project_subject_ids_clause = ""
    if project_subject_ids:
        iri_list = " ".join(f"<{project_iri}/{psid}>" for psid in project_subject_ids)
        project_subject_ids_clause = f"VALUES ?projectSubjectIRI {{ {iri_list} }}"

    if term_iri:
        sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?subjectIRI ?projectSubjectIRI
        FROM <http://ods.nationwidechildrens.org/phebee/{term_source}~{term_source_version}>
        FROM <{project_iri}>
        FROM <http://ods.nationwidechildrens.org/phebee/subjects>
        WHERE {{
            ?projectSubjectIRI phebee:hasProject <{project_iri}> .
            {project_subject_ids_clause}
            ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectIRI .

            ?termlink rdf:type phebee:TermLink ;
                      phebee:sourceNode ?subjectIRI ;
                      phebee:hasTerm ?term .

            ?term rdfs:subClassOf* <{term_iri}> .
        }}
        """
    else:
        sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

        SELECT ?subjectIRI ?projectSubjectIRI
        FROM <{project_iri}>
        FROM <http://ods.nationwidechildrens.org/phebee/subjects>
        WHERE {{
            ?projectSubjectIRI phebee:hasProject <{project_iri}> .
            {project_subject_ids_clause}
            ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectIRI .
        }}
        """

    result = execute_query(sparql)
    logger.info(f"Subjects matching term: {term_iri}")
    logger.info(result)

    subjects = []
    for binding in result["results"]["bindings"]:
        subject_iri = binding["subjectIRI"]["value"]
        project_subject_iri = binding["projectSubjectIRI"]["value"]
        entry = {
            "subject_iri": subject_iri,
            "project_subject_iri": project_subject_iri,
            "project_subject_id": project_subject_iri.split("/")[-1],
        }

        entry["term_links"] = get_term_links_for_node(
            subject_iri, hpo_version, mondo_version
        )

        subjects.append(entry)

    return subjects


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
    PREFIX dc: <http://purl.org/dc/terms/>
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


def get_term_links_for_node(
    source_node_iri: str, hpo_version: str, mondo_version: str
) -> list[dict]:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT
    ?link ?term ?term_label ?termlink_creator
    ?termlink_creator_id ?termlink_creator_version ?termlink_creator_title ?termlink_creator_type
    ?evidence ?evidence_type ?evidence_creator
    ?evidence_creator_id ?evidence_creator_version ?evidence_creator_title ?evidence_creator_type

    WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            ?link rdf:type phebee:TermLink ;
                phebee:sourceNode <{source_node_iri}> ;
                phebee:hasTerm ?term .

            OPTIONAL {{ ?link phebee:hasEvidence ?evidence . }}
            OPTIONAL {{ ?link phebee:creator ?termlink_creator . }}

            OPTIONAL {{ ?termlink_creator rdf:type ?termlink_creator_type . }}
            OPTIONAL {{ ?termlink_creator dc:title ?termlink_creator_title . }}
            OPTIONAL {{ ?termlink_creator dc:hasVersion ?termlink_creator_version . }}
            OPTIONAL {{ ?termlink_creator phebee:creatorId ?termlink_creator_id . }}
        }}

        OPTIONAL {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/hpo~{hpo_version}> {{
                ?term rdfs:label ?term_label .
            }}
        }}
        OPTIONAL {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/mondo~{mondo_version}> {{
                ?term rdfs:label ?term_label .
            }}
        }}

        OPTIONAL {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                ?evidence rdf:type ?evidence_type .
                ?evidence phebee:creator ?evidence_creator .

                OPTIONAL {{ ?evidence_creator rdf:type ?evidence_creator_type . }}
                OPTIONAL {{ ?evidence_creator dc:title ?evidence_creator_title . }}
                OPTIONAL {{ ?evidence_creator dc:hasVersion ?evidence_creator_version . }}
                OPTIONAL {{ ?evidence_creator phebee:creatorId ?evidence_creator_id . }}
            }}
        }}
    }}
    """

    result = execute_query(sparql)
    links = {}

    for row in result["results"]["bindings"]:
        link_iri = row["link"]["value"]
        term_iri = row["term"]["value"]
        term_label = row.get("term_label", {}).get("value")

        # TermLink creator
        termlink_creator_iri = row.get("termlink_creator", {}).get("value")
        termlink_creator_id = row.get("termlink_creator_id", {}).get("value")
        termlink_creator_title = row.get("termlink_creator_title", {}).get("value")
        termlink_creator_version = row.get("termlink_creator_version", {}).get("value")
        if termlink_creator_iri:
            creator = {
                "creator_iri": termlink_creator_iri,
                "creator_id": termlink_creator_id,
                "creator_title": termlink_creator_title,
                "creator_version": termlink_creator_version
            }
        else:
            creator = None

        # Initialize top-level link record
        link = links.setdefault(
            link_iri,
            {
                "termlink_iri": link_iri,
                "term_iri": term_iri,
                "term_label": term_label,
                "creator": creator,
                "evidence": {},
            },
        )

        # Parse evidence block
        evidence_iri = row.get("evidence", {}).get("value")
        if evidence_iri:
            if (
                row.get("evidence_type", {}).get("value")
                == "http://ods.nationwidechildrens.org/phebee#TermLink"
            ):
                continue  # Skip malformed nested TermLinks

            ev = link["evidence"].setdefault(
                evidence_iri,
                {
                    "evidence_iri": evidence_iri,
                    "evidence_type": row.get("evidence_type", {}).get("value"),
                    "creator": None,
                    "properties": {},
                },
            )

            # Add creator if present
            evidence_creator_iri = row.get("evidence_creator", {}).get("value")
            evidence_creator_id = row.get("evidence_creator_id", {}).get("value")
            evidence_creator_title = row.get("evidence_creator_title", {}).get("value")
            evidence_creator_version = row.get("evidence_creator_version", {}).get("value")
            if evidence_creator_iri:
                ev["creator"] = {
                    "creator_iri": evidence_creator_iri,
                    "creator_id": evidence_creator_id,
                    "creator_title": evidence_creator_title,
                    "creator_version": evidence_creator_version
                }

            # Add any extra evidence properties
            p = row.get("p", {}).get("value")
            o = row.get("o", {}).get("value")
            if p and o:
                ev["properties"][p] = o

    return [
        {
            "termlink_iri": link["termlink_iri"],
            "term_iri": link["term_iri"],
            "term_label": link["term_label"],
            "creator": link["creator"],
            "evidence": list(link["evidence"].values()),
        }
        for link in links.values()
    ]


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
                            phebee:subject <{subject_iri}> .
        }}
    }}
    """
    execute_update(sparql)


def get_encounter(subject_iri: str, encounter_id: str) -> dict:
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
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
):
    clinical_note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    now_iso = get_current_timestamp()

    triples = [
        f"<{clinical_note_iri}> rdf:type phebee:ClinicalNote",
        f'<{clinical_note_iri}> phebee:clinicalNoteId "{clinical_note_id}"',
        f"<{clinical_note_iri}> phebee:hasEncounter <{encounter_iri}>",
        f'<{clinical_note_iri}> dc:created "{now_iso}"^^xsd:dateTime',
    ]

    if note_timestamp:
        triples.append(
            f'<{clinical_note_iri}> phebee:noteTimestamp "{note_timestamp}"^^xsd:dateTime'
        )

    triples_block = " .\n        ".join(triples)

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
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
    PREFIX dc: <http://purl.org/dc/terms/>
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
    creator_id_safe = quote(creator_id, safe="")
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
        f'<{creator_iri}> dc:created "{now_iso}"^^xsd:dateTime',
    ]

    if name:
        triples.append(f'<{creator_iri}> dc:title "{name}"')
    if creator_type == "automated":
        if not version:
            raise ValueError("version is required for automated creators")
        triples.append(f'<{creator_iri}> dc:hasVersion "{version}"')

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block}
        }}
    }}
    """
    execute_update(sparql)


def get_creator(creator_id: str) -> dict:
    creator_id_safe = quote(creator_id, safe="")
    creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
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


def delete_creator(creator_id: str):
    creator_id_safe = quote(creator_id, safe="")
    creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"
    sparql = f"""
    DELETE WHERE {{
        <{creator_iri}> ?p ?o .
    }};
    DELETE WHERE {{
        ?s ?p <{creator_iri}> .
    }}
    """
    execute_update(sparql)


def create_text_annotation(
    text_source_iri: str,
    span_start: int = None,
    span_end: int = None,
    creator_iri: str = None,
    term_iri: str = None,
    metadata: str = None,
) -> str:
    annotation_id = str(uuid.uuid4())
    annotation_iri = f"{text_source_iri}/annotation/{annotation_id}"
    created = get_current_timestamp()

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
        f'<{annotation_iri}> dc:created "{created}"^^xsd:dateTime',
        f"<{annotation_iri}> phebee:evidenceType <{evidence_type_iri}>",
        f"<{annotation_iri}> phebee:assertionType <{assertion_type_iri}>",
    ]

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
        triples.append(f'<{annotation_iri}> phebee:metadata """{metadata}"""')

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
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
    PREFIX dc: <http://purl.org/dc/terms/>
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
    source_node_iri: str, term_iri: str, creator_iri: str, evidence_iris: list[str]
) -> str:
    termlink_id = str(uuid.uuid4())
    termlink_iri = f"{source_node_iri}/term-link/{termlink_id}"
    created = get_current_timestamp()

    triples = [
        f"<{termlink_iri}> rdf:type phebee:TermLink",
        f"<{termlink_iri}> phebee:sourceNode <{source_node_iri}>",
        f"<{termlink_iri}> phebee:hasTerm <{term_iri}>",
        f"<{termlink_iri}> phebee:creator <{creator_iri}>",
        f'<{termlink_iri}> dc:created "{created}"^^xsd:dateTime',
    ]

    if evidence_iris:
        for evidence_iri in evidence_iris:
            triples.append(f"<{termlink_iri}> phebee:hasEvidence <{evidence_iri}>")

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            {triples_block}
        }}
    }}
    """
    execute_update(sparql)
    return termlink_iri


def get_term_link(termlink_iri: str) -> dict:
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
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
