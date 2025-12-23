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
import os
from collections import defaultdict
from .hash import generate_termlink_hash
from typing import List, Optional, Sequence
try:
    from aws_lambda_powertools import Metrics, Logger, Tracer
    logger = Logger()
    tracer = Tracer()
    metrics = Metrics()
except ImportError:
    # For testing environments where aws_lambda_powertools isn't available
    class NoOpLogger:
        def info(self, *args, **kwargs): pass
        def debug(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
        def warning(self, *args, **kwargs): pass
    
    logger = NoOpLogger()
    tracer = None
    metrics = None

from datetime import datetime
from urllib.parse import quote
from collections import defaultdict
from phebee.constants import SPARQL_SEPARATOR, PHEBEE
from .neptune import execute_query, execute_update
from .aws import get_current_timestamp


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


def get_subject_iri_from_project_subject_id(project_id: str, project_subject_id: str) -> str:
    """Get the subject IRI for a given project_id and project_subject_id"""
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"
    subject = get_subject(project_subject_iri)
    return subject["subject_iri"] if subject else None


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

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> rdf:type phebee:Subject .
        }}
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId "{project_subject_id}" ;
                            phebee:hasProject <{project_iri}> .
        }}
    }}
    """
    execute_update(sparql)
    return subject_iri


def link_subject_to_project(
    subject_iri: str, project_id: str, project_subject_id: str
) -> None:
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"
    
    sparql = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT DATA {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                    phebee:hasProject <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> .
        }}
    }}
    """
    execute_update(sparql)


def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_term_labels(term_iris: list[str], hpo_version: str, mondo_version: str) -> dict:
    """Get labels for a list of term IRIs efficiently."""
    if not term_iris:
        return {}
    
    term_values = " ".join(f"<{iri}>" for iri in term_iris)
    
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
    SELECT ?term ?label
    FROM <http://ods.nationwidechildrens.org/phebee/hpo~{hpo_version}>
    FROM <http://ods.nationwidechildrens.org/phebee/mondo~{mondo_version}>
    WHERE {{
        VALUES ?term {{ {term_values} }}
        ?term rdfs:label ?label .
    }}
    """
    
    result = execute_query(query)
    return {b["term"]["value"]: b["label"]["value"] 
            for b in result["results"]["bindings"]}


def build_qualifier_filter(qualifiers):
    """Build SPARQL filter for qualifier matching.
    
    Args:
        qualifiers: List of qualifier IRIs
        
    Returns:
        SPARQL filter string for qualifier matching
    """
    if not qualifiers:
        return "FILTER NOT EXISTS { ?link phebee:hasQualifyingTerm ?q }"
    
    # Build required qualifier patterns
    required_patterns = []
    for qualifier in qualifiers:
        required_patterns.append(f"?link phebee:hasQualifyingTerm <{qualifier}> .")
    
    # Build exclusion filter for other qualifiers
    qualifier_list = " ".join(f"<{q}>" for q in qualifiers)
    exclusion_filter = f"""FILTER NOT EXISTS {{
        ?link phebee:hasQualifyingTerm ?other .
        FILTER(?other NOT IN ({qualifier_list}))
    }}"""
    
    # Combine required patterns and exclusion
    return "\\n            ".join(required_patterns) + "\\n            " + exclusion_filter


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
    include_qualified: bool = False,
    include_child_terms: bool = True,
) -> dict:
    # Extract project_id from project_iri
    project_id = project_iri.split("/")[-1]
    
    # Track if we originally had specific subject IDs
    has_specific_subjects = bool(project_subject_ids)
    
    # Build project filtering - filter by project using ProjectSubjectId nodes
    project_filter_clause = f"?subjectIRI phebee:hasProjectSubjectId ?projectSubjectIdNode ."
    project_filter_clause += f"\n    ?projectSubjectIdNode phebee:hasProject <{project_iri}> ."
    
    # Build subject ID filtering clause if specific project_subject_ids provided
    if has_specific_subjects:
        # Filter to specific project subject IDs using the ProjectSubjectId node IRIs
        psid_iris = " ".join(f'<http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{psid}>' for psid in project_subject_ids)
        subject_id_filter_clause = f"VALUES ?projectSubjectIdNode {{ {psid_iris} }}"
    else:
        # No specific filtering - get all subjects in project
        subject_id_filter_clause = ""

    # Build term filtering clause if term_iri is provided
    term_filter_clause = ""
    
    if term_iri:
        # Determine term source version
        if term_source == "hpo":
            actual_term_source_version = hpo_version or "latest"
        elif term_source == "mondo":
            actual_term_source_version = mondo_version or "latest"
        else:
            actual_term_source_version = term_source_version or "latest"
            
        # Build qualifier filter based on include_qualified parameter
        if include_qualified:
            qualifier_filter = ""
        else:
            qualifier_filter = """
            FILTER NOT EXISTS {
                ?termlink phebee:hasQualifyingTerm ?excludedQualifier .
                VALUES ?excludedQualifier {
                    <http://ods.nationwidechildrens.org/phebee/qualifier/negated>
                    <http://ods.nationwidechildrens.org/phebee/qualifier/hypothetical>
                    <http://ods.nationwidechildrens.org/phebee/qualifier/family>
                }
            }"""
        
        # Build term hierarchy clause based on include_child_terms parameter
        if include_child_terms:
            term_hierarchy_clause = f"?term rdfs:subClassOf* <{term_iri}> ."
        else:
            term_hierarchy_clause = f"FILTER (?term = <{term_iri}>)"
            
        term_filter_clause = f"""
            # Find termlinks for these subjects with the specified term
            ?subjectIRI phebee:hasTermLink ?termlink .
            ?termlink rdf:type phebee:TermLink ;
                      phebee:hasTerm ?term .
            
            # Term hierarchy check
            {term_hierarchy_clause}
            
            # Qualifier filtering
            {qualifier_filter}
        """
    else:
        # No term filtering - just get all subjects
        term_filter_clause = ""
        term_hierarchy_clause = ""

    # Build cursor filtering clause
    cursor_clause = f"FILTER (str(?subjectIRI) > \"{cursor}\")" if cursor else ""
    
    # Build the SPARQL query based on whether we have term filtering or not
    if term_iri:
        # Query with term filtering - include ontology graphs for hierarchy
        subjects_query = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?subjectIRI ?projectSubjectIdNode
        FROM <http://ods.nationwidechildrens.org/phebee/subjects>
        FROM <http://ods.nationwidechildrens.org/phebee/projects/{project_id}>
        FROM <http://ods.nationwidechildrens.org/phebee/hpo~{hpo_version}>
        FROM <http://ods.nationwidechildrens.org/phebee/mondo~{mondo_version}>
        WHERE {{
            {subject_id_filter_clause}
            
            ?subjectIRI rdf:type phebee:Subject .
            
            {term_filter_clause}
            
            # Project filtering
            {project_filter_clause}
            
            {cursor_clause}
        }}
        ORDER BY str(?subjectIRI)
        LIMIT {limit + 1}"""
    else:
        # Query without term filtering - no need for ontology graphs
        subjects_query = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT DISTINCT ?subjectIRI ?projectSubjectIdNode
        FROM <http://ods.nationwidechildrens.org/phebee/subjects>
        FROM <http://ods.nationwidechildrens.org/phebee/projects/{project_id}>
        WHERE {{
            {subject_id_filter_clause}
            
            ?subjectIRI rdf:type phebee:Subject .
            
            # Project filtering
            {project_filter_clause}
            
            {cursor_clause}
        }}
        ORDER BY str(?subjectIRI)
        LIMIT {limit + 1}"""
    
    # Execute subjects query
    subjects_result = execute_query(subjects_query)
    logger.info("Subjects query returned %d subjects", len(subjects_result["results"]["bindings"]))
    
    # Process pagination
    subject_bindings = subjects_result["results"]["bindings"]
    has_more = len(subject_bindings) > limit
    if has_more:
        subject_bindings = subject_bindings[:limit]
        next_cursor = subject_bindings[-1]["subjectIRI"]["value"] if subject_bindings else None
    else:
        next_cursor = None
    
    # Build subjects map from first query
    subjects_map = {}
    for binding in subject_bindings:
        subject_iri = binding["subjectIRI"]["value"]
        project_subject_id_node = binding.get("projectSubjectIdNode", {}).get("value")
        
        # Extract project_subject_id from the ProjectSubjectId node IRI
        project_subject_id = project_subject_id_node.split("/")[-1] if project_subject_id_node else None
        project_subject_iri = project_subject_id_node  # The node IRI is the project_subject_iri
        
        subjects_map[subject_iri] = {
            "subject_iri": subject_iri,
            "project_subject_iri": project_subject_iri,
            "project_subject_id": project_subject_id,
            "term_links": {}
        }
    
    # Second query: Get term links for these specific subjects
    if subjects_map:
        subject_values = " ".join(f"<{iri}>" for iri in subjects_map.keys())
        
        term_links_query = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT ?subjectIRI ?termlink ?term
        WHERE {{
            VALUES ?subjectIRI {{ {subject_values} }}
            
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                ?subjectIRI phebee:hasTermLink ?termlink .
                ?termlink rdf:type phebee:TermLink ;
                          phebee:hasTerm ?term .
            }}
        }}
        ORDER BY str(?subjectIRI) str(?termlink)"""
        
        # Execute term links query
        term_links_result = execute_query(term_links_query)
        logger.info("Term links query returned %d term links", len(term_links_result["results"]["bindings"]))
        
        # Add term links to subjects
        for binding in term_links_result["results"]["bindings"]:
            subject_iri = binding["subjectIRI"]["value"]
            termlink_iri = binding["termlink"]["value"]
            term_iri = binding["term"]["value"]
            
            if subject_iri in subjects_map:
                if termlink_iri not in subjects_map[subject_iri]["term_links"]:
                    subjects_map[subject_iri]["term_links"][termlink_iri] = {
                        "term_iri": term_iri,
                        "qualifiers": []
                    }
    
    # Get ordered subject IRIs for consistent output
    subject_iris = [binding["subjectIRI"]["value"] for binding in subject_bindings]
    paginated_subjects_map = subjects_map
    
    # If no subjects found, return empty result
    if not paginated_subjects_map:
        return {
            "subjects": [],
            "pagination": {
                "limit": limit,
                "has_more": False,
                "next_cursor": None,
                "cursor": cursor
            }
        }
    
    # Get term labels for all terms
    unique_terms = set()
    for subject in paginated_subjects_map.values():
        for term_link in subject["term_links"].values():
            unique_terms.add(term_link["term_iri"])
    
    term_labels = get_term_labels(list(unique_terms), hpo_version, mondo_version) if unique_terms else {}
    
    # Convert to final format
    subjects = []
    for subject_iri in subject_iris:  # Use ordered subject_iris for consistent ordering
        subject = paginated_subjects_map[subject_iri]
        
        # Use the project_subject_id from the SPARQL result
        project_subject_id = subject["project_subject_id"]
        
        # Convert term_links to final format
        term_links = []
        for term_link in subject["term_links"].values():
            term_links.append({
                "term_iri": term_link["term_iri"],
                "term_label": term_labels.get(term_link["term_iri"]),
                "qualifiers": term_link["qualifiers"]
            })
        
        subjects.append({
            "subject_iri": subject_iri,
            "project_subject_iri": f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}" if project_subject_id else None,
            "project_subject_id": project_subject_id,
            "term_links": sorted(term_links, key=lambda x: x["term_iri"])
        })
    
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


def get_term_links_with_counts(
    subject_id: str,
    encounter_id: str | None = None,
    note_id: str | None = None,
    hpo_version: str | None = None,
    mondo_version: str | None = None,
) -> list[dict]:
    """
    Query Iceberg for term links with evidence counts, grouped by term + qualifiers.
    
    Args:
        subject_id: The subject UUID
        encounter_id: Optional encounter filter
        note_id: Optional clinical note filter  
        hpo_version: HPO version (for future term label lookup)
        mondo_version: MONDO version (for future term label lookup)
    
    Returns:
      [
        {
          "term_iri": str,
          "term_label": Optional[str],
          "qualifiers": [str, ...],
          "evidence_count": int
        },
        ...
      ]
    """
    from phebee.utils.iceberg import query_iceberg_evidence
    
    # Build filter conditions
    filters = [f"subject_id = '{subject_id}'"]
    
    if encounter_id:
        filters.append(f"encounter_id = '{encounter_id}'")
    if note_id:
        filters.append(f"clinical_note_id = '{note_id}'")
    
    where_clause = " AND ".join(filters)
    
    # Get database name from environment
    database_name = os.environ.get('ICEBERG_DATABASE')
    if not database_name:
        raise ValueError("ICEBERG_DATABASE environment variable is required")
    
    # Query Iceberg for evidence data grouped by term and qualifiers
    query = f"""
    SELECT 
        term_iri,
        qualifiers,
        termlink_id,
        COUNT(*) as evidence_count
    FROM {database_name}.evidence
    WHERE {where_clause}
    GROUP BY term_iri, qualifiers, termlink_id
    ORDER BY term_iri
    """
    
    try:
        results = query_iceberg_evidence(query)
        
        # Group by term_iri + qualifiers combination
        term_groups = {}
        
        for row in results:
            term_iri = row['term_iri']
            qualifiers_json = row.get('qualifiers', '[]')
            termlink_id = row['termlink_id']
            evidence_count = int(row['evidence_count'])
            
            # Parse qualifiers array
            qualifiers = []
            try:
                import json
                qualifiers_list = json.loads(qualifiers_json) if qualifiers_json else []
                # Extract qualifier types where value = '1' (active)
                qualifiers = [
                    q['qualifier_type'] for q in qualifiers_list 
                    if q.get('qualifier_value') == '1'
                ]
            except:
                pass
            
            # Create group key from term + sorted qualifiers
            qualifiers_key = tuple(sorted(qualifiers))
            group_key = (term_iri, qualifiers_key)
            
            if group_key not in term_groups:
                term_groups[group_key] = {
                    "term_iri": term_iri,
                    "term_label": None,  # TODO: Add term label lookup
                    "qualifiers": qualifiers,
                    "evidence_count": 0
                }
            
            # Accumulate counts
            term_groups[group_key]["evidence_count"] += evidence_count
        
        # Convert to list and add term labels if versions provided
        links = list(term_groups.values())
        
        # TODO: Add term label lookup using hpo_version/mondo_version
        # This would require querying the ontology data
        
        return links
        
    except Exception as e:
        logger.error(f"Error querying Iceberg for term links: {e}")
        return []

    # Optional labels for terms
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
        
        for link_obj in links:
            link_obj["term_label"] = label_map.get(link_obj["term_iri"])

    return links


def create_term_link(
    subject_iri: str, term_iri: str, creator_iri: str, qualifiers=None
) -> dict:
    """
    Create a term link between a subject and a term.
    
    Args:
        subject_iri (str): The IRI of the subject
        term_iri (str): The IRI of the term
        creator_iri (str): The IRI of the creator
        qualifiers (list): List of qualifier IRIs
        
    Returns:
        dict: Dictionary with the term link IRI and whether it was created
    """
    # Generate deterministic hash based on subject, term, and qualifiers
    termlink_hash = generate_termlink_hash(subject_iri, term_iri, qualifiers)
    termlink_iri = f"{subject_iri}/term-link/{termlink_hash}"
    
    # Check if the term link already exists
    link_exists = node_exists(termlink_iri)
    
    if link_exists:
        logger.info("Term link already exists: %s", termlink_iri)
        return {"termlink_iri": termlink_iri, "created": False}
    
    # Create the term link if it doesn't exist
    created = get_current_timestamp()

    triples = [
        f"<{termlink_iri}> rdf:type phebee:TermLink",
        f"<{termlink_iri}> phebee:hasTerm <{term_iri}>",
        f"<{termlink_iri}> phebee:creator <{creator_iri}>",
        f'<{termlink_iri}> dcterms:created "{created}"^^xsd:dateTime',
        f"<{subject_iri}> phebee:hasTermLink <{termlink_iri}>",
    ]
            
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
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
    SELECT ?term ?qualifier
    WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{termlink_iri}> a phebee:TermLink ;
                           phebee:hasTerm ?term .
            
            OPTIONAL {{
                <{termlink_iri}> phebee:hasQualifyingTerm ?qualifier .
            }}
        }}
    }}
    """
    results = execute_query(sparql)
    bindings = results["results"]["bindings"]
    
    if not bindings:
        return None
    
    # Get basic term link info from first row
    first_row = bindings[0]
    term_iri = first_row["term"]["value"]
    
    # Extract subject_id from termlink_iri
    subject_id = termlink_iri.split("/subjects/")[1].split("/term-link/")[0]
    
    # Get term label
    term_label = None
    label_query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?label WHERE {{
        <{term_iri}> rdfs:label ?label .
    }}
    """
    label_results = execute_query(label_query)
    if label_results["results"]["bindings"]:
        term_label = label_results["results"]["bindings"][0]["label"]["value"]
    
    # Collect qualifiers
    qualifiers = set()
    for row in bindings:
        if "qualifier" in row:
            qualifiers.add(row["qualifier"]["value"])
    
    # Get evidence from Iceberg
    from phebee.utils.iceberg import get_evidence_for_termlink
    
    # Extract subject_id from termlink_iri
    subject_id = termlink_iri.split("/subjects/")[1].split("/term-link/")[0]
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    evidence = get_evidence_for_termlink(subject_id, term_iri, list(qualifiers))
    
    return {
        "termlink_iri": termlink_iri,
        "term_iri": term_iri,
        "term_label": term_label,
        "subject_iri": subject_iri,
        "qualifiers": sorted(list(qualifiers)),
        "evidence": evidence
    }


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
