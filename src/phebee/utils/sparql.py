import re
import uuid
from collections import defaultdict
from typing import List
from aws_lambda_powertools import Metrics, Logger, Tracer
from datetime import datetime
from urllib.parse import quote

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


def get_subject(project_id: str, project_subject_id: str) -> dict:
    # Get project node with IRI matching project_id
    # Get project-subject id nodes pointing at project node
    # Create a project-subject id IRI matching our project's namespace and provided project_subject_id
    # Find the subject node connected to the created project-subject id
    sparql = f"""
        PREFIX phebeePredicate: <http://ods.nationwidechildrens.org/phebee#>

        SELECT ?subject ?project ?projectSubjectId
        WHERE {{
            ?projectParam phebeePredicate:hasProjectId "{project_id}" .
            
            BIND(IRI(CONCAT(STR(?projectParam), "#", "{project_subject_id}")) AS ?projectSubjectIdParam)
            
            ?subject phebeePredicate:hasProjectSubjectId ?projectSubjectIdParam .
            
            ?subject phebeePredicate:hasProjectSubjectId ?projectSubjectId .
            ?projectSubjectId phebeePredicate:hasProject ?project
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
            "iri": subject_iri,
            # Yes, we passsed this value in, but this keeps the return format consistent with get_subjects_for_project
            "project_subject_id": project_subject_id,
        }


@tracer.capture_method
def get_subjects_for_project(
    project_id: str,
    term_source: str = None,
    term_source_version: str = None,
    term_iri: str = None,
) -> list:
    if term_iri:
        # Get only subjects matching the given term
        sparql = f"""
            PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

            SELECT ?subjectIRI ?projectSubjectId
            FROM <http://ods.nationwidechildrens.org/phebee/{term_source}~{term_source_version}>
            FROM <http://ods.nationwidechildrens.org/phebee/projects/{project_id}>
            FROM <http://ods.nationwidechildrens.org/phebee/subjects>
            WHERE {{
                ?projectSubjectId phebee:hasProject <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> .
                ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectId .
                ?subjectIRI phebee:hasSubjectTermLink ?subjectTermLink .
                ?subjectTermLink phebee:hasTerm ?term .
                ?term rdfs:subClassOf* <{term_iri}>
            }}
        """
    else:
        # Get all subjects
        sparql = f"""
            PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

            SELECT ?subjectIRI ?projectSubjectId
            FROM <http://ods.nationwidechildrens.org/phebee/projects/{project_id}>
            WHERE {{
                ?projectSubjectId phebee:hasProject <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> .
                ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectId
            }}
        """

    result = execute_query(sparql)

    logger.info(result)

    subjects = [
        {
            "iri": binding["subjectIRI"]["value"],
            "project_subject_id": binding["projectSubjectId"]["value"],
        }
        for binding in result["results"]["bindings"]
    ]

    return subjects


def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def validate_optional_fields(optional_fields: list) -> None:
    """
    Validate the user-provided optional fields.
    :param optional_fields: List of optional fields to be validated.
    :raises ValueError: If any field is not valid.
    """
    valid_fields = [
        "creatorVersion",
        "termSource",
        "termSourceVersion",
        "evidenceText",
        "evidenceCreated",
        "evidenceReferenceId",
        "evidenceReferenceDescription",
    ]
    for field in optional_fields:
        if field not in valid_fields:
            raise ValueError(f"Invalid optional evidence field: {field}")


@tracer.capture_method
def get_subjects(
    project_id: str,
    project_subject_ids: List[str] = None,
    term_iri: str = None,
    term_source: str = None,
    term_source_version: str = None,
    return_excluded_terms: bool = False,
    include_descendants: bool = False,
    include_phenotypes: bool = False,
    include_evidence: bool = False,
    optional_evidence: List[str] = None,
    return_raw_json: bool = False,
    optional_clause: str = None,
    # s3_path: str = None,
) -> list:
    # TODO We pass in an unqualified project_id and project_subject_ids, but return fully qualified IRIs.  If this presents a problem we may want to allow full IRI inputs.

    # These flags require more information to be useful:
    if include_descendants and not term_iri:
        raise ValueError("include_descendants requires a term IRI.")

    if return_excluded_terms and not (include_phenotypes):
        raise ValueError(
            "Returning excluded terms requires include_phenotypes to resolve status."
        )

    if include_evidence and not include_phenotypes:
        raise ValueError("Include evidence requires phenotypes.")

    if optional_evidence:
        validate_optional_fields(optional_evidence)

    prefix_clause = """
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    """

    # FROM clause
    from_clause = f"""
    FROM <http://ods.nationwidechildrens.org/phebee/projects/{project_id}>
    FROM <http://ods.nationwidechildrens.org/phebee/subjects>
    """

    # TODO validate term_source and term_source_version against db to avoid empty responses
    if term_iri or include_phenotypes:
        if not term_source or not term_source_version:
            raise ValueError(
                "Need term_source and term_source_version if terms are used for querying or returned as results."
            )
        from_clause += f"""FROM <http://ods.nationwidechildrens.org/phebee/{term_source}~{term_source_version}>
        """

    # SELECT clause
    select_clause = """
    SELECT ?subjectIRI ?projectSubjectId """

    if include_phenotypes:
        select_clause += " ?term ?termLabel "

    if return_excluded_terms:
        select_clause += " ?excluded "

    if include_evidence:
        select_clause += " ?creator ?created ?assertion_method ?evidence_type "

        if optional_evidence:
            for field in optional_evidence:
                select_clause += f"?{camel_to_snake(field)} "

    select_clause += "\n"

    # WHERE clause
    if project_subject_ids:
        values_clause = "\n        ".join(
            [
                f"<http://ods.nationwidechildrens.org/phebee/projects/{project_id}#{subject_id}>"
                for subject_id in project_subject_ids
            ]
        )

        where_clause = f"""
    WHERE {{
        # Use list of project subject ids
        ?projectSubjectId phebee:hasProject <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> .
        # Filter by project subject id list
        VALUES ?projectSubjectId {{ 
        {values_clause} 
            }}
        ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectId .
        """
    else:
        where_clause = f"""
    WHERE {{
        # Project-specific subjects
        ?projectSubjectId phebee:hasProject <http://ods.nationwidechildrens.org/phebee/projects/{project_id}> .
        ?subjectIRI phebee:hasProjectSubjectId ?projectSubjectId .
    """

    if term_iri or include_phenotypes:
        where_clause += """
        ?subjectIRI phebee:hasSubjectTermLink ?subjectTermLink .
        ?subjectTermLink phebee:hasSubjectTermEvidence ?stle .
        # Handle excluded phenotypes
        OPTIONAL {{ ?stle phebee:excluded ?excluded . }}
    """

    if term_iri:
        # Add query term IRI to the WHERE clause
        if include_descendants:
            where_clause += f"""
        # Include descendents
        ?subjectTermLink phebee:hasTerm ?term .
        ?term rdfs:subClassOf* <{term_iri}> .
            """
        else:
            where_clause += f"""
        # Match the term exactly, don't include descendents
        # We need to bind the fixed term to the ?term variable for the query to work
        BIND(<{term_iri}>  AS ?term) .
        ?subjectTermLink phebee:hasTerm ?term .
            """
    elif include_phenotypes:
        # We don't have a query term, but are returning terms, so need to define ?term variable
        where_clause += """
        ?subjectTermLink phebee:hasTerm ?term .
        """

    # Drop the excluded terms unless explicitly requested
    if term_iri or include_phenotypes and not return_excluded_terms:
        where_clause += """
        # Exclude cases where ?excluded is true
        FILTER (!BOUND(?excluded) || ?excluded = false)
    """

    if include_phenotypes:
        where_clause += """
        # Phenotypes
        ?term rdfs:label ?termLabel .
        """

    if include_evidence:
        where_clause += """
        # Evidence details
        ?stle dcterms:creator ?creator .
        ?stle dcterms:created ?created .
        ?stle phebee:assertionMethod ?assertion_method .
        ?stle phebee:evidenceType ?evidence_type .
        """

        # Add optional evidence fields dynamically
        if optional_evidence:
            for field in optional_evidence:
                where_clause += f"""
        OPTIONAL {{ ?stle phebee:{field} ?{camel_to_snake(field)} . }}
                """

    # Close the WHERE clause
    where_clause += """
    }
    """

    # Combine everything
    sparql = prefix_clause + select_clause + from_clause + where_clause

    # Optional clause to allow debugging e.g. LIMIT and OFFSET
    # TODO Consider pagination and result caching for large queries (only available for Gremlin?)
    # https://aws.amazon.com/blogs/database/part-2-accelerate-graph-query-performance-with-caching-in-amazon-neptune/
    if optional_clause:
        sparql += optional_clause

    # Execute the query and process results
    result = execute_query(sparql)
    logger.info(result)

    if return_raw_json:
        return result
    else:
        return flatten_sparql_results(result, group_subjects=True)


def get_subject_term_info(subject_iri: str) -> dict:
    terms_sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?stl ?term_label ?term_iri ?assertion_method ?creator ?created ?creator_version ?evidence_type ?evidence_text ?evidence_created ?evidence_reference_id ?evidence_reference_description ?term_source ?term_source_version ?excluded

        WHERE {{
            <{subject_iri}> phebee:hasSubjectTermLink ?stl .
            ?stl phebee:hasTerm ?term_iri .
            ?term_iri rdfs:label ?term_label .
            ?stl phebee:hasSubjectTermEvidence ?stle .
            ?stle dcterms:creator ?creator .
            ?stle dcterms:created ?created .
            ?stle phebee:assertionMethod ?assertion_method .
            ?stle phebee:evidenceType ?evidence_type .
            OPTIONAL {{?stle phebee:creatorVersion ?creator_version .}}
            OPTIONAL {{?stle phebee:termSource ?term_source .}}
            OPTIONAL {{?stle phebee:termSourceVersion ?term_source_version .}}
            OPTIONAL {{?stle phebee:evidenceText ?evidence_text .}}
            OPTIONAL {{?stle phebee:evidenceCreated ?evidence_created .}}
            OPTIONAL {{?stle phebee:evidenceReferenceId ?evidence_reference_id .}}
            OPTIONAL {{?stle phebee:evidenceReferenceDescription ?evidence_reference_description .}}
            OPTIONAL {{?stle phebee:excluded ?excluded}}
        }}
    """

    terms_result = execute_query(terms_sparql)

    logger.info(terms_result)

    terms = {}

    for binding in terms_result["results"]["bindings"]:
        logger.info(binding)

        term = binding["term_iri"]["value"]

        # If we haven't seen the term from this term link yet, add it to our list
        if term not in terms:
            terms[term] = {"evidence": []}

            # If we have a label for our term, add it to the response
            if binding["term_label"]["value"]:
                terms[term]["label"] = binding["term_label"]["value"]

        evidence = {
            "creator": binding["creator"]["value"],
            "created": binding["created"]["value"],
            "assertion_method": binding["assertion_method"]["value"],
            "evidence_type": binding["evidence_type"]["value"],
            "evidence_created": binding["evidence_created"]["value"],
        }

        add_optional_evidence_property(evidence, "excluded", binding)
        add_optional_evidence_property(evidence, "creator_version", binding)
        add_optional_evidence_property(evidence, "term_source", binding)
        add_optional_evidence_property(evidence, "term_source_version", binding)
        add_optional_evidence_property(evidence, "evidence_text", binding)
        add_optional_evidence_property(evidence, "evidence_reference_id", binding)
        add_optional_evidence_property(
            evidence, "evidence_reference_description", binding
        )

        terms[term]["evidence"].append(evidence)

    return terms


def add_optional_evidence_property(evidence, property_name, binding):
    if property_name in binding:
        evidence[property_name] = binding[property_name]["value"]


def subject_term_link_exists(subject_iri: str, term_iri: str) -> dict:
    sparql = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

        SELECT ?subjectTermLink WHERE {{
            <{subject_iri}> phebee:hasSubjectTermLink ?subjectTermLink .
            ?subjectTermLink phebee:hasTerm <{term_iri}> .
        }}
    """

    result = execute_query(sparql)

    logger.info(result)

    if len(result["results"]["bindings"]) == 0:
        return {"link_exists": False}
    else:
        subject_term_link_uuid = result["results"]["bindings"][0]["subjectTermLink"][
            "value"
        ]

        return {"link_exists": True, "link_id": subject_term_link_uuid}


def create_subject_term_link(subject_iri: str, term_iri: str) -> dict:
    link_uuid = uuid.uuid4()
    subject_term_link_uuid = f"{subject_iri}/{link_uuid}"

    sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        INSERT DATA {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects>
            {{
                <{subject_iri}> phebee:hasSubjectTermLink <{subject_term_link_uuid}> .
                <{subject_term_link_uuid}> rdf:type phebee:SubjectTermLink ;
                    phebee:hasTerm <{term_iri}> ;
                    dcterms:created "{get_current_timestamp()}"^^xsd:dateTime .
            }}
        }}
    """

    execute_update(sparql)

    return {"link_created": True, "link_id": subject_term_link_uuid}


def create_subject_term_evidence(subject_term_link_id: str, evidence: dict) -> dict:
    excluded = evidence.get("excluded", "").lower() == "true"
    excluded_optional_triple = "phebee:excluded true ;" if excluded else ""

    creator = evidence["creator"]
    created = get_current_timestamp()

    creator_version = evidence.get("creator_version")
    creator_version_optional_triple = (
        f'phebee:creatorVersion "{creator_version}" ;' if creator_version else ""
    )

    term_source = evidence.get("term_source")
    term_source_optional_triple = (
        f'phebee:termSource "{term_source}" ;' if term_source else ""
    )

    term_source_version = evidence.get("term_source_version")
    term_source_version_optional_triple = (
        f'phebee:termSourceVersion "{term_source_version}" ;'
        if term_source_version
        else ""
    )

    evidence_text = evidence.get("evidence_text")
    evidence_text_optional_triple = (
        f'phebee:evidenceText "{evidence_text}" ;' if evidence_text else ""
    )

    evidence_reference_id = evidence.get("evidence_reference_id")
    evidence_reference_id_optional_triple = (
        f'phebee:evidenceReferenceId "{evidence_reference_id}" ;'
        if evidence_reference_id
        else ""
    )

    evidence_reference_description = evidence.get("evidence_reference_description")
    evidence_reference_description_optional_triple = (
        f'phebee:evidenceReferenceDescription "{evidence_reference_description}" ;'
        if evidence_reference_description
        else ""
    )

    evidence_created = evidence.get("evidence_created")
    evidence_created_triple = (
        f'phebee:evidenceCreated "{evidence_created}"^^xsd:dateTime ;'
        if evidence_created
        else f'phebee:evidenceCreated "{created}"^^xsd:dateTime ;'
    )

    evidence_type = evidence["evidence_type"]
    if not evidence_type.startswith("http://purl.obolibrary.org/obo/ECO_"):
        raise Exception(
            "Evidence type should be a term from the Evidence Ontology descended from http://purl.obolibrary.org/obo/ECO_0000000"
        )

    assertion_method = evidence["assertion_method"]
    if assertion_method not in [
        "http://purl.obolibrary.org/obo/ECO_0000218",
        "http://purl.obolibrary.org/obo/ECO_0000203",
    ]:
        raise Exception(
            "Assertion method should be either ECO:0000218  (manual assertion) or ECO:0000203 (automatic assertion) from the Evidence Ontology"
        )

    evidence_uuid = uuid.uuid4()

    subject_term_evidence_uuid = f"{subject_term_link_id}/{evidence_uuid}"

    sparql = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        INSERT DATA {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects>
            {{
                <{subject_term_link_id}> phebee:hasSubjectTermEvidence <{subject_term_evidence_uuid}> .
                <{subject_term_evidence_uuid}> rdf:type phebee:SubjectTermEvidence ;
                    {excluded_optional_triple}
                    dcterms:created "{created}"^^xsd:dateTime ;
                    dcterms:creator "{creator}" ;
                    {creator_version_optional_triple}
                    {term_source_optional_triple}
                    {term_source_version_optional_triple}
                    {evidence_text_optional_triple}
                    {evidence_reference_id_optional_triple}
                    {evidence_reference_description_optional_triple}
                    {evidence_created_triple}
                    phebee:assertionMethod <{assertion_method}> ;
                    phebee:evidenceType <{evidence_type}> .
            }}
        }}
    """

    logger.info(sparql)

    execute_update(sparql)

    return {"evidence_id": subject_term_evidence_uuid}


def create_encounter(subject_iri: str, encounter_id: str):
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    now_iso = get_current_timestamp()
    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>

    INSERT DATA {{
        <{encounter_iri}> rdf:type phebee:Encounter ;
                          phebee:encounterId "{encounter_id}" ;
                          dcterms:created "{now_iso}" ;
                          phebee:subject <{subject_iri}> .
    }}
    """
    execute_update(sparql)


def get_encounter(subject_iri: str, encounter_id: str) -> dict:
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    sparql = f"""
    SELECT ?p ?o WHERE {{
        <{encounter_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    for binding in results["results"]["bindings"]:
        predicate = binding["p"]["value"]
        obj = binding["o"]["value"]

        # Extract unprefixed name from IRI (e.g. ...#encounterType → encounterType)
        key = predicate.split("#")[-1] if "#" in predicate else predicate.split("/")[-1]
        properties[key] = obj

    return {
        "encounter_iri": encounter_iri,
        "subject_iri": subject_iri,
        "encounter_id": encounter_id,
        "properties": properties,
    }


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
        {triples_block} .
    }}
    """

    execute_update(sparql)


def get_clinical_note(encounter_iri: str, clinical_note_id: str) -> dict:
    clinical_note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    sparql = f"""
    SELECT ?p ?o WHERE {{
        <{clinical_note_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    for binding in results["results"]["bindings"]:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = pred.split("#")[-1] if "#" in pred else pred.split("/")[-1]
        properties[key] = obj

    return {
        "clinical_note_iri": clinical_note_iri,
        "encounter_iri": encounter_iri,
        "clinical_note_id": clinical_note_id,
        "properties": properties,
    }


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
        {triples_block}
    }}
    """
    execute_update(sparql)


def get_creator(creator_id: str) -> dict:
    creator_id_safe = quote(creator_id, safe="")
    creator_iri = f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id_safe}"

    sparql = f"""
    SELECT ?p ?o WHERE {{
        <{creator_iri}> ?p ?o .
    }}
    """

    results = execute_query(sparql)
    properties = {}
    for binding in results["results"]["bindings"]:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = pred.split("#")[-1] if "#" in pred else pred.split("/")[-1]
        properties[key] = obj

    return {
        "creator_iri": creator_iri,
        "creator_id": creator_id,
        "properties": properties,
    }


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

    triples = [
        f"<{annotation_iri}> rdf:type phebee:TextAnnotation",
        f"<{annotation_iri}> phebee:textSource <{text_source_iri}>",
        f'<{annotation_iri}> dc:created "{created}"^^xsd:dateTime',
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
        {triples_block}
    }}
    """
    execute_update(sparql)
    return annotation_iri


def get_text_annotation(annotation_iri: str) -> dict:
    sparql = f"""
    SELECT ?p ?o WHERE {{
        <{annotation_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    for binding in results["results"]["bindings"]:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = pred.split("#")[-1] if "#" in pred else pred.split("/")[-1]
        properties[key] = obj

    return {"annotation_iri": annotation_iri, "properties": properties}


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
    source_node_iri: str,
    term_iri: str,
    creator_iri: str,
    evidence_iris: list[str]
) -> str:
    if not evidence_iris:
        raise ValueError("At least one evidence IRI is required.")

    termlink_id = str(uuid.uuid4())
    termlink_iri = f"{source_node_iri}/term-link/{termlink_id}"
    created = get_current_timestamp()

    triples = [
        f"<{termlink_iri}> rdf:type phebee:TermLink",
        f"<{termlink_iri}> phebee:hasTerm <{term_iri}>",
        f"<{termlink_iri}> phebee:creator <{creator_iri}>",
        f"<{termlink_iri}> dc:created \"{created}\"^^xsd:dateTime"
    ]

    for evidence_iri in evidence_iris:
        triples.append(f"<{termlink_iri}> phebee:hasEvidence <{evidence_iri}>")

    triples_block = " .\n    ".join(triples) + " ."

    sparql = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {{
        {triples_block}
    }}
    """
    execute_update(sparql)
    return termlink_iri


def get_term_link(termlink_iri: str) -> dict:
    sparql = f"""
    SELECT ?p ?o WHERE {{
        <{termlink_iri}> ?p ?o .
    }}
    """
    results = execute_query(sparql)

    properties = {}
    for binding in results["results"]["bindings"]:
        pred = binding["p"]["value"]
        obj = binding["o"]["value"]
        key = pred.split("#")[-1] if "#" in pred else pred.split("/")[-1]
        properties.setdefault(key, []).append(obj)

    return {
        "termlink_iri": termlink_iri,
        "properties": properties
    }


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
