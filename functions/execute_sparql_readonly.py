"""
Execute Read-Only SPARQL Queries on Neptune

This Lambda function provides a safe interface for integration tests and debugging
to query Neptune without direct VPC access. It restricts queries to:
- Subject graph: http://ods.nationwidechildrens.org/phebee/subjects
- Ontology graphs: HPO, MONDO, ECO

Project graphs (which may contain PHI) are blocked.

Input:
    {
        "query": "SELECT ?s WHERE { ... }",
        "graphRestriction": "subjects" | "ontology" | "any"  # optional, defaults to "subjects"
    }

Output:
    {
        "success": true,
        "results": { ... SPARQL results ... },
        "rowCount": N
    }
"""

import json
import logging
import os
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Execute read-only SPARQL query with graph restrictions.
    """
    try:
        from phebee.utils import sparql

        query = event.get('query')
        if not query:
            raise ValueError("Missing required parameter: query")

        graph_restriction = event.get('graphRestriction', 'subjects')

        # Validate query is read-only (SELECT, ASK, DESCRIBE, CONSTRUCT)
        # Strip PREFIX declarations and comments to find the actual query operation
        query_upper = query.strip().upper()

        # Remove PREFIX declarations (PREFIX x: <...>)
        query_without_prefix = re.sub(r'PREFIX\s+\w+:\s*<[^>]+>\s*', '', query_upper, flags=re.IGNORECASE)
        query_without_prefix = query_without_prefix.strip()

        # Check if query starts with allowed read-only operation
        if not any(query_without_prefix.startswith(op) for op in ['SELECT', 'ASK', 'DESCRIBE', 'CONSTRUCT']):
            raise ValueError(f"Only read-only queries (SELECT, ASK, DESCRIBE, CONSTRUCT) are allowed. Got: {query[:50]}...")

        # Block write operations
        write_keywords = ['INSERT', 'DELETE', 'DROP', 'CLEAR', 'LOAD', 'CREATE', 'COPY', 'MOVE', 'ADD']
        for keyword in write_keywords:
            if keyword in query_upper:
                raise ValueError(f"Write operation '{keyword}' is not allowed in read-only queries")

        # Validate graph restrictions
        allowed_graphs = get_allowed_graphs(graph_restriction)
        validate_query_graphs(query, allowed_graphs)

        logger.info(f"Executing read-only SPARQL query (restriction: {graph_restriction})")
        logger.info(f"Query: {query[:200]}{'...' if len(query) > 200 else ''}")

        # Execute query
        result = sparql.execute_query(query)

        if not result:
            return {
                "success": False,
                "error": "Query execution returned no results"
            }

        # Count rows
        row_count = 0
        if 'results' in result and 'bindings' in result['results']:
            row_count = len(result['results']['bindings'])

        logger.info(f"Query executed successfully. Rows returned: {row_count}")

        return {
            "success": True,
            "results": result,
            "rowCount": row_count
        }

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            "success": False,
            "error": f"Validation error: {str(e)}"
        }

    except Exception as e:
        logger.error(f"Error executing SPARQL query: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def get_allowed_graphs(restriction):
    """
    Get list of allowed graph IRIs/patterns based on restriction level.

    Returns a dict with:
    - 'exact': list of exact graph IRIs that are allowed
    - 'patterns': list of regex patterns for allowed graph IRI patterns
    """
    subjects_graph = 'http://ods.nationwidechildrens.org/phebee/subjects'

    # Allow versioned PheBee ontology graphs (e.g., hpo~v2026-02-16, mondo~v2026-04-07)
    ontology_patterns = [
        r'^http://ods\.nationwidechildrens\.org/phebee/(hpo|mondo|eco)~v\d{4}-\d{2}-\d{2}$',
        r'^http://purl\.obolibrary\.org/obo/(hp|mondo|eco)\.owl$'
    ]

    if restriction == 'subjects':
        return {'exact': [subjects_graph], 'patterns': []}
    elif restriction == 'ontology':
        return {'exact': [], 'patterns': ontology_patterns}
    elif restriction == 'any':
        return {'exact': [subjects_graph], 'patterns': ontology_patterns}
    else:
        raise ValueError(f"Invalid graphRestriction: {restriction}. Must be 'subjects', 'ontology', or 'any'")


def validate_query_graphs(query, allowed_graphs):
    """
    Validate that query only accesses allowed graphs.
    Blocks access to project graphs (which may contain PHI).

    Args:
        query: SPARQL query string
        allowed_graphs: Dict with 'exact' (list of exact IRIs) and 'patterns' (list of regex patterns)
    """
    # Extract all graph IRIs from query
    # Pattern: GRAPH <...> or FROM <...>
    graph_pattern = r'(?:GRAPH|FROM)\s+<([^>]+)>'
    found_graphs = re.findall(graph_pattern, query, re.IGNORECASE)

    # Block project graphs (pattern: .../projects/...)
    for graph_iri in found_graphs:
        if '/projects/' in graph_iri.lower():
            raise ValueError(
                f"Access to project graphs is not allowed (may contain PHI). "
                f"Blocked graph: {graph_iri}"
            )

    # If query specifies graphs explicitly, verify they're allowed
    if found_graphs:
        exact_allowed = allowed_graphs.get('exact', [])
        pattern_allowed = allowed_graphs.get('patterns', [])

        for graph_iri in found_graphs:
            # Check exact match
            if graph_iri in exact_allowed:
                continue

            # Check pattern match
            matched = False
            for pattern in pattern_allowed:
                if re.match(pattern, graph_iri):
                    matched = True
                    break

            if not matched:
                # Build helpful error message
                allowed_examples = exact_allowed + ['<ontology graphs matching patterns>']
                raise ValueError(
                    f"Access to graph '{graph_iri}' is not allowed. "
                    f"Allowed: subjects graph, versioned PheBee ontology graphs (e.g., .../hpo~v2026-02-16)"
                )

    logger.info(f"Query validation passed. Graphs referenced: {found_graphs if found_graphs else 'default'}")
