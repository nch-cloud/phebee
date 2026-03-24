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
        query_upper = query.strip().upper()
        if not any(query_upper.startswith(op) for op in ['SELECT', 'ASK', 'DESCRIBE', 'CONSTRUCT']):
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
    """Get list of allowed graph IRIs based on restriction level."""
    subjects_graph = 'http://ods.nationwidechildrens.org/phebee/subjects'
    ontology_graphs = [
        'http://purl.obolibrary.org/obo/hp.owl',
        'http://purl.obolibrary.org/obo/mondo.owl',
        'http://purl.obolibrary.org/obo/eco.owl'
    ]

    if restriction == 'subjects':
        return [subjects_graph]
    elif restriction == 'ontology':
        return ontology_graphs
    elif restriction == 'any':
        return [subjects_graph] + ontology_graphs
    else:
        raise ValueError(f"Invalid graphRestriction: {restriction}. Must be 'subjects', 'ontology', or 'any'")


def validate_query_graphs(query, allowed_graphs):
    """
    Validate that query only accesses allowed graphs.
    Blocks access to project graphs (which may contain PHI).
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
        for graph_iri in found_graphs:
            if graph_iri not in allowed_graphs:
                raise ValueError(
                    f"Access to graph '{graph_iri}' is not allowed. "
                    f"Allowed graphs: {', '.join(allowed_graphs)}"
                )

    logger.info(f"Query validation passed. Graphs referenced: {found_graphs if found_graphs else 'default'}")
