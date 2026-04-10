"""
Clear Neptune Graph Lambda

Clears the subjects graph in Neptune by deleting one batch of triples.
Designed to be called repeatedly by Step Functions until graph is empty.
"""

import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Delete one batch of triples from Neptune graph.
    Returns status indicating if more batches remain.

    Input:
        {
            "graphUri": "http://ods.nationwidechildrens.org/phebee/subjects",
            "batchSize": 10000,           // optional, triples per batch
            "totalTriplesDeleted": 0,     // optional, cumulative triple count
            "iteration": 0                // optional, iteration counter
        }

    Output:
        {
            "graphUri": "...",
            "status": "IN_PROGRESS" | "COMPLETE",
            "remainingTriples": 123456,
            "totalTriplesDeleted": 10000,
            "iteration": 1
        }
    """
    try:
        # Import phebee utilities (from Lambda layer)
        from phebee.utils import sparql

        graph_uri = event['graphUri']
        batch_size = event.get('batchSize', 10000)  # Number of triples per batch
        total_triples_deleted = event.get('totalTriplesDeleted', 0)
        iteration = event.get('iteration', 0)

        iteration += 1

        logger.info(f"Clearing Neptune graph: {graph_uri}")
        logger.info(f"Iteration {iteration}, batch size: {batch_size} triples, total triples deleted so far: {total_triples_deleted:,}")

        # Get a batch of triples from the graph
        select_query = f"""
        SELECT ?s ?p ?o
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }}
        LIMIT {batch_size}
        """

        logger.info(f"Fetching batch of up to {batch_size} triples")
        result = sparql.execute_query(select_query)

        if not result or 'results' not in result or 'bindings' not in result['results']:
            logger.error("No results from SELECT query")
            raise Exception("Failed to fetch triples from graph")

        triples = result['results']['bindings']
        triple_count = len(triples)

        if triple_count == 0:
            logger.info("No triples found, graph is empty")
        else:
            logger.info(f"Found {triple_count} triples to delete")

            # Build DELETE query for these specific triples using VALUES
            # Format each triple for VALUES clause
            def format_rdf_term(term):
                """Format an RDF term for use in SPARQL VALUES clause."""
                if term['type'] == 'uri':
                    return f"<{term['value']}>"
                elif term['type'] == 'literal':
                    value = term['value'].replace('\\', '\\\\').replace('"', '\\"')
                    if 'datatype' in term:
                        return f'"{value}"^^<{term["datatype"]}>'
                    elif 'xml:lang' in term:
                        return f'"{value}"@{term["xml:lang"]}'
                    else:
                        return f'"{value}"'
                elif term['type'] == 'bnode':
                    return f"_:{term['value']}"
                else:
                    raise ValueError(f"Unknown RDF term type: {term['type']}")

            values_list = []
            for triple in triples:
                s = format_rdf_term(triple['s'])
                p = format_rdf_term(triple['p'])
                o = format_rdf_term(triple['o'])
                values_list.append(f"({s} {p} {o})")

            values_clause = '\n        '.join(values_list)

            delete_query = f"""
            DELETE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
            }}
            WHERE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o
                }}
                VALUES (?s ?p ?o) {{
                    {values_clause}
                }}
            }}
            """

            logger.info(f"Deleting {triple_count} triples")
            sparql.execute_update(delete_query)

        # Check remaining triple count
        count_query = f"""
        SELECT (COUNT(*) AS ?tripleCount)
        WHERE {{
            GRAPH <{graph_uri}> {{
                ?s ?p ?o
            }}
        }}
        """

        result = sparql.execute_query(count_query)
        remaining_triples = 0

        if result and 'results' in result and 'bindings' in result['results']:
            bindings = result['results']['bindings']
            if bindings and 'tripleCount' in bindings[0]:
                remaining_triples = int(bindings[0]['tripleCount']['value'])

        logger.info(f"Remaining: {remaining_triples:,} triples")

        new_total_triples = total_triples_deleted + triple_count

        if remaining_triples == 0:
            logger.info(f"Graph cleared successfully!")
            logger.info(f"Total triples deleted: {new_total_triples:,} (in {iteration} iterations)")

            return {
                "graphUri": graph_uri,
                "status": "COMPLETE",
                "remainingTriples": 0,
                "totalTriplesDeleted": new_total_triples,
                "iteration": iteration,
                "batchSize": batch_size
            }
        else:
            logger.info(f"More data remains, will continue...")

            return {
                "graphUri": graph_uri,
                "status": "IN_PROGRESS",
                "remainingTriples": remaining_triples,
                "totalTriplesDeleted": new_total_triples,
                "iteration": iteration,
                "batchSize": batch_size
            }

    except KeyError as e:
        logger.error(f"Missing required parameter: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error clearing Neptune graph: {str(e)}", exc_info=True)
        raise
