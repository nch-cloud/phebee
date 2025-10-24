import json
from aws_lambda_powertools import Logger, Tracer
from phebee.utils.sparql import execute_query
from phebee.constants import PHEBEE

logger = Logger()
tracer = Tracer()

def build_lineage_query(entity_iri: str) -> str:
    """Build SPARQL query to trace entity lineage."""
    return f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?activity ?agent ?input ?timestamp WHERE {{
        <{entity_iri}> prov:wasGeneratedBy ?activity .
        
        OPTIONAL {{
            ?activity prov:wasAssociatedWith ?agent .
        }}
        
        OPTIONAL {{
            ?activity prov:used ?input .
        }}
        
        OPTIONAL {{
            ?activity prov:startedAtTime ?timestamp .
        }}
    }}
    """

def build_activity_query(run_id: str) -> str:
    """Build SPARQL query for run activities."""
    return f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?activity ?agent ?startTime ?input ?output WHERE {{
        GRAPH <{PHEBEE}/provenance/run/{run_id}> {{
            ?activity a prov:Activity .
            
            OPTIONAL {{
                ?activity prov:wasAssociatedWith ?agent .
            }}
            
            OPTIONAL {{
                ?activity prov:startedAtTime ?startTime .
            }}
            
            OPTIONAL {{
                ?activity prov:used ?input .
            }}
            
            OPTIONAL {{
                ?activity prov:generated ?output .
            }}
        }}
    }}
    ORDER BY ?activity
    """

def build_entity_query(run_id: str) -> str:
    """Build SPARQL query for entities created in a run."""
    return f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity ?activity ?timestamp WHERE {{
        GRAPH <{PHEBEE}/provenance/run/{run_id}> {{
            ?entity prov:wasGeneratedBy ?activity .
            
            OPTIONAL {{
                ?entity prov:generatedAtTime ?timestamp .
            }}
        }}
    }}
    ORDER BY ?entity
    """

def build_agent_query(run_id: str) -> str:
    """Build SPARQL query for agents in a run."""
    return f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?agent ?activity WHERE {{
        GRAPH <{PHEBEE}/provenance/run/{run_id}> {{
            ?agent a prov:Agent .
            ?activity prov:wasAssociatedWith ?agent .
        }}
    }}
    """

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info("Event: %s", event)
    
    try:
        body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
        
        query_type = body.get("query_type")
        run_id = body.get("run_id")
        entity_iri = body.get("entity_iri")
        custom_sparql = body.get("custom_sparql")
        
        if custom_sparql:
            sparql = custom_sparql
        elif query_type == "lineage" and entity_iri:
            sparql = build_lineage_query(entity_iri)
        elif query_type == "activity" and run_id:
            sparql = build_activity_query(run_id)
        elif query_type == "entities" and run_id:
            sparql = build_entity_query(run_id)
        elif query_type == "agents" and run_id:
            sparql = build_agent_query(run_id)
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid query parameters"})
            }
        
        logger.info("Executing SPARQL: %s", sparql)
        result = execute_query(sparql)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "query_type": query_type,
                "results": result["results"]["bindings"]
            })
        }
        
    except Exception as e:
        logger.exception("Query failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
