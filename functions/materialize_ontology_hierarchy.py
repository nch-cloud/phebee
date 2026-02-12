import json
import logging
import os
import boto3
from typing import Dict, List, Set, Any
from aws_lambda_powertools import Logger

logger = Logger()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda handler to parse OBO file and prepare batches for distributed materialization.

    Parses the OBO file, computes ancestor closure and depth, then splits terms into
    batches and writes them to S3 for parallel processing by a Distributed Map state.

    Expected input:
        {
            "obo_file_s3_path": "s3://bucket/path/to/hp.obo",
            "ontology_source": "HPO",
            "version": "2024-04-26"
        }

    Returns:
        {
            "ontology_source": "HPO",
            "version": "2024-04-26",
            "total_terms": 17000,
            "batch_count": 85,
            "batch_size": 200,
            "s3_bucket": "phebee-bucket",
            "s3_prefix": "ontology-hierarchy/batches/HPO/2024-04-26",
            "status": "success"
        }
    """
    try:
        logger.info("Materializing ontology hierarchy", extra={"event": event})

        # Extract parameters
        obo_file_s3_path = event.get('obo_file_s3_path')
        ontology_source = event.get('ontology_source')
        version = event.get('version')

        if not obo_file_s3_path or not ontology_source or not version:
            raise ValueError("Missing required parameters: obo_file_s3_path, ontology_source, version")

        # Parse S3 path
        if not obo_file_s3_path.startswith('s3://'):
            raise ValueError("obo_file_s3_path must be an S3 URI")

        path_parts = obo_file_s3_path[5:].split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''

        logger.info(f"Downloading OBO file from s3://{bucket}/{key}")

        # Download OBO file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        obo_content = response['Body'].read().decode('utf-8')

        logger.info(f"Parsing OBO file for {ontology_source} version {version}")

        # Parse OBO file
        terms = parse_obo_file(obo_content, ontology_source)

        logger.info(f"Parsed {len(terms)} terms from OBO file")

        # Compute ancestor closure for all terms
        terms_with_ancestors = compute_ancestor_closure(terms)

        logger.info(f"Computed ancestor closure for {len(terms_with_ancestors)} terms")

        # Get bucket name from environment
        bucket_name = os.environ.get('PHEBEE_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("PHEBEE_BUCKET_NAME environment variable is required")

        # Split terms into batches and write to S3
        batch_size = 200  # Increased from 100 - comfortably under Athena 256KB query limit
        batch_count = 0
        s3_prefix = f"ontology-hierarchy/batches/{ontology_source}/{version}"

        logger.info(f"Writing {len(terms_with_ancestors)} terms to S3 in batches of {batch_size}")

        for i in range(0, len(terms_with_ancestors), batch_size):
            batch = terms_with_ancestors[i:i + batch_size]
            batch_num = i // batch_size

            # Create batch payload
            batch_payload = {
                "ontology_source": ontology_source,
                "version": version,
                "terms": batch
            }

            # Write batch to S3
            batch_key = f"{s3_prefix}/batch-{batch_num:03d}.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=batch_key,
                Body=json.dumps(batch_payload),
                ContentType='application/json'
            )

            batch_count += 1
            if batch_count % 10 == 0:
                logger.info(f"Written {batch_count} batches to S3")

        logger.info(f"Successfully wrote {batch_count} batches to s3://{bucket_name}/{s3_prefix}/")

        return {
            "statusCode": 200,
            "ontology_source": ontology_source,
            "version": version,
            "total_terms": len(terms_with_ancestors),
            "batch_count": batch_count,
            "batch_size": batch_size,
            "s3_bucket": bucket_name,
            "s3_prefix": s3_prefix,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to materialize ontology hierarchy: {str(e)}")
        raise


def parse_obo_file(obo_content: str, ontology_source: str) -> List[Dict[str, Any]]:
    """
    Parse OBO file and extract term information.

    Only includes terms matching the ontology source prefix (e.g., HP: for HPO, MONDO: for MONDO).
    Cross-referenced terms from other ontologies are filtered out.

    Returns list of term dictionaries with:
        - term_id: e.g., "HP:0000001"
        - term_iri: e.g., "http://purl.obolibrary.org/obo/HP_0000001"
        - term_label: e.g., "All"
        - parent_ids: list of direct parent term IDs (only includes parents from same ontology)
    """
    # Map ontology source to expected term ID prefix
    # Note: ontology_source is lowercase (hpo, mondo), but OBO term IDs use uppercase prefixes
    prefix_map = {
        'hpo': 'HP:',
        'mondo': 'MONDO:',
        'eco': 'ECO:'
    }
    expected_prefix = prefix_map.get(ontology_source.lower())
    if not expected_prefix:
        raise ValueError(f"Unknown ontology source: {ontology_source}. Expected one of: {list(prefix_map.keys())}")

    terms = []
    current_term = None

    for line in obo_content.split('\n'):
        line = line.strip()

        # Start of a new term stanza
        if line == '[Term]':
            if current_term:
                terms.append(current_term)
            current_term = {
                'parent_ids': []
            }

        # Skip non-term stanzas
        elif line.startswith('[') and line != '[Term]':
            if current_term:
                terms.append(current_term)
            current_term = None

        # Parse term fields
        elif current_term is not None:
            if line.startswith('id:'):
                term_id = line[3:].strip()
                current_term['term_id'] = term_id
                # Convert to IRI format
                current_term['term_iri'] = term_id_to_iri(term_id)

            elif line.startswith('name:'):
                current_term['term_label'] = line[5:].strip()

            elif line.startswith('is_a:'):
                # Extract parent term ID (before the "!" comment and qualifier annotations)
                # Format: is_a: PARENT_ID {qualifier} ! comment
                parent_part = line[5:].strip()
                # Remove comment (after "!")
                if '!' in parent_part:
                    parent_id = parent_part.split('!')[0].strip()
                else:
                    parent_id = parent_part
                # Remove qualifier annotations (after "{")
                if '{' in parent_id:
                    parent_id = parent_id.split('{')[0].strip()
                # Only include parents from the same ontology
                if parent_id.startswith(expected_prefix):
                    current_term['parent_ids'].append(parent_id)

    # Add last term
    if current_term:
        terms.append(current_term)

    # Filter: only keep terms from this ontology with required fields
    valid_terms = []
    for term in terms:
        if ('term_id' in term and 'term_iri' in term and 'term_label' in term
            and term['term_id'].startswith(expected_prefix)):
            valid_terms.append(term)

    logger.info(f"Filtered to {len(valid_terms)} {ontology_source} terms (removed cross-references from other ontologies)")

    return valid_terms


def term_id_to_iri(term_id: str) -> str:
    """
    Convert term ID to IRI format.
    E.g., "HP:0000001" -> "http://purl.obolibrary.org/obo/HP_0000001"
    """
    # Replace colon with underscore
    iri_suffix = term_id.replace(':', '_')
    return f"http://purl.obolibrary.org/obo/{iri_suffix}"


def compute_ancestor_closure(terms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compute transitive closure of ancestors and depth for each term.

    Takes terms with parent_ids and returns terms with ancestor_term_ids, depth.
    Ancestors are ordered by depth (root to leaf).
    """
    from collections import deque

    # Build parent and children maps
    parent_map = {}
    children_map = {}

    for term in terms:
        term_id = term['term_id']
        parent_map[term_id] = term.get('parent_ids', [])
        children_map[term_id] = []

    # Build children map (reverse of parent map)
    for term_id, parents in parent_map.items():
        for parent_id in parents:
            if parent_id in children_map:
                children_map[parent_id].append(term_id)

    # Find root terms (terms with no parents)
    roots = [term_id for term_id, parents in parent_map.items() if not parents]

    logger.info(f"Found {len(roots)} root terms: {roots[:5]}...")

    # Compute depth for all terms using BFS from roots
    depth_map = {}
    queue = deque()

    # Initialize with roots at depth 0
    for root_id in roots:
        depth_map[root_id] = 0
        queue.append(root_id)

    # BFS to compute depth
    while queue:
        current_id = queue.popleft()
        current_depth = depth_map[current_id]

        # Process all children
        for child_id in children_map.get(current_id, []):
            # Child depth is max of all parent depths + 1 (handles multiple inheritance)
            child_depth = current_depth + 1

            if child_id not in depth_map:
                depth_map[child_id] = child_depth
                queue.append(child_id)
            else:
                # Update depth if we found a longer path (further from root)
                depth_map[child_id] = max(depth_map[child_id], child_depth)

    logger.info(f"Computed depth for {len(depth_map)} terms")

    # Compute ancestors for each term using DFS
    def get_all_ancestors(term_id: str, visited: Set[str] = None) -> Set[str]:
        """Recursively get all ancestors of a term (including itself)."""
        if visited is None:
            visited = set()

        # Avoid cycles
        if term_id in visited:
            return visited

        visited.add(term_id)

        # Get direct parents
        parents = parent_map.get(term_id, [])

        # Recursively get ancestors of parents
        for parent_id in parents:
            get_all_ancestors(parent_id, visited)

        return visited

    # Compute ancestor closure for each term
    terms_with_ancestors = []
    for term in terms:
        term_id = term['term_id']

        # Get all ancestors (including self)
        ancestors = get_all_ancestors(term_id)

        # Order ancestors by depth (root to leaf)
        ordered_ancestors = sorted(
            ancestors,
            key=lambda tid: (depth_map.get(tid, 999), tid)  # Sort by depth, then by ID for stability
        )

        # Create new term dict with ancestor_term_ids and depth
        term_with_ancestors = {
            'term_id': term['term_id'],
            'term_iri': term['term_iri'],
            'term_label': term['term_label'],
            'ancestor_term_ids': ordered_ancestors,
            'depth': depth_map.get(term_id, 0)
        }

        terms_with_ancestors.append(term_with_ancestors)

    return terms_with_ancestors
