"""
Lambda function to populate DynamoDB cache with HPO ontology hierarchy paths.

This function:
1. Downloads hp.obo from S3
2. Parses is_a relationships to build the ontology graph
3. Computes all paths from root to each term (handling multiple inheritance)
4. Writes paths to DynamoDB cache table as TERM_PATH# entries

Called by: update_hpo.asl.json state machine after installing HPO to Neptune
"""

import os
import json
import boto3
import logging
import time
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque
from botocore.exceptions import ClientError

# Import cache utilities
import sys
sys.path.insert(0, '/opt')  # Lambda layer path
from phebee.utils.dynamodb_cache import (
    build_term_path_pk,
    build_term_direct_pk,
    build_ancestor_path_sk,
    _get_cache_table
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Populate DynamoDB cache with HPO hierarchy paths.

    Input event:
        {
            "source": "hpo",
            "version": "2026-01-08",
            "obo_asset_path": "s3://bucket/path/to/hp.obo"
        }

    Returns:
        {
            "terms_processed": 18000,
            "paths_written": 36000,
            "version": "2026-01-08"
        }
    """
    source = event.get("source", "hpo")
    version = event.get("version")
    obo_path = event.get("obo_asset_path")

    if not version:
        raise ValueError("Missing required parameter: version")
    if not obo_path:
        raise ValueError("Missing required parameter: obo_asset_path")

    print(f"Populating ontology cache for {source} version {version}")
    print(f"OBO file: {obo_path}")

    # Download OBO file from S3
    obo_content = _download_from_s3(obo_path)
    print(f"Downloaded OBO file: {len(obo_content)} bytes")

    # Parse OBO file to extract hierarchy and labels
    ontology_graph, term_labels = _parse_obo(obo_content)
    print(f"Parsed ontology: {len(ontology_graph)} terms with parents, {len(term_labels)} terms with labels")

    # Find root term(s)
    roots = _find_roots(ontology_graph)
    print(f"Found {len(roots)} root term(s): {roots}")

    # Compute all paths from root to each term
    all_paths = _compute_all_paths(ontology_graph, roots)
    total_paths = sum(len(paths) for paths in all_paths.values())
    print(f"Computed paths: {len(all_paths)} terms, {total_paths} total paths")

    # Write paths to DynamoDB cache
    paths_written = _write_paths_to_cache(all_paths, term_labels, source, version)
    print(f"Wrote {paths_written} path entries to cache")

    return {
        "terms_processed": len(all_paths),
        "paths_written": paths_written,
        "version": version,
        "source": source
    }


def _download_from_s3(s3_path: str, max_retries: int = 3) -> str:
    """
    Download file from S3 with retry logic.

    Args:
        s3_path: S3 URI (s3://bucket/key/path)
        max_retries: Maximum number of retry attempts

    Returns:
        File contents as string

    Raises:
        ValueError: If S3 path is invalid
        ClientError: If S3 download fails after retries
    """
    # Parse s3://bucket/key/path
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    if not key:
        raise ValueError(f"S3 path missing key: {s3_path}")

    s3 = boto3.client("s3")

    # Retry loop with exponential backoff
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading from S3: s3://{bucket}/{key} (attempt {attempt + 1}/{max_retries})")
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            logger.info(f"Successfully downloaded {len(content)} bytes")
            return content
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.warning(f"S3 download attempt {attempt + 1} failed: {error_code} - {str(e)}")

            if attempt < max_retries - 1:
                # Exponential backoff: 1s, 2s, 4s
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"S3 download failed after {max_retries} attempts")
                raise


def _parse_obo(obo_content: str) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """
    Parse OBO file to extract is_a relationships and term labels.

    Args:
        obo_content: Raw OBO file content

    Returns:
        Tuple of (graph, labels):
            - graph: Dict mapping term_id -> list of parent term_ids
            - labels: Dict mapping term_id -> human-readable label

    Raises:
        ValueError: If OBO file is invalid or empty

    Example OBO stanza:
        [Term]
        id: HP:0001627
        name: Abnormal heart morphology
        is_a: HP:0000118 ! Phenotypic abnormality
        is_a: HP:0001626 ! Abnormality of cardiovascular system
    """
    # Validate OBO header
    lines = obo_content.split("\n")
    if not lines:
        raise ValueError("OBO file is empty")

    # Check for format-version header (required in OBO files)
    has_header = any(line.startswith("format-version:") for line in lines[:20])
    if not has_header:
        logger.warning("OBO file missing 'format-version' header - may be invalid format")

    graph = defaultdict(list)
    labels = {}
    current_term = None
    terms_found = 0

    for line in lines:
        line = line.strip()

        if line == "[Term]":
            current_term = None
            terms_found += 1
        elif line.startswith("id: "):
            # Extract term ID (e.g., "HP:0001627" -> "HP_0001627")
            term_id = line[4:].strip()
            term_id = term_id.replace(":", "_")
            current_term = term_id
        elif line.startswith("name: ") and current_term:
            # Extract human-readable label
            label = line[6:].strip()
            labels[current_term] = label
        elif line.startswith("is_a: ") and current_term:
            # Extract parent ID (format: "is_a: HP:0001626 ! comment")
            parent_part = line[6:].split("!")[0].strip()
            parent_id = parent_part.replace(":", "_")
            graph[current_term].append(parent_id)

    logger.info(f"Parsed {terms_found} [Term] stanzas, {len(graph)} terms with parents, {len(labels)} terms with labels")

    if len(graph) == 0:
        raise ValueError(f"No terms with parent relationships found in OBO file")

    return dict(graph), labels


def _find_roots(graph: Dict[str, List[str]]) -> List[str]:
    """
    Find root terms (terms that have no parents).

    In HPO, the root is typically HP:0000001 (All).
    """
    all_terms = set(graph.keys())
    all_children = set()

    for parents in graph.values():
        all_children.update(parents)

    # Terms that appear as parents but not as children are roots
    roots = [term for term in all_children if term not in all_terms]

    # If no explicit roots found, assume HP_0000001
    if not roots:
        roots = ["HP_0000001"]

    return roots


def _compute_all_paths(
    graph: Dict[str, List[str]],
    roots: List[str]
) -> Dict[str, List[List[str]]]:
    """
    Compute all paths from root to each term using BFS.

    Handles multiple inheritance by finding all possible paths.

    Returns:
        Dict mapping term_id -> list of paths (each path is a list of term_ids from root)

    Example:
        {
            "HP_0001627": [
                ["HP_0000001", "HP_0000118", "HP_0001627"],
                ["HP_0000001", "HP_0001626", "HP_0001627"]
            ]
        }
    """
    # Invert graph: child -> parents becomes parent -> children
    children_map = defaultdict(list)
    for child, parents in graph.items():
        for parent in parents:
            children_map[parent].append(child)

    # BFS from each root, tracking all paths
    all_paths = defaultdict(list)

    for root in roots:
        # Queue contains (current_term, path_to_current)
        queue = deque([(root, [root])])

        while queue:
            current, path = queue.popleft()

            # Record this path for the current term
            all_paths[current].append(path)

            # Explore children
            for child in children_map.get(current, []):
                queue.append((child, path + [child]))

    return dict(all_paths)


def _write_paths_to_cache(
    all_paths: Dict[str, List[List[str]]],
    labels: Dict[str, str],
    source: str,
    version: str
) -> int:
    """
    Write all paths to DynamoDB cache table using dual-write strategy.

    Each path is written as TWO DynamoDB items:

    1. Shared PK for descendant queries:
        PK = "TERM_PATH#hpo|2026-01-08"
        SK = "HP_0000001|HP_0000118|HP_0001627"
        term_id = "HP_0001627"
        label = "Abnormal heart morphology"

    2. Unique PK for direct term lookups:
        PK = "TERM#hpo|2026-01-08|HP_0001627"
        SK = "HP_0000001|HP_0000118|HP_0001627"
        label = "Abnormal heart morphology"
        path_length = 3

    Args:
        all_paths: Dict mapping term_id to list of ancestor paths
        labels: Dict mapping term_id to human-readable label
        source: Ontology source (e.g., "hpo")
        version: Ontology version (e.g., "2026-01-08")

    Returns:
        Number of items written (includes both row types)
    """
    table = _get_cache_table()
    items_written = 0

    # Batch write in chunks of 25 (DynamoDB limit)
    batch_items = []

    # Build shared PK once (used by all paths for descendant queries)
    shared_pk = build_term_path_pk(source, version)

    for term_id, paths in all_paths.items():
        # Get label for this term (may be None if not in OBO file)
        label = labels.get(term_id)

        # Build unique PK once per term (used for direct lookups)
        direct_pk = build_term_direct_pk(source, version, term_id)

        for path in paths:
            sk = build_ancestor_path_sk(path)

            # Row Type 1: Shared PK for descendant queries
            shared_item = {
                "PK": shared_pk,
                "SK": sk,
                "term_id": term_id,
                "ontology": source,
                "version": version,
                "path_length": len(path)
            }
            if label:
                shared_item["label"] = label

            batch_items.append(shared_item)

            # Row Type 2: Unique PK for direct term lookups
            direct_item = {
                "PK": direct_pk,
                "SK": sk,
                "ontology": source,
                "version": version,
                "path_length": len(path)
            }
            if label:
                direct_item["label"] = label

            batch_items.append(direct_item)

            # Write batch when we hit 25 items
            if len(batch_items) >= 25:
                _write_batch(table, batch_items)
                items_written += len(batch_items)
                batch_items = []

    # Write remaining items
    if batch_items:
        _write_batch(table, batch_items)
        items_written += len(batch_items)

    return items_written


def _write_batch(table, items: List[Dict]):
    """
    Write a batch of items to DynamoDB with error handling.

    Args:
        table: DynamoDB Table resource
        items: List of items to write

    Raises:
        Exception: If batch write fails
    """
    try:
        logger.info(f"Writing batch of {len(items)} items to cache table")
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        logger.info(f"Successfully wrote {len(items)} items")
    except Exception as e:
        logger.error(f"Failed to write batch of {len(items)} items: {str(e)}")
        raise
