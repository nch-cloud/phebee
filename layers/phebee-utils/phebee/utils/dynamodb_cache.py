"""
DynamoDB Query Cache for PheBee

This module provides a query cache layer for PheBee cohort queries, addressing
Neptune SPARQL performance limitations at scale.

Design:
- Separate DynamoDB table (not the main PheBee DynamoDB table)
- Disposable materialized view (Iceberg remains source of truth)
- Two key patterns:
  1. Project data: PK=project_id, SK=ancestor_path||qualifier||subject_id
  2. Ontology paths: PK=TERM_PATH#ontology|version|term, SK=ancestor_path

Key benefits:
- Single-query hierarchy expansion (prefix matching on SK)
- O(1) path lookups for direct term queries
- Easy to clear/rebuild for iteration
"""

import os
from typing import Dict, List, Optional
import boto3
import logging

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

def _get_cache_table_name() -> str:
    """
    Get cache table name from environment variable.

    Raises:
        ValueError: If cache table name is not configured
    """
    table_name = os.environ.get("PheBeeDynamoCacheTable", os.environ.get("PHEBEE_CACHE_TABLE"))
    if not table_name:
        raise ValueError(
            "Cache table name not configured. Set PheBeeDynamoCacheTable environment variable."
        )
    return table_name


def _get_cache_table():
    """Get boto3 DynamoDB Table resource for cache table."""
    table_name = _get_cache_table_name()
    return boto3.resource("dynamodb").Table(table_name)


# -----------------------------------------------------------------------------
# HPO Ontology Path Management
# -----------------------------------------------------------------------------
#
# Design: Dual-write strategy for efficient query patterns
#
# Row Type 1 - Descendant queries (shared PK):
#   PK = "TERM_PATH#hpo|2026-01-08"                    (shared by all terms)
#   SK = "HP_0000001|HP_0000118|HP_0001627"            (full ancestor path)
#   term_id = "HP_0001627"
#   label = "Abnormal heart morphology"
#   - Enables begins_with on SK to find all descendants of a term
#   - Example: "Find all terms under HP_0000118" → begins_with("HP_0000118|")
#
# Row Type 2 - Direct term lookups (unique PK):
#   PK = "TERM#hpo|2026-01-08|HP_0001627"              (unique per term)
#   SK = "HP_0000001|HP_0000118|HP_0001627"            (full ancestor path)
#   label = "Abnormal heart morphology"
#   path_length = 3
#   - Enables efficient lookup of all paths for a specific term (no FilterExpression)
#   - Enables efficient label lookup for a specific term
#   - Query by PK returns all paths for the term (handles multiple inheritance)
#
# Trade-offs:
#   - 2x storage (one item per path × 2 row types)
#   - 2x write operations
#   - Zero FilterExpression overhead for all three query patterns
#
# Populated by: PopulateOntologyCacheFunction (runs during ontology update)
# Used by: Query lambdas to resolve term hierarchies for cohort queries
# -----------------------------------------------------------------------------


def build_term_path_pk(ontology: str, version: str) -> str:
    """
    Build shared partition key for descendant queries.

    This PK is shared by ALL terms in the ontology version, enabling
    begins_with queries on SK to find descendants.

    Args:
        ontology: Ontology identifier (e.g., "hpo", "mondo")
        version: Ontology version (e.g., "2026-01-08", "v2024-10-01")

    Returns:
        Partition key string: "TERM_PATH#hpo|2026-01-08"

    Example:
        >>> build_term_path_pk("hpo", "2026-01-08")
        "TERM_PATH#hpo|2026-01-08"
    """
    return f"TERM_PATH#{ontology}|{version}"


def build_term_direct_pk(ontology: str, version: str, term_id: str) -> str:
    """
    Build unique partition key for direct term lookups.

    This PK is unique per term, enabling efficient queries for:
    - All ancestor paths for a specific term (no FilterExpression)
    - Label lookup for a specific term

    Args:
        ontology: Ontology identifier (e.g., "hpo", "mondo")
        version: Ontology version (e.g., "2026-01-08", "v2024-10-01")
        term_id: Term identifier without namespace (e.g., "HP_0001627")

    Returns:
        Partition key string: "TERM#hpo|2026-01-08|HP_0001627"

    Example:
        >>> build_term_direct_pk("hpo", "2026-01-08", "HP_0001627")
        "TERM#hpo|2026-01-08|HP_0001627"
    """
    return f"TERM#{ontology}|{version}|{term_id}"


def build_ancestor_path_sk(ancestor_ids: List[str]) -> str:
    """
    Build sort key from ancestor path (ordered list from root to term).

    Args:
        ancestor_ids: List of term IDs from root to current term
                     e.g., ["HP_0000001", "HP_0000118", "HP_0001627"]

    Returns:
        Sort key string with pipe-delimited path

    Example:
        >>> build_ancestor_path_sk(["HP_0000001", "HP_0000118", "HP_0001627"])
        "HP_0000001|HP_0000118|HP_0001627"

    Note:
        - Root terms have empty ancestor list: build_ancestor_path_sk([]) -> ""
        - Enables prefix matching: begins_with("HP_0000001|HP_0000118|")
    """
    return "|".join(ancestor_ids)


def parse_term_id_from_iri(term_iri: str) -> str:
    """
    Extract term ID from full IRI.

    Args:
        term_iri: Full term IRI (e.g., "http://purl.obolibrary.org/obo/HP_0001627")

    Returns:
        Term ID without namespace (e.g., "HP_0001627")

    Example:
        >>> parse_term_id_from_iri("http://purl.obolibrary.org/obo/HP_0001627")
        "HP_0001627"
    """
    return term_iri.split("/")[-1]


def get_term_ancestor_paths(ontology: str, version: str, term_id: str, table=None) -> List[List[str]]:
    """
    Get all ancestor paths for a specific term from the cache.

    Uses the TERM# (direct lookup) PK for efficient retrieval without FilterExpression.
    Returns all paths due to multiple inheritance (one term can have multiple parent paths).

    Args:
        ontology: Ontology identifier (e.g., "hpo", "mondo")
        version: Ontology version (e.g., "2026-01-08")
        term_id: Term identifier without namespace (e.g., "HP_0001627")
        table: Optional DynamoDB Table resource (for testing)

    Returns:
        List of paths, where each path is a list of term IDs from root to term.
        Returns empty list if term not found in cache.

    Example:
        >>> paths = get_term_ancestor_paths("hpo", "2026-01-08", "HP_0001627")
        >>> print(paths)
        [
            ["HP_0000001", "HP_0000118", "HP_0001627"],
            ["HP_0000001", "HP_0001626", "HP_0001627"]
        ]

    Raises:
        ValueError: If cache table is not configured (when table not provided)
        ClientError: If DynamoDB query fails
    """
    if table is None:
        table = _get_cache_table()

    pk = build_term_direct_pk(ontology, version, term_id)

    try:
        response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": pk
            }
        )

        items = response.get("Items", [])

        # Convert SK strings to lists of term IDs
        paths = []
        for item in items:
            sk = item.get("SK", "")
            if sk:
                # SK format: "HP_0000001|HP_0000118|HP_0001627"
                path = sk.split("|")
                paths.append(path)

        logger.info(f"Found {len(paths)} ancestor path(s) for {term_id}")
        return paths

    except Exception as e:
        logger.error(f"Failed to get ancestor paths for {term_id}: {str(e)}")
        raise


def get_term_label(ontology: str, version: str, term_id: str, table=None) -> Optional[str]:
    """
    Get the human-readable label for a specific term from the cache.

    Uses the TERM# (direct lookup) PK for efficient retrieval without FilterExpression.
    Returns the label from the first path entry (all paths for a term have the same label).

    Args:
        ontology: Ontology identifier (e.g., "hpo", "mondo")
        version: Ontology version (e.g., "2026-01-08")
        term_id: Term identifier without namespace (e.g., "HP_0001627")
        table: Optional DynamoDB Table resource (for testing)

    Returns:
        Human-readable label string, or None if term not found or has no label.

    Example:
        >>> label = get_term_label("hpo", "2026-01-08", "HP_0001627")
        >>> print(label)
        "Abnormal heart morphology"

    Raises:
        ValueError: If cache table is not configured (when table not provided)
        ClientError: If DynamoDB query fails
    """
    if table is None:
        table = _get_cache_table()

    pk = build_term_direct_pk(ontology, version, term_id)

    try:
        response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={
                ":pk": pk
            },
            Limit=1  # Only need one item to get the label
        )

        items = response.get("Items", [])

        if items:
            label = items[0].get("label")
            logger.info(f"Found label for {term_id}: {label}")
            return label
        else:
            logger.warning(f"No cache entry found for {term_id}")
            return None

    except Exception as e:
        logger.error(f"Failed to get label for {term_id}: {str(e)}")
        raise


def reset_dynamodb_cache_table():
    """
    Deletes ALL items from the cache table (paged scan + batch_writer).

    This function performs a full table scan followed by batch deletion of all items.
    It is intended for dev/test environments and should be called only through the
    reset_database Lambda. Point-in-time recovery (PITR) is enabled in the CloudFormation
    template for safety.

    Raises:
        ValueError: If cache table name environment variable is not set
        ClientError: If DynamoDB operations fail

    Note:
        - Uses paginated scan to handle tables larger than 1MB
        - Batch deletes up to 25 items at a time (DynamoDB limit)
        - Requires PheBeeDynamoCacheTable environment variable to be set
    """
    table = _get_cache_table()
    items_deleted = 0

    logger.info("Starting cache table reset")

    # Paginated scan + batch delete
    scan_kwargs = {}

    while True:
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])

        if items:
            try:
                # Batch delete items
                with table.batch_writer() as batch:
                    for item in items:
                        batch.delete_item(Key={
                            'PK': item['PK'],
                            'SK': item['SK']
                        })
                items_deleted += len(items)
                logger.info(f"Deleted {len(items)} items ({items_deleted} total)")
            except Exception as e:
                logger.error(f"Failed to delete batch of {len(items)} items: {str(e)}")
                raise

        # Check if there are more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    logger.info(f"Cache table reset complete. Total items deleted: {items_deleted}")
    return items_deleted


# -----------------------------------------------------------------------------
# Implementation Status
# -----------------------------------------------------------------------------
#
# IMPLEMENTED:
#   Phase 1 - Infrastructure & Population:
#     - DynamoDB cache table infrastructure (template.yaml)
#     - Ontology path cache population (populate_ontology_cache.py)
#     - Dual-write strategy (TERM_PATH# and TERM# row types)
#     - Key building functions (build_term_path_pk, build_term_direct_pk)
#     - IAM policies and Lambda configurations
#
#   Phase 2 - Read Functions:
#     - get_term_ancestor_paths() - Lookup all ancestor paths for a term
#     - get_term_label() - Lookup human-readable label for a term
#
# NOT YET IMPLEMENTED (Future phases):
#   - get_descendant_terms() - Find all terms descended from a parent term
#   - query_subjects_by_term() - Main cohort query using cache
#   - write_term_link() - Write subject-term links to cache during import
#   - Cache invalidation/versioning strategy
#   - SPARQL fallback when cache is unavailable
#
# Next Steps:
#   1. Implement get_descendant_terms() using TERM_PATH# shared PK (Phase 2)
#   2. Update bulk import pipeline to WRITE to cache (Phase 3)
#   3. Update query lambdas to use cache instead of Neptune (Phase 4)
# -----------------------------------------------------------------------------
