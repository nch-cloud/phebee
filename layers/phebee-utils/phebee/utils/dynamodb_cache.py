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
# Design Decision: Pipe-delimited ancestor paths in SK
#   - SK format: "HP_0000001|HP_0000118|HP_0001627" (path from root to term)
#   - Enables efficient hierarchy queries with DynamoDB begins_with operator
#   - Compact storage (~50 bytes per path vs ~500 bytes for full IRIs)
#   - Handles multiple inheritance: one DynamoDB item per path
#
# Key Schema for Ontology Path Cache:
#   PK = "TERM_PATH#hpo|2026-01-08|HP_0001627"  (ontology|version|term)
#   SK = "HP_0000001|HP_0000118|HP_0001627"      (full ancestor path)
#
# Populated by: PopulateOntologyCacheFunction (runs during ontology update)
# Used by: Query lambdas to resolve term hierarchies for cohort queries
# -----------------------------------------------------------------------------


def build_term_path_pk(ontology: str, version: str, term_id: str) -> str:
    """
    Build partition key for ontology term path lookup.

    Args:
        ontology: Ontology identifier (e.g., "hpo", "mondo")
        version: Ontology version (e.g., "2026-01-08", "v2024-10-01")
        term_id: Term identifier without namespace (e.g., "HP_0001627")

    Returns:
        Partition key string: "TERM_PATH#hpo|2026-01-08|HP_0001627"

    Example:
        >>> build_term_path_pk("hpo", "2026-01-08", "HP_0001627")
        "TERM_PATH#hpo|2026-01-08|HP_0001627"
    """
    return f"TERM_PATH#{ontology}|{version}|{term_id}"


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
# IMPLEMENTED (Phase 1):
#   - DynamoDB cache table infrastructure (template.yaml)
#   - Ontology path cache population (populate_ontology_cache.py)
#   - Key building functions (above)
#   - IAM policies and Lambda configurations
#
# NOT YET IMPLEMENTED (Future phases):
#   - get_term_ancestor_path() - Lookup cached path for a term
#   - query_subjects_by_term() - Main cohort query using cache
#   - write_term_link() - Write subject-term links to cache during import
#   - Cache invalidation/versioning strategy
#   - SPARQL fallback when cache is unavailable
#
# Next Steps:
#   1. Implement query functions to READ from cache (Phase 2)
#   2. Update bulk import pipeline to WRITE to cache (Phase 3)
#   3. Update query lambdas to use cache instead of Neptune (Phase 4)
# -----------------------------------------------------------------------------
