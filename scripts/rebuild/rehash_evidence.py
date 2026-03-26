#!/usr/bin/env python3
"""
Rehash Evidence Script for PheBee Rebuild Pipeline

Recalculates evidence_id and termlink_id hashes for all evidence records
using updated hash functions. Uses Iceberg MERGE INTO for in-place updates
to preserve partition statistics and enable rollback via time travel.

Usage:
    spark-submit rehash_evidence.py \
        --run-id <uuid> \
        --iceberg-database <database> \
        --evidence-table <table> \
        --region <aws-region>
"""

import argparse
import hashlib
import logging
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, lit, monotonically_increasing_id
from pyspark.sql.types import StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Hash Functions (KEEP IN SYNC WITH hash.py and bulk_evidence_processor.py)
# ============================================================================

def normalize_qualifier_type(qualifier_type):
    """
    Normalize qualifier type to canonical form.
    KEEP IN SYNC WITH qualifier.py implementation!

    Rules:
    - Internal PheBee qualifiers: Extract short name from IRI if prefixed
      "http://ods.nationwidechildrens.org/phebee/qualifier/negated" → "negated"
    - External qualifiers: Keep full IRI
      "http://purl.obolibrary.org/obo/HP_0012823" → stays as-is
    - Already short form: Keep as-is
      "negated" → "negated"
    """
    # Handle None or empty string
    if not qualifier_type:
        return qualifier_type

    PHEBEE_QUALIFIER_PREFIX = "http://ods.nationwidechildrens.org/phebee/qualifier/"

    if qualifier_type.startswith(PHEBEE_QUALIFIER_PREFIX):
        # Extract short name from PheBee internal qualifier IRI
        return qualifier_type[len(PHEBEE_QUALIFIER_PREFIX):]
    else:
        # External IRI or already short form - keep as-is
        return qualifier_type


def normalize_qualifiers(qualifiers):
    """
    Normalize qualifiers for hash computation.
    Supports hybrid qualifier approach:
    - Internal qualifiers (negated, hypothetical, family): Use short form "name:value"
    - External qualifiers (full IRIs): Preserve full IRI format "http://.../:value"
    """
    if not qualifiers:
        return []

    normalized = []
    for qualifier in qualifiers:
        if not qualifier:  # Skip empty strings
            continue

        # Check if it's an external IRI first
        is_external = qualifier.startswith('http://') or qualifier.startswith('https://')

        if is_external:
            # For external IRIs, check if there's a value after the last slash
            last_slash_idx = qualifier.rfind('/')
            colon_after_slash = qualifier.find(':', last_slash_idx + 1)

            if colon_after_slash != -1:
                # Has a value component - extract type and value, then normalize type
                qualifier_type = qualifier[:colon_after_slash]
                value = qualifier[colon_after_slash + 1:]

                # Normalize the type (converts internal IRIs to short form)
                normalized_type = normalize_qualifier_type(qualifier_type)

                # Filter false values
                if value.lower() not in ["false", "0"]:
                    normalized.append(f"{normalized_type}:{value}")
            else:
                # No value - normalize type and add ":true"
                normalized_type = normalize_qualifier_type(qualifier)
                normalized.append(f"{normalized_type}:true")
        else:
            # Internal qualifier (already short form)
            if ":" in qualifier:
                # Already has a value component - filter false values
                name, value = qualifier.split(":", 1)
                if value.lower() not in ["false", "0"]:
                    normalized.append(qualifier)
            else:
                # No value - add ":true"
                normalized.append(f"{qualifier}:true")

    # Sort for deterministic ordering
    return sorted(normalized)


def generate_termlink_hash(source_node_iri: str, term_iri: str, qualifiers: list = None) -> str:
    """Generate a deterministic hash for a term link based on its components."""
    normalized_qualifiers = normalize_qualifiers(qualifiers)
    qualifier_contexts = ",".join(normalized_qualifiers)
    hash_input = f"{source_node_iri}|{term_iri}|{qualifier_contexts}"
    return hashlib.sha256(hash_input.encode()).hexdigest()


def generate_evidence_hash(
    clinical_note_id: str,
    encounter_id: str,
    term_iri: str,
    span_start: int = None,
    span_end: int = None,
    qualifiers: list = None,
    subject_id: str = None,
    creator_id: str = None
) -> str:
    """Generate deterministic evidence ID from content."""
    normalized_qualifiers = normalize_qualifiers(qualifiers)

    content_parts = [
        clinical_note_id or "",
        encounter_id or "",
        term_iri,
        str(span_start) if span_start is not None else "",
        str(span_end) if span_end is not None else "",
        "|".join(normalized_qualifiers),
        subject_id or "",
        creator_id or ""
    ]
    content = "|".join(content_parts)
    return hashlib.sha256(content.encode()).hexdigest()


# ============================================================================
# PySpark UDF Wrappers
# ============================================================================

def create_termlink_hash_wrapper(subject_id, term_iri, qualifiers_array):
    """Wrapper for PySpark UDF - extracts qualifiers from Iceberg struct array"""
    if not qualifiers_array:
        qualifiers = []
    else:
        # Extract qualifier_type:qualifier_value pairs from struct array
        qualifiers = []
        for q in qualifiers_array:
            if q and hasattr(q, 'qualifier_type') and hasattr(q, 'qualifier_value'):
                q_type = q.qualifier_type
                q_val = q.qualifier_value

                # Normalize qualifier type (converts internal IRIs to short form)
                normalized_type = normalize_qualifier_type(q_type)

                # Filter out false values
                if q_val and str(q_val).lower() not in ["false", "0"]:
                    qualifiers.append(f"{normalized_type}:{q_val}")

    # Build subject IRI
    subject_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"

    return generate_termlink_hash(subject_iri, term_iri, qualifiers)


def create_evidence_hash_wrapper(
    subject_id, clinical_note_id, encounter_id, term_iri,
    span_start, span_end, qualifiers_array, evidence_creator_id
):
    """Wrapper for PySpark UDF to generate evidence hash"""
    if not qualifiers_array:
        qualifiers = []
    else:
        # Extract qualifier_type:qualifier_value pairs from struct array
        qualifiers = []
        for q in qualifiers_array:
            if q and hasattr(q, 'qualifier_type') and hasattr(q, 'qualifier_value'):
                q_type = q.qualifier_type
                q_val = q.qualifier_value

                # Normalize qualifier type (converts internal IRIs to short form)
                normalized_type = normalize_qualifier_type(q_type)

                # Filter out false values
                if q_val and str(q_val).lower() not in ["false", "0"]:
                    qualifiers.append(f"{normalized_type}:{q_val}")

    return generate_evidence_hash(
        clinical_note_id,
        encounter_id,
        term_iri,
        int(span_start) if span_start is not None else None,
        int(span_end) if span_end is not None else None,
        qualifiers,
        subject_id,
        evidence_creator_id
    )


def create_spark_session(app_name="RehashEvidence"):
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .getOrCreate()


def main():
    parser = argparse.ArgumentParser(description='Rehash evidence table with updated hash functions')
    parser.add_argument('--run-id', required=True, help='Rebuild run ID')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--evidence-table', required=True, help='Evidence table name')
    parser.add_argument('--region', required=True, help='AWS region')

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"Rehash Evidence - Run ID: {args.run_id}")
    logger.info(f"Database: {args.iceberg_database}")
    logger.info(f"Table: {args.evidence_table}")
    logger.info(f"Region: {args.region}")
    logger.info("=" * 80)

    start_time = time.time()

    # Create Spark session
    spark = create_spark_session(f"RehashEvidence-{args.run_id}")

    # Register UDFs
    generate_termlink_hash_udf = udf(create_termlink_hash_wrapper, StringType())
    generate_evidence_hash_udf = udf(create_evidence_hash_wrapper, StringType())

    table_name = f"glue_catalog.{args.iceberg_database}.{args.evidence_table}"

    try:
        # Step 1: Read entire evidence table
        logger.info(f"Reading evidence table: {table_name}")
        evidence_df = spark.table(table_name)

        initial_count = evidence_df.count()
        logger.info(f"Total evidence records: {initial_count:,}")

        # Step 2: Add correlation ID for matching (use existing evidence_id as correlation)
        # We'll match on (subject_id, old_evidence_id) to update each row
        logger.info("Computing new hashes...")

        rehashed_df = evidence_df \
            .withColumn("old_evidence_id", col("evidence_id")) \
            .withColumn("new_termlink_id", generate_termlink_hash_udf(
                col("subject_id"),
                col("term_iri"),
                col("qualifiers")
            )) \
            .withColumn("new_evidence_id", generate_evidence_hash_udf(
                col("subject_id"),
                col("clinical_note_id"),
                col("encounter_id"),
                col("term_iri"),
                col("text_annotation.span_start"),
                col("text_annotation.span_end"),
                col("qualifiers"),
                col("creator.creator_id")
            ))

        # Step 3: Check for duplicate (subject_id, evidence_id) pairs within subjects
        logger.info("Checking for duplicate (subject_id, evidence_id) pairs...")
        within_subject_duplicates = rehashed_df.groupBy("subject_id", "old_evidence_id").agg(
            F.count("*").alias("row_count")
        ).filter("row_count > 1")

        within_subject_dup_count = within_subject_duplicates.count()
        if within_subject_dup_count > 0:
            logger.error(f"CRITICAL: Found {within_subject_dup_count:,} duplicate (subject_id, evidence_id) pairs!")
            logger.error("This will cause MERGE cardinality violation!")

            # Log examples for debugging
            sample_dupes = within_subject_duplicates.limit(10).collect()
            for row in sample_dupes:
                logger.error(f"  Duplicate: subject_id={row.subject_id}, evidence_id={row.old_evidence_id}, count={row.row_count}")

            raise ValueError(f"Evidence table contains {within_subject_dup_count} duplicate (subject_id, evidence_id) pairs")
        else:
            logger.info("No duplicate (subject_id, evidence_id) pairs found - proceeding with MERGE")

        # Step 4: Check for duplicate evidence_ids across subjects (informational only)
        logger.info("Checking for evidence_id duplicates across subjects...")
        duplicate_check = rehashed_df.groupBy("old_evidence_id").agg(
            F.countDistinct("subject_id").alias("subject_count"),
            F.count("*").alias("row_count")
        ).filter("row_count > 1")

        dup_count = duplicate_check.count()
        if dup_count > 0:
            logger.info(f"Found {dup_count:,} evidence_ids shared across multiple subjects")
            logger.info("This is expected from old hash function that didn't include subject_id")
            logger.info("New hash function will create unique evidence_ids per subject")

            # Log a few examples for verification
            sample_dupes = duplicate_check.limit(5).collect()
            for row in sample_dupes:
                logger.info(f"  Example: evidence_id with {row.subject_count} subjects, {row.row_count} total rows")
        else:
            logger.info("No duplicate evidence_ids found across subjects")

        # Step 4: Create temp view for MERGE INTO
        logger.info("Creating temporary view for merge operation...")
        rehashed_df.createOrReplaceTempView("rehashed_evidence")

        # Step 5: Use MERGE INTO to update hashes in-place
        # Match on (subject_id, old_evidence_id) to identify each row uniquely
        logger.info("Executing MERGE INTO to update hashes...")

        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING rehashed_evidence AS source
        ON target.subject_id = source.subject_id
           AND target.evidence_id = source.old_evidence_id
        WHEN MATCHED THEN UPDATE SET
            target.evidence_id = source.new_evidence_id,
            target.termlink_id = source.new_termlink_id
        """

        spark.sql(merge_query)

        logger.info("MERGE INTO completed successfully")

        # Step 6: Validate results
        logger.info("Validating results...")

        updated_df = spark.table(table_name)
        final_count = updated_df.count()

        # Check row count unchanged
        if final_count != initial_count:
            raise ValueError(f"Row count mismatch! Before: {initial_count:,}, After: {final_count:,}")

        # Check for NULL hashes
        null_hashes = updated_df.filter(
            col("evidence_id").isNull() | col("termlink_id").isNull()
        ).count()

        if null_hashes > 0:
            raise ValueError(f"Found {null_hashes:,} records with NULL hashes after update!")

        logger.info(f"Validation passed:")
        logger.info(f"  - Row count unchanged: {final_count:,}")
        logger.info(f"  - No NULL hashes found")

        elapsed_time = time.time() - start_time

        logger.info("=" * 80)
        logger.info(f"Rehash completed successfully!")
        logger.info(f"  Records processed: {final_count:,}")
        logger.info(f"  Execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        logger.info("=" * 80)

        # Return success statistics
        return {
            "status": "SUCCESS",
            "records_processed": final_count,
            "execution_time_seconds": elapsed_time,
            "run_id": args.run_id
        }

    except Exception as e:
        logger.error(f"Rehash failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
