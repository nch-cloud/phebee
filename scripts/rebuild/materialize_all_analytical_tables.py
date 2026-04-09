#!/usr/bin/env python3
"""
Materialize All Analytical Tables Script for PheBee Rebuild Pipeline

Rebuilds both analytical tables (subject_terms_by_subject and subject_terms_by_project_term)
from evidence table for ALL projects. Uses full rebuild approach (TRUNCATE + INSERT)
since tables are cleared before this step runs.

Usage:
    spark-submit materialize_all_analytical_tables.py \
        --run-id <uuid> \
        --iceberg-database <database> \
        --evidence-table <table> \
        --by-subject-table <table> \
        --by-project-term-table <table> \
        --dynamodb-table <table> \
        --region <aws-region>
"""

import argparse
import sys
import time
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="MaterializeAllAnalyticalTables"):
    """Create Spark session with Iceberg configuration and high shuffle partitions"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .getOrCreate()


def main():
    parser = argparse.ArgumentParser(description='Materialize analytical tables from evidence for ALL projects')
    parser.add_argument('--run-id', required=True, help='Rebuild run ID')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--evidence-table', required=True, help='Evidence table name')
    parser.add_argument('--by-subject-table', required=True, help='By-subject output table name')
    parser.add_argument('--by-project-term-table', required=True, help='By-project-term output table name')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name for subject mappings')
    parser.add_argument('--region', required=True, help='AWS region')

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"Materialize All Analytical Tables - Run ID: {args.run_id}")
    logger.info(f"Database: {args.iceberg_database}")
    logger.info(f"Evidence table: {args.evidence_table}")
    logger.info(f"Output tables: {args.by_subject_table}, {args.by_project_term_table}")
    logger.info(f"DynamoDB table: {args.dynamodb_table}")
    logger.info("=" * 80)

    start_time = time.time()

    # Create Spark session
    spark = create_spark_session(f"MaterializeAllProjects-{args.run_id}")

    try:
        # Step 1: Query DynamoDB for ALL subject mappings (not filtered by project_id)
        logger.info(f"Querying DynamoDB table {args.dynamodb_table} for ALL subjects...")
        dynamodb = boto3.resource('dynamodb', region_name=args.region)
        table = dynamodb.Table(args.dynamodb_table)

        # Scan for all subject mappings (SUBJECT# in PK)
        mapping_records = []
        scan_kwargs = {
            'FilterExpression': 'begins_with(PK, :pk_prefix)',
            'ExpressionAttributeValues': {
                ':pk_prefix': 'SUBJECT#'
            }
        }

        done = False
        while not done:
            response = table.scan(**scan_kwargs)

            for item in response.get('Items', []):
                # PK format: "SUBJECT#{subject_id}"
                # SK format: "PROJECT#{project_id}#SUBJECT#{project_subject_id}"
                pk_parts = item['PK'].split('#')
                sk_parts = item['SK'].split('#')

                if len(pk_parts) >= 2 and len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
                    subject_id = pk_parts[1]
                    project_id = sk_parts[1]
                    project_subject_id = sk_parts[3]

                    mapping_records.append({
                        'subject_id': subject_id,
                        'project_id': project_id,
                        'project_subject_id': project_subject_id
                    })

            # Handle pagination
            if 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                done = True

        logger.info(f"Loaded {len(mapping_records):,} subject mappings from DynamoDB")

        if not mapping_records:
            logger.error("ERROR: No subjects found in DynamoDB")
            sys.exit(1)

        # Create Spark DataFrame from mapping records and broadcast it
        mapping_df = spark.createDataFrame(mapping_records)
        mapping_df = F.broadcast(mapping_df)  # Broadcast for efficient joins
        logger.info(f"Created broadcast DataFrame with {len(mapping_records):,} mappings")

        # Step 2: Read entire evidence table (no filtering)
        logger.info(f"Reading evidence table: glue_catalog.{args.iceberg_database}.{args.evidence_table}")
        evidence_df = spark.table(f"glue_catalog.{args.iceberg_database}.{args.evidence_table}")

        evidence_count = evidence_df.count()
        logger.info(f"Total evidence records: {evidence_count:,}")

        # Step 3: Process qualifiers and explode to individual rows
        # This creates one row per (evidence_id, qualifier) for efficient aggregation
        # Keep full struct (type + value) to preserve qualifier details
        logger.info("Processing qualifiers and exploding to individual rows...")
        evidence_with_single_qualifiers = evidence_df.select(
            "evidence_id",
            "subject_id",
            "term_iri",
            "termlink_id",
            "created_date",
            F.col("note_context.note_date").alias("note_date"),
            # Explode active qualifiers to individual rows, keeping full struct
            F.explode_outer(
                F.filter(
                    F.coalesce(F.col("qualifiers"), F.array()),
                    lambda q: F.lower(q.qualifier_value).isin(["false", "0"]) == False
                )
            ).alias("qualifier_struct")
        )

        # Step 4: Aggregate to (subject, term, termlink) level for by_subject table
        # Note: termlink_id hash includes qualifiers, so different qualifiers = different termlinks
        logger.info("Aggregating for subject_terms_by_subject...")
        by_subject_df = evidence_with_single_qualifiers.groupBy("subject_id", "term_iri", "termlink_id").agg(
            F.concat(
                F.lit("http://ods.nationwidechildrens.org/phebee/subjects/"),
                F.col("subject_id")
            ).alias("subject_iri"),
            F.countDistinct("evidence_id").alias("evidence_count"),
            F.min("note_date").alias("first_evidence_date"),
            F.max("note_date").alias("last_evidence_date"),
            # Collect qualifier structs directly (matches evidence table schema)
            F.collect_set("qualifier_struct").alias("qualifiers")
        ).select(
            "subject_id",
            "subject_iri",
            "term_iri",
            # Extract term_id from term_iri (e.g., http://purl.obolibrary.org/obo/HP_0001234 -> HP:0001234)
            F.regexp_replace(
                F.regexp_replace(F.col("term_iri"), ".*/obo/", ""),
                "_", ":"
            ).alias("term_id"),
            F.lit(None).cast(StringType()).alias("term_label"),
            "qualifiers",
            "evidence_count",
            "termlink_id",
            "first_evidence_date",
            "last_evidence_date"
        )

        # Write by_subject table using INSERT (tables are pre-cleared)
        logger.info(f"Writing by_subject table...")
        by_subject_df.writeTo(f"glue_catalog.{args.iceberg_database}.{args.by_subject_table}").append()

        by_subject_count = by_subject_df.count()
        logger.info(f"Successfully wrote {by_subject_count:,} rows to by_subject table")

        # Step 5: Aggregate for by_project_term table (partitioned by project_id, term_id)
        logger.info("Aggregating for subject_terms_by_project_term...")

        # Join with mapping to get project_subject_id and project_id
        evidence_with_mapping = evidence_with_single_qualifiers.join(
            mapping_df.select("subject_id", "project_id", "project_subject_id"),
            "subject_id",
            "inner"
        )

        by_project_term_df = evidence_with_mapping.groupBy(
            "project_id", "subject_id", "project_subject_id", "term_iri", "termlink_id"
        ).agg(
            F.concat(
                F.lit("http://ods.nationwidechildrens.org/phebee/subjects/"),
                F.col("subject_id")
            ).alias("subject_iri"),
            F.concat(
                F.lit("http://ods.nationwidechildrens.org/phebee/projects/"),
                F.col("project_id"),
                F.lit("/"),
                F.col("project_subject_id")
            ).alias("project_subject_iri"),
            F.countDistinct("evidence_id").alias("evidence_count"),
            F.min("note_date").alias("first_evidence_date"),
            F.max("note_date").alias("last_evidence_date"),
            # Collect qualifier structs directly (matches evidence table schema)
            F.collect_set("qualifier_struct").alias("qualifiers")
        ).select(
            "project_id",
            "subject_id",
            "project_subject_id",
            "subject_iri",
            "project_subject_iri",
            "term_iri",
            F.regexp_replace(
                F.regexp_replace(F.col("term_iri"), ".*/obo/", ""),
                "_", ":"
            ).alias("term_id"),
            F.lit(None).cast(StringType()).alias("term_label"),
            "qualifiers",
            "evidence_count",
            "termlink_id",
            "first_evidence_date",
            "last_evidence_date"
        )

        # Write by_project_term table using INSERT (tables are pre-cleared)
        logger.info(f"Writing by_project_term table...")
        by_project_term_df.writeTo(f"glue_catalog.{args.iceberg_database}.{args.by_project_term_table}").append()

        by_project_term_count = by_project_term_df.count()
        logger.info(f"Successfully wrote {by_project_term_count:,} rows to by_project_term table")

        elapsed_time = time.time() - start_time

        logger.info("=" * 80)
        logger.info(f"Materialization completed successfully!")
        logger.info(f"  Evidence records processed: {evidence_count:,}")
        logger.info(f"  Subjects processed: {len(mapping_records):,}")
        logger.info(f"  By-subject rows: {by_subject_count:,}")
        logger.info(f"  By-project-term rows: {by_project_term_count:,}")
        logger.info(f"  Execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        logger.info("=" * 80)

        return {
            "status": "SUCCESS",
            "evidence_records": evidence_count,
            "subjects_processed": len(mapping_records),
            "by_subject_rows": by_subject_count,
            "by_project_term_rows": by_project_term_count,
            "execution_time_seconds": elapsed_time,
            "run_id": args.run_id
        }

    except Exception as e:
        logger.error(f"Materialization failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
