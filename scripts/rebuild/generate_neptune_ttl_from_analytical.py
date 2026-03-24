#!/usr/bin/env python3
"""
Generate Neptune TTL from Analytical Tables for PheBee Rebuild Pipeline

Generates TTL files for Neptune bulk load from analytical tables (not raw evidence).
Uses pre-computed termlink_id from analytical tables for consistency with materialized data.

Usage:
    spark-submit generate_neptune_ttl_from_analytical.py \
        --run-id <uuid> \
        --iceberg-database <database> \
        --by-subject-table <table> \
        --bucket <s3-bucket> \
        --dynamodb-table <table> \
        --region <aws-region>
"""

import argparse
import sys
import time
import logging
import uuid
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_ttl_partition(partition_iterator, bucket, run_id, region):
    """
    Process a partition of analytical table data and write TTL files to S3.

    Args:
        partition_iterator: Iterator of Row objects from analytical table
        bucket: S3 bucket name
        run_id: Rebuild run ID
        region: AWS region
    """
    import boto3
    import uuid

    s3 = boto3.client('s3', region_name=region)
    partition_id = str(uuid.uuid4())[:8]

    subjects_ttl = []
    declared_subjects = set()
    declared_termlinks = set()
    record_count = 0

    for row in partition_iterator:
        record_count += 1

        # Skip if we've already processed this termlink
        if row.termlink_id in declared_termlinks:
            continue
        declared_termlinks.add(row.termlink_id)

        # Generate subject and termlink URIs
        subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}>"
        termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}/term-link/{row.termlink_id}>"
        term_uri = f"<{row.term_iri}>"

        # Subject type declaration (only once per subject)
        if row.subject_id not in declared_subjects:
            subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            declared_subjects.add(row.subject_id)

        # TermLink structure
        subjects_ttl.append(f"{termlink_uri} rdf:type phebee:TermLink .")
        subjects_ttl.append(f"{subject_uri} phebee:hasTermLink {termlink_uri} .")
        subjects_ttl.append(f"{termlink_uri} phebee:hasTerm {term_uri} .")

        # Add qualifiers if present
        # In analytical table, qualifiers is an array of structs (qualifier_type, qualifier_value)
        qualifiers = getattr(row, 'qualifiers', None)
        if qualifiers and len(qualifiers) > 0:
            for qualifier_struct in qualifiers:
                if qualifier_struct:  # Skip null/empty
                    qualifier_type = qualifier_struct.qualifier_type
                    # Format qualifier as IRI if it's a full URI, otherwise use phebee namespace
                    if qualifier_type and (qualifier_type.startswith('http://') or qualifier_type.startswith('https://')):
                        qualifier_uri = f"<{qualifier_type}>"
                    else:
                        qualifier_uri = f"phebee:{qualifier_type}"
                    subjects_ttl.append(f"{termlink_uri} phebee:hasQualifyingTerm {qualifier_uri} .")

    if record_count == 0:
        return  # Empty partition

    # Write subjects TTL
    if subjects_ttl:
        # Add TTL prefixes
        ttl_prefixes = [
            "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            ""
        ]
        subjects_content = '\n'.join(ttl_prefixes + subjects_ttl)
        subjects_key = f"rebuild/{run_id}/neptune/subjects/partition_{partition_id}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=subjects_key,
            Body=subjects_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        print(f"Wrote subjects TTL: s3://{bucket}/{subjects_key} ({len(subjects_ttl)} triples, {len(declared_subjects)} subjects, {len(declared_termlinks)} termlinks)")

    print(f"Partition {partition_id} processed {record_count} records")


def create_spark_session(app_name="GenerateNeptuneTTLFromAnalytical"):
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
    parser = argparse.ArgumentParser(description='Generate Neptune TTL from analytical tables')
    parser.add_argument('--run-id', required=True, help='Rebuild run ID')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--by-subject-table', required=True, help='By-subject analytical table name')
    parser.add_argument('--bucket', required=True, help='S3 bucket for TTL output')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name for subject mappings')
    parser.add_argument('--region', required=True, help='AWS region')

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"Generate Neptune TTL from Analytical Tables - Run ID: {args.run_id}")
    logger.info(f"Database: {args.iceberg_database}")
    logger.info(f"Source table: {args.by_subject_table}")
    logger.info(f"Output bucket: {args.bucket}")
    logger.info(f"Output path: s3://{args.bucket}/rebuild/{args.run_id}/neptune/subjects/")
    logger.info("=" * 80)

    start_time = time.time()

    # Create Spark session
    spark = create_spark_session(f"GenerateNeptuneTTL-{args.run_id}")

    try:
        # Step 1: Read analytical table (much smaller than evidence table)
        logger.info(f"Reading analytical table: glue_catalog.{args.iceberg_database}.{args.by_subject_table}")
        analytical_df = spark.table(f"glue_catalog.{args.iceberg_database}.{args.by_subject_table}")

        row_count = analytical_df.count()
        logger.info(f"Analytical table contains {row_count:,} (subject, term) combinations")

        # Step 2: Generate TTL using foreachPartition
        # Each partition writes its own TTL file to S3
        logger.info("Generating TTL files...")

        analytical_df.foreachPartition(
            lambda partition: write_ttl_partition(partition, args.bucket, args.run_id, args.region)
        )

        # Step 3: List generated TTL files for verification
        logger.info("Verifying TTL files...")
        s3 = boto3.client('s3', region_name=args.region)
        ttl_prefix = f"rebuild/{args.run_id}/neptune/subjects/"

        response = s3.list_objects_v2(
            Bucket=args.bucket,
            Prefix=ttl_prefix
        )

        ttl_files = response.get('Contents', [])
        total_size = sum(obj['Size'] for obj in ttl_files)

        logger.info(f"Generated {len(ttl_files)} TTL files")
        logger.info(f"Total size: {total_size:,} bytes ({total_size / (1024**2):.2f} MB)")

        elapsed_time = time.time() - start_time

        logger.info("=" * 80)
        logger.info(f"TTL generation completed successfully!")
        logger.info(f"  Source records: {row_count:,}")
        logger.info(f"  TTL files: {len(ttl_files)}")
        logger.info(f"  Output location: s3://{args.bucket}/{ttl_prefix}")
        logger.info(f"  Execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        logger.info("=" * 80)

        return {
            "status": "SUCCESS",
            "source_records": row_count,
            "ttl_files": len(ttl_files),
            "total_size_bytes": total_size,
            "output_prefix": f"s3://{args.bucket}/{ttl_prefix}",
            "execution_time_seconds": elapsed_time,
            "run_id": args.run_id
        }

    except Exception as e:
        logger.error(f"TTL generation failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
