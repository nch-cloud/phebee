#!/usr/bin/env python3
"""
Generate All Neptune TTL from Analytical Tables for PheBee Rebuild Pipeline

Generates TTL files for Neptune bulk load for BOTH subjects and projects:
- Subjects: Generated from subject_terms_by_subject analytical table
- Projects: Generated from DynamoDB subject-project mappings

This script combines subjects and projects TTL generation into a single EMR job
for the full database reset rebuild strategy.

Usage:
    spark-submit generate_all_ttl_from_analytical.py \
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


def write_subjects_ttl_partition(partition_iterator, bucket, run_id, region):
    """
    Process a partition of analytical table data and write subjects TTL files to S3.

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


def generate_projects_ttl(bucket, run_id, region, dynamodb_table):
    """
    Generate project TTL files from DynamoDB subject-project mappings.

    Args:
        bucket: S3 bucket name
        run_id: Rebuild run ID
        region: AWS region
        dynamodb_table: DynamoDB table name for subject mappings

    Returns:
        dict: Summary with project count and file info
    """
    logger.info("=" * 80)
    logger.info("Generating Projects TTL from DynamoDB mappings")
    logger.info(f"DynamoDB table: {dynamodb_table}")
    logger.info("=" * 80)

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table)
    s3 = boto3.client('s3', region_name=region)

    # Scan DynamoDB for all subject-project mappings
    logger.info("Scanning DynamoDB for subject-project mappings...")
    mapping_records = []
    scan_kwargs = {
        'FilterExpression': 'begins_with(PK, :pk_prefix)',
        'ExpressionAttributeValues': {
            ':pk_prefix': 'SUBJECT#'
        }
    }

    done = False
    scan_count = 0
    while not done:
        response = table.scan(**scan_kwargs)
        scan_count += 1

        for item in response.get('Items', []):
            # PK format: "SUBJECT#{subject_id}"
            # SK format: "PROJECT#{project_id}#SUBJECT#{project_subject_id}"
            pk_parts = item['PK'].split('#')
            sk_parts = item['SK'].split('#')

            if len(pk_parts) >= 2 and len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
                subject_id = pk_parts[1]
                project_id = sk_parts[1]
                project_subject_id = sk_parts[3]  # FIXED: was sk_parts[2] in old code

                mapping_records.append({
                    'subject_id': subject_id,
                    'project_id': project_id,
                    'project_subject_id': project_subject_id
                })

        # Handle pagination
        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            logger.info(f"Scan iteration {scan_count}: Found {len(mapping_records):,} mappings so far...")
        else:
            done = True

    logger.info(f"Loaded {len(mapping_records):,} subject-project mappings from DynamoDB")

    if not mapping_records:
        logger.warning("No subject-project mappings found in DynamoDB")
        return {
            "project_count": 0,
            "mapping_count": 0,
            "ttl_files": 0
        }

    # Group subjects by project_id
    projects_by_id = {}
    for mapping in mapping_records:
        project_id = mapping['project_id']
        if project_id not in projects_by_id:
            projects_by_id[project_id] = []
        projects_by_id[project_id].append(mapping)

    logger.info(f"Generating TTL files for {len(projects_by_id):,} projects...")

    # Write TTL file for each project
    ttl_files_written = 0
    for project_id, project_subjects in projects_by_id.items():
        project_ttl = [
            "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            ""
        ]

        # Add project declaration
        project_uri = f"<http://ods.nationwidechildrens.org/phebee/projects/{project_id}>"
        project_ttl.append(f"{project_uri} rdf:type phebee:Project .")

        # Add subjects for this project
        for mapping in project_subjects:
            subject_id = mapping['subject_id']
            project_subject_id = mapping['project_subject_id']

            # Subject declaration
            subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
            project_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")

            # Project relationships
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"

            # Project-subject relationships
            escaped_project_subject_id = project_subject_id.replace('"', '\\"').replace('\\', '\\\\')
            project_ttl.append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
            project_ttl.append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
            project_ttl.append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
            project_ttl.append(f"{project_subject_id_iri} rdfs:label \"{escaped_project_subject_id}\" .")

        # Write project-specific TTL file
        project_content = '\n'.join(project_ttl)
        project_key = f"rebuild/{run_id}/neptune/projects/{project_id}/all_subjects.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=project_key,
            Body=project_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        ttl_files_written += 1

        if ttl_files_written % 100 == 0:
            logger.info(f"  Written {ttl_files_written}/{len(projects_by_id)} project TTL files...")

    logger.info(f"Generated TTL files for {len(projects_by_id):,} projects")
    logger.info(f"Total mappings: {len(mapping_records):,}")

    return {
        "project_count": len(projects_by_id),
        "mapping_count": len(mapping_records),
        "ttl_files": ttl_files_written
    }


def create_spark_session(app_name="GenerateAllTTLFromAnalytical"):
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
    parser = argparse.ArgumentParser(description='Generate all Neptune TTL (subjects + projects) from analytical tables')
    parser.add_argument('--run-id', required=True, help='Rebuild run ID')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--by-subject-table', required=True, help='By-subject analytical table name')
    parser.add_argument('--bucket', required=True, help='S3 bucket for TTL output')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name for subject mappings')
    parser.add_argument('--region', required=True, help='AWS region')

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"Generate All Neptune TTL (Subjects + Projects) - Run ID: {args.run_id}")
    logger.info(f"Database: {args.iceberg_database}")
    logger.info(f"Source table: {args.by_subject_table}")
    logger.info(f"DynamoDB table: {args.dynamodb_table}")
    logger.info(f"Output bucket: {args.bucket}")
    logger.info(f"Output path: s3://{args.bucket}/rebuild/{args.run_id}/neptune/")
    logger.info("=" * 80)

    overall_start_time = time.time()

    # Create Spark session
    spark = create_spark_session(f"GenerateAllTTL-{args.run_id}")

    try:
        # ===== PART 1: Generate Subjects TTL from Analytical Table =====
        logger.info("")
        logger.info("=" * 80)
        logger.info("PART 1: Generating Subjects TTL from Analytical Table")
        logger.info("=" * 80)

        subjects_start_time = time.time()

        logger.info(f"Reading analytical table: glue_catalog.{args.iceberg_database}.{args.by_subject_table}")
        analytical_df = spark.table(f"glue_catalog.{args.iceberg_database}.{args.by_subject_table}")

        row_count = analytical_df.count()
        logger.info(f"Analytical table contains {row_count:,} (subject, term) combinations")

        # Generate subjects TTL using foreachPartition
        logger.info("Generating subjects TTL files...")
        analytical_df.foreachPartition(
            lambda partition: write_subjects_ttl_partition(partition, args.bucket, args.run_id, args.region)
        )

        # Verify subjects TTL files
        logger.info("Verifying subjects TTL files...")
        s3 = boto3.client('s3', region_name=args.region)
        subjects_ttl_prefix = f"rebuild/{args.run_id}/neptune/subjects/"

        response = s3.list_objects_v2(
            Bucket=args.bucket,
            Prefix=subjects_ttl_prefix
        )

        subjects_ttl_files = response.get('Contents', [])
        subjects_total_size = sum(obj['Size'] for obj in subjects_ttl_files)

        subjects_elapsed = time.time() - subjects_start_time

        logger.info(f"Generated {len(subjects_ttl_files)} subjects TTL files")
        logger.info(f"Total size: {subjects_total_size:,} bytes ({subjects_total_size / (1024**2):.2f} MB)")
        logger.info(f"Execution time: {subjects_elapsed:.2f} seconds ({subjects_elapsed/60:.2f} minutes)")

        # ===== PART 2: Generate Projects TTL from DynamoDB =====
        logger.info("")
        logger.info("=" * 80)
        logger.info("PART 2: Generating Projects TTL from DynamoDB")
        logger.info("=" * 80)

        projects_start_time = time.time()

        projects_summary = generate_projects_ttl(
            bucket=args.bucket,
            run_id=args.run_id,
            region=args.region,
            dynamodb_table=args.dynamodb_table
        )

        # Verify projects TTL files
        projects_ttl_prefix = f"rebuild/{args.run_id}/neptune/projects/"
        response = s3.list_objects_v2(
            Bucket=args.bucket,
            Prefix=projects_ttl_prefix
        )

        projects_ttl_files = response.get('Contents', [])
        projects_total_size = sum(obj['Size'] for obj in projects_ttl_files)

        projects_elapsed = time.time() - projects_start_time

        logger.info(f"Total size: {projects_total_size:,} bytes ({projects_total_size / (1024**2):.2f} MB)")
        logger.info(f"Execution time: {projects_elapsed:.2f} seconds ({projects_elapsed/60:.2f} minutes)")

        # ===== FINAL SUMMARY =====
        overall_elapsed = time.time() - overall_start_time

        logger.info("")
        logger.info("=" * 80)
        logger.info("TTL GENERATION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info("SUBJECTS:")
        logger.info(f"  Source records: {row_count:,}")
        logger.info(f"  TTL files: {len(subjects_ttl_files)}")
        logger.info(f"  Size: {subjects_total_size / (1024**2):.2f} MB")
        logger.info(f"  Location: s3://{args.bucket}/{subjects_ttl_prefix}")
        logger.info("")
        logger.info("PROJECTS:")
        logger.info(f"  Projects: {projects_summary['project_count']:,}")
        logger.info(f"  Mappings: {projects_summary['mapping_count']:,}")
        logger.info(f"  TTL files: {len(projects_ttl_files)}")
        logger.info(f"  Size: {projects_total_size / (1024**2):.2f} MB")
        logger.info(f"  Location: s3://{args.bucket}/{projects_ttl_prefix}")
        logger.info("")
        logger.info("TOTALS:")
        logger.info(f"  Total TTL files: {len(subjects_ttl_files) + len(projects_ttl_files)}")
        logger.info(f"  Total size: {(subjects_total_size + projects_total_size) / (1024**2):.2f} MB")
        logger.info(f"  Total execution time: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
        logger.info("=" * 80)

        return {
            "status": "SUCCESS",
            "subjects": {
                "source_records": row_count,
                "ttl_files": len(subjects_ttl_files),
                "total_size_bytes": subjects_total_size,
                "output_prefix": f"s3://{args.bucket}/{subjects_ttl_prefix}"
            },
            "projects": {
                "project_count": projects_summary['project_count'],
                "mapping_count": projects_summary['mapping_count'],
                "ttl_files": len(projects_ttl_files),
                "total_size_bytes": projects_total_size,
                "output_prefix": f"s3://{args.bucket}/{projects_ttl_prefix}"
            },
            "execution_time_seconds": overall_elapsed,
            "run_id": args.run_id
        }

    except Exception as e:
        logger.error(f"TTL generation failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
