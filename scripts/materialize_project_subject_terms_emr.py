#!/usr/bin/env python3

import argparse
import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType
from pyspark import StorageLevel

def main():
    parser = argparse.ArgumentParser(description='Materialize subject-terms analytical tables using EMR Serverless')
    parser.add_argument('--region', required=True, help='AWS region (e.g., us-east-2)')
    parser.add_argument('--project-id', required=True, help='Project ID to materialize')
    parser.add_argument('--run-ids', required=True, help='Comma-separated run IDs to process incrementally')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--evidence-table', required=True, help='Evidence table name')
    parser.add_argument('--by-subject-table', required=True, help='By-subject output table name')
    parser.add_argument('--by-project-term-table', required=True, help='By-project-term output table name')
    parser.add_argument('--dynamodb-table', required=True, help='DynamoDB table name for subject mappings')
    parser.add_argument('--lookback-days', type=int, default=1000, help='Days to look back for partition pruning (default: 1000 for wide historical coverage)')

    args = parser.parse_args()

    region = args.region
    iceberg_database = args.iceberg_database
    evidence_table = args.evidence_table
    by_subject_table = args.by_subject_table
    by_project_term_table = args.by_project_term_table
    dynamodb_table = args.dynamodb_table

    print(f"Starting materialization for project {args.project_id}")
    print(f"  Region: {region}")
    print(f"  Iceberg database: {iceberg_database}")
    print(f"  Evidence table: {evidence_table}")
    print(f"  By-subject table: {by_subject_table}")
    print(f"  By-project-term table: {by_project_term_table}")
    print(f"  DynamoDB table: {dynamodb_table}")

    # Initialize Spark with Iceberg support
    # High shuffle partition count for safety with large aggregations
    # Incremental processing by run_id keeps data volumes manageable
    spark = SparkSession.builder \
        .appName(f"MaterializeProjectSubjectTerms-{args.project_id}") \
        .config("spark.sql.shuffle.partitions", "1000") \
        .getOrCreate()

    try:
        print(f"\nStarting materialization for project {args.project_id}")
        print(f"Evidence table: {iceberg_database}.{evidence_table}")
        print(f"Output tables: {by_subject_table}, {by_project_term_table}")

        # Query DynamoDB for subject mappings
        print(f"\nQuerying DynamoDB table {dynamodb_table} for project {args.project_id}")
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(dynamodb_table)

        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': f'PROJECT#{args.project_id}',
                ':sk_prefix': 'SUBJECT#'
            }
        )

        # Parse DynamoDB results into mapping records
        mapping_records = []
        for item in response.get('Items', []):
            # Parse SK: "SUBJECT#{project_subject_id}"
            sk_parts = item['SK'].split('#')
            if len(sk_parts) >= 2:
                project_subject_id = sk_parts[1]
                subject_id = item.get('subject_id')
                if subject_id:
                    mapping_records.append({
                        'project_id': args.project_id,
                        'project_subject_id': project_subject_id,
                        'subject_id': subject_id
                    })

        print(f"Loaded {len(mapping_records)} subject mappings from DynamoDB")

        if not mapping_records:
            print("ERROR: No subjects found for project in DynamoDB")
            sys.exit(1)

        # Create Spark DataFrame from mapping records
        mapping_df = spark.createDataFrame(mapping_records)
        mapping_df.createOrReplaceTempView("subject_mapping")

        # Extract subject_ids for filtering
        subject_ids = [row.subject_id for row in mapping_df.select("subject_id").distinct().collect()]
        print(f"Found {len(subject_ids)} unique subjects for project {args.project_id}")

        # Read evidence table with filters
        print(f"\nReading evidence table...")
        print(f"Processing run_ids={args.run_ids} with {args.lookback_days} day lookback")
        evidence_df = spark.table(f"glue_catalog.{iceberg_database}.{evidence_table}")

        # Partition pruning: filter by created_date first (leverages Iceberg partitioning)
        # This allows Iceberg to skip reading old data files entirely
        lookback_date = F.current_date() - F.expr(f"INTERVAL {args.lookback_days} DAYS")
        evidence_df = evidence_df.filter(F.col("created_date") >= lookback_date)

        # Precise filtering: filter by specific run_id(s)
        run_id_list = [rid.strip() for rid in args.run_ids.split(',')]
        evidence_df = evidence_df.filter(F.col("run_id").isin(run_id_list))
        print(f"Applied partition pruning (created_date >= {args.lookback_days} days ago) and run_id filter")

        # Filter to only this project's subjects
        project_evidence_df = evidence_df.filter(F.col("subject_id").isin(subject_ids))
        evidence_count = project_evidence_df.count()
        print(f"Filtered to {evidence_count} evidence records for project subjects and specified run_ids")

        if evidence_count == 0:
            print("ERROR: No evidence found for project subjects")
            sys.exit(1)

        # Use native partitioning from Iceberg - avoid expensive repartition shuffle on 113M records
        # The repartition was causing OOM even with multiple executors due to massive shuffle overhead
        native_partitions = project_evidence_df.rdd.getNumPartitions()
        print(f"Using native Iceberg partitioning: {native_partitions} partitions")
        print(f"Estimated records per partition: {evidence_count // native_partitions:,}")

        # Process qualifiers: extract active qualifier types and explode to individual rows
        # REVISED APPROACH: Explode creates more rows (~339M) but simpler aggregation
        # With 1000 partitions, this is ~339K rows per partition with scalar values
        # collect_set on scalar strings is much more memory-efficient than collect_list on arrays
        print("Processing qualifiers and exploding to individual rows...")
        evidence_with_single_qualifiers = project_evidence_df.select(
            "evidence_id",  # Keep evidence_id for accurate counting
            "subject_id",
            "term_iri",
            "termlink_id",
            "created_date",
            F.col("note_context.note_date").alias("note_date"),  # Extract note timestamp for date ranges
            # Explode active qualifiers to individual rows (one row per qualifier type)
            F.explode_outer(
                F.transform(
                    F.filter(
                        F.coalesce(F.col("qualifiers"), F.array()),  # Handle null qualifiers
                        lambda q: (q.qualifier_value == "true") | (q.qualifier_value == "1")
                    ),
                    lambda q: q.qualifier_type
                )
            ).alias("qualifier_type")
        )

        # Aggregate to (subject, term) level with scalar string collection
        print("Aggregating for subject_terms_by_subject...")
        by_subject_df = evidence_with_single_qualifiers.groupBy("subject_id", "term_iri").agg(
            F.concat(
                F.lit("http://ods.nationwidechildrens.org/phebee/subjects/"),
                F.col("subject_id")
            ).alias("subject_iri"),
            F.countDistinct("evidence_id").alias("evidence_count"),  # Count distinct evidence records by evidence_id
            F.min("note_date").alias("first_evidence_date"),  # Use note_date for clinical observation date
            F.max("note_date").alias("last_evidence_date"),  # Use note_date for clinical observation date
            F.first("termlink_id").alias("termlink_id"),
            # collect_set on scalar strings - much more memory efficient than arrays of arrays
            F.collect_set("qualifier_type").alias("qualifiers")
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

        # Skip count operation to avoid OOM - count() forces materialization of all 27.5M records
        # Just proceed directly to write
        num_partitions = by_subject_df.rdd.getNumPartitions()
        print(f"Writing by_subject table with {num_partitions} partitions...")

        # Use MERGE INTO for idempotent incremental updates
        # - Adds evidence counts together
        # - Extends date ranges (min/max)
        # - Unions qualifier sets
        # - Inserts new (subject, term) combinations
        by_subject_df.createOrReplaceTempView("new_by_subject_data")

        print(f"Executing MERGE INTO for by_subject table (incremental update)...")
        spark.sql(f"""
            MERGE INTO glue_catalog.{iceberg_database}.{by_subject_table} AS target
            USING new_by_subject_data AS source
            ON target.subject_id = source.subject_id
               AND target.term_iri = source.term_iri
            WHEN MATCHED THEN UPDATE SET
                subject_iri = source.subject_iri,
                term_id = source.term_id,
                term_label = source.term_label,
                evidence_count = target.evidence_count + source.evidence_count,
                first_evidence_date = LEAST(target.first_evidence_date, source.first_evidence_date),
                last_evidence_date = GREATEST(target.last_evidence_date, source.last_evidence_date),
                termlink_id = source.termlink_id,
                qualifiers = array_union(target.qualifiers, source.qualifiers)
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Successfully wrote by_subject table")

        # Aggregate for by_project_term table (partitioned by project_id, term_id)
        print("Aggregating for subject_terms_by_project_term...")

        # Join with mapping to get project_subject_id
        evidence_with_mapping = evidence_with_single_qualifiers.join(
            mapping_df.select("subject_id", "project_id", "project_subject_id"),
            "subject_id",
            "inner"
        )

        by_project_term_df = evidence_with_mapping.groupBy(
            "project_id", "subject_id", "project_subject_id", "term_iri"
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
            F.countDistinct("evidence_id").alias("evidence_count"),  # Count distinct evidence records by evidence_id
            F.min("note_date").alias("first_evidence_date"),  # Use note_date for clinical observation date
            F.max("note_date").alias("last_evidence_date"),  # Use note_date for clinical observation date
            F.first("termlink_id").alias("termlink_id"),
            # collect_set on scalar strings - much more memory efficient
            F.collect_set("qualifier_type").alias("qualifiers")
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

        # Skip count operation to avoid OOM - count() forces materialization
        num_partitions = by_project_term_df.rdd.getNumPartitions()
        print(f"Writing by_project_term table with {num_partitions} partitions...")

        # Use MERGE INTO for idempotent incremental updates
        by_project_term_df.createOrReplaceTempView("new_by_project_term_data")

        print(f"Executing MERGE INTO for by_project_term table (incremental update)...")
        spark.sql(f"""
            MERGE INTO glue_catalog.{iceberg_database}.{by_project_term_table} AS target
            USING new_by_project_term_data AS source
            ON target.project_id = source.project_id
               AND target.subject_id = source.subject_id
               AND target.term_iri = source.term_iri
            WHEN MATCHED THEN UPDATE SET
                project_subject_id = source.project_subject_id,
                subject_iri = source.subject_iri,
                project_subject_iri = source.project_subject_iri,
                term_id = source.term_id,
                term_label = source.term_label,
                evidence_count = target.evidence_count + source.evidence_count,
                first_evidence_date = LEAST(target.first_evidence_date, source.first_evidence_date),
                last_evidence_date = GREATEST(target.last_evidence_date, source.last_evidence_date),
                termlink_id = source.termlink_id,
                qualifiers = array_union(target.qualifiers, source.qualifiers)
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Successfully wrote by_project_term table")

        # Final statistics
        print(f"\nMaterialization complete:")
        print(f"  - Run IDs processed: {args.run_ids}")
        print(f"  - Subjects in project: {len(subject_ids)}")
        print(f"  - Evidence records processed: {evidence_count}")
        print(f"  - Tables updated: subject_terms_by_subject, subject_terms_by_project_term")

    except Exception as e:
        print(f"Error during materialization: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
