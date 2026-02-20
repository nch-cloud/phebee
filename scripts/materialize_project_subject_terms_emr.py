#!/usr/bin/env python3

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType

def main():
    parser = argparse.ArgumentParser(description='Materialize subject-terms analytical tables using EMR Serverless')
    parser.add_argument('--project-id', required=True, help='Project ID to materialize')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--evidence-table', required=True, help='Evidence table name')
    parser.add_argument('--by-subject-table', required=True, help='Output table for subject_terms_by_subject')
    parser.add_argument('--by-project-term-table', required=True, help='Output table for subject_terms_by_project_term')
    parser.add_argument('--mapping-file', required=True, help='S3 path to subject mapping JSON')
    parser.add_argument('--region', required=True, help='AWS region')

    args = parser.parse_args()

    # Initialize Spark with Iceberg support
    spark = SparkSession.builder \
        .appName(f"MaterializeProjectSubjectTerms-{args.project_id}") \
        .getOrCreate()

    try:
        print(f"Starting materialization for project {args.project_id}")
        print(f"Evidence table: {args.iceberg_database}.{args.evidence_table}")
        print(f"Output tables: {args.by_subject_table}, {args.by_project_term_table}")

        # Load subject mapping file
        print(f"Loading subject mapping from {args.mapping_file}")
        mapping_df = spark.read.json(args.mapping_file)
        mapping_df.createOrReplaceTempView("subject_mapping")

        mapping_count = mapping_df.count()
        print(f"Loaded {mapping_count} subject mappings")

        # Filter to only subjects in this project's mapping
        subject_ids = [row.subject_id for row in mapping_df.select("subject_id").distinct().collect()]
        print(f"Found {len(subject_ids)} unique subjects for project {args.project_id}")

        if not subject_ids:
            print("ERROR: No subjects found in mapping file")
            sys.exit(1)

        # Read evidence table with filters
        print(f"Reading evidence table...")
        evidence_df = spark.table(f"glue_catalog.{args.iceberg_database}.{args.evidence_table}")

        # Filter to only this project's subjects
        project_evidence_df = evidence_df.filter(F.col("subject_id").isin(subject_ids))
        evidence_count = project_evidence_df.count()
        print(f"Filtered to {evidence_count} evidence records for project subjects")

        if evidence_count == 0:
            print("ERROR: No evidence found for project subjects")
            sys.exit(1)

        # Process qualifiers: extract active qualifier types from the qualifiers array
        # We explode qualifiers to identify active ones, but group back by evidence_id
        # to avoid counting the same evidence multiple times
        evidence_exploded = project_evidence_df.select(
            "evidence_id",  # Use evidence_id as the unique key for each record
            "subject_id",
            "term_iri",
            "termlink_id",
            "created_date",
            F.explode_outer("qualifiers").alias("qualifier")
        )

        # Extract active qualifiers (those with value='true' or '1')
        evidence_with_active_qualifiers = evidence_exploded.select(
            "evidence_id",
            "subject_id",
            "term_iri",
            "termlink_id",
            "created_date",
            F.when(
                (F.col("qualifier.qualifier_value") == "true") |
                (F.col("qualifier.qualifier_value") == "1"),
                F.col("qualifier.qualifier_type")
            ).alias("active_qualifier")
        )

        # CRITICAL: Group by evidence_id FIRST to avoid counting exploded rows
        # This collapses 12 exploded qualifier rows back to 4 evidence records
        # Using evidence_id is cleaner than composite key (subject_id, term_iri, termlink_id, created_date)
        evidence_with_qualifiers = evidence_with_active_qualifiers.groupBy(
            "evidence_id", "subject_id", "term_iri", "termlink_id", "created_date"
        ).agg(
            F.collect_set(
                F.when(F.col("active_qualifier").isNotNull(), F.col("active_qualifier"))
            ).alias("active_qualifiers_for_record")
        )

        # Now aggregate to (subject, term) level - counts will be correct
        print("Aggregating for subject_terms_by_subject...")
        by_subject_df = evidence_with_qualifiers.groupBy("subject_id", "term_iri").agg(
            F.concat(
                F.lit("http://ods.nationwidechildrens.org/phebee/subjects/"),
                F.col("subject_id")
            ).alias("subject_iri"),
            F.count("*").alias("evidence_count"),  # Now counts 4 evidence records, not 12 exploded rows
            F.min("created_date").alias("first_evidence_date"),
            F.max("created_date").alias("last_evidence_date"),
            F.first("termlink_id").alias("termlink_id"),
            # Flatten all qualifier sets into a single set
            F.array_distinct(F.flatten(F.collect_list("active_qualifiers_for_record"))).alias("qualifiers")
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

        by_subject_count = by_subject_df.count()
        print(f"Generated {by_subject_count} records for by_subject table")

        # Insert into by_subject table
        print(f"Writing to {args.iceberg_database}.{args.by_subject_table}...")
        by_subject_df.writeTo(f"glue_catalog.{args.iceberg_database}.{args.by_subject_table}") \
            .append()
        print(f"Successfully wrote {by_subject_count} records to by_subject table")

        # Aggregate for by_project_term table (partitioned by project_id, term_id)
        print("Aggregating for subject_terms_by_project_term...")

        # Join with mapping to get project_subject_id
        evidence_with_mapping = evidence_with_qualifiers.join(
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
            F.count("*").alias("evidence_count"),
            F.min("created_date").alias("first_evidence_date"),
            F.max("created_date").alias("last_evidence_date"),
            F.first("termlink_id").alias("termlink_id"),
            # Flatten all qualifier sets into a single set
            F.array_distinct(F.flatten(F.collect_list("active_qualifiers_for_record"))).alias("qualifiers")
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

        by_project_term_count = by_project_term_df.count()
        print(f"Generated {by_project_term_count} records for by_project_term table")

        # Insert into by_project_term table
        print(f"Writing to {args.iceberg_database}.{args.by_project_term_table}...")
        by_project_term_df.writeTo(f"glue_catalog.{args.iceberg_database}.{args.by_project_term_table}") \
            .append()
        print(f"Successfully wrote {by_project_term_count} records to by_project_term table")

        # Final statistics
        print(f"\nMaterialization complete:")
        print(f"  - Subjects: {len(subject_ids)}")
        print(f"  - Evidence records: {evidence_count}")
        print(f"  - by_subject records: {by_subject_count}")
        print(f"  - by_project_term records: {by_project_term_count}")

    except Exception as e:
        print(f"Error during materialization: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
