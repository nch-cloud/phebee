#!/usr/bin/env python3

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from urllib.parse import quote

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set default region if not specified
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("BulkEvidenceProcessor") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .getOrCreate()

def get_subject_id(table_name: str, project_id: str, project_subject_id: str) -> Optional[str]:
    """Get subject_id for a given project_id and project_subject_id"""
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(table_name)
    
    try:
        response = table.get_item(
            Key={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}'
            }
        )
        return response.get('Item', {}).get('subject_id')
    except Exception:
        return None

def create_subject_mapping(table_name: str, project_id: str, project_subject_id: str) -> str:
    """Create a new subject mapping and return the generated subject_id"""
    import uuid
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(table_name)
    
    subject_id = str(uuid.uuid4())
    
    # Write both directions
    try:
        with table.batch_writer() as batch:
            # Direction 1: Project → Subject
            batch.put_item(Item={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}',
                'subject_id': subject_id
            })
            
            # Direction 2: Subject → Project
            batch.put_item(Item={
                'PK': f'SUBJECT#{subject_id}',
                'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
            })
        
        return subject_id
    except Exception as e:
        raise Exception(f"Failed to create subject mapping: {e}")

def resolve_subjects_batch(spark, project_subject_pairs: Set[Tuple[str, str]], table_name: str) -> Dict[Tuple[str, str], str]:
    """Resolve subject IRIs using DynamoDB batch operations"""
    logger.info(f"Resolving {len(project_subject_pairs)} subject pairs using DynamoDB table {table_name}")
    
    subject_map = {}
    
    # First, try to get existing mappings
    for project_id, project_subject_id in project_subject_pairs:
        subject_id = get_subject_id(table_name, project_id, project_subject_id)
        if subject_id:
            subject_map[(project_id, project_subject_id)] = subject_id
    
    # Create new mappings for any that don't exist
    missing_pairs = project_subject_pairs - set(subject_map.keys())
    for project_id, project_subject_id in missing_pairs:
        subject_id = create_subject_mapping(table_name, project_id, project_subject_id)
        subject_map[(project_id, project_subject_id)] = subject_id
    
    logger.info(f"Resolved {len(subject_map)} subject mappings")
    return subject_map

def extract_evidence_records(data: List[dict], subject_map: Dict[Tuple[str, str], str], run_id: str) -> List[dict]:
    """Extract evidence records from input data"""
    evidence_records = []
    
    for entry in data:
        project_id = entry['project_id']
        project_subject_id = entry['project_subject_id']
        term_iri = entry['term_iri']
        
        subject_key = (project_id, project_subject_id)
        subject_iri = subject_map.get(subject_key)
        
        if not subject_iri:
            logger.warning(f"No subject IRI found for {subject_key}")
            continue
            
        subject_id = subject_iri.split('/')[-1]
        
        for evidence in entry.get('evidence', []):
            evidence_id = f"evidence_{hash((run_id, subject_id, term_iri, str(evidence)))}"
            
            # Build evidence record
            record = {
                'evidence_id': evidence_id,
                'run_id': run_id,
                'batch_id': '1',  # Single batch for EMR processing
                'evidence_type': 'clinical_annotation',
                'assertion_type': 'manual_assertion',
                'created_date': datetime.utcnow().strftime('%Y-%m-%d'),
                'updated_date': datetime.utcnow().isoformat(),
                'subject_id': subject_id,
                'reference_id': term_iri,
                'note_context': {
                    'note_id': evidence.get('clinical_note_id'),
                    'note_type': evidence.get('note_type'),
                    'note_date': evidence.get('note_timestamp'),
                    'encounter_id': evidence.get('encounter_id')
                },
                'creator': {
                    'creator_id': evidence.get('evidence_creator_id'),
                    'creator_type': evidence.get('evidence_creator_type'),
                    'creator_name': evidence.get('evidence_creator_name')
                },
                'text_annotation': {
                    'span_start': evidence.get('span_start'),
                    'span_end': evidence.get('span_end'),
                    'text_span': None,
                    'confidence_score': None
                },
                'qualifiers': [
                    {'qualifier_type': k, 'qualifier_value': str(v)}
                    for k, v in (evidence.get('contexts') or {}).items()
                ],
                'metadata': {}
            }
            
            evidence_records.append(record)
    
    return evidence_records

def write_to_iceberg(spark, evidence_records: List[dict], database: str, table: str):
    """Write evidence records to Iceberg table"""
    logger.info(f"Writing {len(evidence_records)} records to {database}.{table}")
    
    # Define schema
    schema = StructType([
        StructField("evidence_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("evidence_type", StringType(), False),
        StructField("assertion_type", StringType(), False),
        StructField("created_date", StringType(), False),
        StructField("updated_date", StringType(), False),
        StructField("subject_id", StringType(), False),
        StructField("reference_id", StringType(), True),
        StructField("note_context", StructType([
            StructField("note_id", StringType(), True),
            StructField("note_type", StringType(), True),
            StructField("note_date", StringType(), True),
            StructField("encounter_id", StringType(), True)
        ]), True),
        StructField("creator", StructType([
            StructField("creator_id", StringType(), True),
            StructField("creator_type", StringType(), True),
            StructField("creator_name", StringType(), True)
        ]), True),
        StructField("text_annotation", StructType([
            StructField("span_start", IntegerType(), True),
            StructField("span_end", IntegerType(), True),
            StructField("text_span", StringType(), True),
            StructField("confidence_score", DoubleType(), True)
        ]), True),
        StructField("qualifiers", ArrayType(StructType([
            StructField("qualifier_type", StringType(), True),
            StructField("qualifier_value", StringType(), True)
        ])), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(evidence_records, schema)
    
    # Write to Iceberg
    df.writeTo(f"glue_catalog.{database}.{table}").append()
    
    logger.info("Successfully wrote to Iceberg")

def main():
    parser = argparse.ArgumentParser(description='Process bulk evidence data')
    parser.add_argument('--run-id', required=True, help='Run ID')
    parser.add_argument('--input-path', required=True, help='S3 input path')
    parser.add_argument('--mapping-file', required=True, help='S3 path to subject mapping file')
    parser.add_argument('--iceberg-database', required=True, help='Iceberg database name')
    parser.add_argument('--iceberg-table', required=True, help='Iceberg table name')
    parser.add_argument('--output-bucket', required=True, help='S3 output bucket')
    parser.add_argument('--neptune-cluster', required=True, help='Neptune cluster endpoint')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    s3 = boto3.client('s3', region_name=AWS_REGION)
    
    try:
        # Read subject mapping file
        logger.info(f"Reading subject mapping from {args.mapping_file}")
        mapping_bucket = args.mapping_file.split('/')[2]
        mapping_key = '/'.join(args.mapping_file.split('/')[3:])
        
        obj = s3.get_object(Bucket=mapping_bucket, Key=mapping_key)
        subject_mapping = json.loads(obj['Body'].read().decode('utf-8'))
        logger.info(f"Loaded {len(subject_mapping)} subject mappings")
        
        # Read input data using Spark
        logger.info(f"Reading JSONL files from {args.input_path}")
        
        # Read all JSONL files recursively
        df = spark.read.option("multiline", "false") \
                      .option("recursiveFileLookup", "true") \
                      .json(args.input_path + "*.jsonl")
        
        logger.info(f"Loaded DataFrame with {df.count()} entries")
        
        # Convert subject mapping to DataFrame for joining
        mapping_rows = [(key.split('|')[0], key.split('|')[1], subject_id) 
                       for key, subject_id in subject_mapping.items()]
        mapping_df = spark.createDataFrame(mapping_rows, ["project_id", "project_subject_id", "subject_id"])
        
        # Join to add subject_id to the DataFrame
        df_with_subjects = df.join(mapping_df, ["project_id", "project_subject_id"], "left")
        
        # Explode evidence array to create one row per evidence item
        from pyspark.sql.functions import explode, col, lit
        evidence_df = df_with_subjects.select(
            "subject_id",
            "project_id", 
            "project_subject_id",
            "term_iri",
            explode("evidence").alias("evidence_item")
        )
        
        # Flatten evidence structure to match Iceberg schema
        from pyspark.sql.functions import sha2, concat_ws, coalesce, lit as spark_lit, current_timestamp, to_date, struct, array, when, to_json
        flattened_df = evidence_df.select(
            "subject_id",
            "term_iri",
            col("evidence_item.type").alias("evidence_type"),
            col("evidence_item.clinical_note_id").alias("clinical_note_id"),
            col("evidence_item.encounter_id").alias("encounter_id"),
            col("evidence_item.evidence_creator_id").alias("evidence_creator_id"),
            col("evidence_item.evidence_creator_type").alias("evidence_creator_type"),
            col("evidence_item.evidence_creator_name").alias("evidence_creator_name"),
            col("evidence_item.note_timestamp").alias("note_timestamp"),
            col("evidence_item.note_type").alias("note_type"),
            col("evidence_item.span_start").alias("span_start"),
            col("evidence_item.span_end").alias("span_end"),
            col("evidence_item.contexts.negated").alias("negated"),
            col("evidence_item.contexts.family").alias("family"),
            col("evidence_item.contexts.hypothetical").alias("hypothetical")
        ).withColumn("run_id", lit(args.run_id)) \
         .withColumn("evidence_id", sha2(concat_ws("|", 
            "subject_id", 
            "term_iri",
            coalesce("encounter_id", spark_lit("")),
            coalesce("clinical_note_id", spark_lit("")),
            coalesce("span_start", spark_lit("")),
            coalesce("span_end", spark_lit("")),
            coalesce("negated", spark_lit("")),
            coalesce("family", spark_lit("")),
            coalesce("hypothetical", spark_lit(""))
         ), 256)) \
         .withColumn("batch_id", input_file_name()) \
         .withColumn("assertion_type", 
            when(col("creator.creator_type") == "automated", "http://purl.obolibrary.org/obo/ECO_0000203")
            .when(col("creator.creator_type") == "manual", "http://purl.obolibrary.org/obo/ECO_0000218")
            .otherwise(raise_error(concat(lit("Unknown creator_type: "), col("creator.creator_type"))))
         ) \
         .withColumn("created_timestamp", current_timestamp()) \
         .withColumn("created_date", to_date(current_timestamp())) \
         .withColumn("source_level", lit("clinical_note")) \
         .withColumn("termlink_id", sha2(concat_ws("|", 
            "subject_id", 
            "term_iri",
            # Hardcoded sorted contexts for true determinism across schema changes
            concat_ws(",",
                concat(lit("family:"), coalesce(col("family").cast("string"), lit("null"))),
                concat(lit("hypothetical:"), coalesce(col("hypothetical").cast("string"), lit("null"))),
                concat(lit("negated:"), coalesce(col("negated").cast("string"), lit("null")))
                # Add new context fields here in alphabetical order
            )
         ), 256)) \
         .withColumn("note_context", struct(
            col("clinical_note_id").alias("note_id"),
            col("note_type"),
            col("note_timestamp").cast("timestamp").alias("note_date"),
            col("encounter_id")
         )) \
         .withColumn("creator", struct(
            col("evidence_creator_id").alias("creator_id"),
            col("evidence_creator_type").alias("creator_type"),
            col("evidence_creator_name").alias("creator_name")
         )) \
         .withColumn("text_annotation", struct(
            col("span_start"),
            col("span_end"),
            lit("").alias("text_span"),
            lit(1.0).alias("confidence_score")
         )) \
         .withColumn("qualifiers", array(
            struct(
                lit("negated").alias("qualifier_type"),
                col("negated").cast("string").alias("qualifier_value")
            ),
            struct(
                lit("family").alias("qualifier_type"),
                col("family").cast("string").alias("qualifier_value")
            ),
            struct(
                lit("hypothetical").alias("qualifier_type"),
                col("hypothetical").cast("string").alias("qualifier_value")
            )
         )) \
         .withColumn("metadata", lit(None).cast("map<string,string>")) \
         .select(
            "evidence_id", "run_id", "batch_id", "evidence_type", "assertion_type",
            "created_timestamp", "created_date", "source_level", "subject_id",
            "encounter_id", "clinical_note_id", "termlink_id", "term_iri",
            "note_context", "creator", "text_annotation", "qualifiers", "metadata"
         )
        
        evidence_count = flattened_df.count()
        logger.info(f"Flattened to {evidence_count} evidence records")
        
        # Write to Iceberg table
        logger.info(f"Writing to Iceberg table {args.iceberg_database}.{args.iceberg_table}")
        flattened_df.write \
                   .format("iceberg") \
                   .mode("append") \
                   .save(f"glue_catalog.{args.iceberg_database}.{args.iceberg_table}")
        
        logger.info(f"Successfully processed {evidence_count} evidence records")
        logger.info(f"Data written to Iceberg table: {args.iceberg_database}.{args.iceberg_table}")
        
        # Output results for Step Function
        result = {
            'run_id': args.run_id,
            'evidence_count': evidence_count,
            'iceberg_table': f"{args.iceberg_database}.{args.iceberg_table}"
        }
        
        print(json.dumps(result))
        
    except Exception as e:
        logger.exception("Processing failed")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
