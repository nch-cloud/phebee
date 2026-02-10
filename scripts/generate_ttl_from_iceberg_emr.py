#!/usr/bin/env python3
"""
EMR job to generate TTL files from Iceberg evidence table
Reads evidence data and generates Neptune-compatible TTL files

Also writes subject-term links to DynamoDB cache for efficient cohort queries.
"""

import sys
import boto3
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast
from collections import defaultdict
import uuid

def get_current_ontology_versions(dynamodb_table_name, region):
    """
    Get current HPO and MONDO versions from DynamoDB metadata table.
    Queries SOURCE~{name} and finds the most recent version by InstallTimestamp.

    Returns:
        tuple: (hpo_version, mondo_version)
    """
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)

        # Get HPO version
        print(f"Looking up HPO version from DynamoDB table {dynamodb_table_name}...")
        hpo_response = dynamodb.query(
            TableName=dynamodb_table_name,
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': {'S': 'SOURCE~hpo'}}
        )

        hpo_items = hpo_response.get('Items', [])
        print(f"Found {len(hpo_items)} HPO source records")

        # Sort by InstallTimestamp and get the most recent
        hpo_installed = [
            item for item in hpo_items
            if 'InstallTimestamp' in item and 'S' in item['InstallTimestamp']
        ]

        if hpo_installed:
            hpo_installed.sort(key=lambda x: x['InstallTimestamp']['S'], reverse=True)
            hpo_version = hpo_installed[0].get('Version', {}).get('S')
            print(f"HPO lookup result: version={hpo_version}")
        else:
            hpo_version = None
            print("No HPO version found with InstallTimestamp")

        # Get MONDO version
        mondo_response = dynamodb.query(
            TableName=dynamodb_table_name,
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': {'S': 'SOURCE~mondo'}}
        )

        mondo_items = mondo_response.get('Items', [])
        mondo_installed = [
            item for item in mondo_items
            if 'InstallTimestamp' in item and 'S' in item['InstallTimestamp']
        ]

        if mondo_installed:
            mondo_installed.sort(key=lambda x: x['InstallTimestamp']['S'], reverse=True)
            mondo_version = mondo_installed[0].get('Version', {}).get('S')
            print(f"MONDO lookup result: version={mondo_version}")
        else:
            mondo_version = None

        return hpo_version, mondo_version
    except Exception as e:
        print(f"ERROR: Could not get ontology versions from DynamoDB: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def get_subject_mappings_from_s3(bucket, subjects_path, region):
    """Load subject mappings from S3 file and return as dict"""
    s3 = boto3.client('s3', region_name=region)
    
    print(f"Loading subject mappings from: s3://{bucket}/{subjects_path}")
    
    try:
        # Parse S3 path to get bucket and key
        if subjects_path.startswith('s3://'):
            # Remove s3:// prefix and split bucket/key
            path_parts = subjects_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1]
        else:
            # Assume it's just the key part
            key = subjects_path
        
        response = s3.get_object(Bucket=bucket, Key=key)
        subjects_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Convert to mapping dict format
        mapping_dict = {}
        for subject in subjects_data:
            subject_id = subject['subject_id']
            mapping_dict[subject_id] = {
                'project_id': subject['project_id'],
                'project_subject_id': subject['project_subject_id']
            }
        
        print(f"Loaded {len(mapping_dict)} subject mappings from S3")
        return mapping_dict
        
    except Exception as e:
        print(f"Error loading subject mappings from S3: {e}")
        raise
    """Load subject mappings from DynamoDB and return as dict"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table_name)
    
    # We need to scan for all SUBJECT# records to get the mappings
    # This is expensive but necessary for the broadcast approach
    print("Scanning DynamoDB for all subject mappings...")
    
    mapping_dict = {}
    
    # Scan with filter for SUBJECT# records
    response = table.scan(
        FilterExpression='begins_with(PK, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'SUBJECT#'}
    )
    
    # Process first page
    for item in response.get('Items', []):
        # PK format: SUBJECT#{subject_id}
        # SK format: PROJECT#{project_id}#SUBJECT#{project_subject_id}
        subject_id = item['PK'].replace('SUBJECT#', '')
        
        # Parse SK to get project info
        sk_parts = item['SK'].split('#')
        if len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
            project_id = sk_parts[1]
            project_subject_id = sk_parts[3]
            
            mapping_dict[subject_id] = {
                'project_id': project_id,
                'project_subject_id': project_subject_id
            }
    
    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression='begins_with(PK, :pk_prefix)',
            ExpressionAttributeValues={':pk_prefix': 'SUBJECT#'},
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        
        for item in response.get('Items', []):
            subject_id = item['PK'].replace('SUBJECT#', '')
            sk_parts = item['SK'].split('#')
            if len(sk_parts) >= 4 and sk_parts[0] == 'PROJECT' and sk_parts[2] == 'SUBJECT':
                project_id = sk_parts[1]
                project_subject_id = sk_parts[3]
                
                mapping_dict[subject_id] = {
                    'project_id': project_id,
                    'project_subject_id': project_subject_id
                }
    
    print(f"Loaded {len(mapping_dict)} subject mappings from DynamoDB")
    return mapping_dict

def write_ttl_partition(partition_iterator, broadcast_mappings, bucket, run_id, region, cache_table_name, hpo_version, mondo_version, dynamodb_table_name):
    """
    Process a partition and write TTL files to S3 + cache to DynamoDB.

    Dual-writes:
    1. TTL files for Neptune bulk load (existing functionality)
    2. Subject-term links to DynamoDB cache for efficient queries (new)
    """
    import boto3
    import os
    from collections import defaultdict

    # Inline cache helper functions to avoid Lambda layer dependency
    def parse_term_id_from_iri(term_iri):
        """Extract term ID from IRI (e.g., HP_0001234 from http://...HP_0001234)"""
        return term_iri.split('/')[-1].replace('_', '_')

    def build_project_data_pk(project_id):
        """Build partition key for project data"""
        return f"PROJECT#{project_id}"

    def build_project_data_sk(ancestor_path, subject_id):
        """Build sort key: ancestor_path||subject_id"""
        path_str = "|".join(ancestor_path)
        return f"{path_str}||{subject_id}"

    def get_term_ancestor_paths(ontology, version, term_id, table):
        """Get all ancestor paths for a term from cache"""
        pk = f"TERM#{ontology}|{version}|{term_id}"
        try:
            response = table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )
            paths = []
            for item in response.get("Items", []):
                sk = item.get("SK", "")
                if sk:
                    # SK format: "HP_0000001|HP_0000118|HP_0001627" (no prefix)
                    path = sk.split("|")
                    paths.append(path)
            return paths
        except Exception as e:
            print(f"Error querying ancestor paths for {term_id}: {e}")
            return []

    def get_term_label(ontology, version, term_id, table):
        """Get term label from cache"""
        pk = f"TERM#{ontology}|{version}|{term_id}"
        try:
            response = table.get_item(Key={"PK": pk, "SK": "LABEL"})
            item = response.get("Item", {})
            return item.get("label")
        except Exception as e:
            print(f"Error querying label for {term_id}: {e}")
            return None

    def get_projects_for_subject(subject_id, dynamodb_table):
        """
        Get all project IDs that contain a given subject by querying DynamoDB.

        Queries: PK=SUBJECT#{subject_id}, parses SK=PROJECT#{project_id}#SUBJECT#{project_subject_id}
        """
        pk = f"SUBJECT#{subject_id}"
        print(f"DEBUG: Querying projects for subject {subject_id} with PK={pk}")
        try:
            response = dynamodb_table.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": pk}
            )

            items_count = len(response.get("Items", []))
            print(f"DEBUG: Query returned {items_count} items for subject {subject_id}")

            project_ids = []
            for item in response.get("Items", []):
                sk = item.get("SK", "")
                # Parse SK format: PROJECT#{project_id}#SUBJECT#{project_subject_id}
                if sk.startswith("PROJECT#"):
                    parts = sk.split("#")
                    if len(parts) >= 2:
                        project_id = parts[1]
                        project_ids.append(project_id)
                        print(f"DEBUG: Found project {project_id} for subject {subject_id}")

            print(f"DEBUG: get_projects_for_subject returning {len(project_ids)} projects for subject {subject_id}")
            return project_ids
        except Exception as e:
            print(f"ERROR: Failed to query projects for subject {subject_id}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def write_subject_term_links_batch(links, table):
        """Write subject-term links to cache"""
        if not links:
            return 0

        items_to_write = []
        for link in links:
            project_id = link["project_id"]
            subject_id = link["subject_id"]
            ancestor_paths = link.get("ancestor_paths", [])

            # Write one entry per ancestor path (handles multiple inheritance)
            for path in ancestor_paths:
                item = {
                    "PK": build_project_data_pk(project_id),
                    "SK": build_project_data_sk(path, subject_id),
                    "termlink_id": link["termlink_id"],
                    "term_iri": link["term_iri"],
                    "term_label": link.get("term_label", link["term_iri"]),
                    "qualifiers": link.get("qualifiers", {})
                }
                items_to_write.append(item)

        # Deduplicate by PK+SK to avoid batch write errors
        # Keep the last occurrence (most recent termlink for this path)
        seen_keys = {}
        for item in items_to_write:
            key = (item["PK"], item["SK"])
            seen_keys[key] = item
        items_to_write = list(seen_keys.values())

        # Write in batches of 25 (DynamoDB limit)
        count = 0
        batch_size = 25
        for i in range(0, len(items_to_write), batch_size):
            batch = items_to_write[i:i + batch_size]
            try:
                with table.batch_writer() as writer:
                    for item in batch:
                        writer.put_item(Item=item)
                        count += 1
            except Exception as e:
                print(f"Error writing batch to cache: {e}")

        return count

    s3 = boto3.client('s3', region_name=region)
    partition_id = str(uuid.uuid4())[:8]  # Short unique ID for this partition

    print(f"DEBUG: Partition {partition_id} starting - cache_table_name={cache_table_name}, dynamodb_table_name={dynamodb_table_name}, hpo_version={hpo_version}")

    subjects_ttl = []
    declared_subjects = set()  # Track declared subjects to avoid duplicates
    declared_termlinks = set()  # Track declared termlinks to avoid duplicates
    record_count = 0

    # Collect cache entries as we process
    cache_links = []
    cache_terms_lookup = {}  # Cache ancestor paths to avoid duplicate lookups

    # Get the broadcast mappings
    subject_mappings = broadcast_mappings.value

    # Get DynamoDB resources
    cache_table = None
    dynamodb_table = None
    if cache_table_name and dynamodb_table_name:
        try:
            print(f"DEBUG: Connecting to DynamoDB tables: {dynamodb_table_name}, {cache_table_name}")
            dynamodb = boto3.resource('dynamodb', region_name=region)
            cache_table = dynamodb.Table(cache_table_name)
            dynamodb_table = dynamodb.Table(dynamodb_table_name)
            print(f"DEBUG: Successfully connected to DynamoDB tables")
        except Exception as e:
            print(f"ERROR: Could not connect to DynamoDB tables: {e}")
            import traceback
            traceback.print_exc()
            cache_table = None
            dynamodb_table = None
    elif cache_table_name:
        print("WARNING: cache_table_name provided but dynamodb_table_name missing, cannot query multi-project relationships")
    else:
        print("WARNING: cache_table_name not provided, cache writes disabled")

    for row in partition_iterator:
        record_count += 1
        
        # Skip if we've already processed this termlink in this partition
        if row.termlink_id in declared_termlinks:
            continue
            
        declared_termlinks.add(row.termlink_id)
        
        # Generate subjects TTL - match Lambda format exactly
        subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}>"
        termlink_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{row.subject_id}/term-link/{row.termlink_id}>"
        term_uri = f"<{row.term_iri}>"
        
        # Subject type declaration (only once per subject)
        if row.subject_id not in declared_subjects:
            subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            declared_subjects.add(row.subject_id)
        
        # TermLink structure (same as Lambda)
        subjects_ttl.append(f"{termlink_uri} rdf:type phebee:TermLink .")
        subjects_ttl.append(f"{subject_uri} phebee:hasTermLink {termlink_uri} .")
        subjects_ttl.append(f"{termlink_uri} phebee:hasTerm {term_uri} .")
        
        # Add qualifiers if present
        qualifiers_data = getattr(row, 'qualifiers', None)
        qualifiers_dict = {}  # For cache storage

        if qualifiers_data and qualifiers_data != '[]':
            try:
                import json
                qualifiers = json.loads(qualifiers_data) if isinstance(qualifiers_data, str) else qualifiers_data
            except:
                # Fallback regex parsing for malformed JSON (same as Lambda)
                import re
                matches = re.findall(r'\{qualifier_type=([^,]+), qualifier_value=([^}]+)\}', qualifiers_data)
                qualifiers = [{'qualifier_type': qt, 'qualifier_value': qv} for qt, qv in matches]

            for qualifier in qualifiers:
                try:
                    # Qualifiers are Spark Row objects with qualifier_type and qualifier_value fields
                    qualifier_type = qualifier.qualifier_type
                    qualifier_value = qualifier.qualifier_value

                    # Store qualifier in cache (both true and false values)
                    qualifiers_dict[qualifier_type] = str(qualifier_value).lower()

                    # Only add to TTL if true
                    if qualifier_value == "true":
                        subjects_ttl.append(f"{termlink_uri} phebee:hasQualifyingTerm phebee:{qualifier_type} .")
                except Exception as e:
                    # Log and skip any problematic qualifiers
                    print(f"Warning: Failed to process qualifier {qualifier}: {e}")
                    continue

        # Collect data for cache write
        # Query DynamoDB to get ALL projects that contain this subject
        if cache_table and dynamodb_table:
            # Get ALL projects for this subject (not just the import project)
            print(f"DEBUG: Looking up projects for subject {row.subject_id}")
            project_ids = get_projects_for_subject(row.subject_id, dynamodb_table)

            if project_ids:
                print(f"DEBUG: Subject {row.subject_id} belongs to {len(project_ids)} projects: {project_ids}")
                term_id = parse_term_id_from_iri(row.term_iri)

                # Create cache entry for EACH project the subject belongs to
                for project_id in project_ids:
                    cache_links.append({
                        'project_id': project_id,
                        'subject_id': row.subject_id,
                        'term_id': term_id,
                        'term_iri': row.term_iri,
                        'termlink_id': row.termlink_id,
                        'qualifiers': qualifiers_dict
                    })
            else:
                print(f"DEBUG: No projects found for subject {row.subject_id}, skipping cache write")
        else:
            print(f"DEBUG: Cache or DynamoDB table not available: cache_table={cache_table is not None}, dynamodb_table={dynamodb_table is not None}")
    
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
        subjects_key = f"runs/{run_id}/neptune/subjects/partition_{partition_id}.ttl"
        s3.put_object(
            Bucket=bucket,
            Key=subjects_key,
            Body=subjects_content.encode('utf-8'),
            ContentType='text/turtle'
        )
        print(f"Wrote subjects TTL: s3://{bucket}/{subjects_key} ({len(subjects_ttl)} triples)")

    # Write to DynamoDB cache
    if cache_links and cache_table and hpo_version:
        print(f"Writing {len(cache_links)} links to cache...")

        # Look up ancestor paths for all unique terms
        unique_terms = {}  # term_id -> term_iri
        for link in cache_links:
            term_id = link['term_id']
            if term_id not in unique_terms:
                unique_terms[term_id] = link['term_iri']

        # Look up paths for each term
        term_paths_map = {}  # term_id -> list of ancestor paths
        term_labels_map = {}  # term_id -> label
        for term_id, term_iri in unique_terms.items():
            # Determine ontology
            if term_id.startswith("HP_"):
                ontology = "hpo"
                version = hpo_version
            elif term_id.startswith("MONDO_"):
                ontology = "mondo"
                version = mondo_version
            else:
                print(f"Warning: Unknown ontology for term {term_id}, skipping cache write")
                continue

            try:
                paths = get_term_ancestor_paths(ontology, version, term_id, table=cache_table)
                label = get_term_label(ontology, version, term_id, table=cache_table)

                if paths:
                    term_paths_map[term_id] = paths
                    term_labels_map[term_id] = label or term_iri
                else:
                    print(f"Warning: No ancestor paths found for {term_id}, skipping")
            except Exception as e:
                print(f"Warning: Failed to look up paths for {term_id}: {e}")
                continue

        # Build cache entries with ancestor paths
        cache_entries = []
        for link in cache_links:
            term_id = link['term_id']
            if term_id in term_paths_map:
                cache_entries.append({
                    'project_id': link['project_id'],
                    'subject_id': link['subject_id'],
                    'term_id': term_id,
                    'term_iri': link['term_iri'],
                    'term_label': term_labels_map.get(term_id, link['term_iri']),
                    'ancestor_paths': term_paths_map[term_id],
                    'qualifiers': link['qualifiers'],
                    'termlink_id': link['termlink_id']
                })

        # Write to cache in batch
        if cache_entries:
            try:
                count = write_subject_term_links_batch(cache_entries, table=cache_table)
                print(f"Successfully wrote {count} cache items for partition {partition_id}")
            except Exception as e:
                print(f"Warning: Failed to write to cache: {e}")

    print(f"Partition {partition_id} processed {record_count} records")

def main():
    if len(sys.argv) != 9:
        print("Usage: generate_ttl_from_iceberg_emr.py <run_id> <database> <table> <bucket> <mapping_file> <region> <dynamodb_table> <cache_table>")
        sys.exit(1)

    run_id = sys.argv[1]
    database = sys.argv[2]
    table = sys.argv[3]
    bucket = sys.argv[4]
    mapping_file = sys.argv[5]  # S3 path to subject_mapping.json from resolve step
    region = sys.argv[6]
    dynamodb_table_name = sys.argv[7]
    cache_table_name = sys.argv[8]

    # Set environment variables for phebee utils before they're imported
    os.environ['DYNAMODB_TABLE_NAME'] = dynamodb_table_name
    os.environ['PheBeeDynamoTable'] = dynamodb_table_name
    os.environ['PheBeeDynamoCacheTable'] = cache_table_name

    print(f"Starting TTL generation for run_id: {run_id}")
    print(f"Reading from: {database}.{table}")
    print(f"Writing to: s3://{bucket}/runs/{run_id}/neptune/")
    print(f"Using DynamoDB table: {dynamodb_table_name}")
    print(f"Using cache table: {cache_table_name}")

    # Get ontology versions from environment if available
    hpo_version = os.environ.get('HPO_VERSION')
    mondo_version = os.environ.get('MONDO_VERSION')

    # If versions not in environment, look them up from DynamoDB
    if not hpo_version:
        print("Ontology versions not in environment, looking up from DynamoDB...")
        hpo_version, mondo_version = get_current_ontology_versions(dynamodb_table_name, region)
        if hpo_version:
            print(f"Retrieved versions from DynamoDB: HPO={hpo_version}, MONDO={mondo_version or 'N/A'}")

    if cache_table_name and hpo_version:
        print(f"Cache enabled: table={cache_table_name}, HPO={hpo_version}, MONDO={mondo_version or 'N/A'}")
    else:
        print("Cache disabled: missing CACHE_TABLE_NAME or HPO_VERSION")
        cache_table_name = None
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"GenerateTTLFromIceberg-{run_id}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()
    
    try:
        # Clean up any existing TTL files from previous runs
        print("Cleaning up existing TTL files...")
        s3 = boto3.client('s3', region_name=region)
        
        # List and delete existing TTL files for this run
        for prefix in [f"runs/{run_id}/neptune/subjects/", f"runs/{run_id}/neptune/projects/"]:
            try:
                # Handle pagination for large numbers of files
                continuation_token = None
                while True:
                    list_params = {'Bucket': bucket, 'Prefix': prefix}
                    if continuation_token:
                        list_params['ContinuationToken'] = continuation_token
                    
                    response = s3.list_objects_v2(**list_params)
                    
                    if 'Contents' in response:
                        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.ttl')]
                        if objects_to_delete:
                            # Delete in batches of 1000 (S3 limit)
                            for i in range(0, len(objects_to_delete), 1000):
                                batch = objects_to_delete[i:i+1000]
                                s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                            print(f"Deleted {len(objects_to_delete)} old TTL files from {prefix}")
                    
                    if not response.get('IsTruncated'):
                        break
                    continuation_token = response.get('NextContinuationToken')
                    
            except Exception as e:
                print(f"Warning: Could not clean up {prefix}: {e}")
        
        # Load subject mappings from S3 (from resolve step)
        print("Loading subject mappings from resolve step...")
        subject_mappings = get_subject_mappings_from_s3(bucket, mapping_file, region)
        print(f"Loaded {len(subject_mappings)} subject mappings")
        
        # Broadcast the mappings to all executors
        broadcast_mappings = spark.sparkContext.broadcast(subject_mappings)
        
        # Read evidence data from Iceberg
        print("Reading evidence data from Iceberg...")
        evidence_df = spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
        filtered_df = evidence_df.filter(f"run_id = '{run_id}'")
        
        record_count = filtered_df.count()
        print(f"Found {record_count} evidence records for run_id: {run_id}")
        
        if record_count == 0:
            print("No records found, skipping TTL generation")
            return
        
        # Generate TTL files and write to cache using foreachPartition
        print("Generating TTL files and writing to cache...")
        filtered_df.foreachPartition(
            lambda partition: write_ttl_partition(
                partition,
                broadcast_mappings,
                bucket,
                run_id,
                region,
                cache_table_name,
                hpo_version,
                mondo_version,
                dynamodb_table_name
            )
        )
        
        # Generate subject-only TTL for all mapped subjects (driver-level, runs once)
        print(f"Generating subject and project nodes for all {len(subject_mappings)} mapped subjects...")
        
        all_subjects_ttl = [
            "@prefix phebee: <http://ods.nationwidechildrens.org/phebee#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            ""
        ]
        
        declared_projects = set()
        
        for subject_id, mapping in subject_mappings.items():
            project_id = mapping['project_id']
            project_subject_id = mapping['project_subject_id']
            
            # Subject declaration
            subject_uri = f"<http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}>"
            all_subjects_ttl.append(f"{subject_uri} rdf:type phebee:Subject .")
            
            # Project relationships
            project_uri_base = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            project_uri = f"<{project_uri_base}>"
            project_subject_id_iri = f"<{project_uri_base}/{project_subject_id}>"
            
            # Project type declaration (only once per project)
            if project_id not in declared_projects:
                all_subjects_ttl.append(f"{project_uri} rdf:type phebee:Project .")
                declared_projects.add(project_id)
            
            # Project-subject relationships
            escaped_project_subject_id = project_subject_id.replace('"', '\\"').replace('\\', '\\\\')
            all_subjects_ttl.append(f"{subject_uri} phebee:hasProjectSubjectId {project_subject_id_iri} .")
            all_subjects_ttl.append(f"{project_subject_id_iri} rdf:type phebee:ProjectSubjectId .")
            all_subjects_ttl.append(f"{project_subject_id_iri} phebee:hasProject {project_uri} .")
            all_subjects_ttl.append(f"{project_subject_id_iri} rdfs:label \"{escaped_project_subject_id}\" .")
        
        # Write all-subjects TTL file to projects folder organized by project_id
        s3 = boto3.client('s3', region_name=region)
        subjects_content = '\n'.join(all_subjects_ttl)
        
        # Group subjects by project_id and write separate TTL files for each project
        projects_by_id = {}
        for subject_id, mapping in subject_mappings.items():
            project_id = mapping['project_id']
            if project_id not in projects_by_id:
                projects_by_id[project_id] = []
            projects_by_id[project_id].append((subject_id, mapping))
        
        # Write TTL file for each project
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
            for subject_id, mapping in project_subjects:
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
            subjects_key = f"runs/{run_id}/neptune/projects/{project_id}/all_subjects.ttl"
            s3.put_object(
                Bucket=bucket,
                Key=subjects_key,
                Body=project_content.encode('utf-8'),
                ContentType='text/turtle'
            )
            print(f"Uploaded project TTL: {subjects_key}")
        
        print(f"Generated TTL files for {len(projects_by_id)} projects")
        
        print(f"TTL generation completed for run_id: {run_id}")
        
        # Return summary information
        result = {
            'run_id': run_id,
            'record_count': record_count,
            'ttl_prefix': f"s3://{bucket}/runs/{run_id}/neptune/",
            'status': 'completed'
        }
        
        # Write result to S3 for Step Function to pick up
        s3 = boto3.client('s3', region_name=region)
        result_key = f"runs/{run_id}/ttl_generation_result.json"
        s3.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=json.dumps(result).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Result written to: s3://{bucket}/{result_key}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
