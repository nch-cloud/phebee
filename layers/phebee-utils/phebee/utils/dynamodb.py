import boto3
import uuid
import os
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from botocore.exceptions import ClientError


# Cache for term source versions to avoid repeated DynamoDB queries
# Persists across warm Lambda invocations
_TERM_SOURCE_VERSION_CACHE = {}


def _get_table_name():
    return os.environ["PheBeeDynamoTable"]


def _get_client():
    return boto3.client("dynamodb")


def _get_table():
    return boto3.resource("dynamodb").Table(_get_table_name())


def reset_dynamodb_table():
    """
    Deletes ALL items from the table (paged scan + batch_writer).
    For dev/test and use through reset lambda only. PITR is enabled in template for safety.
    """
    print("Starting DynamoDB table reset...")
    table = _get_table()

    # Paginated scan + batch delete
    scan_kwargs = {}
    item_count = 0
    page_count = 0
    start_time = datetime.utcnow()

    while True:
        page_count += 1
        page_start = datetime.utcnow()

        response = table.scan(**scan_kwargs)
        batch_size = len(response.get('Items', []))
        item_count += batch_size

        scan_elapsed = (datetime.utcnow() - page_start).total_seconds()
        print(f"DynamoDB scan page {page_count}: {batch_size} items ({scan_elapsed:.2f}s)")

        # Batch delete items
        delete_start = datetime.utcnow()
        with table.batch_writer() as batch:
            for item in response.get('Items', []):
                batch.delete_item(Key={
                    'PK': item['PK'],
                    'SK': item['SK']
                })

        delete_elapsed = (datetime.utcnow() - delete_start).total_seconds()
        print(f"DynamoDB delete page {page_count}: {batch_size} items deleted ({delete_elapsed:.2f}s)")
        print(f"Running total: {item_count} items processed")

        # Check if there are more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    total_elapsed = (datetime.utcnow() - start_time).total_seconds()
    print(f"DynamoDB reset complete: {item_count} total items deleted in {total_elapsed:.2f}s ({page_count} pages)")


# ---------------------------
# Source version utilities
# ---------------------------

def get_source_records(source_name: str, dynamodb=None):
    """
    Returns all DynamoDB items for a given ontology/source name.
    NOTE: Uses the low-level client to keep the { "S": ... } wire format,
    since existing callers expect that structure.
    """
    client = dynamodb or _get_client()
    query_args = {
        "TableName": _get_table_name(),
        "KeyConditionExpression": "PK = :pk_value",
        "ExpressionAttributeValues": {":pk_value": {"S": f"SOURCE~{source_name}"}},
    }
    resp = client.query(**query_args)
    return resp.get("Items", [])


def get_current_term_source_version(source_name: str, dynamodb=None):
    """
    Returns the most recent installed 'Version' for a given source, based on InstallTimestamp.
    Ignores records without InstallTimestamp.

    Results are cached at module level to avoid repeated DynamoDB queries across
    warm Lambda invocations. Ontology versions only change when new versions are installed.
    """
    global _TERM_SOURCE_VERSION_CACHE

    # Check cache first
    if source_name in _TERM_SOURCE_VERSION_CACHE:
        return _TERM_SOURCE_VERSION_CACHE[source_name]

    # Cache miss - query DynamoDB
    source_records = get_source_records(source_name, dynamodb)

    sorted_records = sorted(
        (r for r in source_records if "InstallTimestamp" in r and "S" in r["InstallTimestamp"]),
        key=lambda x: x["InstallTimestamp"]["S"],
        reverse=True,
    )

    version = None
    if sorted_records:
        version = sorted_records[0]["Version"]["S"]

    # Cache the result (even if None)
    _TERM_SOURCE_VERSION_CACHE[source_name] = version

    return version


# ---------------------------
# Subject resolution utilities  
# ---------------------------

def get_subject_iri_from_project_subject_id(project_id: str, project_subject_id: str, region: str = 'us-east-2') -> Optional[str]:
    """Get subject IRI for a given project_id and project_subject_id"""
    table_name = _get_table_name()
    subject_id = get_subject_id(table_name, project_id, project_subject_id, region)
    if subject_id:
        # Construct the full PheBee subject IRI from the UUID
        return f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
    return None

def get_all_project_subject_ids(project_id: str, region: str = 'us-east-2') -> List[str]:
    """Get all project_subject_ids for a given project_id"""
    table_name = _get_table_name()
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    try:
        response = table.query(
            KeyConditionExpression='PK = :pk AND begins_with(SK, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': f'PROJECT#{project_id}',
                ':sk_prefix': 'SUBJECT#'
            }
        )
        
        project_subject_ids = []
        for item in response.get('Items', []):
            # Parse SK: "SUBJECT#{project_subject_id}"
            sk_parts = item['SK'].split('#')
            if len(sk_parts) >= 2:
                project_subject_id = sk_parts[1]
                project_subject_ids.append(project_subject_id)
        
        return project_subject_ids
    except ClientError:
        return []

def get_subject_id(table_name: str, project_id: str, project_subject_id: str, region: str = 'us-east-2') -> Optional[str]:
    """Get subject_id for a given project_id and project_subject_id"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    try:
        response = table.get_item(
            Key={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}'
            }
        )
        return response.get('Item', {}).get('subject_id')
    except ClientError:
        return None

def get_project_subjects(table_name: str, subject_id: str, region: str = 'us-east-2') -> List[Tuple[str, str]]:
    """Get all (project_id, project_subject_id) pairs for a given subject_id"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression='PK = :pk',
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'},
            ConsistentRead=True
        )

        pairs = []
        for item in response.get('Items', []):
            # Parse SK: "PROJECT#{project_id}#SUBJECT#{project_subject_id}"
            sk_parts = item['SK'].split('#')
            if len(sk_parts) >= 4:
                project_id = sk_parts[1]
                project_subject_id = sk_parts[3]
                pairs.append((project_id, project_subject_id))

        return pairs
    except ClientError:
        return []

def get_projects_for_subject(subject_id: str, region: str = 'us-east-2') -> List[str]:
    """
    Get all unique project IDs that have a given subject.

    Args:
        subject_id: The subject UUID
        region: AWS region (default: us-east-2)

    Returns:
        List of unique project IDs that have this subject
    """
    table_name = _get_table_name()
    pairs = get_project_subjects(table_name, subject_id, region)

    # Extract unique project_ids
    project_ids = list(set(project_id for project_id, _ in pairs))
    return project_ids

def create_subject_mapping(table_name: str, project_id: str, project_subject_id: str, region: str = 'us-east-2') -> str:
    """Create a new subject mapping and return the generated subject_id"""
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    subject_id = str(uuid.uuid4())
    
    # Write both directions in a transaction
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
    except ClientError as e:
        raise Exception(f"Failed to create subject mapping: {e}")

def resolve_subjects_batch(table_name: str, project_subject_pairs: Set[Tuple[str, str]], region: str = 'us-east-2') -> Dict[Tuple[str, str], str]:
    """Resolve multiple subject mappings, creating new ones if they don't exist"""
    subject_map = {}

    # First, try to get existing mappings
    for project_id, project_subject_id in project_subject_pairs:
        subject_id = get_subject_id(table_name, project_id, project_subject_id, region)
        if subject_id:
            subject_map[(project_id, project_subject_id)] = subject_id

    # Create new mappings for any that don't exist
    missing_pairs = project_subject_pairs - set(subject_map.keys())
    for project_id, project_subject_id in missing_pairs:
        subject_id = create_subject_mapping(table_name, project_id, project_subject_id, region)
        subject_map[(project_id, project_subject_id)] = subject_id

    return subject_map


# ---------------------------
# Term descendants cache utilities
# ---------------------------

def get_term_descendants_from_cache(term_id: str, term_source: str, term_source_version: str) -> Optional[List[str]]:
    """
    Get cached term descendants from DynamoDB.

    Args:
        term_id: The term ID (e.g., "HP:0001627")
        term_source: The term source (e.g., "hpo", "mondo")
        term_source_version: The term source version (e.g., "v2026-01-08")

    Returns:
        List of descendant term IDs if cached, None if cache miss
    """
    table = _get_table()

    try:
        response = table.get_item(
            Key={
                'PK': f'TERM_DESCENDANTS#{term_source.upper()}#{term_source_version}',
                'SK': term_id
            }
        )

        item = response.get('Item')
        if item and 'descendants' in item:
            return item['descendants']

        return None
    except ClientError as e:
        # Log error but don't fail - just cache miss
        return None


def put_term_descendants_to_cache(term_id: str, term_source: str, term_source_version: str, descendants: List[str]) -> None:
    """
    Cache term descendants in DynamoDB.

    Args:
        term_id: The term ID (e.g., "HP:0001627")
        term_source: The term source (e.g., "hpo", "mondo")
        term_source_version: The term source version (e.g., "v2026-01-08")
        descendants: List of descendant term IDs
    """
    table = _get_table()

    try:
        table.put_item(
            Item={
                'PK': f'TERM_DESCENDANTS#{term_source.upper()}#{term_source_version}',
                'SK': term_id,
                'descendants': descendants,
                'descendant_count': len(descendants),
                'cached_at': datetime.utcnow().isoformat() + 'Z'
            }
        )
    except ClientError as e:
        # Log error but don't fail the request
        pass
