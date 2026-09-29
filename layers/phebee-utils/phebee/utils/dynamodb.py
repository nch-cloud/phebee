import boto3
import uuid
import os
import time
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

def _project_members_query(project_id: str) -> Dict:
    return {
        'KeyConditionExpression': 'PK = :pk AND begins_with(SK, :sk_prefix)',
        'ExpressionAttributeValues': {
            ':pk': f'PROJECT#{project_id}',
            ':sk_prefix': 'SUBJECT#',
        },
    }


def _member_from_item(item: Dict) -> Tuple[str, str]:
    # SK is "SUBJECT#{project_subject_id}"; split once, since a
    # project_subject_id may itself contain '#'.
    return item['SK'].split('#', 1)[1], item['subject_id']


def count_project_subjects(project_id: str) -> int:
    """Count a project's members from its forward mapping items.

    Select=COUNT returns no items, but DynamoDB still reads them, so this walks
    about 1 MB of items per call (~7k members): ~15 calls at 100k.
    """
    table = _get_table()
    kwargs = _project_members_query(project_id)
    kwargs['Select'] = 'COUNT'
    total = 0
    while True:
        response = table.query(**kwargs)
        total += response['Count']
        if 'LastEvaluatedKey' not in response:
            return total
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']


def get_project_subjects_page(project_id: str, offset: int, limit: int) -> List[Tuple[str, str]]:
    """One page of a project's members, as (project_subject_id, subject_id).

    Members come back in project_subject_id order, the sort key's order.
    DynamoDB has no offset, so the walk to it counts items with Select=COUNT
    and Limit set to what is left to skip, then resumes from the key where
    that stopped.
    """
    table = _get_table()
    kwargs = _project_members_query(project_id)

    if offset > 0:
        skip = dict(kwargs, Select='COUNT')
        remaining = offset
        while remaining > 0:
            skip['Limit'] = remaining
            response = table.query(**skip)
            remaining -= response['Count']
            if 'LastEvaluatedKey' not in response:
                # Reached the end of the project at or before the offset. Without
                # a key to resume from, a page query would restart at the top.
                return []
            skip['ExclusiveStartKey'] = response['LastEvaluatedKey']
        kwargs['ExclusiveStartKey'] = skip['ExclusiveStartKey']

    members = []
    while len(members) < limit:
        kwargs['Limit'] = limit - len(members)
        response = table.query(**kwargs)
        members.extend(_member_from_item(item) for item in response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    return members


def get_project_subjects_by_ids(project_id: str, project_subject_ids: List[str]) -> List[Tuple[str, str]]:
    """Look up the given project_subject_ids in a project, as (project_subject_id, subject_id).

    Ids that are not members of the project are left out. The result is in
    project_subject_id order, matching get_project_subjects_page.
    """
    dynamodb = boto3.resource('dynamodb')
    table_name = _get_table_name()
    # BatchGetItem rejects a request that names the same key twice.
    unique_ids = list(dict.fromkeys(project_subject_ids))

    members = []
    for start in range(0, len(unique_ids), 100):  # BatchGetItem's per-call key limit
        request = {table_name: {'Keys': [
            {'PK': f'PROJECT#{project_id}', 'SK': f'SUBJECT#{psid}'}
            for psid in unique_ids[start:start + 100]
        ]}}
        attempt = 0
        while request:
            response = dynamodb.batch_get_item(RequestItems=request)
            members.extend(_member_from_item(item)
                           for item in response.get('Responses', {}).get(table_name, []))
            # Keys can come back unprocessed under throttling; retry just those.
            request = response.get('UnprocessedKeys') or None
            if request:
                attempt += 1
                if attempt > 8:
                    raise RuntimeError(f"DynamoDB left {len(request[table_name]['Keys'])} keys unprocessed")
                time.sleep(min(0.05 * 2 ** attempt, 2.0))
    return sorted(members)


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
        pk = f'TERM_DESCENDANTS#{term_source.upper()}#{term_source_version}'
        print(f"[CACHE_READ] Checking cache for {term_id} (PK={pk})")
        response = table.get_item(
            Key={
                'PK': pk,
                'SK': term_id
            }
        )

        item = response.get('Item')
        if item and 'descendants' in item:
            print(f"[CACHE_HIT] Found {len(item['descendants'])} descendants for {term_id} in cache")
            return item['descendants']

        print(f"[CACHE_MISS] No cache entry found for {term_id} (PK={pk}, SK={term_id})")
        return None
    except ClientError as e:
        print(f"[CACHE_READ_ERROR] Failed to read term descendants cache for {term_id}: {e}")
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
        pk = f'TERM_DESCENDANTS#{term_source.upper()}#{term_source_version}'
        print(f"[CACHE_WRITE] Writing {len(descendants)} descendants for {term_id} to DynamoDB (PK={pk})")
        table.put_item(
            Item={
                'PK': pk,
                'SK': term_id,
                'descendants': descendants,
                'descendant_count': len(descendants),
                'cached_at': datetime.utcnow().isoformat() + 'Z'
            }
        )
        print(f"[CACHE_WRITE_SUCCESS] Successfully wrote cache entry (PK={pk}, SK={term_id})")
    except ClientError as e:
        print(f"[CACHE_WRITE_ERROR] Failed to write term descendants cache for {term_id} ({len(descendants)} descendants): {e}")
        pass
