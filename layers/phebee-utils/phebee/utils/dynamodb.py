import boto3
import uuid
import os
from typing import Dict, List, Set, Tuple, Optional
from botocore.exceptions import ClientError


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
    table = _get_table()
    
    # Paginated scan + batch delete
    scan_kwargs = {}
    
    while True:
        response = table.scan(**scan_kwargs)
        
        # Batch delete items
        with table.batch_writer() as batch:
            for item in response.get('Items', []):
                batch.delete_item(Key={
                    'PK': item['PK'],
                    'SK': item['SK']
                })
        
        # Check if there are more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']


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
    """
    source_records = get_source_records(source_name, dynamodb)

    sorted_records = sorted(
        (r for r in source_records if "InstallTimestamp" in r and "S" in r["InstallTimestamp"]),
        key=lambda x: x["InstallTimestamp"]["S"],
        reverse=True,
    )

    if not sorted_records:
        return None

    return sorted_records[0]["Version"]["S"]


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
            ExpressionAttributeValues={':pk': f'SUBJECT#{subject_id}'}
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
