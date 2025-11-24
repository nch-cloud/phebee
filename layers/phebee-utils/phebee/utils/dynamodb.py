import boto3
import uuid
from typing import Dict, List, Set, Tuple, Optional
from botocore.exceptions import ClientError

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
