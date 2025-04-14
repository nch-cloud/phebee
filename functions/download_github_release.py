import boto3
import os
import requests
import zipfile
from smart_open import open
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.aws import get_current_timestamp

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = boto3.client("s3")
dynamodb = boto3.client("dynamodb")


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    source_name = event["source_name"]
    user = event["user"]
    repo = event["repo"]
    extract_zip = event.get("extract_zip", "false").lower() == "true"
    asset_names = event["asset_names"]

    # Added logic to trigger downloads for tests
    is_test = event.get("test", False)

    newest_release = find_newest_release(user, repo)
    version = newest_release["tag_name"]
    bucket_name = os.environ["PheBeeBucketName"]

    # If test flag is set, skip cache check and process the existing version as new
    if is_test or not release_cache_exists(source_name, version):
        logger.info(
            f"Retrieving {source_name} version {version} (Test mode: {is_test})"
        )

        if extract_zip:
            download_and_explode_zip(newest_release)

            logger.info(f"Storing assets from zip: {asset_names}")

            asset_metadatas = [
                get_and_store_zip_asset(asset_name, version, source_name, bucket_name)
                for asset_name in asset_names
            ]
        else:
            asset_metadatas = [
                get_and_store_asset(
                    newest_release, asset_name, version, source_name, bucket_name
                )
                for asset_name in asset_names
            ]

        store_release_metadata(source_name, version, asset_metadatas)

        return {"downloaded": True, "version": version, "assets": asset_metadatas}
    else:
        logger.info(
            f"Source {source_name} version {version} is already cached, not downloading."
        )

        return {"downloaded": False, "version": version}


def find_newest_release(user: str, repo: str):
    response = requests.get(
        f"https://api.github.com/repos/{user}/{repo}/releases?per_page=1&page=1"
    )
    releases = response.json()
    return releases[0]


def release_cache_exists(source_name: str, version: str):
    logger.info(f"Checking if source exists: {source_name} version {version}")

    # Our dynamo table doesn't have version as a key field, so we need to query for
    # all versions of the source and check that one with the correct version exists.

    query_args = {
        "TableName": os.environ["PheBeeDynamoTable"],
        "KeyConditionExpression": "PK = :pk_value",
        "ExpressionAttributeValues": {":pk_value": {"S": f"SOURCE~{source_name}"}},
    }

    dynamo_response = dynamodb.query(**query_args)

    logger.info(dynamo_response)

    items_matching_version = [
        item for item in dynamo_response["Items"] if item["Version"]["S"] == version
    ]

    return len(items_matching_version) > 0


def store_release_metadata(source_name: str, version: str, asset_metadata: list[dict]):
    logger.info(f"Storing release metadata for {source_name} version {version}")

    creation_timestamp = get_current_timestamp()

    response = dynamodb.put_item(
        TableName=os.environ["PheBeeDynamoTable"],
        Item={
            "PK": {"S": f"SOURCE~{source_name}"},
            "SK": {"S": creation_timestamp},
            "CreationTimestamp": {"S": creation_timestamp},
            "Version": {"S": version},
            "GraphName": {"S": f"{source_name}~{version}"},
            "Assets": {"L": serialize_asset_metadata(asset_metadata)},
        },
    )

    logger.info(response)

    return response["ResponseMetadata"]


def get_asset(release: dict, asset_name: str):
    asset_gen = (asset for asset in release["assets"] if asset["name"] == asset_name)
    return next(asset_gen)


def store_asset(
    asset: dict, asset_name: str, version: str, source_name: str, bucket_name: str
):
    asset_key = f"sources/{source_name}/{version}/assets/{asset_name}"
    asset_path = f"s3://{bucket_name}/{asset_key}"
    download_url = asset["browser_download_url"]

    logger.info(f"Storing asset: {asset_name}")

    logger.info(f"Downloading asset from {download_url}...")

    tmp_file = f"/tmp/{asset_name}"
    r = requests.get(download_url)
    with open(tmp_file, "wb") as output:
        output.write(r.content)

    logger.info(f"Storing asset to {asset_path}...")

    s3_client.upload_file(tmp_file, bucket_name, asset_key)

    logger.info(f"Successfully stored {asset_name}")

    return {
        "asset_name": asset_name,
        "asset_path": asset_path,
    }


def store_zip_asset(asset_name: str, version: str, source_name: str, bucket_name: str):
    asset_key = f"sources/{source_name}/{version}/assets/{asset_name}"
    asset_path = f"s3://{bucket_name}/{asset_key}"

    logger.info(f"Storing zip asset: {asset_name}")

    tmp_file = f"/tmp/zip_contents/{asset_name}"

    logger.info(f"Using extracted asset from {tmp_file}...")

    logger.info(f"Storing asset to {asset_path}...")

    s3_client.upload_file(tmp_file, bucket_name, asset_key)

    logger.info(f"Successfully stored {asset_name}")

    return {
        "asset_name": asset_name,
        "asset_path": asset_path,
    }


def get_and_store_asset(
    release: dict, asset_name: str, version: str, source_name: str, bucket_name: str
):
    asset = get_asset(release, asset_name)
    return store_asset(asset, asset_name, version, source_name, bucket_name)


def get_and_store_zip_asset(
    asset_name: str, version: str, source_name: str, bucket_name: str
):
    return store_zip_asset(asset_name, version, source_name, bucket_name)


def serialize_asset_metadata(asset_metadata: list[dict]):
    return [
        {
            "M": {
                "asset_name": {"S": a["asset_name"]},
                "asset_path": {"S": a["asset_path"]},
            }
        }
        for a in asset_metadata
    ]


def download_and_explode_zip(release: dict):
    zipball_url = release["zipball_url"]

    logger.info(f"Downloading zipball from {zipball_url}...")

    zip_file_path = "/tmp/zipball.zip"
    r = requests.get(zipball_url)
    with open(zip_file_path, "wb") as output:
        output.write(r.content)

    new_top_level_dir_name = "zip_contents"
    extract_to_dir = "/tmp"

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        # Get the list of files and directories in the zip file
        members = zip_ref.namelist()

        # Determine the top-level directory in the zip file
        top_level_dir = None
        for member in members:
            if os.path.dirname(member) and not top_level_dir:
                top_level_dir = os.path.dirname(member).split("/")[0]
                break

        # Extract files with the top-level directory renamed
        for member in members:
            # Replace the top-level directory with the new name
            if member.startswith(top_level_dir):
                member_without_top = member[len(top_level_dir) :]
                new_member = os.path.join(
                    new_top_level_dir_name, member_without_top.lstrip("/")
                )

                # Construct the full path for the extracted file
                file_path = os.path.join(extract_to_dir, new_member)

                # Ensure the directory exists
                file_dir = os.path.dirname(file_path)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)

                # Extract the file
                if not member.endswith("/"):  # Skip directory entries
                    with (
                        zip_ref.open(member) as source,
                        open(file_path, "wb") as target,
                    ):
                        target.write(source.read())

    print(
        f"Unzipped {zip_file_path} to {extract_to_dir} with top-level directory renamed to {new_top_level_dir_name}"
    )
