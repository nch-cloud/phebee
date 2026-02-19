from botocore.exceptions import ClientError
from urllib.parse import urlparse
from phebee.utils.aws import get_client


def check_s3_file_exists(s3_url):
    # Parse the S3 URL
    parsed_url = urlparse(s3_url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")

    # Initialize the S3 client
    s3 = get_client("s3")

    try:
        # Try to fetch the head object, which only checks metadata
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # If a 404 error is returned, the file doesn't exist
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Something else went wrong, so raise an exception
            raise e


def delete_s3_prefix(bucket, prefix):
    s3_client = get_client("s3")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Check if there are objects to delete
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            delete_response = s3_client.delete_objects(
                Bucket=bucket, Delete={"Objects": objects_to_delete}
            )
            print("Deleted the following objects:")
            for deleted in delete_response.get("Deleted", []):
                print(deleted["Key"])
        else:
            print("No objects found under the specified prefix.")

    except Exception as delete_error:
        print(f"Failed to delete objects: {delete_error}")
