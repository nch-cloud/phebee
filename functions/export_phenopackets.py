import os
from aws_lambda_powertools import Metrics, Logger, Tracer
import json
import io
import zipfile
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid

from phebee.utils.aws import parse_s3_path, get_client
from phebee.utils.dynamodb import get_current_term_source_version

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = get_client("s3")


# Build a phenopacket given the input data of a single subject
def lambda_handler(event, context):

    logger.info("Event: ", event)

    # Handle S3 input files
    if "s3_path" in event:

        bucket, key = parse_s3_path(event["s3_path"])
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            input_data = response["Body"].read().decode("utf-8")
            input_data = json.loads(input_data)
        except ClientError as e:
            print(f"Error reading data from S3: {e}")
            raise
    else:
        # Handle input data passed as json directly
        input_data = event["input_data"]

    if "output_s3_path" in event:
        output_s3_path = event["output_s3_path"]
    else:
        # Generate a filename with timestamp and unique 8-character UUID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"export_{timestamp}_{unique_id}"
        bucket = os.environ["PheBeeBucketName"]
        output_s3_path = f"s3://{bucket}/export_phenopacket/{filename}.zip"

    phenopacket_list = []

    # TODO add robust input validation
    # if isinstance(input_data, dict):
    input_data = list(input_data.values())

    # use current term source version for HPO and MONDO
    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    # Ensure input_data is a list of lists
    if isinstance(input_data, list):
        for subject_data in input_data:
            # Ensure each subject_data is a list of dictionaries
            if isinstance(subject_data, list):
                phenopacket = convert_to_phenopacket(
                    subject_data, hpo_version=hpo_version, mondo_version=mondo_version
                )
                phenopacket_list.append(phenopacket)
            else:
                raise ValueError("Each subject's data must be a list of dictionaries.")
    else:
        raise ValueError("Input data must be a list.")

    s3_path = write_phenopackets(phenopacket_list, output_s3_path)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_packets": len(phenopacket_list),
            "s3_path": s3_path
        }),
        "headers": {
            "Content-Type": "application/json"
        }
    }

def convert_to_phenopacket(
    flattened_results, hpo_version, mondo_version, extended=False
):
    """
    Converts flattened SPARQL results to Phenopacket JSON format, with optional extended source metadata.

    Parameters:
    - flattened_results: A list of dictionaries, each representing a result row from the SPARQL query.
    - extended: If True, preserves all phenotype annotations and their metadata;
                if False, returns a de-duplicated version of the phenotypic features (compatible with Phenopacket standard).
    - hpo_version: The version of the Human Phenotype Ontology (HPO) used in the data.
    - mondo_version: The version of the Mondo Disease Ontology used in the data.

    Returns:
    - A dictionary representing the phenopacket structure.

    """

    # Check that all results are from the same subject
    subject_ids = {result["projectSubjectId"] for result in flattened_results}
    if len(subject_ids) > 1:
        raise ValueError(
            f"Multiple subjects found: {subject_ids}. All results must be from the same subject."
        )

    subject_id = flattened_results[0]["projectSubjectId"].split("#")[-1]

    # Initialize the phenopacket structure
    phenopacket = {
        "id": flattened_results[0]["projectSubjectId"].split("#")[1],
        "subject": {"id": subject_id},
        "phenotypicFeatures": [],
        "metaData": {
            # TODO Standard Phenopacket schema has only single creation date - could use earliest or latest update, or current date.
            "created": flattened_results[0].get("created"),
            "createdBy": flattened_results[0].get("creator"),
            "phenopacketSchemaVersion": "2.0",
            # Resources are hardcoded, but don't expect them to change very often
            "resources": [
                {
                    "id": "hp",
                    "name": "human phenotype ontology",
                    "url": "http://purl.obolibrary.org/obo/hp.owl",
                    "version": hpo_version,
                    "namespacePrefix": "HP",
                    "iriPrefix": "http://purl.obolibrary.org/obo/HP_",
                },
                {
                    "id": "mondo",
                    "name": "Mondo Disease Ontology",
                    "url": "http://purl.obolibrary.org/obo/mondo.obo",
                    "version": mondo_version,
                    "namespacePrefix": "MONDO",
                    "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_",
                },
            ],
        },
    }

    if extended:
        # Extended mode: Include all phenotypes with detailed source metadata
        for result in flattened_results:
            term_id = result["term"].split("/")[-1].replace("_", ":")
            term_label = result["termLabel"]
            excluded = result.get("excluded") == "true"
            evidence_type = result.get("evidence_type")
            assertion_method = result.get("assertion_method")
            created = result.get("created")
            source = result.get("source")

            # Build the phenotype feature with all metadata
            phenotypic_feature = {
                "type": {"id": term_id, "label": term_label},
                "excluded": excluded,
                "evidence": {
                    "evidenceType": evidence_type,
                    "assertionMethod": assertion_method,
                    "created": created,
                    "source": source,
                },
            }

            # Append each source-specific phenotype annotation
            phenopacket["phenotypicFeatures"].append(phenotypic_feature)

    else:
        # Simplified mode: Deduplicate by term ID, handle excluded and basic metadata
        phenotypic_features = {}

        # Iterate over flattened results to populate phenotypic features
        for result in flattened_results:
            term_id = result["term"].split("/")[-1].replace("_", ":")
            term_label = result["termLabel"]
            excluded = result.get("excluded") == "true"

            # Create a phenotypic feature entry if it doesn't already exist
            if term_id not in phenotypic_features:
                phenotypic_features[term_id] = {
                    "type": {"id": term_id, "label": term_label}
                }

            # Handle excluded field
            if excluded:
                phenotypic_features[term_id]["excluded"] = True

        # Add the unique phenotypic features to the phenopacket
        phenopacket["phenotypicFeatures"] = list(phenotypic_features.values())

    return phenopacket


def write_phenopackets(phenopacket_list, s3_path):
    """
    Writes phenopackets to a ZIP file stored in memory. Optionally uploads the ZIP file to S3.

    :param phenopacket_list: A list of phenopackets (dictionaries) to write.
    :param filename: The name of the final ZIP file or the uncompressed JSON file.
    :param prefix: Prefix for the S3 key (if uploading to S3).
    :return: A dictionary with the status or S3 paths.
    """
    # Use BytesIO to store the ZIP file in memory
    zip_buffer = io.BytesIO()

    try:
        # Create a ZIP archive and add each phenopacket as a separate JSON file
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for phenopacket in phenopacket_list:
                # Use the subject ID as the filename for each phenopacket
                subject_id = phenopacket["subject"]["id"]
                json_data = json.dumps(phenopacket, indent=2)
                # Create a virtual file in the ZIP archive for each phenopacket
                zip_file.writestr(f"{subject_id}.json", json_data)

        # The final ZIP file is now stored in memory (in zip_buffer)
        zip_buffer.seek(0)
        # return {"filename": filename, "data": zip_buffer.getvalue()}
    except Exception as e:
        print(f"Error writing phenopackets: {e}")
        raise

    bucket, key = parse_s3_path(s3_path)

    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=zip_buffer)

    return f"s3://{bucket}/{key}"
