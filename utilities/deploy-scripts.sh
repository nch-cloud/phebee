#!/bin/bash

# Deploy EMR scripts to S3
# Usage: ./deploy-scripts.sh [stack-name]

set -e

# Get stack name from parameter or default to phebee-dev
if [ -n "$1" ]; then
    STACK_NAME="$1"
else
    # Default to phebee-dev
    STACK_NAME="phebee-dev"
fi

echo "Looking up bucket name from CloudFormation stack: $STACK_NAME"

# Look up bucket name from CloudFormation stack
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`PheBeeBucketName`].OutputValue' \
    --output text)

if [ -z "$BUCKET_NAME" ]; then
    echo "Error: Could not find PheBeeBucketName output in stack $STACK_NAME"
    echo "Make sure the stack exists and has been deployed"
    exit 1
fi

echo "Deploying EMR scripts to s3://$BUCKET_NAME/scripts/"

# Upload EMR scripts
aws s3 cp scripts/bulk_evidence_processor.py "s3://$BUCKET_NAME/scripts/" \
    --content-type "text/x-python"

aws s3 cp scripts/resolve_subject_mappings_emr.py "s3://$BUCKET_NAME/scripts/" \
    --content-type "text/x-python"

aws s3 cp scripts/generate_ttl_from_iceberg_emr.py "s3://$BUCKET_NAME/scripts/" \
    --content-type "text/x-python"

aws s3 cp scripts/materialize_project_subject_terms_emr.py "s3://$BUCKET_NAME/scripts/" \
    --content-type "text/x-python"

echo "Scripts deployed successfully to $BUCKET_NAME"
