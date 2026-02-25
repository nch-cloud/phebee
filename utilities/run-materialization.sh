#!/bin/bash

# Direct EMR Serverless job submission for MaterializeProjectSubjectTerms
# This bypasses the Step Functions state machine and runs materialization directly
#
# Usage: ./run-materialization.sh <stack_name> <project_id> <run_ids> [lookback_days]
#   stack_name: Required - CloudFormation stack name (e.g., phebee-dev)
#   project_id: Required - Project ID to materialize
#   run_ids: Required - Comma-separated run IDs to process incrementally
#   lookback_days: Optional - Days to look back for partition pruning (default: 1000)
#
# Examples:
#   Single run: ./run-materialization.sh phebee-dev test_project_4d507041 import-perf-37a51f4fbe
#   Multiple runs: ./run-materialization.sh phebee-dev test_project_4d507041 "run1,run2,run3"
#   With custom lookback: ./run-materialization.sh phebee-dev test_project_4d507041 import-perf-37a51f4fbe 30
#
# For initial deployment:
#   Process the big run first, then the smaller ones sequentially

STACK_NAME="$1"
PROJECT_ID="$2"
RUN_IDS="$3"
LOOKBACK_DAYS="${4:-1000}"

if [ -z "$STACK_NAME" ] || [ -z "$PROJECT_ID" ] || [ -z "$RUN_IDS" ]; then
  echo "Error: Missing required parameters"
  echo "Usage: $0 <stack_name> <project_id> <run_ids> [lookback_days]"
  echo "Example: $0 phebee-dev test_project_4d507041 import-perf-37a51f4fbe"
  exit 1
fi

echo "Processing run_ids=$RUN_IDS for project=$PROJECT_ID using stack=$STACK_NAME (lookback=$LOOKBACK_DAYS days)"

# Look up resources from CloudFormation stack
echo "Looking up resources from stack outputs..."
EMR_APP_ID=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`EMRServerlessApplicationId`].OutputValue' --output text)
EMR_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`EMRExecutionRoleArn`].OutputValue' --output text)
S3_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`PheBeeBucketName`].OutputValue' --output text)
REGION=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`Region`].OutputValue' --output text)
ICEBERG_DATABASE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`AthenaDatabase`].OutputValue' --output text)
EVIDENCE_TABLE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`AthenaEvidenceTable`].OutputValue' --output text)
BY_SUBJECT_TABLE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`AthenaSubjectTermsBySubjectTable`].OutputValue' --output text)
BY_PROJECT_TERM_TABLE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`AthenaSubjectTermsByProjectTermTable`].OutputValue' --output text)
DYNAMODB_TABLE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`DynamoDBTableName`].OutputValue' --output text)

if [ -z "$EMR_APP_ID" ] || [ -z "$EMR_ROLE_ARN" ] || [ -z "$S3_BUCKET" ] || [ -z "$REGION" ] || [ -z "$ICEBERG_DATABASE" ] || [ -z "$EVIDENCE_TABLE" ] || [ -z "$BY_SUBJECT_TABLE" ] || [ -z "$BY_PROJECT_TERM_TABLE" ] || [ -z "$DYNAMODB_TABLE" ]; then
  echo "Error: Failed to lookup required stack outputs"
  exit 1
fi

echo "Using EMR Application: $EMR_APP_ID"
echo "Using EMR Role: $EMR_ROLE_ARN"
echo "Using S3 Bucket: $S3_BUCKET"
echo "Using Region: $REGION"
echo "Using Iceberg Database: $ICEBERG_DATABASE"
echo "Using Evidence Table: $EVIDENCE_TABLE"
echo "Using By-Subject Table: $BY_SUBJECT_TABLE"
echo "Using By-Project-Term Table: $BY_PROJECT_TERM_TABLE"
echo "Using DynamoDB Table: $DYNAMODB_TABLE"

# Build entry point arguments
ENTRY_POINT_ARGS='[
  "--region", "'"$REGION"'",
  "--project-id", "'"$PROJECT_ID"'",
  "--run-ids", "'"$RUN_IDS"'",
  "--iceberg-database", "'"$ICEBERG_DATABASE"'",
  "--evidence-table", "'"$EVIDENCE_TABLE"'",
  "--by-subject-table", "'"$BY_SUBJECT_TABLE"'",
  "--by-project-term-table", "'"$BY_PROJECT_TERM_TABLE"'",
  "--dynamodb-table", "'"$DYNAMODB_TABLE"'",
  "--lookback-days", "'"$LOOKBACK_DAYS"'"
]'

aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --name "MaterializeProjectSubjectTerms-${PROJECT_ID}-$(date +%s)" \
  --region "$REGION" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'"$S3_BUCKET"'/scripts/materialize_project_subject_terms_emr.py",
      "entryPointArguments": '"$ENTRY_POINT_ARGS"',
      "sparkSubmitParameters": "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://'"$S3_BUCKET"'/iceberg-warehouse --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.executor.cores=16 --conf spark.emr-serverless.memoryOverheadFactor=0.10 --conf spark.memory.fraction=0.8 --conf spark.memory.storageFraction=0.3 --conf spark.task.cpus=8 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=15 --conf spark.dynamicAllocation.minExecutors=15 --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.shuffle.spill.compress=true --conf spark.shuffle.io.maxRetries=10 --conf spark.shuffle.io.retryWait=60s"
    }
  }'
