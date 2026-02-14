# Integration Tests Quick Start

## Running Your First Test

```bash
# Replace 'phebee-dev' with your actual CloudFormation stack name
pytest tests/integration-new/test_create_project.py -v \
  --existing-stack phebee-dev
```

## Required Command-Line Options

### --existing-stack (REQUIRED)

Specifies which CloudFormation stack to test against:

```bash
--existing-stack <your-stack-name>
```

**Example stack names:**
- `phebee-dev`
- `phebee-staging`
- `phebee-it-a2f` (auto-generated integration test stack)

### --profile (Optional)

Specifies which AWS profile to use:

```bash
--profile <profile-name>
```

If not provided, uses your default AWS credentials.

## Common Commands

### Test a Specific File
```bash
pytest tests/integration-new/test_create_project.py -v \
  --existing-stack phebee-dev
```

### Test a Specific Function
```bash
pytest tests/integration-new/test_create_project.py::test_create_project_success -v \
  --existing-stack phebee-dev
```

### Run All Tests in Directory
```bash
pytest tests/integration-new/ -v \
  --existing-stack phebee-dev
```

### Show Test Output (Print Statements)
```bash
pytest tests/integration-new/test_create_project.py -v -s \
  --existing-stack phebee-dev
```

### Stop on First Failure
```bash
pytest tests/integration-new/ -v -x \
  --existing-stack phebee-dev
```

## Troubleshooting

### Error: "physical_resources did not yield a value"

**Cause**: Missing `--existing-stack` option

**Solution**: Always provide the stack name:
```bash
pytest tests/integration-new/test_create_project.py -v \
  --existing-stack phebee-dev  # ← Don't forget this!
```

### Error: "Stack does not exist"

**Cause**: Invalid stack name or stack not deployed

**Solution**:
1. Check your stack name: `aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE`
2. Deploy the stack: `sam deploy --config-env dev`
3. Use the correct stack name in your command

### Error: "Unable to locate credentials"

**Cause**: AWS credentials not configured

**Solution**:
```bash
# Option 1: Use default credentials
aws configure

# Option 2: Use a specific profile
pytest tests/integration-new/ -v \
  --existing-stack phebee-dev \
  --profile my-profile
```

## What Stack Name Should I Use?

**For local development:**
- Use your personal dev stack: `phebee-dev` or `phebee-dev-<yourname>`

**For CI/CD:**
- Tests will create a temporary stack automatically
- Or use a shared integration test stack: `phebee-it-shared`

**To check available stacks:**
```bash
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --query "StackSummaries[?starts_with(StackName, 'phebee')].StackName" \
  --output table
```

## Next Steps

- See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for full documentation
- See [TESTING_GUIDE.md](TESTING_GUIDE.md) for testing best practices
- See [NEPTUNE_FREE_TESTING.md](NEPTUNE_FREE_TESTING.md) for verification strategies
